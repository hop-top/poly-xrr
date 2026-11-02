# xrr run — record/replay external CLIs for any test suite

`xrr run` wraps a test command (bats, pytest, vitest, `make test`, any shell
script) and puts shims for chosen external CLIs on `PATH`. No code changes
and no mocking library: the suite calls `az`, `op`, `gh`, ... like always.

- **record**: the shim runs the real binary (resolved from the original
  `PATH`), passes its output through, and writes an exec cassette.
- **replay**: the shim answers from the cassette; the real binary is never
  executed and need not be installed.
- **deny**: side-effect commands (`open`, `osascript`, `sendmail`, ...) are
  blocked and reported; the real binary is never executed.

Cassettes are the standard exec format (`exec-<fingerprint>.{req,resp}.yaml`,
fingerprint over `{argv, stdin}`; see
[cassette-format-v1](../spec/cassette-format-v1.md)), so any port replays them.

## Install

```sh
cd go && go build -o ~/.local/bin/xrr ./cmd/xrr     # from a checkout
go install hop.top/xrr/cmd/xrr@latest               # or via the module
```

POSIX only (shims are `sh` stubs).

## Usage

```sh
xrr run [--mode record|replay|passthrough] [--cassettes DIR]
        [--shim cmd,...] [--deny cmd,...] [--no-default-deny]
        [--strict[=false]] [--env NAME,...] [--stdin-wait 250ms]
        [--scrub NAME,...] [--no-scrub NAME,...]
        [--report-file FILE] [--config FILE] -- <cmd> [args...]
```

| Flag | Meaning |
|------|---------|
| `--mode` | `record`, `replay` or `passthrough` (shims run the real binary, deny still enforced). Default `replay`. |
| `--cassettes` | Cassette dir. Default `./cassettes`. Created in record mode; must exist in replay. |
| `--shim` | Commands to record/replay. Repeatable, comma-separated. Required in record/replay. |
| `--deny` | Extra commands to block, added to the default set. |
| `--no-default-deny` | Drop the built-in set: `open xdg-open wslview explorer.exe start sendmail mail osascript say pbcopy notify-send`. An explicit `--shim` of one of these wins over the default. |
| `--strict` | Default on. Replay: a shimmed call with no cassette fails with `cassette miss`. `--strict=false` falls through to the real binary. |
| `--env` | Env var names captured into the cassette request (redacted like any field). Not part of the fingerprint. |
| `--stdin-wait` | Replay: stop reading a shim's stdin after this long with no data. |
| `--scrub` | Env var names (or `path.Match` globs, e.g. `MY_*`) whose values are always scrubbed from cassettes. Repeatable, comma-separated. |
| `--no-scrub` | Env var names/globs whose values are never scrubbed, even when the name looks like a credential. |
| `--report-file` | Also write the summary as JSON. |
| `--config` | Project config file; replaces `./.xrr.yaml`. Must exist. |

Typical flow:

```sh
# once, with the real tools and credentials
xrr run --mode record --cassettes test/cassettes --shim az,op,gh -- bats test/
# everywhere else (CI, laptops without the tools): offline and deterministic
xrr run --mode replay --cassettes test/cassettes --shim az,op,gh -- bats test/
```

## Report and exit codes

A summary goes to stderr after the run:

```
xrr run: replay, 3 shimmed call(s)
  replayed     2
  denied       1
  denied: open https://example.test/auth - blocked, real binary not executed
```

| Exit | Meaning |
|------|---------|
| the child's | Child exited non-zero: its code wins. |
| `3` | Child passed but a command was denied, a strict miss happened, or a shim failed. |
| `2` | Usage / configuration error. |
| `126` / `125` / `127` | Seen by the *suite* from a shim: denied / cassette miss / real binary not found. |

A suite that swallows a shim failure (`open x || true`) still fails the run.

## Configuration

Every option can come from a config file or the environment, not only flags.
Sources, highest first:

| # | Source | Notes |
|---|--------|-------|
| 1 | flags | Only flags actually passed count. |
| 2 | environment | `XRR_*`, see below. Empty = unset. |
| 3 | project file | `./.xrr.yaml` (cwd only, no parent lookup), or `--config FILE`. |
| 4 | user file | `$XDG_CONFIG_HOME/xrr/config.yaml`, default `~/.config/xrr/config.yaml`. |

Missing default files are ignored; a missing `--config` file is an error.

**Merge rules.** Scalars: the highest source that sets one wins. Lists
(`shim`, `deny`, `env`, `scrub.*`): union of all sources, so a lower layer
can be extended but not shrunk. To undo a lower-layer entry use the opposite
control (`no_default_deny`, `scrub.allow`, or point `--config` at a different
file). Cross-list conflicts after merging are usage errors: a name in both
`scrub.deny` and `scrub.allow`, or in both `shim` and `deny`.

**Schema** (`.xrr.yaml`; unknown keys are an error):

```yaml
mode: replay             # record | replay | passthrough        env XRR_MODE
cassettes: test/cass     # relative paths resolve against this   env XRR_CASSETTE_DIR
                         #   file's directory (env/flags: cwd)
shim: [az, op, gh]       # commands to record/replay             env XRR_SHIM
deny: [rm-rf-prod]       # added to the default deny set         env XRR_DENY
no_default_deny: false   # drop the built-in deny set            env XRR_NO_DEFAULT_DENY
strict: true             # replay: miss fails                    env XRR_STRICT
env: [REGION]            # env vars captured into cassettes      env XRR_ENV
stdin_wait: 250ms        # replay stdin idle limit               env XRR_STDIN_WAIT
report_file: report.json # JSON summary (relative: file's dir)   env XRR_REPORT_FILE
scrub:
  deny: [PUBLIC_NOTE, MY_*]   # always scrub     flag --scrub     env XRR_SCRUB
  allow: [FIXTURE_TOKEN]      # never scrub      flag --no-scrub  env XRR_NO_SCRUB
```

Env lists are comma-separated. Booleans accept what Go's `strconv.ParseBool` does.

## Redaction

Cassettes pass through the same redaction as every xrr adapter
(`XRR_REDACT_ALLOW`, `XRR_REDACT_DENY`, `XRR_REDACT_DISABLE`), plus:

- argv: `--password X`, `--api-key=X`, `TOKEN=X` become
  `<redacted:NAME>`; vendor-shaped tokens (`ghp_`, `AKIA`, JWT, ...) anywhere.
- stdout/stderr/stdin/argv: the *value* of an env var present when the shim
  runs is replaced with its placeholder. Which variables, highest first:
  1. name matches `scrub.deny` (`--scrub`): scrubbed, any length;
  2. name matches `scrub.allow` (`--no-scrub`): kept;
  3. credential-looking name (`*_TOKEN`, `*_SECRET`, `*PASSWORD*`, ...) with
     a value of 8+ characters: scrubbed. This is the default behaviour.
- The scrub lists govern env *values*. Redaction of a captured `env:` field
  by its *name* (e.g. `API_TOKEN`) is still controlled by `XRR_REDACT_ALLOW`.
- The fingerprint is always computed over the live argv, so redaction never
  changes a cassette key and replay keeps matching.
- Existing field rule: a value matching a vendor pattern inside `stdout`
  replaces the whole field.

## Behaviour and limits

- Matching is by `argv` + `stdin` only. The same call twice replays the same
  answer; stateful sequences (call N differs from call 1) are not modelled.
- Only commands resolved through `PATH` are shimmed. `/usr/bin/open x` or a
  hard-coded absolute path bypasses the shim. The wrapped `<cmd>` itself is
  resolved from the original `PATH` and is not shimmed.
- Commands a real binary spawns itself (`az` calling `python`) run unshimmed
  and are part of that one recorded call.
- stdin: replay must read stdin to pick a cassette. An inherited pipe that is
  open and silent (bats, CI) counts as empty after `--stdin-wait`; a producer
  slower than that misses. Record streams stdin live and is not affected.
- stdout and stderr are stored separately; their interleaving is not kept.
- `cwd` is not part of the fingerprint (it is a Go-only extension), keeping
  cassettes replayable from the other ports.

## Examples

[`docs/examples/xrr-run/`](examples/xrr-run) has the same three tests for
each runner, a stand-in `az`, and `demo.sh <bats|pytest|vitest>` that records
then replays:

```bash
# bats
@test "account show" { run az account show; [ "$status" -eq 0 ]; }
```

```python
# pytest
p = subprocess.run(["az", "account", "show"], capture_output=True, text=True)
```

```ts
// vitest
const p = spawnSync("az", ["account", "show"], { encoding: "utf8" });
```
