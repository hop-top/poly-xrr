# Cassette Format v1

Spec for the xrr on-disk cassette format. Language-agnostic; all ports MUST conform.

## Directory Layout

```
<session-dir>/
  <adapter-id>-<fingerprint>.req.yaml
  <adapter-id>-<fingerprint>.resp.yaml
```

## Adapter ID Rules

- Pattern: `[a-z][a-z0-9-]*`
- Examples: `exec`, `http`, `grpc`, `redis`, `sql`

## Fingerprint Algorithm

```
fingerprint = sha256(canonical(request))[:8]
```

Where `canonical(request)` = deterministic JSON with sorted keys of the fields
that uniquely identify the interaction (adapter-defined).

Result: 8 lowercase hex characters, e.g. `a3f9c1b2`.

## File Naming

```
<adapter-id>-<fingerprint>.req.yaml   ← serialized request
<adapter-id>-<fingerprint>.resp.yaml  ← serialized response
```

## Envelope Schema

Both `.req.yaml` and `.resp.yaml` share this wrapper:

```yaml
xrr: "1"                      # format version — required; always string "1"
adapter: exec                 # adapter id — required
fingerprint: "a3f9c1b2"       # 8-char hex — required
recorded_at: "2026-04-01T12:00:00Z"  # RFC3339 UTC — required
payload:                      # adapter-specific — required, MUST be an object
  <adapter fields>
```

### Required Fields (both req and resp)

| Field        | Type   | Description                        |
|--------------|--------|------------------------------------|
| xrr          | string | Format version, always `"1"`       |
| adapter      | string | Adapter ID matching `[a-z][a-z0-9-]*` |
| fingerprint  | string | 8 hex chars                        |
| recorded_at  | string | RFC3339 UTC timestamp              |
| payload      | object | Adapter-specific request/response. MUST be a non-null object (writers MUST normalize an absent or null payload to `{}`). |

### Optional Fields (`.resp.yaml` only)

| Field | Type   | Description                                                                                                                                                                                                                                                                                       |
|-------|--------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| error | string | Recorded error message from the original interaction. If present and non-empty, replay MUST re-emit a non-nil error alongside the response payload. Empty or absent ⇒ success. Recordings written before this field existed replay as success. **`.req.yaml` MUST NOT carry this field.** |

Any other additional top-level fields are ignored by loaders (forward compat).

## Request Envelope Example (exec)

```yaml
xrr: "1"
adapter: exec
fingerprint: "a3f9c1b2"
recorded_at: "2026-04-01T12:00:00Z"
payload:
  argv: ["gh", "pr", "view", "123"]
  stdin: ""
  cwd: "/workspace/repo"   # optional — see Exec Fingerprint Inputs below
  env: {}
```

### Exec Fingerprint Inputs (v1)

The v1 canonical exec fingerprint is the sha256 of canonical JSON over
`{argv, stdin}`, truncated to 8 hex chars. **All ports MUST hash these
two fields and only these two fields** to preserve the cross-runtime
replay guarantee: a cassette recorded in any language port MUST replay
in any other port.

`cwd` and `env` MAY appear in the serialized request payload for
debugging, auditing, or adopter-side use, but they do not participate
in the v1 fingerprint. Relying on per-cwd or per-env discrimination at
the v1 fingerprint level is not guaranteed.

#### Go-only extension: `cwd` in fingerprint (non-canonical)

The Go port (`hop.top/xrr` as of v0.1.0-alpha.3) additionally hashes
`cwd` into the exec fingerprint **only when non-empty**, so the same
command run in different working directories produces distinct
cassette keys. This is a deliberate extension to unblock cross-process
e2e adopters (e.g. one parent `XRR_CASSETTE_DIR` capturing many
subprocess invocations from different temp dirs).

The extension is backward compatible in one direction only:

- Go-recorded cassettes **with empty `cwd`** still hash as v1 canonical
  and replay cleanly in ts / py / rs / php ports.
- Go-recorded cassettes **with non-empty `cwd`** will produce a
  fingerprint that no other port currently computes, so they will
  **NOT replay in non-Go ports** until those ports adopt the same
  rule. Until then, using non-empty `cwd` is a Go-only contract.

Other ports are expected to adopt the same rule (tracked as follow-up
tasks in the xrr project). Once adoption is complete, this extension
becomes a v1 clarification rather than a Go-specific behavior.

## fs Adapter (v1)

The `fs` adapter records filesystem mutation operations. Reads are
not supported — tests should pre-seed disk state via fixtures and
use xrr only to assert on mutations.

### Operations

| Op         | Required fields           | Optional fields              |
|------------|---------------------------|------------------------------|
| `write`    | `path`, `data`            | `mode`, `flags`              |
| `mkdir`    | `path`                    | `mode`                       |
| `remove`   | `path`                    | `recursive`                  |
| `rename`   | `path`, `dest`            |                              |
| `chmod`    | `path`, `mode`            |                              |
| `chown`    | `path`, `uid`, `gid`      |                              |
| `symlink`  | `path` (target), `dest` (link) |                         |
| `hardlink` | `path` (old), `dest` (new)|                              |
| `truncate` | `path`, `size`            |                              |

### Fingerprint Inputs

```
fingerprint = sha256(canonical(fields))[:8]
```

Where `fields` is a map containing `op` and normalized `path`, plus
the following fields when present:

- `data_sha256` — the full sha256 hex of the `data` field's UTF-8
  bytes, when `data` is non-empty. **The raw bytes do NOT participate
  in the fingerprint**, only their hash; this keeps the cassette
  filename bounded regardless of payload size. The raw string still
  appears in the `.req.yaml` payload for human inspection and exact
  replay.
- `mode` — when set (presence-bearing, distinct from zero).
- `uid`, `gid` — when set.
- `dest` — when non-empty, after path normalization.
- `size` — when set.
- `flags` — when non-zero.
- `recursive` — when true.

`canonical(fields)` is JSON with keys sorted lexicographically.

### Path Normalization

Cassettes store paths in their **post-normalizer** form. A test
that writes to `/var/folders/abc/T/Test123/file` records the path
as `$TMP/file` (or whatever the normalizer rewrites it to). Replay
loaders read paths verbatim from the cassette and never re-derive
them — they do not need to know the original tmpdir.

This is the cross-runtime contract: ts/py/rs/php ports MUST replay
the normalized path as stored and MUST apply normalization on
record-side when producing cassettes for cross-runtime replay.

### Data Field Encoding

The `data` field on `write` requests is a **UTF-8 string**, not a
raw byte sequence. This keeps cassettes human-diffable for the
overwhelmingly common text payload case (config files, JSON, SQL,
generated source) — `data: "key: value\n"` renders as itself in
YAML across every language port.

**Binary payloads:** if the underlying call writes non-UTF-8 bytes
(images, compiled artifacts), the caller MUST base64-encode the
bytes BEFORE passing them to the wrapper, and base64-decode on
read. The cassette records the base64 string verbatim. xrr does
NOT auto-detect or auto-encode binary data — this keeps the
cross-runtime contract simple (every YAML library handles strings
identically; binary tag handling varies between libraries).

The fingerprint hashes the UTF-8 bytes of the `data` string. The
hash is the same whether the string contains text or base64-encoded
binary — the field is opaque from the fingerprint's perspective.

### Extension Status

The fs adapter is part of v1 as of 2026-05-11. Like exec's
`cwd`, omit-on-zero rules mean adding a previously-unset field to
a request shape invalidates existing cassettes recorded by adopters
who didn't populate it. Adopters who change which optional fields
they populate should expect cassette re-recording.

### Request Envelope Example (fs write)

```yaml
xrr: "1"
adapter: fs
fingerprint: "<8hex>"
recorded_at: "2026-05-11T12:00:00Z"
payload:
  op: write
  path: "$TMP/config.yaml"
  data: "key: value\n"
  mode: 420
```

### Response Envelope Example (fs write, success)

```yaml
xrr: "1"
adapter: fs
fingerprint: "<8hex>"
recorded_at: "2026-05-11T12:00:00Z"
payload:
  duration_ms: 2
  bytes_written: 11
```

### Response Envelope Example (fs write, failure)

```yaml
xrr: "1"
adapter: fs
fingerprint: "<8hex>"
recorded_at: "2026-05-11T12:00:00Z"
error: "open $TMP/config.yaml: permission denied"
payload:
  duration_ms: 0
  bytes_written: 0
```

## Response Envelope Example (exec, success)

```yaml
xrr: "1"
adapter: exec
fingerprint: "a3f9c1b2"
recorded_at: "2026-04-01T12:00:00Z"
payload:
  stdout: "title: My PR\n"
  stderr: ""
  exit_code: 0
  duration_ms: 142
```

## Response Envelope Example (exec, failure)

```yaml
xrr: "1"
adapter: exec
fingerprint: "deadbeef"
recorded_at: "2026-04-01T12:00:00Z"
error: "exit status 1"
payload:
  stdout: ""
  stderr: "boom\n"
  exit_code: 1
  duration_ms: 8
```

On replay, the session re-emits a non-nil error whose `Error()` string equals
the recorded `error` field, alongside the deserialized response payload.

## Secret Redaction (v1 clarification)

Cassettes are committed to version control, so recorders MUST NOT
persist credentials. Redaction is a **record-side** concern: it happens
before serialization, so a secret never reaches disk.

### Format impact: none

Redaction introduces **no new envelope field and no version bump**. A
redacted value is an ordinary string in an existing field, so a v1
reader parses a redacted cassette exactly like any other. Ports may
adopt redaction independently without breaking replay compatibility.

### Placeholder form

```
<redacted:FIELD_NAME>
```

`FIELD_NAME` is the field's own key, uppercased, with its original
separators preserved (`Authorization` → `<redacted:AUTHORIZATION>`,
`X-Api-Key` → `<redacted:X-API-KEY>`).

The placeholder MUST derive **only** from the field name — never from
the secret's value, a hash of it, a counter, or a timestamp. This makes
re-recording byte-stable, so committed cassettes do not churn.

### Fingerprint interaction — REQUIRED reading for ports

Redaction MUST NOT change any fingerprint. This holds today because no
v1 fingerprint input includes a credential-bearing field:

| Adapter | Fingerprint inputs        | Redactable fields (not hashed) |
|---------|---------------------------|--------------------------------|
| exec    | `argv`, `stdin`, `cwd?`   | `env`                          |
| http    | `method`, path+query, `body_hash` | `headers`              |
| grpc    | `service`, `method`, `msg_hash`   | —                      |
| redis   | `command`, `args`         | —                              |
| sql     | normalized `query`, `args`| —                              |

Ports MUST redact only fields outside their fingerprint inputs.
Redacting a **hashed** field (e.g. rewriting an exec `argv` element or
an http body) would shift the fingerprint and break cross-runtime
replay, because a port that redacts differently would compute a
different cassette key. Fields inside the fingerprint MUST be left
verbatim; adopters who pass credentials as command-line arguments
should scrub them before handing the request to xrr.

### Minimum default policy

A conforming recorder SHOULD redact, with zero configuration:

- Field names matching the credential words `TOKEN`, `SECRET`,
  `PASSWORD`, `PASSPHRASE`, `CREDENTIAL(S)`, `KEY`, `APIKEY`, `AUTH`,
  `COOKIE`, `SESSION`, `SIGNATURE`, `PRIVATE`, `BEARER` as
  underscore-delimited words, plus the `AWS_` namespace. Matching is
  case-insensitive and treats `-` and `_` as equivalent so HTTP header
  and env-var spellings classify identically.
- HTTP headers `Authorization`, `Proxy-Authorization`, `Cookie`,
  `Set-Cookie`, `X-Api-Key`, `X-Auth-Token`, `X-Amz-Security-Token`.
- Values matching high-confidence vendor credential shapes (GitHub
  `ghp_`/`github_pat_`, AWS `AKIA`, `sk-`, Slack `xox*-`, Google
  `AIza`, Stripe, JWT, PEM private-key headers, `Bearer`/`Basic`).

Value-pattern matching SHOULD stay narrow. A false positive silently
corrupts a cassette, so generic "looks random" heuristics MUST NOT be
used — they would redact commit SHAs, UUIDs, and base64 payloads.

Empty values MUST be left alone: there is nothing to leak, and a
placeholder would falsely imply a secret was present.

### Escape hatch

Recorders MUST offer a way to preserve a named field verbatim and to
add custom field names, and MAY offer a global off switch. The Go port
exposes these as `XRR_REDACT_ALLOW`, `XRR_REDACT_DENY`, and
`XRR_REDACT_DISABLE`.

### Adoption status

Implemented in the Go port. ts / py / php / rs record verbatim until
they adopt this section. Because redaction is record-side only, mixed
adoption does not break replay in either direction — an unredacted
cassette replays in a redacting port and vice versa.

## Cross-Language Conformance

All language ports MUST be able to replay cassettes written by any other port.
Conformance fixtures live in `spec/fixtures/`. Each fixture dir contains:
- `*.req.yaml` + `*.resp.yaml` pairs
- `manifest.yaml` listing all `adapter`+`fingerprint` pairs to replay
