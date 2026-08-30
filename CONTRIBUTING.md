# Contributing to xrr

## Quick start

```
git clone ...
make test        # Go (canonical) — fast default
make test-all    # all 5 languages
make lint-all    # all 5 linters
make check-all   # lint + test + e2e, all 5 languages (pre-merge gate)
```

Per-language targets (`test-py`, `lint-rs`, `e2e-ts`, …) are in the
[Makefile](Makefile). Host lint tooling is pinned in `mise.toml`
(`mise install` provisions it). For a container with every toolchain
preinstalled: `make dev-up && make dev-exec`.

## Adding an adapter

1. Implement `Adapter` interface in target language (4 methods: `id`, `fingerprint`,
   `serialize`, `deserialize`) — use `go/adapters/exec/` as reference.
2. Add conformance fixture: `spec/fixtures/<adapter>-happy/` with `manifest.yaml`,
   `<adapter>-<fp>.req.yaml`, `<adapter>-<fp>.resp.yaml`.
3. Run conformance in all ports — every port must pass new fixture without code change.
4. Open PR; link to `spec/cassette-format-v1.md` and the relevant interface docs.

## Porting to a new language

See the [Porting Guide](README.md#porting-guide) in the root README.

## Commit style

Conventional Commits: `feat|fix|refactor|build|ci|chore|docs|style|perf|test`.

## Code of Conduct

See [CODE_OF_CONDUCT.md](CODE_OF_CONDUCT.md).
