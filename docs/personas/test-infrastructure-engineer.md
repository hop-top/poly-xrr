# Persona: Test Infrastructure Engineer

**Primary Role:** Backend or platform engineer who owns the test reliability stack at a
mid-to-large team — responsible for keeping CI fast, deterministic, and free of flaky
network-dependent tests.

**Extends:** [individuals/platform-engineer](~/.w/ideacrafterslabs/.docs/personas/individuals/platform-engineer.md)

---

## Goals

- Eliminate flaky tests caused by real network calls (HTTP, Redis, SQL, gRPC, exec).
- Record real interactions once; replay forever in CI without external dependencies.
- Cassettes committed to VCS — zero infra needed to run the full suite.

---

## Interaction Pattern

### Record real call (dev machine)

```pseudocode
XRR_MODE=record go test ./... -run TestCheckoutFlow
// cassettes written to testdata/cassettes/
```

### Commit cassettes + replay in CI

```pseudocode
git add testdata/cassettes/
git commit -m "chore(cassettes): record checkout flow"

// CI: no Redis, no Postgres, no external APIs
XRR_MODE=replay go test ./...
```

### Adapter selection per layer

```pseudocode
session := xrr.NewSession(mode, cassette)
resp, _ := session.Record(ctx, sql.NewAdapter(), sqlReq, func() { ... })
resp, _ := session.Record(ctx, http.NewAdapter(), httpReq, func() { ... })
```

---

## Key Pain Points

- Flaky tests from external service timeouts or rate limits kill CI reliability.
- VCR-style libs are language-specific; polyglot teams maintain 3+ solutions.
- Cassettes recorded in one language can't be shared across a Go service + Python script.

---

## System Leverage

### Language-agnostic cassettes

YAML format replays in Go, Python, TypeScript, PHP, Rust — one cassette set for
cross-language integration scenarios.

### Pluggable adapters

Exec, HTTP, gRPC, Redis, SQL, fs adapters cover most test surface; passthrough mode
keeps tests runnable against real infra during development.

### Conformance fixtures

`spec/fixtures/` shared cross-language; any port that passes conformance suite is
cassette-compatible.

---

## User Stories

- [[US-0101]](../stories/US-0101-record-first-cassette.md) — Record first cassette in CI setup
- [[US-0102]](../stories/US-0102-replay-in-ci.md) — Replay cassettes in CI without infra
- [[US-0103]](../stories/US-0103-cross-language-replay.md) — Replay Go cassette in Python test
- [[US-0104]](../stories/US-0104-adapter-selection.md) — Swap adapter per channel type
- [[US-0105]](../stories/US-0105-cassette-miss.md) — Handle cassette miss in replay mode

---

## Success Metrics

- Zero flaky test failures due to external service unavailability.
- CI runtime reduced by eliminating real network round-trips.
- Cassettes in VCS; any developer can replay full suite with no service setup.

---

## Collaboration with Other Personas

- **AI Agent Developer:** shares cassette replay needs; agent developer uses xrr to
  isolate tool calls during agent testing.
- **OSS Contributor:** contributor's new adapters directly unblock test infra engineer
  (e.g., Kafka adapter for event-driven pipelines).
