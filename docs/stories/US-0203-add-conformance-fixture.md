# User Story: Add Conformance Fixture for New Adapter

**System:** xrr
**Personas:** [OSS Contributor](../personas/oss-contributor.md)

---

## User Goal

As an OSS Contributor, I want to add conformance fixtures for a new adapter so that
all ports can prove cross-language cassette compatibility via the shared test suite.

---

## Context

Contributor has implemented a new adapter. Before PR, they add a fixture set to
`spec/fixtures/` that every port's conformance test automatically picks up. This
proves the adapter produces compatible cassettes across all languages.

---

## Acceptance Criteria

- [ ] Fixture dir created: `spec/fixtures/<adapter>-<scenario>/`.
- [ ] `manifest.yaml` lists interaction(s) with `adapter`, `fingerprint`, `description`.
- [ ] Computed fingerprints are marked `verify_fingerprint: true` so every port
      recomputes them from the payload instead of only loading the pair
      (see `spec/fixtures/README.md`, "Pinned unary fingerprints").
- [ ] `.req.yaml` and `.resp.yaml` follow the cassette envelope schema exactly.
- [ ] All existing conformance tests (`go/conformance_test.go`, etc.) pick up new
      fixture automatically without code change.
- [ ] All ports pass new fixture: load manifest, fingerprint matches, replay returns
      expected response.

---

## Implementation Notes

```pseudocode
// manifest.yaml structure
interactions:
  - adapter: kafka
    fingerprint: "a3f9c1b2"
    verify_fingerprint: true   // walkers recompute it from the req payload
    description: "produce message to orders topic"

// kafka-a3f9c1b2.req.yaml
xrr: "1"
adapter: kafka
fingerprint: "a3f9c1b2"
recorded_at: "2026-04-01T12:00:00Z"
payload:
  topic: orders
  partition: 0
  value: "order-created:42"

// kafka-a3f9c1b2.resp.yaml
xrr: "1"
adapter: kafka
fingerprint: "a3f9c1b2"
recorded_at: "2026-04-01T12:00:00Z"
payload:
  offset: 101
  error: null
```

### Key Files

- `spec/fixtures/exec-happy/`: reference fixture structure
- `go/conformance_test.go`: auto-discovery pattern for new fixtures
- `spec/cassette-format-v1.md`: envelope schema

---

## E2E / Verification Checklist

- [ ] Add fixture dir + files; run `go test ./...` — new fixture auto-discovered.
- [ ] All 5 ports pass new fixture without code change.
- [ ] Fingerprint in `manifest.yaml` matches actual computed fingerprint
      (enforced by every walker for entries marked `verify_fingerprint: true`).
- [ ] `manifest.yaml` schema validated (required fields present).
- [ ] Streamed fixture only: `make emit-all` regenerates `spec/emitted/<port>/`
      for every port; each port's cross-port re-emission test passes.

---

## Related Stories

- [[US-0202]](./US-0202-port-adapter.md) — Port an adapter
- [[US-0204]](./US-0204-cross-lang-bug.md) — Debug cross-language mismatch
