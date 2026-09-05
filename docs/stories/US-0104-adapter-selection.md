# User Story: Pick the Right Adapter for a Channel

**System:** xrr
**Personas:** [Solo Developer](../personas/solo-developer.md)

---

## User Goal

As a Solo Developer, I want a clear adapter for each channel type (exec, HTTP, Redis,
SQL, gRPC, fs) so I can wrap any interaction without writing fingerprint or serialization
logic myself.

---

## Context

Developer's code makes a mix of HTTP calls and SQL queries. They need two adapters
in the same session, each fingerprinting its own request type correctly.

---

## Acceptance Criteria

- [ ] Each adapter has a stable, documented `id` (`exec`, `http`, `grpc`, `redis`, `sql`, `fs`).
- [ ] Adapter fingerprints are deterministic: same input always produces same fingerprint.
- [ ] Two adapters can be used in one session without collision (cassette filenames distinct).
- [ ] Adapter serialize/deserialize round-trips losslessly for all documented request fields.
- [ ] README adapter table lists all adapters, their fingerprint fields, and supported ports.

---

## Implementation Notes

```pseudocode
session = NewSession(RECORD, cassette)

// HTTP: fingerprint = method + path+query + sha256(body)[:8]
resp_http = session.Record(ctx, http.NewAdapter(), httpReq, callAPI)

// SQL: fingerprint = normalized_query + args
resp_sql  = session.Record(ctx, sql.NewAdapter(), sqlReq, runQuery)

// cassettes named distinctly:
// http-<fp>.req.yaml, sql-<fp>.req.yaml
```

### Key Files

- `go/adapters/http/`, `go/adapters/sql/`, etc.: adapter implementations
- `spec/cassette-format-v1.md`: fingerprint algorithm per adapter

---

## E2E / Verification Checklist

- [ ] Record HTTP + SQL in one session; verify distinct cassette files per adapter.
- [ ] Re-run with same requests; verify same fingerprints (deterministic).
- [ ] Serialize + deserialize HTTP request; verify all fields round-trip.
- [ ] README adapter table is accurate and up to date.

---

## Related Stories

- [[US-0101]](./US-0101-record-first-cassette.md) — Record first cassette
- [[US-0102]](./US-0102-replay-in-ci.md) — Replay in CI
