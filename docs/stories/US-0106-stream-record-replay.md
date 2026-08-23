# User Story: Record and Replay a Streaming gRPC Call

**System:** xrr
**Personas:** [Solo Developer](../personas/solo-developer.md)

---

## User Goal

As a Solo Developer, I want to record a server-streaming gRPC interaction once and
replay it in CI so that streaming tests run with no live server, no network, and no
credentials.

---

## Context

Dev is testing code that runs remote commands over a server-streaming gRPC API
(exec-style: one request out, stdout chunks streaming back). Recording the real
stream once and committing the cassette lets CI replay the whole conversation —
chunks, end-of-stream, even mid-stream errors — with the server stopped.

Recording is not interceptor-only: every port (Go, ts, py, rs, php) ships a
session-level stream API — open a recording, append frames, finish; open a
replay, send/receive against the recorded conversation. The Go gRPC
interceptor is a convenience built on that API, and a cassette recorded in
any port replays in every other.

---

## Acceptance Criteria

- [ ] `grpc.WithStreamInterceptor(xgrpc.StreamClientInterceptor(session))` records
      server-, client-, and bidi-streaming RPCs in record mode.
- [ ] One cassette pair per stream: `grpc-<fp>.req.yaml` + `grpc-<fp>.resp.yaml`,
      both carrying the `stream` frame log per the streaming spec.
- [ ] Replay serves recorded messages in order and ends with the recorded terminal
      (end-of-stream or the original status error); the network is never dialed.
- [ ] Replay validates sent messages byte-for-byte; divergent sends fail with a
      stream mismatch (not silent wrong data).
- [ ] If the cassette pair is missing: `ErrCassetteMiss` (not a hang or a dial).
- [ ] The recorded pair replays through every port's stream session API
      (`openStreamReplay` / `open_stream_replay` per language) — same frames,
      same terminal, byte-validated sends.

---

## Implementation Notes

```pseudocode
// Record once against the real server
session = NewSession(mode=RECORD, cassette=FileCassette("testdata/cassettes"))
conn = grpc.NewClient(target, WithStreamInterceptor(StreamClientInterceptor(session)))
stream = client.Run(ctx, req)          // server-streaming RPC
for chunk in stream: consume(chunk)    // every frame teed into the cassette

// CI: server stopped, replay only
session = NewSession(mode=REPLAY, cassette=FileCassette("testdata/cassettes"))
conn = grpc.NewClient(target, WithStreamInterceptor(StreamClientInterceptor(session)))
stream = client.Run(ctx, req)          // served from the cassette, no dial
```

### Key Files

- `go/adapters/grpc/stream.go`: `StreamClientInterceptor` record/replay dispatch
- `go/stream_session.go`: stream open/record/replay core
- `ts/src/streamSession.ts`, `py/src/xrr/stream_session.py`,
  `rs/src/stream_session.rs`, `php/src/Stream/`: port stream session APIs
- `spec/cassette-format-streaming.md`: streamed-interaction format
- `go/e2e_grpc_stream_test.go`: live-record → server-stopped-replay reference

---

## E2E / Verification Checklist

- [ ] Record against a live server; verify one req/resp YAML pair per stream, with
      `stream` present on both files.
- [ ] Stop the server; rerun the same driver in replay mode; verify identical
      messages and terminal, and zero dial attempts.
- [ ] Replay a recorded mid-stream error; verify the status code survives replay.
- [ ] Delete the pair; verify `ErrCassetteMiss`.
- [ ] Replay the same pair in at least one other port via its stream session
      API; verify identical frames and terminal.
- [ ] Confirm no secret-bearing frames were recorded — frames are verbatim
      unless the Go scrub hook (`NewSessionWithStreamScrub`) is installed on
      both the recording and replaying session; the other ports have no hook
      yet.

---

## Related Stories

- [[US-0102]](./US-0102-replay-in-ci.md) — Replay cassettes in CI without infra
- [[US-0103]](./US-0103-cross-language-replay.md) — Replay Go cassette in Python test
