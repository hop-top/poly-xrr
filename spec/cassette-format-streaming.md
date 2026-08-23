# Cassette Format v1 — Streamed Interactions

Addendum to [cassette-format-v1.md](cassette-format-v1.md). Extends the v1
cassette format to record and replay streamed interactions: gRPC
server/client/bidi streams today, with a frame layer designed to carry future
streaming channels (HTTP SSE/chunked, redis pub/sub) without change.

Language-agnostic; all ports MUST conform to the format layer
(parse/emit/validate) even if they ship no streaming adapter. gRPC-specific
semantics are confined to the [gRPC Adapter Mapping](#grpc-adapter-mapping)
section.

## Design Decisions

Summary of the load-bearing choices; normative detail follows.

1. **v1-additive, not v2.** Streamed interactions ride the existing v1
   envelope (`xrr: "1"`) plus one new optional top-level field, `stream`. v1
   states "any other additional top-level fields are ignored by loaders
   (forward compat)" — this is that hook. Existing cassettes stay valid
   byte-for-byte; existing loaders keep working; ports extend one format
   instead of maintaining two.
2. **Pairwise file layout retained.** One `.req.yaml` (client→server side) +
   one `.resp.yaml` (server→client side) per streamed interaction. Directory
   layout, naming, manifest shape, and cassette IO conventions all carry over
   unchanged; no per-frame files, no third file kind.
3. **Frames are a list under a top-level `stream` key**, not inside
   `payload`. `payload` remains the adapter's open-request / terminal-response
   object (v1 shape); the frame log is format-layer owned and identical
   across adapters. Ports implement the stream schema once, in core cassette
   IO, separate from any adapter.
4. **Global sequence numbers record observed total order.** Every event
   (send frame, recv frame, client half-close, terminal) gets a unique `seq`
   from one per-interaction counter. Replay enforces order **per direction
   only**; cross-direction interleaving is descriptive, never a replay gate —
   gating recv delivery on recorded send positions can deadlock ping-pong
   clients whose recorded interleaving was concurrent. Deterministic and
   deadlock-free beats faithful-but-flaky.
5. **Half-close is a positioned scalar event** (`half_close: {seq}` on the
   req side), not a pseudo-frame. It occurs at most once, so a field beats a
   frame type; giving it a `seq` preserves its position in the total order.
6. **Terminal is a positioned event on the resp side** (`end: {seq}`), and
   error terminals reuse the v1 envelope `error` field verbatim: non-empty
   `error` ⇒ replay re-emits an error. Mid-stream errors are therefore "N
   recv frames, then `end` with envelope `error`" — no new error channel.
7. **Fingerprint MUST be computable at stream open.** The send side may be a
   sequence that does not exist yet when replay must locate the cassette, so
   send frames are excluded from the fingerprint. Conversation fidelity is
   enforced instead by strict replay-time send validation (order + bytes).
   Where the open DOES carry a message (gRPC server-streaming), its hash is
   fingerprint input, mirroring unary. Where it does not (client/bidi), a
   deterministic per-session occurrence counter disambiguates repeated opens.
8. **Streaming fingerprint inputs are disjoint from unary inputs.** Every
   streaming fingerprint hashes a `stream` discriminator, so the canonical
   input strings can never coincide with unary ones. The fingerprints
   themselves are 32-bit truncations sharing one filename namespace, so the
   residual truncation-collision risk is the same one v1 already accepts;
   short of such a collision, a pre-streaming loader replaying a unary
   request gets a loud cassette miss rather than a degenerate hit.
9. **Message bytes: exactly one of `message_b64` or `message_text` per
   frame.** Binary payloads (protobuf wire bytes) are standard base64;
   valid-UTF-8 payloads MAY use a plain string for human diffability
   (the fs `data` precedent). Fingerprints and comparisons always operate on
   decoded bytes, so both encodings are equivalent. No YAML `!!binary` tags —
   tag handling varies between YAML libraries.
10. **Timing recorded, ignored on replay by default.** Every event carries
    `at_ms` (offset from stream open). Replaying gaps faithfully makes tests
    slow and flaky, so replay ignores timing unless the adopter opts in to
    replay-timing mode.

## Directory Layout and Naming

Unchanged from v1:

```
<session-dir>/
  <adapter-id>-<fingerprint>.req.yaml
  <adapter-id>-<fingerprint>.resp.yaml
```

A streamed interaction is one req/resp pair, exactly like a unary one. The
fingerprint is computed by the adapter's streaming fingerprint rules (see
[Fingerprinting](#fingerprinting-streamed-interactions)).

## Envelope Extension

Both files keep the full v1 envelope (`xrr: "1"`, `adapter`, `fingerprint`,
`recorded_at`, `payload`, and `error` on resp) and gain one optional
top-level field:

| Field  | Type   | Description                                            |
|--------|--------|--------------------------------------------------------|
| stream | object | Present on BOTH files of a streamed interaction. Absent on unary interactions. |

Rules:

- Presence of `stream` on the req envelope marks the interaction as
  streamed. Writers MUST put `stream` on both files; a pair where only one
  file carries `stream` is malformed and readers MUST reject it.
- `xrr` stays `"1"`. This addendum is part of v1 as of 2026-08-23.
- `payload` remains required, non-null, adapter-defined on both files (open
  request on req, terminal response on resp).
- The v1 resp `error` field keeps its exact v1 semantics and doubles as the
  error-terminal marker: non-empty ⇔ the stream terminated with an error.
- `fingerprint` MUST be written as a quoted YAML string, in envelopes and
  in manifests: an all-digit fingerprint otherwise parses as an integer,
  and a leading-zero form parses as octal under YAML 1.1 readers — either
  way the value no longer matches its filename and replay misses
  spuriously.
- Pre-streaming loaders ignore `stream` per the v1 forward-compat rule.
  Streaming-aware implementations MUST NOT replay a streamed cassette
  through a unary code path, nor a unary cassette through a streaming code
  path — both are shape-mismatch errors, distinct from a cassette miss.

## Stream Object Schema

### `.req.yaml` — `stream`

Carries the client→server half.

```yaml
stream:
  type: bidi                # required: server | client | bidi
  frames:                   # required: client→server message frames, ascending seq
    - seq: 0
      message_b64: "cGluZy0x"
      at_ms: 0
  half_close:               # optional: client closed its send side
    seq: 4
    at_ms: 45
```

| Field       | Type   | Required | Description                                      |
|-------------|--------|----------|--------------------------------------------------|
| type        | string | yes      | `server`, `client`, or `bidi` — direction shape of the stream. Unary interactions never use this format. |
| frames      | list   | yes      | Client→server message frames. MUST be present; `[]` when the client sent none. Readers MUST treat an absent key as `[]`. |
| half_close  | object | no       | Present iff the client half-closed its send side before the stream terminated. `seq` required, `at_ms` optional. Absent ⇒ the stream terminated (server finish, error, or cancel) before the client half-closed. |

### `.resp.yaml` — `stream`

Carries the server→client half.

```yaml
stream:
  frames:                   # required: server→client message frames, ascending seq
    - seq: 1
      message_b64: "cG9uZy0x"
      at_ms: 3
  end:                      # required: terminal event
    seq: 5
    at_ms: 47
```

| Field  | Type   | Required | Description                                           |
|--------|--------|----------|-------------------------------------------------------|
| frames | list   | yes      | Server→client message frames. MUST be present; `[]` when the server sent none. Readers MUST treat an absent key as `[]`. |
| end    | object | yes      | The terminal event — the moment the stream ended (status/trailers observed, or local abort). `seq` required, `at_ms` optional. Every recorded stream has exactly one. |

`type` appears only on the req side; the pair is always loaded together.

### Frame Schema

Each element of either `frames` list:

| Field        | Type   | Required | Description                                     |
|--------------|--------|----------|-------------------------------------------------|
| seq          | int    | yes      | Global sequence number, ≥ 0. See [Ordering](#ordering). |
| message_b64  | string | see note | Message bytes, standard base64 (RFC 4648, with padding). `""` encodes an empty message. |
| message_text | string | see note | Message bytes as a plain UTF-8 string.          |
| at_ms        | int    | no       | Milliseconds elapsed since stream open. See [Timing](#timing-metadata). |

Exactly one of `message_b64` / `message_text` MUST be present per frame.
Writers MAY use `message_text` only when the bytes are valid UTF-8 and
round-trip losslessly through the port's YAML library; otherwise they MUST
use `message_b64`. Readers MUST accept either. All hashing and comparison is
over the **decoded bytes**, so the two encodings are interchangeable.

Writers MUST emit `message_text` as a quoted scalar (single- or
double-quoted style), and readers MUST decode it as a string regardless of
scalar resolution. YAML 1.2 emitters legitimately leave values like `on`,
`12:30`, or `null` unquoted, and a YAML 1.1 reader then corrupts them
(`on` → boolean true, `12:30` → sexagesimal 750); mandatory quoting plus
resolution-blind reading closes both directions.

`message_b64` MUST contain only base64 alphabet characters and padding —
no whitespace or line breaks. Readers MUST validate strictly and reject
invalid characters: several standard decoders silently discard them by
default (e.g. Python's `b64decode`), and ports MUST NOT rely on such
defaults.

Direction is implied by which file a frame lives in (req = send,
resp = recv); frames carry no direction field.

## Ordering

- One per-interaction counter assigns `seq` values in observed order to
  every event: each send frame, each recv frame, the `half_close` event (if
  any), and the `end` event.
- Writers MUST emit dense values `0..N-1` with no duplicates across the
  pair, and MUST list frames in ascending `seq` within each file.
- `end.seq` MUST be the maximum `seq` of the interaction. Recorders MUST NOT
  record events after the terminal (a client `half_close` attempted after
  the stream already ended is dropped, matching its real-world no-op). This
  ordering is race-free: the recorder runs client-side and observes the
  client's own half-close synchronously at the call site, so a half-close
  that precedes the terminal is always sequenced before it.
- Readers MUST reject duplicate `seq` values (across the pair) and
  non-ascending frame lists. Readers MAY accept sparse numbering (gaps) —
  replay needs only relative order — but writers MUST NOT produce it.
- **Replay enforces order per direction only.** Recv events are delivered in
  ascending `seq`; send events are validated in ascending `seq`. The
  relative order of send vs recv events is recorded truth about one real
  run, but replay MUST NOT gate recv delivery on send progress: recorded
  interleaving of concurrent events is arbitrary, and gating on it can
  deadlock a correct client (client waits for a recv that the recording
  places after its next send). See [Replay Semantics](#matching-and-replay-semantics).
  (Non-normative: a future opt-in strict mode could additionally assert
  that all recorded sends were observed before stream teardown; it would
  be additive and requires no format change.)

## Half-Close Semantics

Who finishes first differs by stream type; the encoding does not:

- **server**: the client sends its single request message and half-closes
  immediately. `half_close` is present, directly after the sole send frame.
- **client**: the client sends N messages, half-closes, then the server
  responds. `half_close` present after the last send frame; the response
  message and terminal follow it in `seq` order.
- **bidi**: `half_close` sits wherever the client actually closed relative
  to the observed event order — possibly before, between, or after recv
  frames, possibly absent entirely if the stream terminated first.

Absence of `half_close` is valid for any type: a stream may end (server
finish, error, cancellation) before the client closes its side.

## Mid-Stream Errors

A stream that delivered N messages and then failed records:

- all N recv frames in `resp.stream.frames`,
- `end` positioned after them,
- a non-empty v1 `error` field on the resp envelope,
- adapter terminal fields in the resp `payload` (for gRPC: the status code).

Replay delivers the N frames in order, then surfaces the recorded error in
place of end-of-stream (exact contract below). There is no partial-frame or
frame-level error encoding: errors are always terminal, which matches gRPC
(one status per RPC) and keeps the frame schema uniform for future adapters.

## Empty Streams

A direction with zero messages records `frames: []`. Degenerate but valid
examples:

- server-stream whose server sent nothing before OK: resp
  `frames: []`, first replayed read yields end-of-stream.
- client-stream where the client half-closed immediately: req
  `frames: []`, `half_close: {seq: 0}`.
- bidi with no traffic at all: req `frames: []`, `half_close: {seq: 0}`;
  resp `frames: []`, `end: {seq: 1}`.

## Timing Metadata

- `at_ms` is the integer count of milliseconds elapsed between the
  adapter's observation of stream open and the event, measured on a
  monotonic clock. Values are ≥ 0 and SHOULD be non-decreasing with `seq`.
- Recorders MUST populate `at_ms` on every frame and on `half_close`/`end`.
  Readers MUST tolerate its absence (hand-authored cassettes).
- **Replay ignores timing by default.** Frames are delivered as fast as the
  client consumes them. Real frame-arrival gaps are recorded behaviour worth
  keeping, but replaying them faithfully makes tests slow and flaky.
- Implementations MAY offer an opt-in replay-timing mode. When enabled:
  recv frame `j` and the terminal MUST NOT be delivered before their
  recorded `at_ms` has elapsed relative to the replayed stream open; send
  events are never delayed. Implementations MAY scale or cap the delays;
  the opt-in and any scaling are out of conformance scope.

## Fingerprinting Streamed Interactions

Format-layer requirements (all adapters):

- The fingerprint MUST be computable from information available at stream
  open, because replay must locate the cassette before any frames exist.
  Send/recv frame sequences are therefore never fingerprint inputs;
  conversation fidelity is enforced by replay-time send validation instead.
- The algorithm remains v1: `sha256(canonical(inputs))[:8]` — 8 lowercase
  hex chars of the sha256 of deterministic JSON with lexicographically
  sorted keys, no insignificant whitespace.
- Streaming fingerprint inputs MUST include a `stream` discriminator so the
  streaming fingerprint space is disjoint from the adapter's unary space.
- When the open does not fully identify the interaction (no message
  available), adapters use the **occurrence counter** `n`: the 0-based count
  of streamed interactions with the same identifying inputs already opened
  in the current session. **One session object is one counter domain**: the
  counter is created with the session, keyed by the adapter's identifying
  tuple, incremented at each open, and counted identically in record and
  replay modes. Determinism therefore assumes the test opens streams of a
  given key in a deterministic order — the same assumption v1 makes about
  issuing identical unary requests.

**Cross-process warning.** In the cross-process adoption model (a parent
sets the cassette dir; N child processes each construct their own session
from the environment), each child owns an independent counter domain. Two
children opening the same identifying tuple both count `n = 0` and produce
the same fingerprint: in record mode last-write-wins destroys one of the
two conversations; in replay mode both children receive the same recording,
silently swapping responses whenever their send sequences are identical.
Multi-process adopters MUST keep counter-fingerprinted tuples disjoint per
process. A future adapter-level discriminator (in the spirit of exec's
`cwd` extension) is the escape hatch if that constraint proves unworkable.

Adapter-specific inputs are defined in each adapter's mapping section.

## Matching and Replay Semantics

Let `S` = number of recorded send frames, `R` = number of recorded recv
frames.

**Open.** Compute the fingerprint per the adapter's streaming rules and load
the pair. No pair ⇒ cassette miss (same error condition as v1). A pair
without `stream` ⇒ shape-mismatch error (unary cassette, streamed request).

**Send side.** The i-th (0-based) message sent by the replaying client:

- `i < S`: decoded bytes MUST equal recorded send frame i's decoded bytes.
  Equal ⇒ the send succeeds (the message is discarded). Unequal ⇒ stream
  mismatch: the stream MUST fail with a mismatch error whose message SHOULD
  identify the ordinal and the expected vs actual content (e.g. by sha256).
- `i ≥ S`: the recorded stream was already past its last observed send. If
  the recorded terminal is an error, the send MUST return that recorded
  error (the real stream was dead too). If the terminal is OK, the send
  MUST return the adapter's post-completion send signal (for gRPC:
  EOF/stream-done) and MUST NOT poison the recv side — "send until the
  stream reports done, then read the status" is a canonical flow-controlled
  producer pattern, and the recorder drops post-terminal sends, so treating
  them as mismatches would fail correct clients. Byte content at `i ≥ S` is
  never compared: divergence there is undetectable by construction, since
  the real server never saw those bytes either.

A client half-close after exactly `S` sends is always accepted, whether or
not the recording has `half_close`. A half-close after fewer than `S` sends
is a stream mismatch. (Presence of `half_close` in the recording never
obliges the replaying client to half-close; terminal delivery does not
depend on it.)

**Recv side.** The j-th (0-based) read by the replaying client:

- `j < R`: returns recorded recv frame j's decoded bytes.
- `j = R`: returns the terminal — the recorded error when the resp envelope
  `error` is non-empty, otherwise the adapter's end-of-stream signal.
- `j > R`: identical to `j = R` — the terminal result repeats and is
  replayable indefinitely.

Reads MUST NOT block on send-side progress. A client that stops reading
early leaves frames undelivered; that is not an error.

**Mismatch is terminal.** Stream mismatch arises only from byte-divergent
sends at `i < S` and from short half-close. After a mismatch, all subsequent
operations on that stream MUST fail. A mismatch error is distinct from a
cassette miss and from a recorded (replayed) error.

**Cancellation.** A replaying client that cancels locally gets its runtime's
normal cancellation behaviour; the cassette is read-only and unaffected.

**Record mode.** The recorder observes the live stream, assigns `seq` from
one monotonic counter in arrival order, stamps `at_ms` from open, and
persists both files when the terminal is observed. A stream that never
reaches terminal (process crash) produces no cassette. Recording a streamed
interaction whose fingerprint already has files overwrites them (v1
last-write-wins, unchanged).

**Passthrough.** Unchanged: live calls, cassette untouched.

## Validation Rules

Readers MUST reject a streamed pair when:

1. `stream` is present on one file of the pair but not the other.
2. `req.stream.type` is missing or not one of `server` / `client` / `bidi`.
3. A frame lacks `seq`, or has both or neither of
   `message_b64` / `message_text`.
4. A `frames` list is not strictly ascending in `seq`.
5. Any `seq` value is duplicated across the pair (frames, `half_close`,
   `end`).
6. `resp.stream.end` is missing, or `end.seq` is not the maximum `seq` in
   the pair.
7. `message_b64` is not valid standard base64.

Readers SHOULD additionally verify adapter-mapping constraints (e.g. frame
counts by type) when the adapter is known. Unknown extra fields inside
`stream`, frames, `half_close`, or `end` are ignored (forward compat, same
policy as the envelope).

## gRPC Adapter Mapping

Everything in this section is gRPC-specific. Ports without a gRPC adapter
implement none of it, but their format layer still parses these cassettes.

### Stream Types

| `stream.type` | gRPC RPC kind        | Send frames         | Recv frames        |
|---------------|----------------------|---------------------|--------------------|
| `server`      | server-streaming     | exactly 1           | 0..n               |
| `client`      | client-streaming     | 0..n                | 0 or 1             |
| `bidi`        | bidirectional        | 0..n                | 0..n               |

Unary RPCs keep the existing v1 unary shape (`payload.message`, no `stream`
key) — they do not migrate to this format.

Mapping constraints (readers SHOULD verify, writers MUST satisfy):

- `server`: exactly one send frame, and `half_close` present immediately
  after it (generated clients half-close implicitly with the request).
- `client`: at most one recv frame; zero only when the stream terminated
  in an error before the response message.

### Message Encoding

Frames carry protobuf wire bytes. gRPC writers MUST use `message_b64` and
MUST NOT use `message_text`.

### Request Payload

```yaml
payload:
  service: files.FileService
  method: Download
```

For `client` and `bidi` streams, writers MUST also record the occurrence
ordinal used at open:

```yaml
payload:
  service: chat.ChatService
  method: Converse
  n: 0
```

The payload `n` is informational: it makes the occurrence recoverable from
disk (loaders ignore unknown payload fields, so this is additive). Replay
recomputes its own counter and MUST NOT read the payload `n` to drive
matching. Server-stream payloads omit `n`. Streamed gRPC req payloads MUST
NOT carry the unary `message` field.

### Response Payload and Errors

```yaml
payload:
  status_code: 0            # gRPC status code, int; 0 = OK
```

- `status_code` is required.
- The envelope `error` field MUST be non-empty iff `status_code != 0`.
  Writers SHOULD record the client library's rendering of the status; when
  the status message is empty they MUST still synthesize a non-empty error
  string. Replaying gRPC adapters SHOULD reconstruct the status from
  `status_code`, treating the error string as the status/description text.
- Initial and trailing metadata are not recorded, matching the unary gRPC
  adapter. Adapters MAY add payload fields later; loaders ignore unknown
  payload fields.

### Fingerprint Algorithms

All three produce `fingerprint = sha256(canonical_json)[:8]`, in v1
truncation notation: `[:8]` is the first 8 characters of the lowercase hex
digest (equivalently, the hex of the digest's first 4 bytes). The server
form additionally reuses the unary building block
`msg_hash = sha256(message_bytes)[:8]`.

**server** — the single request message is available at open:

```
canonical = {"method":<method>,"msg_hash":<msg_hash>,"service":<service>,"stream":"server"}
```

**Server-stream collision warning.** Identical request bytes for the same
service/method share one cassette: record mode is last-write-wins, exactly
as v1 unary. Streamed outputs of identical requests are likelier to differ
across calls than unary ones (status polling, retries), so a poll loop
recorded against a live server replays its final "done" stream on the
first iteration — a false green. The mirror-of-unary shape is deliberate;
adding an occurrence counter later would re-fingerprint every existing
server-stream cassette, hence this warning now. Adopters whose repeated
identical opens must stay distinct should carry a distinguishing field in
the request message.

**client** and **bidi** — no message at open; use the occurrence counter:

```
canonical = {"method":<method>,"n":<n>,"service":<service>,"stream":"client"}
canonical = {"method":<method>,"n":<n>,"service":<service>,"stream":"bidi"}
```

`n` is a JSON integer: the 0-based count of prior streamed opens with the
same `(service, method, stream type)` tuple in the current session. It is
always included, even when 0 (no omit-on-zero).

Keys are shown in their sorted order; canonical JSON has no whitespace.
Service and method names are proto identifiers (`[A-Za-z0-9_.]`), so JSON
string escaping never varies between ports.

Because every input set includes `"stream"`, streaming canonical inputs
are disjoint from unary ones by construction. The fingerprints themselves
are 32-bit truncations sharing one filename namespace, so the residual
truncation-collision risk is the one v1 already accepts; short of such a
collision, a unary replay against a session recorded as streaming misses
loudly instead of hitting a degenerate response.

Test vectors (verifiable byte-for-byte):

| Case | Inputs | Canonical JSON | Fingerprint |
|------|--------|----------------|-------------|
| server | `files.FileService/Download`, message `{"path":"/etc/hosts"}` (msg_hash `f1e315a5`) | `{"method":"Download","msg_hash":"f1e315a5","service":"files.FileService","stream":"server"}` | `58a4bf3f` |
| server | `files.FileService/Download`, message `{"path":"/var/log/big.log"}` (msg_hash `164658bd`) | `{"method":"Download","msg_hash":"164658bd","service":"files.FileService","stream":"server"}` | `9e8c4d4c` |
| client | `files.FileService/Upload`, n=0 | `{"method":"Upload","n":0,"service":"files.FileService","stream":"client"}` | `2bebfd6f` |
| bidi | `chat.ChatService/Converse`, n=0 | `{"method":"Converse","n":0,"service":"chat.ChatService","stream":"bidi"}` | `c6233d2e` |

## Worked Example: Server-Stream

Client calls `files.FileService/Download` with request
`{"path":"/etc/hosts"}` (wire bytes shown as ASCII for readability); server
streams three chunks and finishes OK.

`grpc-58a4bf3f.req.yaml`:

```yaml
xrr: "1"
adapter: grpc
fingerprint: "58a4bf3f"
recorded_at: "2026-08-23T12:00:00Z"
payload:
  service: files.FileService
  method: Download
stream:
  type: server
  frames:
    - seq: 0
      message_b64: "eyJwYXRoIjoiL2V0Yy9ob3N0cyJ9"
      at_ms: 0
  half_close:
    seq: 1
    at_ms: 0
```

`grpc-58a4bf3f.resp.yaml`:

```yaml
xrr: "1"
adapter: grpc
fingerprint: "58a4bf3f"
recorded_at: "2026-08-23T12:00:00Z"
payload:
  status_code: 0
stream:
  frames:
    - seq: 2
      message_b64: "Y2h1bmstb25lCg=="
      at_ms: 12
    - seq: 3
      message_b64: "Y2h1bmstdHdvCg=="
      at_ms: 15
    - seq: 4
      message_b64: "Y2h1bmstdGhyZWUK"
      at_ms: 19
  end:
    seq: 5
    at_ms: 21
```

Replay: reads yield `chunk-one\n`, `chunk-two\n`, `chunk-three\n`, then
end-of-stream. Timing (12/15/19 ms) is recorded but not replayed by default.

## Worked Example: Bidi

Client converses with `chat.ChatService/Converse` (first open of this tuple
in the session ⇒ n=0): two ping/pong rounds, client half-closes, server
finishes OK.

`grpc-c6233d2e.req.yaml`:

```yaml
xrr: "1"
adapter: grpc
fingerprint: "c6233d2e"
recorded_at: "2026-08-23T12:00:00Z"
payload:
  service: chat.ChatService
  method: Converse
  n: 0
stream:
  type: bidi
  frames:
    - seq: 0
      message_b64: "cGluZy0x"
      at_ms: 0
    - seq: 2
      message_b64: "cGluZy0y"
      at_ms: 40
  half_close:
    seq: 4
    at_ms: 45
```

`grpc-c6233d2e.resp.yaml`:

```yaml
xrr: "1"
adapter: grpc
fingerprint: "c6233d2e"
recorded_at: "2026-08-23T12:00:00Z"
payload:
  status_code: 0
stream:
  frames:
    - seq: 1
      message_b64: "cG9uZy0x"
      at_ms: 3
    - seq: 3
      message_b64: "cG9uZy0y"
      at_ms: 44
  end:
    seq: 5
    at_ms: 47
```

Replay: sends are validated in order (`ping-1`, then `ping-2` — any other
byte content is a stream mismatch); reads yield `pong-1`, `pong-2`, then
end-of-stream. The recorded interleaving (seq 0–5) documents the real run;
replay does not gate reads on sends, so a client that reads both pongs
before sending `ping-2` still mismatches only if its send bytes diverge.

## Worked Example Fragment: Mid-Stream Error

`files.FileService/Download` of `{"path":"/var/log/big.log"}` delivered two
chunks, then the server went away. Resp file (`grpc-9e8c4d4c.resp.yaml`):

```yaml
xrr: "1"
adapter: grpc
fingerprint: "9e8c4d4c"
recorded_at: "2026-08-23T12:00:00Z"
error: "rpc error: code = Unavailable desc = connection reset"
payload:
  status_code: 14
stream:
  frames:
    - seq: 2
      message_b64: "bG9nLWNodW5rLTEK"
      at_ms: 10
    - seq: 3
      message_b64: "bG9nLWNodW5rLTIK"
      at_ms: 14
  end:
    seq: 4
    at_ms: 30
```

Replay: reads yield `log-chunk-1\n`, `log-chunk-2\n`, then the recorded
error (as a gRPC status with code 14) instead of end-of-stream.

## Future Adapters (Non-Normative)

The frame layer is adapter-neutral by construction: HTTP SSE or chunked
responses map to `type: server` with one frame per event/chunk
(`message_text` for text events); redis pub/sub subscriptions map to
`type: server` with one frame per delivered message. Those adapters define
their own payload shapes and fingerprint inputs in their own mapping
sections when they are specified — nothing here changes for them. Per-frame
adapter extras (an SSE event name or id, a pub/sub channel) can ride as
additional frame fields under the ignore-unknown rule.

## Manifest Extension

Conformance fixture manifests MAY mark streamed entries:

```yaml
interactions:
  - adapter: grpc
    fingerprint: "58a4bf3f"
    streamed: true
```

`streamed` defaults to false when absent. Runners use it to route the entry
through the streaming replay path.

## Conformance Obligations

Fixture dirs land under `spec/fixtures/` in a follow-up; the obligations are
fixed here.

**Format layer — ALL ports (ts/py/rs/php included, no gRPC adapter
required):**

- Parse every streaming fixture pair into the stream model (type, frames,
  half_close, end, timing) and re-emit it losslessly. Equality is judged
  field-for-field with messages compared over decoded bytes; YAML
  formatting may differ, and the message-encoding choice (`message_b64` vs
  `message_text`) is free on re-emit.
- Decode a scalar-hazard fixture whose `message_text` payloads include
  `on`, `12:30`, `null`, and strings with leading/trailing whitespace,
  yielding exactly those characters.
- Enforce the [Validation Rules](#validation-rules): reject one-sided
  `stream`, bad `type`, dual/absent message encoding, non-ascending or
  duplicate `seq`, missing `end`, non-maximal `end.seq`, invalid base64.
- Reject a malformed-base64 fixture (invalid characters or embedded
  whitespace in `message_b64`) instead of silently discarding the bad
  characters.
- Treat absent `frames` as `[]`; accept absent `at_ms`.

**Adapter level — ports with a gRPC adapter:**

| Fixture case | Must demonstrate on replay |
|--------------|----------------------------|
| server-stream | Fingerprint recomputed from `(service, method, msg_hash, "server")` locates the pair; recv frames delivered in `seq` order; end-of-stream after the last. |
| client-stream | Occurrence-counter fingerprint (`n`) locates the pair; sends validated in order and byte content (divergent bytes ⇒ stream mismatch; short half-close ⇒ stream mismatch); single response frame then end-of-stream. |
| bidi | Interleaved global `seq` parsed; per-direction ordering enforced; reads never block on send progress. |
| mid-stream error | All recorded recv frames delivered, then the recorded error (status reconstructed from `status_code`) in place of end-of-stream; post-terminal sends return the same error. |
| empty stream | `frames: []` parsed; first read yields end-of-stream immediately. |

The client-stream obligations include an `n = 1` case — a second open of
the same tuple within one session — which requires a scripted two-open
fixture (sequenced opens driven by a runner, not static files alone).

All ports MUST replay fixture cassettes regardless of which port recorded
them — the v1 cross-runtime guarantee extends to streams unchanged.
