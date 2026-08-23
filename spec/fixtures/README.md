# Conformance fixtures

Static cassette material backing the conformance obligations in
`../cassette-format-streaming.md` and `../cassette-format-v1.md`. Every port
replays these; a cassette recorded by any port must replay on all others.

## Frame payloads are not protobuf

The gRPC fixture dirs carry hand-authored JSON and ASCII bytes in
`message_b64`, not marshalled protobuf. This is deliberate — see the spec's
[Message Encoding](../cassette-format-streaming.md#message-encoding)
section, which is normative on the point.

The short version: the frame layer never decodes a payload. `msg_hash` is
`sha256` over the raw bytes, send validation compares bytes, and validation
rule 7 checks base64 well-formedness only. Nothing in the format parses a
message, so nothing needs a protobuf runtime, a descriptor, or a `.proto`.
The `message_b64` requirement binds the encoding (base64, never
`message_text`) — not the payload's schema.

Two things follow, and both are the point:

- **Fixtures stay reviewable.** They diff as readable text rather than as
  base64-of-binary, which the spec's Practical Limits section explicitly
  values.
- **Format conformance needs no protobuf dependency.** Ports whose gRPC
  adapter is optional (Rust gates `tonic`/`prost` behind `--features grpc`)
  run these unconditionally.

Ports drive these dirs through a byte-transparent identity codec — exactly
the raw-bytes codec case a custom gRPC codec presents. Coverage of real
protobuf marshalling, and of the deterministic-serialization requirement,
belongs in each port's live/e2e tests against an actual gRPC runtime. Do not
"fix" these fixtures by regenerating them as protobuf: the spec's
fingerprint test vectors are computed over exactly these bytes and are
normative as written.

## Manifest order is not an open sequence

`interactions` is an unordered set of pairs that must replay. Runners
establish the open order themselves, ascending by the req payload's `n`
*within* a counter domain — the `(service, method, stream type)` tuple of a
`client`/`bidi` open. Across distinct domains, and for server streams
(content-addressed, no counter, no `n`), order is unconstrained. See
[Manifest Extension](../cassette-format-streaming.md#manifest-extension).

Only `grpc-client-stream-repeat` currently has entries sharing a counter
domain, so it is the only dir where the rule constrains anything; the others
are order-independent by composition. Because a dir that is order-dependent
only incidentally would let the rule rot unnoticed, every port sorts
explicitly rather than relying on file order: reordering any manifest here
must leave all suites green.

## Negative fixtures

`grpc-stream-malformed-b64` MUST fail to load. Its `manifest.yaml` exists
but lists no entries (`interactions: []`): the pair is deliberately NOT
listed, because `interactions` enumerates pairs whose replay must succeed
and the schema cannot mark an expected rejection. Harnesses target it by
path. See that dir's README.
