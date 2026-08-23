# grpc-stream-malformed-b64

Negative fixture: loading this pair MUST fail. The resp `stream.frames`
contain invalid `message_b64` values — frame `seq: 3` has embedded
whitespace, frame `seq: 4` has a character outside the base64 alphabet.
Strict readers MUST reject the pair (Validation Rules, rule 7) instead of
silently discarding the bad characters as some standard decoders do by
default. Frame `seq: 2` is valid base64 of `blob-chunk 1`; the req file is
fully well-formed (fingerprint `8dbfb222` correctly computed from
`(files.FileService, Download, msg_hash("{\"path\":\"/opt/blob.bin\"}"),
"server")`, where the hashed bytes are that literal 24-byte ASCII string —
frame payloads here are not protobuf; see ../README.md).

The pair is deliberately NOT listed in `manifest.yaml`: `interactions`
enumerates pairs whose replay must succeed, and neither the v1 manifest nor
the streaming manifest extension defines a way to mark an expected-rejection
entry. Until the spec adds one, format-conformance harnesses must target
this pair by path (`grpc-8dbfb222.req.yaml` / `grpc-8dbfb222.resp.yaml`) and
assert that strict loading fails.
