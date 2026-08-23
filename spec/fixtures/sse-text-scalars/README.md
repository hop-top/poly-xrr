# sse-text-scalars

Format-layer scalar-hazard fixture (cassette-format-streaming.md, Conformance
Obligations): `message_text` payloads `on`, `12:30`, `null`, and strings with
leading/trailing whitespace. All are quoted scalars per the frame-schema rule;
readers MUST decode each as a string yielding exactly those characters
(`on` is not a boolean, `12:30` is not sexagesimal 750, `null` is not nil,
whitespace is preserved).

The adapter is `sse` because gRPC writers MUST NOT use `message_text`; the
spec's future-adapters section (non-normative) maps SSE to `type: server`
with `message_text` frames. No sse fingerprint algorithm is specified yet, so
the fingerprint here is an opaque identifier — derived, for reproducibility
only, as `sha256(canonical({"stream":"server","url":"https://example.test/events"}))[:8]`
= `66ecc77a`. Format-level runners treat it as opaque; nothing recomputes it.
