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

## Pinned unary fingerprints

A manifest entry may carry `verify_fingerprint: true`. It marks a unary
entry whose `fingerprint` is a computed value, not an opaque label: walkers
MUST rebuild the adapter's request from the `.req.yaml` payload, recompute
the fingerprint with the adapter's own algorithm, and compare it with the
manifest value. Loading alone proves nothing about the algorithm — a port
that canonicalises differently still loads the files fine; the key it would
derive on replay is what differs, and that is the cross-runtime miss.

Entries without the flag are load-only, which is how `exec-happy` stays
green: its `a3f9c1b2` is the format example's placeholder, not a computed
value.

## Escaping hazard fixtures

The `*-escaping` dirs (`exec-escaping`, `http-escaping`, `sql-escaping`,
`fs-write-escaping`, `fs-rename-escaping`, `redis-escaping`) pin the
canonical-JSON string escaping that unary fingerprints depend on. Every
fingerprinted string field carries one token per hazard class, labelled so a
mismatch names the class that forked:

| Token | Character | Standard JSON escaping |
|-------|-----------|------------------------|
| `amp=` | `&` | literal (never `\u0026`) |
| `lt=` / `gt=` | `<` / `>` | literal (never `\u003c` / `\u003e`) |
| `slash=` | `/` | literal (never `\/`) |
| `eacute=` | `é` U+00E9 | raw UTF-8 (never `\u00e9`) |
| `ls=` / `ps=` | U+2028 / U+2029 | raw UTF-8 (never `\u2028` / `\u2029`) |
| `bs=` / `ff=` | U+0008 / U+000C | `\b` / `\f` |
| `us=` | U+001F | `\u001f` (lowercase hex) |
| `del=` | U+007F | literal byte 0x7f |

The policy is the streaming spec's (cassette-format-streaming.md, "standard
JSON string escaping only, never HTML-safe"): sorted keys, no whitespace,
only `"`, `\` and U+0000–U+001F escaped, everything else emitted as raw
UTF-8. The fingerprints were generated with the TypeScript port
(`JSON.stringify`) and cross-checked byte-for-byte against the Rust port
(`serde_json`); the two agree on every class above. Every entry is marked
`verify_fingerprint: true`, so a port that HTML-escapes, `\u`-escapes
non-ASCII, or escapes U+2028/U+2029 goes red here on exactly the affected
dirs.

Hazards live only where every port passes the string verbatim into
canonical JSON. Deliberately excluded, because they fork for reasons other
than escaping:

- exec `cwd` — a Go-only fingerprint extension (cassette-format-v1.md), so
  the exec fixture omits it.
- `<`, `>`, non-ASCII and control characters in an http URL — the
  TypeScript port's WHATWG `URL` percent-encodes them while the other ports
  keep them raw. Only `&` and `/` survive all five URL parsers unchanged,
  so those are the URL hazards; the full set rides in the body, where it
  reaches the fingerprint through `body_hash` as bytes.
- `\f`, U+001F, U+2028, U+2029 in sql query text — whitespace collapsing
  uses each runtime's `\s` class, which disagree on those. Query text
  carries `&<>/é`; the full set goes in `args`.

Non-fingerprinted fields (exec `env`, http headers) carry the same set so
loaders are exercised on them too, without affecting the key. YAML spelling:
control characters and U+2028/U+2029 use double-quoted escapes (`"\b"`,
`"\x1f"`, `"\u2028"`) so the files stay printable; `é` is raw UTF-8. All
five YAML loaders decode these to identical bytes.
