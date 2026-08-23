# grpc-client-stream-repeat

One session, two sequential opens of the same tuple
`(files.FileService, Upload, client)`:

- open 1 → `n = 0` → fingerprint `2bebfd6f`
- open 2 → `n = 1` → fingerprint `b27b5fe1`

Format layer: both pairs load independently.

Adapter level: the spec's `n = 1` conformance obligation ("a second open of
the same tuple within one session") requires a scripted two-open fixture —
sequenced opens driven by a runner, not static files alone
(cassette-format-streaming.md, Conformance Obligations). The manifest schema
has no affordance to express open sequencing; this directory supplies the
static cassette material for such a runner, which must open the tuple twice
in order against this directory as its session dir.

This is the one dir where open order is load-bearing: both entries share the
counter domain `(files.FileService, Upload, client)`, so opening them in the
wrong order assigns each the other's `n` and both fingerprints miss.
`manifest.yaml` lists them ascending by `n`, but that order is descriptive,
not normative — `interactions` is an unordered set. Runners MUST establish
the order themselves, sorting by the req payload's `n` within the domain,
per the ordering rule in cassette-format-streaming.md (Manifest Extension).
Rewriting this manifest to descending `n` is a legal edit and MUST NOT
change any port's result. Payload `n` remains prohibited as a *matching*
input; sequencing a fixture replay is its only sanctioned use.

Message frames carry plain ASCII, not marshalled protobuf — see the same
spec's Message Encoding section. The frame layer never decodes payloads, so
this dir replays through a byte-transparent codec with no protobuf runtime.
