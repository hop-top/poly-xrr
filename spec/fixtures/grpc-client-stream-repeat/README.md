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
