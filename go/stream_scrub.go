package xrr

// Frame-level secret scrubbing for streamed cassettes.
//
// Stream frames are opaque decoded bytes, base64-encoded on disk — so the
// record-time, field-name-based cassette redaction that covers structured
// payloads cannot see into them, and text-level scrubbing applied to the
// cassette after the fact is defeated by the encoding. The scrub hook
// closes that gap at the only workable seam: the DECODED bytes, before they
// are fingerprinted, persisted, or compared.
//
// Symmetry is the correctness invariant. Replay validates live send bytes
// against recorded frames byte-for-byte, and server-stream fingerprints are
// content-addressed by the open message, so a scrub applied only at record
// time would make every scrubbed cassette unreplayable. The hook therefore
// applies identically in both modes:
//
//   - record: every frame (both directions) is scrubbed before it is
//     persisted, and content-derived identity inputs (gRPC server-stream
//     msg_hash) are computed over scrubbed bytes — the fingerprint
//     addresses the scrubbed content.
//   - replay: live send bytes are scrubbed before comparison, and the same
//     content-derived identity is computed over scrubbed live bytes, so a
//     scrubbed recording and a scrubbed replay of the same traffic meet at
//     the same fingerprint and the same frame bytes. Recorded frames were
//     already scrubbed at record time and are delivered verbatim — never
//     re-scrubbed.
//
// The same hook MUST be installed on the session that recorded a cassette
// and on every session that replays it; replaying a scrubbed cassette
// without the hook (or with a different one) fails loudly as a stream
// mismatch.

// StreamDirection identifies the half of a streamed interaction a frame
// travels on.
type StreamDirection string

const (
	// StreamSend marks client→server frames (req side).
	StreamSend StreamDirection = "send"
	// StreamRecv marks server→client frames (resp side).
	StreamRecv StreamDirection = "recv"
)

// StreamScrubInfo is the identity context handed to a StreamScrubFunc with
// every frame. It carries only inputs known at every scrub site — including
// before content-derived identity fields (such as the gRPC server-stream
// msg_hash) exist, since those are derived FROM the scrubbed bytes — so one
// hook sees one consistent context for the same bytes everywhere.
type StreamScrubInfo struct {
	AdapterID string
	Type      StreamType
}

// StreamScrubFunc rewrites the decoded bytes of one stream frame. It may
// return data unchanged, or a new slice; the core copies the result before
// storing it, and never re-scrubs bytes it already scrubbed.
//
// Requirements on the hook:
//
//   - Deterministic and pure: the same input bytes MUST always produce the
//     same output, across calls, runs, and processes. Nondeterministic
//     scrubbing (counters, timestamps, randomized placeholders) diverges
//     content-addressed fingerprints and send validation, breaking replay.
//   - Structure-preserving for encoded payloads: replayed recv frames are
//     decoded by the client (gRPC frames are protobuf wire bytes), so the
//     scrub must keep the encoding valid — replace secrets with equal-length
//     placeholders, or decode, edit, and deterministically re-encode.
//
// Half-close and terminal events carry no payload and are never scrubbed.
// Adapter payload maps (the open request / terminal response objects) are
// not covered by this hook either: they are structured, named-field YAML —
// the domain of record-time cassette redaction — while this hook exists for
// the frame byte layer that field-name matching cannot reach.
type StreamScrubFunc func(dir StreamDirection, info StreamScrubInfo, data []byte) []byte

// NewSessionWithStreamScrub creates a FileSession whose streamed
// interactions pass every frame through scrub. A nil scrub is identical to
// NewSession: frames record and replay verbatim.
//
// Install the SAME hook when recording and when replaying: scrubbing is
// symmetric by design (see StreamScrubFunc), and a session replaying a
// scrubbed cassette without the hook fails with a stream mismatch.
func NewSessionWithStreamScrub(mode Mode, cassette *FileCassette, scrub StreamScrubFunc) *FileSession {
	return &FileSession{mode: mode, cassette: cassette, streamScrub: scrub}
}

// ScrubStreamFrame applies the session's frame scrub hook to data,
// returning data unchanged when no hook is installed.
//
// Adapters whose open identity derives from message bytes (gRPC
// server-stream msg_hash) MUST compute the derived identity over this
// function's output, in record and replay mode alike, so both modes address
// the cassette by the scrubbed content. Frames handed to the core
// (RecordSend/RecordRecv, replay Send) are scrubbed by the core itself —
// adapters pass them raw and never double-scrub.
func (s *FileSession) ScrubStreamFrame(dir StreamDirection, info StreamScrubInfo, data []byte) []byte {
	if s.streamScrub == nil {
		return data
	}
	return s.streamScrub(dir, info, data)
}
