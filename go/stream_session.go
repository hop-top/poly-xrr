package xrr

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"maps"
	"sync"
	"time"
)

// Session-level streamed record/replay plumbing. Adapters build their
// stream wrappers on top of these handles; see
// spec/cassette-format-streaming.md for the normative semantics.

// nextStreamN returns the 0-based count of prior streamed opens with the
// same identifying key in this session, then increments it. One Session
// object is one counter domain: the counter is created with the session and
// consumed identically in record and replay modes.
func (s *FileSession) nextStreamN(key string) int {
	s.streamMu.Lock()
	defer s.streamMu.Unlock()
	if s.streamN == nil {
		s.streamN = make(map[string]int)
	}
	n := s.streamN[key]
	s.streamN[key] = n + 1
	return n
}

// streamOpenFingerprint computes the open-time fingerprint, consuming the
// occurrence counter for counter-addressed opens. The counter is keyed by
// the adapter id plus the canonical identity (sans "n"), i.e. the adapter's
// identifying tuple. n is -1 for content-addressed opens.
func (s *FileSession) streamOpenFingerprint(open StreamOpen) (fp string, n int, err error) {
	n = -1
	if open.Counter {
		base, cErr := streamCanonical(open, -1)
		if cErr != nil {
			return "", -1, cErr
		}
		n = s.nextStreamN(open.AdapterID + "\x00" + string(base))
	}
	fp, err = StreamFingerprint(open, n)
	return fp, n, err
}

func (s *FileSession) checkStreamOpen(open StreamOpen, want Mode, verb string) error {
	if s.mode != want {
		return fmt.Errorf("xrr: %s requires %s mode (session is %q)", verb, want, s.mode)
	}
	if s.cassette == nil {
		return fmt.Errorf("xrr: %s requires a cassette", verb)
	}
	if open.AdapterID == "" {
		return fmt.Errorf("xrr: %s requires an adapter id", verb)
	}
	return nil
}

// ── record path ────────────────────────────────────────────────────────────

// OpenStreamRecord opens a streamed interaction for recording. The adapter
// observes the live stream and mirrors it into the returned recording:
// RecordSend/RecordRecv per message, RecordHalfClose when the client closes
// its send side, then Finish exactly once when the terminal is observed —
// only Finish persists the pair, so a stream that never reaches terminal
// produces no cassette.
func (s *FileSession) OpenStreamRecord(open StreamOpen) (*StreamRecording, error) {
	if err := s.checkStreamOpen(open, ModeRecord, "OpenStreamRecord"); err != nil {
		return nil, err
	}
	fp, n, err := s.streamOpenFingerprint(open)
	if err != nil {
		return nil, err
	}
	payload := make(map[string]any, len(open.Payload)+1)
	maps.Copy(payload, open.Payload)
	if n >= 0 {
		// Informational occurrence ordinal: recoverable from disk, never
		// read back to drive matching.
		payload["n"] = n
	}
	return &StreamRecording{
		cassette:    s.cassette,
		adapterID:   open.AdapterID,
		fingerprint: fp,
		typ:         open.Type,
		reqPayload:  payload,
		opened:      time.Now(),
		scrub:       s.streamScrub,
	}, nil
}

// StreamRecording accumulates the event log of one live stream and writes
// the cassette pair at terminal. Events are stamped with at_ms (monotonic
// milliseconds since open) and sequenced by one per-interaction counter in
// arrival order. Safe for concurrent use — send and recv sides typically
// run on different goroutines.
type StreamRecording struct {
	cassette    *FileCassette
	adapterID   string
	fingerprint string
	typ         StreamType
	reqPayload  map[string]any
	opened      time.Time
	scrub       StreamScrubFunc

	mu        sync.Mutex
	seq       int
	sends     []StreamFrame
	recvs     []StreamFrame
	halfClose *StreamEvent
	finished  bool
}

// Fingerprint returns the open-time fingerprint of this interaction.
func (r *StreamRecording) Fingerprint() string { return r.fingerprint }

func (r *StreamRecording) elapsedMs() int64 {
	ms := time.Since(r.opened).Milliseconds()
	if ms < 0 {
		return 0
	}
	return ms
}

// scrubFrame applies scrub to data when a hook is installed. Callers own
// the "scrub exactly once per frame" discipline: record scrubs before
// persisting, replay scrubs live send bytes before comparison and never
// re-scrubs recorded frames.
func scrubFrame(scrub StreamScrubFunc, dir StreamDirection, info StreamScrubInfo, data []byte) []byte {
	if scrub == nil {
		return data
	}
	return scrub(dir, info, data)
}

func (r *StreamRecording) scrubInfo() StreamScrubInfo {
	return StreamScrubInfo{AdapterID: r.adapterID, Type: r.typ}
}

// RecordSend logs one client→server message, scrubbed by the session's
// frame scrub hook before it is retained. Dropped after Finish.
func (r *StreamRecording) RecordSend(message []byte) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.finished {
		return
	}
	msg := bytes.Clone(scrubFrame(r.scrub, StreamSend, r.scrubInfo(), message))
	at := r.elapsedMs()
	r.sends = append(r.sends, StreamFrame{Seq: r.seq, Message: msg, AtMs: &at})
	r.seq++
}

// RecordRecv logs one server→client message, scrubbed by the session's
// frame scrub hook before it is retained. Dropped after Finish.
func (r *StreamRecording) RecordRecv(message []byte) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.finished {
		return
	}
	msg := bytes.Clone(scrubFrame(r.scrub, StreamRecv, r.scrubInfo(), message))
	at := r.elapsedMs()
	r.recvs = append(r.recvs, StreamFrame{Seq: r.seq, Message: msg, AtMs: &at})
	r.seq++
}

// RecordHalfClose logs the client closing its send side. It occurs at most
// once; repeats and post-terminal calls are dropped, matching their
// real-world no-op.
func (r *StreamRecording) RecordHalfClose() {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.finished || r.halfClose != nil {
		return
	}
	at := r.elapsedMs()
	r.halfClose = &StreamEvent{Seq: r.seq, AtMs: &at}
	r.seq++
}

// Finish records the terminal event and persists the pair. terminalErr is
// nil for an OK terminal; non-nil errors are persisted as the resp envelope
// error field so replay re-emits them. No events are recorded after Finish,
// and calling it twice is an error.
func (r *StreamRecording) Finish(respPayload map[string]any, terminalErr error) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.finished {
		return fmt.Errorf("xrr: stream already finished")
	}
	r.finished = true

	at := r.elapsedMs()
	end := StreamEvent{Seq: r.seq, AtMs: &at}
	r.seq++

	errStr := ""
	if terminalErr != nil {
		errStr = terminalErr.Error()
	}
	payload := respPayload
	if payload == nil {
		payload = map[string]any{}
	}
	pair := &StreamPair{
		Req:         ReqStream{Type: r.typ, Frames: r.sends, HalfClose: r.halfClose},
		Resp:        RespStream{Frames: r.recvs, End: end},
		ReqPayload:  r.reqPayload,
		RespPayload: payload,
		RecordedErr: errStr,
	}
	if err := r.cassette.SaveStream(r.adapterID, r.fingerprint, pair); err != nil {
		return fmt.Errorf("xrr: save stream: %w", err)
	}
	return nil
}

// ── replay path ────────────────────────────────────────────────────────────

// OpenStreamReplay locates the cassette pair for a streamed open and
// returns a replay handle. Returns ErrCassetteMiss when no pair exists and
// ErrShapeMismatch when the pair is unary. The occurrence counter is
// consumed exactly as in record mode, hit or miss.
func (s *FileSession) OpenStreamReplay(open StreamOpen) (*StreamReplay, error) {
	if err := s.checkStreamOpen(open, ModeReplay, "OpenStreamReplay"); err != nil {
		return nil, err
	}
	fp, _, err := s.streamOpenFingerprint(open)
	if err != nil {
		return nil, err
	}
	pair, err := s.cassette.LoadStream(open.AdapterID, fp)
	if err != nil {
		return nil, err
	}
	if pair.Req.Type != open.Type {
		return nil, fmt.Errorf("xrr: recorded stream type %q, requested %q: %w",
			pair.Req.Type, open.Type, ErrShapeMismatch)
	}
	return &StreamReplay{
		fingerprint: fp,
		pair:        pair,
		scrub:       s.streamScrub,
		info:        StreamScrubInfo{AdapterID: open.AdapterID, Type: open.Type},
	}, nil
}

// StreamReplay serves one recorded streamed interaction. Send-side events
// are validated against the recording (order and bytes); recv-side frames
// are delivered in seq order, never gated on send progress. Timing is
// ignored: frames are delivered as fast as the client consumes them (at_ms
// stays available on the loaded pair for a future opt-in replay-timing
// mode). Safe for concurrent use.
type StreamReplay struct {
	fingerprint string
	pair        *StreamPair
	scrub       StreamScrubFunc
	info        StreamScrubInfo

	mu       sync.Mutex
	sendIdx  int
	recvIdx  int
	mismatch *StreamMismatchError
}

// Fingerprint returns the open-time fingerprint of this interaction.
func (r *StreamReplay) Fingerprint() string { return r.fingerprint }

// Type returns the recorded stream type.
func (r *StreamReplay) Type() StreamType { return r.pair.Req.Type }

// ReqPayload returns the recorded open-request payload.
func (r *StreamReplay) ReqPayload() map[string]any { return r.pair.ReqPayload }

// RespPayload returns the recorded terminal-response payload (for gRPC:
// the status code). Available from open — adapters typically read it only
// at terminal delivery.
func (r *StreamReplay) RespPayload() map[string]any { return r.pair.RespPayload }

// terminalErr is the terminal result: the recorded error when the resp
// envelope error is non-empty, io.EOF (the end-of-stream signal, which
// adapters map to their own) otherwise.
func (r *StreamReplay) terminalErr() error {
	if r.pair.RecordedErr != "" {
		return errors.New(r.pair.RecordedErr)
	}
	return io.EOF
}

func (r *StreamReplay) fail(m *StreamMismatchError) *StreamMismatchError {
	r.mismatch = m
	return m
}

// Send validates the i-th client message against recorded send frame i.
//   - i < S, equal bytes: accepted (the message is discarded).
//   - i < S, divergent bytes: stream mismatch — terminal for the handle.
//   - i ≥ S: the recording was already past its last observed send. With an
//     OK terminal Send returns io.EOF (the post-completion stream-done
//     signal) and does NOT poison the recv side; with an error terminal it
//     returns the recorded error. Bytes at i ≥ S are never compared.
//
// The live bytes are scrubbed by the session's frame scrub hook before the
// comparison — recorded frames were scrubbed at record time, so symmetric
// scrubbing is what makes a scrubbed cassette match its live traffic.
func (r *StreamReplay) Send(message []byte) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.mismatch != nil {
		return r.mismatch
	}
	i := r.sendIdx
	if i >= len(r.pair.Req.Frames) {
		return r.terminalErr()
	}
	message = scrubFrame(r.scrub, StreamSend, r.info, message)
	recorded := r.pair.Req.Frames[i].Message
	if !bytes.Equal(message, recorded) {
		want := sha256.Sum256(recorded)
		got := sha256.Sum256(message)
		return r.fail(&StreamMismatchError{
			Op:      "send",
			Ordinal: i,
			Detail: fmt.Sprintf("expected sha256 %s, got sha256 %s",
				hex.EncodeToString(want[:]), hex.EncodeToString(got[:])),
		})
	}
	r.sendIdx++
	return nil
}

// HalfClose validates the client closing its send side: always accepted
// after all recorded sends were observed (whether or not the recording has
// half_close), a stream mismatch after fewer.
func (r *StreamReplay) HalfClose() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.mismatch != nil {
		return r.mismatch
	}
	if s := len(r.pair.Req.Frames); r.sendIdx < s {
		return r.fail(&StreamMismatchError{
			Op:      "half_close",
			Ordinal: r.sendIdx,
			Detail:  fmt.Sprintf("half-close after %d sends, recording has %d", r.sendIdx, s),
		})
	}
	return nil
}

// Recv delivers the j-th recorded recv frame's bytes, verbatim: frames were
// scrubbed at record time and are never re-scrubbed here. At j = R it
// returns the terminal — the recorded error or io.EOF — and repeats it for
// every later read. Recv never blocks on send-side progress.
func (r *StreamReplay) Recv() ([]byte, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.mismatch != nil {
		return nil, r.mismatch
	}
	if r.recvIdx >= len(r.pair.Resp.Frames) {
		return nil, r.terminalErr()
	}
	msg := bytes.Clone(r.pair.Resp.Frames[r.recvIdx].Message)
	r.recvIdx++
	return msg, nil
}
