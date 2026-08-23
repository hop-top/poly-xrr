package xrr

import (
	"context"
	"errors"
	"fmt"
	"sync"
)

// FileSession dispatches record/replay/passthrough via a Cassette.
type FileSession struct {
	mode     Mode
	cassette *FileCassette

	// Streamed-open occurrence counters: one session object is one
	// counter domain, keyed by adapter id + canonical identity. See
	// stream_session.go.
	streamMu sync.Mutex
	streamN  map[string]int
}

// NewSession creates a FileSession with the given mode and cassette.
func NewSession(mode Mode, cassette *FileCassette) *FileSession {
	return &FileSession{mode: mode, cassette: cassette}
}

// Mode returns the session mode. Streaming adapters dispatch on it to pick
// the record, replay, or passthrough path at stream open.
func (s *FileSession) Mode() Mode { return s.mode }

// RawResponse wraps a replayed payload when the adapter type is unknown.
type RawResponse struct {
	adapterID_ string
	Payload    map[string]any
}

func (r *RawResponse) AdapterID() string { return r.adapterID_ }

// Record executes one interaction according to the session mode.
//
//   - record:      calls do(), saves req+resp+err to cassette, returns
//     (resp, err) verbatim. A non-nil err from do() is
//     persisted as the resp envelope's "error" field and is
//     also returned to the caller, so error semantics are
//     preserved across record sessions. Save failures take
//     precedence over the recorded error.
//   - replay:      loads from cassette, returns (RawResponse, replayedErr)
//     where replayedErr is errors.New(envelope.error) when
//     the recording captured a failure, else nil. do() is
//     NOT called.
//   - passthrough: calls do(), never touches cassette.
func (s *FileSession) Record(
	_ context.Context,
	adapter Adapter,
	req Request,
	do func() (Response, error),
) (Response, error) {
	switch s.mode {
	case ModeRecord:
		return s.record(adapter, req, do)
	case ModeReplay:
		return s.replay(adapter, req)
	case ModePassthrough:
		return do()
	default:
		return nil, fmt.Errorf("xrr: unknown mode %q", s.mode)
	}
}

// Close is a no-op for FileSession.
func (s *FileSession) Close() error { return nil }

func (s *FileSession) record(adapter Adapter, req Request, do func() (Response, error)) (Response, error) {
	resp, doErr := do()

	// Even when do() failed, we still want to persist the interaction so
	// replay can re-emit the same error shape. Skip persistence only if
	// the adapter cannot fingerprint the request — in that case there is
	// no place to file the cassette.
	fp, fpErr := adapter.Fingerprint(req)
	if fpErr != nil {
		return nil, fmt.Errorf("xrr: fingerprint: %w", fpErr)
	}
	if saveErr := s.cassette.Save(adapter.ID(), fp, req, resp, doErr); saveErr != nil {
		return nil, fmt.Errorf("xrr: save: %w", saveErr)
	}
	return resp, doErr
}

func (s *FileSession) replay(adapter Adapter, req Request) (Response, error) {
	fp, err := adapter.Fingerprint(req)
	if err != nil {
		return nil, fmt.Errorf("xrr: fingerprint: %w", err)
	}
	var reqPayload, respPayload map[string]any
	recordedErr, err := s.cassette.Load(adapter.ID(), fp, &reqPayload, &respPayload)
	if err != nil {
		return nil, err // preserves ErrCassetteMiss
	}
	raw := &RawResponse{adapterID_: adapter.ID(), Payload: respPayload}
	if recordedErr != "" {
		return raw, errors.New(recordedErr)
	}
	return raw, nil
}
