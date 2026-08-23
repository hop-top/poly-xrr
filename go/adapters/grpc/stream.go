package grpc

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"

	xrr "hop.top/xrr"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/protoadapt"
)

// Streaming gRPC adapter: records and replays server-, client-, and
// bidi-streamed RPCs through a grpc.StreamClientInterceptor, on top of the
// core stream session API. See spec/cassette-format-streaming.md (gRPC
// Adapter Mapping) for the normative semantics.

const adapterID = "grpc"

// StreamClientInterceptor returns an interceptor that dispatches streamed
// RPCs on the session mode: record tees every message of the live stream
// into a cassette pair, replay serves the recorded conversation with no
// network, passthrough is transparent.
//
// The stream type is derived from the grpc.StreamDesc flags. Unary-shaped
// descs (neither flag) are rejected: unary RPCs keep the v1 unary format
// and never migrate to the stream path.
func StreamClientInterceptor(session *xrr.FileSession) grpc.StreamClientInterceptor {
	return func(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn,
		method string, streamer grpc.Streamer, opts ...grpc.CallOption,
	) (grpc.ClientStream, error) {
		switch session.Mode() {
		case xrr.ModePassthrough:
			return streamer(ctx, desc, cc, method, opts...)
		case xrr.ModeRecord:
			return newRecordStream(ctx, session, desc, cc, method, streamer, opts...)
		case xrr.ModeReplay:
			return newReplayStream(ctx, session, desc, method)
		default:
			return nil, fmt.Errorf("grpc: unknown session mode %q", session.Mode())
		}
	}
}

// streamTypeOf maps grpc.StreamDesc direction flags to the spec's stream
// types.
func streamTypeOf(desc *grpc.StreamDesc) (xrr.StreamType, error) {
	switch {
	case desc.ClientStreams && desc.ServerStreams:
		return xrr.StreamBidi, nil
	case desc.ServerStreams:
		return xrr.StreamServer, nil
	case desc.ClientStreams:
		return xrr.StreamClient, nil
	default:
		return "", fmt.Errorf("grpc: %q is unary-shaped (no stream direction); unary RPCs use the unary adapter path", desc.StreamName)
	}
}

// streamOpen builds the core open value for a gRPC streamed RPC per the
// spec's gRPC mapping: canonical inputs service + method, req payload
// {service, method}. Server streams are content-addressed via
// msg_hash = sha256(message_bytes)[:8] (the wire bytes of the single
// request message); client/bidi opens are counter-addressed.
func streamOpen(typ xrr.StreamType, service, method string, msg []byte) xrr.StreamOpen {
	open := xrr.StreamOpen{
		AdapterID: adapterID,
		Type:      typ,
		Identity:  map[string]any{"service": service, "method": method},
		Payload:   map[string]any{"service": service, "method": method},
	}
	if typ == xrr.StreamServer {
		sum := sha256.Sum256(msg)
		open.Identity["msg_hash"] = hex.EncodeToString(sum[:4])
	} else {
		open.Counter = true
	}
	return open
}

// splitFullMethod splits "/pkg.Service/Method" into its service and method
// identifiers.
func splitFullMethod(full string) (service, method string, err error) {
	s := strings.TrimPrefix(full, "/")
	i := strings.LastIndex(s, "/")
	if i <= 0 || i == len(s)-1 {
		return "", "", fmt.Errorf("grpc: malformed full method %q (want /service/method)", full)
	}
	return s[:i], s[i+1:], nil
}

// marshalMessage produces the protobuf wire bytes of a message crossing the
// adapter boundary. Frames always store wire bytes (spec: message_b64), so
// both the typed proto case and the raw-bytes case (custom byte codecs)
// must be handled.
//
// Marshaling MUST be deterministic: protobuf map entries have no guaranteed
// wire order, and the format's byte-level contracts (content-addressed
// server-stream fingerprints, client/bidi send validation) presume the same
// message always marshals to the same bytes. Plain proto.Marshal follows
// Go's randomized map iteration and breaks both. Raw-bytes paths carry
// caller-provided bytes verbatim and are unaffected.
func marshalMessage(m any) ([]byte, error) {
	det := proto.MarshalOptions{Deterministic: true}
	switch v := m.(type) {
	case proto.Message:
		return det.Marshal(v)
	case protoadapt.MessageV1:
		return det.Marshal(protoadapt.MessageV2Of(v))
	case *[]byte:
		if v == nil {
			return nil, errors.New("grpc: nil *[]byte message")
		}
		return bytes.Clone(*v), nil
	case []byte:
		return bytes.Clone(v), nil
	}
	return nil, fmt.Errorf("grpc: cannot marshal message type %T to wire bytes", m)
}

// unmarshalMessage decodes recorded wire bytes into the caller-supplied
// message. Replay never has a typed value on hand — only the recorded raw
// bytes — so the caller's proto message (or byte buffer) is populated here,
// exactly as a live stream's codec would.
func unmarshalMessage(data []byte, m any) error {
	switch v := m.(type) {
	case proto.Message:
		return proto.Unmarshal(data, v)
	case protoadapt.MessageV1:
		return proto.Unmarshal(data, protoadapt.MessageV2Of(v))
	case *[]byte:
		if v == nil {
			return errors.New("grpc: nil *[]byte message")
		}
		*v = bytes.Clone(data)
		return nil
	}
	return fmt.Errorf("grpc: cannot unmarshal wire bytes into message type %T", m)
}

// ── record ─────────────────────────────────────────────────────────────────

// recordStream wraps the live grpc.ClientStream and tees every observed
// event into a StreamRecording. Header/Trailer/Context pass through to the
// live stream via embedding.
type recordStream struct {
	grpc.ClientStream
	session       *xrr.FileSession
	typ           xrr.StreamType
	service       string
	method        string
	serverStreams bool

	mu       sync.Mutex
	rec      *xrr.StreamRecording // nil until opened (deferred for server streams)
	finished bool
}

func newRecordStream(ctx context.Context, session *xrr.FileSession, desc *grpc.StreamDesc,
	cc *grpc.ClientConn, method string, streamer grpc.Streamer, opts ...grpc.CallOption,
) (grpc.ClientStream, error) {
	typ, err := streamTypeOf(desc)
	if err != nil {
		return nil, err
	}
	service, mth, err := splitFullMethod(method)
	if err != nil {
		return nil, err
	}
	rs := &recordStream{
		session:       session,
		typ:           typ,
		service:       service,
		method:        mth,
		serverStreams: desc.ServerStreams,
	}
	// Client/bidi opens are fingerprinted by the occurrence counter, so the
	// recording opens here, mirroring replay's counter consumption. Server
	// streams are content-addressed by the open message, which grpc-go only
	// surfaces at the first SendMsg — their open is deferred there.
	if typ != xrr.StreamServer {
		rec, err := session.OpenStreamRecord(streamOpen(typ, service, mth, nil))
		if err != nil {
			return nil, err
		}
		rs.rec = rec
	}
	live, err := streamer(ctx, desc, cc, method, opts...)
	if err != nil {
		// No terminal will ever be observed ⇒ no cassette (by design).
		return nil, err
	}
	rs.ClientStream = live
	return rs, nil
}

// ensureOpen opens the deferred server-stream recording with the request
// message bytes. No-op once opened.
func (s *recordStream) ensureOpen(openMsg []byte) (*xrr.StreamRecording, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.rec != nil {
		return s.rec, nil
	}
	rec, err := s.session.OpenStreamRecord(streamOpen(s.typ, s.service, s.method, openMsg))
	if err != nil {
		return nil, err
	}
	s.rec = rec
	return rec, nil
}

func (s *recordStream) recording() *xrr.StreamRecording {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.rec
}

// finish persists the pair exactly once. A save failure is returned loudly:
// a record run whose cassette cannot be written must not pass silently.
func (s *recordStream) finish(code int, terminalErr error) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.finished || s.rec == nil {
		return nil
	}
	s.finished = true
	if terminalErr != nil && code == int(codes.OK) {
		// Spec invariant: error non-empty iff status_code != 0.
		code = int(codes.Unknown)
	}
	return s.rec.Finish(map[string]any{"status_code": code}, terminalErr)
}

func (s *recordStream) SendMsg(m any) error {
	b, err := marshalMessage(m)
	if err != nil {
		return err
	}
	rec, err := s.ensureOpen(b)
	if err != nil {
		return err
	}
	if err := s.ClientStream.SendMsg(m); err != nil {
		// The frame never reached the wire; the terminal shows up on the
		// caller's next RecvMsg, exactly as with a live-only stream.
		return err
	}
	rec.RecordSend(b)
	return nil
}

func (s *recordStream) RecvMsg(m any) error {
	err := s.ClientStream.RecvMsg(m)
	switch {
	case err == nil:
		rec := s.recording()
		if rec == nil {
			// Server stream whose request was never sent: nothing is
			// fingerprintable, nothing is recorded.
			return nil
		}
		b, mErr := marshalMessage(m)
		if mErr != nil {
			return mErr
		}
		rec.RecordRecv(b)
		if !s.serverStreams {
			// Non-server-streaming RPC: the single response message is the
			// OK terminal — grpc-go completes the RPC on this recv and a
			// generated client never reads again.
			if fErr := s.finish(int(codes.OK), nil); fErr != nil {
				return fErr
			}
		}
		return nil
	case errors.Is(err, io.EOF):
		if fErr := s.finish(int(codes.OK), nil); fErr != nil {
			return fErr
		}
		return err
	default:
		if fErr := s.finish(int(status.Code(err)), err); fErr != nil {
			return fErr
		}
		return err
	}
}

func (s *recordStream) CloseSend() error {
	err := s.ClientStream.CloseSend()
	if err == nil {
		if rec := s.recording(); rec != nil {
			rec.RecordHalfClose()
		}
	}
	return err
}

// ── replay ─────────────────────────────────────────────────────────────────

// replayStream implements grpc.ClientStream on top of a StreamReplay: no
// network, no live stream. Messages cross the boundary as recorded wire
// bytes — SendMsg marshals the outgoing message for byte comparison,
// RecvMsg unmarshals recorded bytes into the caller's message, and error
// terminals are reconstructed as *status.Status errors so generated-client
// code behaves identically to a live stream.
type replayStream struct {
	ctx     context.Context
	session *xrr.FileSession
	typ     xrr.StreamType
	service string
	method  string

	mu      sync.Mutex
	rp      *xrr.StreamReplay
	openErr error // sticky deferred-open failure (server streams)
}

func newReplayStream(ctx context.Context, session *xrr.FileSession,
	desc *grpc.StreamDesc, method string,
) (grpc.ClientStream, error) {
	typ, err := streamTypeOf(desc)
	if err != nil {
		return nil, err
	}
	service, mth, err := splitFullMethod(method)
	if err != nil {
		return nil, err
	}
	rs := &replayStream{
		ctx:     ctx,
		session: session,
		typ:     typ,
		service: service,
		method:  mth,
	}
	// Client/bidi cassettes are located by the occurrence counter, known
	// now: misses and shape mismatches surface from the interceptor call
	// itself. Server streams are located by the request message, so their
	// open is deferred to the first SendMsg.
	if typ != xrr.StreamServer {
		rp, err := session.OpenStreamReplay(streamOpen(typ, service, mth, nil))
		if err != nil {
			return nil, err
		}
		rs.rp = rp
	}
	return rs, nil
}

// ensureOpen performs the deferred server-stream open with the request
// message bytes. Open failures (cassette miss, shape mismatch) are sticky.
func (s *replayStream) ensureOpen(openMsg []byte) (*xrr.StreamReplay, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.openErr != nil {
		return nil, s.openErr
	}
	if s.rp != nil {
		return s.rp, nil
	}
	rp, err := s.session.OpenStreamReplay(streamOpen(s.typ, s.service, s.method, openMsg))
	if err != nil {
		s.openErr = err
		return nil, err
	}
	s.rp = rp
	return rp, nil
}

// replay returns the open replay handle, failing for operations that reach
// a deferred server-stream open before any request was sent.
func (s *replayStream) replay() (*xrr.StreamReplay, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.openErr != nil {
		return nil, s.openErr
	}
	if s.rp == nil {
		return nil, status.Error(codes.FailedPrecondition,
			"xrr: server-stream operation before the request message (replay locates the cassette by the open message)")
	}
	return s.rp, nil
}

// mapErr translates core replay results into what a live stream surfaces:
// io.EOF stays io.EOF (end-of-stream and post-completion send signal),
// stream mismatches pass through (errors.Is(err, xrr.ErrStreamMismatch)),
// and a recorded error terminal is rebuilt as a *status.Status error from
// the recorded status_code.
func (s *replayStream) mapErr(rp *xrr.StreamReplay, err error) error {
	switch {
	case err == nil:
		return nil
	case errors.Is(err, io.EOF):
		return io.EOF
	case errors.Is(err, xrr.ErrStreamMismatch):
		return err
	default:
		return recordedStatusErr(rp, err.Error())
	}
}

// recordedStatusErr reconstructs the terminal gRPC status from the resp
// payload status_code, treating the recorded error string as the status
// text (spec). When the string is the standard client rendering
// ("rpc error: code = X desc = ..."), the description is extracted so the
// reconstructed error renders byte-identically to the live one instead of
// nesting.
func recordedStatusErr(rp *xrr.StreamReplay, msg string) error {
	code := codes.Code(statusCodeFrom(rp.RespPayload()))
	if code == codes.OK {
		// An error terminal can never be OK (status.Error(OK) is nil);
		// guard hand-authored cassettes violating the spec invariant.
		code = codes.Unknown
	}
	if desc, ok := strings.CutPrefix(msg, fmt.Sprintf("rpc error: code = %s desc = ", code.String())); ok {
		msg = desc
	}
	return status.Error(code, msg)
}

// statusCodeFrom extracts the recorded status_code, tolerating the integer
// types YAML decoding can produce. Absent or malformed ⇒ Unknown (the spec
// requires the field; replay must still fail loudly, not succeed).
func statusCodeFrom(payload map[string]any) int {
	switch v := payload["status_code"].(type) {
	case int:
		return v
	case int64:
		return int(v)
	case uint64:
		return int(v)
	case float64:
		return int(v)
	}
	return int(codes.Unknown)
}

func (s *replayStream) SendMsg(m any) error {
	b, err := marshalMessage(m)
	if err != nil {
		return err
	}
	rp, err := s.ensureOpen(b)
	if err != nil {
		return err
	}
	return s.mapErr(rp, rp.Send(b))
}

func (s *replayStream) RecvMsg(m any) error {
	rp, err := s.replay()
	if err != nil {
		return err
	}
	b, err := rp.Recv()
	if err != nil {
		return s.mapErr(rp, err)
	}
	return unmarshalMessage(b, m)
}

func (s *replayStream) CloseSend() error {
	rp, err := s.replay()
	if err != nil {
		return err
	}
	return s.mapErr(rp, rp.HalfClose())
}

// Header returns empty metadata: the cassette format records none (spec,
// matching the unary adapter).
func (s *replayStream) Header() (metadata.MD, error) { return metadata.MD{}, nil }

// Trailer returns empty metadata: the cassette format records none.
func (s *replayStream) Trailer() metadata.MD { return metadata.MD{} }

func (s *replayStream) Context() context.Context { return s.ctx }
