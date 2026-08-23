package grpctransport

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"

	xrr "hop.top/xrr"

	"golang.org/x/net/http2"
	"golang.org/x/net/http2/hpack"
	"google.golang.org/grpc/codes"
)

// Replay path: a fake net.Conn that speaks HTTP/2 back to the gRPC client,
// answering from cassettes and never touching the network.
//
// The client stack is entirely real — grpc-go's transport, flow control,
// codec, and generated stubs all run unmodified. What changes is only what
// is on the other end of the socket: instead of a TCP connection to a
// server, the dialer returns an in-memory pipe whose far side is driven by
// this file. That is what makes the replay honest: nothing about the client
// knows it is replaying, so anything the client would do against a real
// server it also does here.
//
// The synthesized server implements the minimum HTTP/2 needed to satisfy
// grpc-go: preface + SETTINGS exchange, per-stream response HEADERS, DATA
// frames carrying length-prefixed messages, and trailers with grpc-status.
// Flow control is handled by advertising a large window and never
// withholding — replay has no backpressure to model.

// ReplayDialer returns a grpc.WithContextDialer function that serves
// recorded streams with no network access at all.
//
// The returned dialer never opens a socket. Pass it to grpc.NewClient
// exactly where the record dialer went; the client behaves as if a server
// were present.
func ReplayDialer(session *xrr.FileSession, cassetteDir string) DialFunc {
	return func(ctx context.Context, addr string) (net.Conn, error) {
		client, server := net.Pipe()
		r := &replayConn{
			session: session,
			dir:     cassetteDir,
			conn:    server,
			framer:  http2.NewFramer(server, server),
			enc:     newHeaderEncoder(),
			streams: make(map[uint32]*replayStream),
			probeN:  make(map[string]int),
		}
		r.framer.ReadMetaHeaders = hpack.NewDecoder(hpackTableSize, nil)
		r.framer.SetMaxReadFrameSize(maxFrameSize)
		go r.serve()
		return client, nil
	}
}

// headerEncoder wraps an HPACK encoder plus its output buffer. HPACK is
// stateful across a connection, so one encoder serves every header block we
// emit and must be used under the write lock.
type headerEncoder struct {
	buf *sliceBuffer
	enc *hpack.Encoder
}

type sliceBuffer struct{ b []byte }

func (s *sliceBuffer) Write(p []byte) (int, error) { s.b = append(s.b, p...); return len(p), nil }
func (s *sliceBuffer) take() []byte                { out := s.b; s.b = nil; return out }

func newHeaderEncoder() *headerEncoder {
	buf := &sliceBuffer{}
	return &headerEncoder{buf: buf, enc: hpack.NewEncoder(buf)}
}

// encode HPACK-encodes one header block.
func (h *headerEncoder) encode(fields ...hpack.HeaderField) []byte {
	for _, f := range fields {
		_ = h.enc.WriteField(f)
	}
	return h.buf.take()
}

// replayConn drives the server side of the in-memory pipe.
type replayConn struct {
	session *xrr.FileSession
	dir     string
	conn    net.Conn
	framer  *http2.Framer
	enc     *headerEncoder

	// wmu serializes frame writes and guards the shared HPACK encoder.
	wmu sync.Mutex

	mu      sync.Mutex
	streams map[uint32]*replayStream
	// probeN shadows the session's occurrence counter so a probe can ask
	// "does the cassette for the NEXT open of this tuple exist?" without
	// consuming the real counter. Keyed identically to the core's counter
	// (adapter id + canonical identity), it is incremented only when an
	// open really happens, keeping the two in lockstep.
	probeN map[string]int
}

// cassetteExists reports whether a streamed pair is on disk for the given
// type, without opening it and without consuming the session's occurrence
// counter. Used only to break the client-vs-bidi ambiguity; see
// inferReplayType.
func (r *replayConn) cassetteExists(typ xrr.StreamType, service, method string, openMsg []byte) bool {
	if r.dir == "" {
		return false
	}
	open := streamOpenFor(r.session, typ, service, method, openMsg)
	n := -1
	if open.Counter {
		n = r.probeN[counterKey(open)]
	}
	fp, err := xrr.StreamFingerprint(open, n)
	if err != nil {
		return false
	}
	for _, kind := range [...]string{"req", "resp"} {
		path := filepath.Join(r.dir, fmt.Sprintf("%s-%s.%s.yaml", adapterID, fp, kind))
		if _, statErr := os.Stat(path); statErr != nil {
			return false
		}
	}
	return true
}

// counterKey mirrors the core's occurrence-counter key: adapter id plus the
// canonical identity with n omitted.
func counterKey(open xrr.StreamOpen) string {
	keys := make([]string, 0, len(open.Identity))
	for k := range open.Identity {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var b strings.Builder
	b.WriteString(open.AdapterID)
	b.WriteString("\x00")
	b.WriteString(string(open.Type))
	for _, k := range keys {
		fmt.Fprintf(&b, "\x00%s=%v", k, open.Identity[k])
	}
	return b.String()
}

// replayStream is one RPC being served from a cassette.
type replayStream struct {
	service string
	method  string
	replay  *xrr.StreamReplay
	// sends counts client messages already validated, so the stream type
	// inference and the half-close check see the same evidence the
	// recorder saw.
	sends int
	// buffered holds client messages seen before the cassette was located.
	buffered [][]byte
	opened   bool
	done     bool
	// typ is decided the same way the recorder decided it.
	typ xrr.StreamType
}

// serve runs the connection: preface, SETTINGS, then the frame loop.
func (r *replayConn) serve() {
	defer r.conn.Close()

	// The client writes the preface before anything else.
	pre := make([]byte, len(http2.ClientPreface))
	if _, err := io.ReadFull(r.conn, pre); err != nil {
		return
	}
	r.wmu.Lock()
	_ = r.framer.WriteSettings()
	r.wmu.Unlock()

	for {
		f, err := r.framer.ReadFrame()
		if err != nil {
			return
		}
		if err := r.handleFrame(f); err != nil {
			return
		}
	}
}

func (r *replayConn) handleFrame(f http2.Frame) error {
	switch v := f.(type) {
	case *http2.SettingsFrame:
		if !v.IsAck() {
			r.wmu.Lock()
			_ = r.framer.WriteSettingsAck()
			r.wmu.Unlock()
		}
		return nil
	case *http2.PingFrame:
		if !v.IsAck() {
			r.wmu.Lock()
			_ = r.framer.WritePing(true, v.Data)
			r.wmu.Unlock()
		}
		return nil
	case *http2.MetaHeadersFrame:
		return r.onHeaders(v)
	case *http2.DataFrame:
		return r.onData(v)
	case *http2.RSTStreamFrame:
		r.mu.Lock()
		delete(r.streams, v.StreamID)
		r.mu.Unlock()
		return nil
	case *http2.GoAwayFrame:
		return errors.New("client sent GOAWAY")
	default:
		// WINDOW_UPDATE, PRIORITY, and friends: nothing to do. Replay
		// advertises a large window and never withholds data.
		return nil
	}
}

// onHeaders begins a new RPC. The cassette is not located yet: the stream
// type (and for server streams the content address) depends on evidence
// that only arrives with the client's messages.
func (r *replayConn) onHeaders(f *http2.MetaHeadersFrame) error {
	service, method, ok := splitPath(f.PseudoValue("path"))
	if !ok {
		return r.writeTrailersOnly(f.StreamID, codes.Unimplemented, "malformed :path")
	}
	r.mu.Lock()
	r.streams[f.StreamID] = &replayStream{service: service, method: method}
	r.mu.Unlock()

	if f.StreamEnded() {
		// Half-close with no message: a client/bidi stream that sent
		// nothing. Nothing more will arrive, so serve it now.
		return r.advance(f.StreamID, true)
	}
	return nil
}

// onData accumulates client messages and, once the shape is known, serves
// the recorded stream.
func (r *replayConn) onData(f *http2.DataFrame) error {
	r.mu.Lock()
	s := r.streams[f.StreamID]
	r.mu.Unlock()
	if s == nil {
		return nil
	}

	sd := &streamDecoder{}
	msgs, err := sd.feed(f.Data())
	if err != nil {
		return r.writeTrailersOnly(f.StreamID, codes.Internal, err.Error())
	}
	r.mu.Lock()
	s.buffered = append(s.buffered, msgs...)
	r.mu.Unlock()

	return r.advance(f.StreamID, f.StreamEnded())
}

// advance opens the cassette once addressable and streams the recorded
// response. halfClosed reports whether the client closed its send side.
func (r *replayConn) advance(id uint32, halfClosed bool) error {
	r.mu.Lock()
	s := r.streams[id]
	if s == nil || s.done {
		r.mu.Unlock()
		return nil
	}
	if s.opened {
		// Already serving; validate any further client sends.
		pending := s.buffered
		s.buffered = nil
		rp := s.replay
		r.mu.Unlock()
		for _, m := range pending {
			if err := rp.Send(m); err != nil && !errors.Is(err, io.EOF) {
				if errors.Is(err, xrr.ErrStreamMismatch) {
					return r.finishStream(id, codes.FailedPrecondition, err.Error())
				}
			}
		}
		return nil
	}

	probe := func(candidate xrr.StreamType, openMsg []byte) bool {
		return r.cassetteExists(candidate, s.service, s.method, openMsg)
	}
	typ, ready := inferReplayType(s, halfClosed, probe)
	if !ready {
		r.mu.Unlock()
		return nil
	}
	s.typ = typ
	msgs := s.buffered
	s.buffered = nil
	s.opened = true
	service, method := s.service, s.method
	r.mu.Unlock()

	var openMsg []byte
	if len(msgs) > 0 {
		openMsg = msgs[0]
	}
	realOpen := streamOpenFor(r.session, typ, service, method, openMsg)
	if realOpen.Counter {
		r.mu.Lock()
		r.probeN[counterKey(realOpen)]++
		r.mu.Unlock()
	}
	rp, err := r.session.OpenStreamReplay(realOpen)
	if err != nil {
		code := codes.NotFound
		if errors.Is(err, xrr.ErrShapeMismatch) {
			code = codes.FailedPrecondition
		}
		return r.writeTrailersOnly(id, code, fmt.Sprintf("xrr: %v", err))
	}

	r.mu.Lock()
	s.replay = rp
	r.mu.Unlock()

	// Validate the client's sends against the recording, exactly as the
	// interceptor-based replay path does.
	for _, m := range msgs {
		if err := rp.Send(m); err != nil {
			if errors.Is(err, xrr.ErrStreamMismatch) {
				return r.finishStream(id, codes.FailedPrecondition, err.Error())
			}
			break // post-terminal send: recorded stream was already done
		}
	}
	if halfClosed {
		if err := rp.HalfClose(); err != nil && errors.Is(err, xrr.ErrStreamMismatch) {
			return r.finishStream(id, codes.FailedPrecondition, err.Error())
		}
	}
	return r.deliver(id, rp)
}

// inferReplayType decides which stream type to address the cassette by,
// from the evidence available so far.
//
// The recorder decides this at TERMINAL, when the whole shape is known.
// Replay has to decide EARLIER — it must answer the client — and before the
// half-close, client-streaming and bidi are genuinely indistinguishable on
// the wire: both are "N client messages, no close yet". Waiting for the
// half-close would resolve it, but a bidi client does not half-close until
// it has read the replies it is waiting for, so waiting deadlocks it.
//
// The tie is therefore broken by asking the cassette directory which
// fingerprint actually exists (probe), which is free of side effects — it
// does not consume the session's occurrence counter. Only once a type is
// chosen does the caller open the replay for real.
//
// Returning ready=false means "not addressable yet": wait for more client
// traffic.
func inferReplayType(s *replayStream, halfClosed bool, probe func(xrr.StreamType, []byte) bool) (xrr.StreamType, bool) {
	n := len(s.buffered)
	var openMsg []byte
	if n > 0 {
		openMsg = s.buffered[0]
	}

	if halfClosed {
		// One message then immediate half-close is the server-streaming
		// signature, but a one-message client-stream looks identical, so
		// the cassette decides.
		if n == 1 && probe(xrr.StreamServer, openMsg) {
			return xrr.StreamServer, true
		}
		if probe(xrr.StreamClient, nil) {
			return xrr.StreamClient, true
		}
		return xrr.StreamBidi, true
	}

	if n == 0 {
		return "", false
	}
	// Still open. A bidi recording is the reason the client is waiting, so
	// prefer it; fall back to server-streaming (a client that has not yet
	// emitted its half-close).
	if probe(xrr.StreamBidi, nil) {
		return xrr.StreamBidi, true
	}
	if probe(xrr.StreamServer, openMsg) {
		return xrr.StreamServer, true
	}
	return "", false
}

// deliver writes the recorded response: initial HEADERS, one DATA frame per
// recorded recv frame, then trailers carrying the recorded status.
func (r *replayConn) deliver(id uint32, rp *xrr.StreamReplay) error {
	r.wmu.Lock()
	hdr := r.enc.encode(
		hpack.HeaderField{Name: ":status", Value: "200"},
		hpack.HeaderField{Name: "content-type", Value: "application/grpc"},
	)
	err := r.framer.WriteHeaders(http2.HeadersFrameParam{
		StreamID:      id,
		BlockFragment: hdr,
		EndHeaders:    true,
	})
	r.wmu.Unlock()
	if err != nil {
		return err
	}

	for {
		msg, rErr := rp.Recv()
		if rErr != nil {
			code, text := terminalOf(rp, rErr)
			return r.finishStream(id, code, text)
		}
		if wErr := r.writeMessage(id, msg); wErr != nil {
			return wErr
		}
	}
}

// terminalOf maps a replay terminal into a gRPC status.
func terminalOf(rp *xrr.StreamReplay, err error) (codes.Code, string) {
	if errors.Is(err, io.EOF) {
		return codes.OK, ""
	}
	code := codes.Unknown
	switch v := rp.RespPayload()["status_code"].(type) {
	case int:
		code = codes.Code(v)
	case int64:
		code = codes.Code(v)
	case uint64:
		code = codes.Code(v)
	case float64:
		code = codes.Code(v)
	}
	if code == codes.OK {
		code = codes.Unknown
	}
	return code, err.Error()
}

// writeMessage frames one recorded message as a gRPC-length-prefixed DATA
// payload, splitting across frames if it exceeds the frame size.
func (r *replayConn) writeMessage(id uint32, msg []byte) error {
	payload := make([]byte, grpcMsgPrefixLen+len(msg))
	payload[0] = 0 // not compressed
	payload[1] = byte(len(msg) >> 24)
	payload[2] = byte(len(msg) >> 16)
	payload[3] = byte(len(msg) >> 8)
	payload[4] = byte(len(msg))
	copy(payload[grpcMsgPrefixLen:], msg)

	const chunk = 16 << 10 // stay well under the default max frame size
	r.wmu.Lock()
	defer r.wmu.Unlock()
	for len(payload) > 0 {
		n := min(len(payload), chunk)
		if err := r.framer.WriteData(id, false, payload[:n]); err != nil {
			return err
		}
		payload = payload[n:]
	}
	return nil
}

// finishStream writes the trailers that terminate an RPC.
func (r *replayConn) finishStream(id uint32, code codes.Code, msg string) error {
	r.mu.Lock()
	if s := r.streams[id]; s != nil {
		s.done = true
	}
	delete(r.streams, id)
	r.mu.Unlock()

	r.wmu.Lock()
	defer r.wmu.Unlock()
	fields := []hpack.HeaderField{
		{Name: "grpc-status", Value: strconv.Itoa(int(code))},
	}
	if msg != "" {
		fields = append(fields, hpack.HeaderField{Name: "grpc-message", Value: encodeGRPCMessage(msg)})
	}
	return r.framer.WriteHeaders(http2.HeadersFrameParam{
		StreamID:      id,
		BlockFragment: r.enc.encode(fields...),
		EndHeaders:    true,
		EndStream:     true,
	})
}

// writeTrailersOnly answers an RPC with a Trailers-Only response: the
// gRPC shape for "this call failed before any message".
func (r *replayConn) writeTrailersOnly(id uint32, code codes.Code, msg string) error {
	r.mu.Lock()
	delete(r.streams, id)
	r.mu.Unlock()

	r.wmu.Lock()
	defer r.wmu.Unlock()
	fields := []hpack.HeaderField{
		{Name: ":status", Value: "200"},
		{Name: "content-type", Value: "application/grpc"},
		{Name: "grpc-status", Value: strconv.Itoa(int(code))},
	}
	if msg != "" {
		fields = append(fields, hpack.HeaderField{Name: "grpc-message", Value: encodeGRPCMessage(msg)})
	}
	return r.framer.WriteHeaders(http2.HeadersFrameParam{
		StreamID:      id,
		BlockFragment: r.enc.encode(fields...),
		EndHeaders:    true,
		EndStream:     true,
	})
}

// encodeGRPCMessage percent-encodes a status message for the grpc-message
// header, which is restricted to a printable ASCII subset.
func encodeGRPCMessage(s string) string {
	out := make([]byte, 0, len(s))
	for i := 0; i < len(s); i++ {
		c := s[i]
		if c >= 0x20 && c <= 0x7E && c != '%' {
			out = append(out, c)
			continue
		}
		out = append(out, '%')
		const hexDigits = "0123456789ABCDEF"
		out = append(out, hexDigits[c>>4], hexDigits[c&0xF])
	}
	return string(out)
}
