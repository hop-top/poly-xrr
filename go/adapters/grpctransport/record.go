package grpctransport

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"

	xrr "hop.top/xrr"

	"golang.org/x/net/http2"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Record path: a net.Conn wrapper that tees both directions of a live
// HTTP/2 connection into cassette pairs.
//
// The tap is passive. It never alters, delays, reorders, or withholds a
// byte: reads and writes go straight through to the real socket, and the
// observed bytes are copied into a decoder running on its own goroutine.
// A recording failure therefore cannot change the behaviour of the program
// being recorded (it surfaces at Close instead).

// recordConn wraps a live connection, mirroring each direction into a
// decoder goroutine.
type recordConn struct {
	net.Conn
	toServer *io.PipeWriter
	toClient *io.PipeWriter

	mu       sync.Mutex
	closed   bool
	wg       sync.WaitGroup
	tracker  *streamTracker
	firstErr error
}

// Read passes server→client bytes through and mirrors them.
func (c *recordConn) Read(b []byte) (int, error) {
	n, err := c.Conn.Read(b)
	if n > 0 {
		// A mirror write can only fail once the decoder is gone, in which
		// case recording is already over; the live path must not care.
		_, _ = c.toClient.Write(b[:n])
	}
	if err != nil {
		_ = c.toClient.CloseWithError(err)
	}
	return n, err
}

// Write passes client→server bytes through and mirrors them.
func (c *recordConn) Write(b []byte) (int, error) {
	n, err := c.Conn.Write(b)
	if n > 0 {
		_, _ = c.toServer.Write(b[:n])
	}
	return n, err
}

// Close tears down the live connection and waits for both decoders to
// drain, so every stream that reached a terminal before close is persisted
// before Close returns.
func (c *recordConn) Close() error {
	err := c.Conn.Close()
	c.mu.Lock()
	already := c.closed
	c.closed = true
	c.mu.Unlock()
	if !already {
		_ = c.toServer.CloseWithError(io.EOF)
		_ = c.toClient.CloseWithError(io.EOF)
		c.wg.Wait()
		c.tracker.abandonAll()
	}
	return err
}

// RecordDialer returns a grpc.WithContextDialer function that records every
// streamed RPC carried by the connections it creates.
//
// This is the whole reason the package exists: it attaches BELOW the gRPC
// library, so it works with clients that expose no interceptor hooks at
// all. Where a StreamClientInterceptor is available, prefer the
// interceptor-based adapter — it sees typed messages and needs no HTTP/2
// decoding. Use this when the library will not let you in.
//
// dial supplies the underlying connection; pass nil for a plain TCP dialer.
func RecordDialer(session *xrr.FileSession, dial DialFunc) DialFunc {
	if dial == nil {
		dial = plainDialer
	}
	return func(ctx context.Context, addr string) (net.Conn, error) {
		conn, err := dial(ctx, addr)
		if err != nil {
			return nil, err
		}
		return newRecordConn(session, conn), nil
	}
}

// DialFunc matches grpc.WithContextDialer's signature.
type DialFunc = func(ctx context.Context, addr string) (net.Conn, error)

func plainDialer(ctx context.Context, addr string) (net.Conn, error) {
	return (&net.Dialer{}).DialContext(ctx, "tcp", addr)
}

func newRecordConn(session *xrr.FileSession, conn net.Conn) net.Conn {
	toServerR, toServerW := io.Pipe()
	toClientR, toClientW := io.Pipe()
	var redactor headerRedactor

	rc := &recordConn{
		Conn:     conn,
		toServer: toServerW,
		toClient: toClientW,
		tracker:  newStreamTracker(session),
	}
	rc.wg.Add(2)
	go rc.decode(dirClientToServer, toServerR, redactor)
	go rc.decode(dirServerToClient, toClientR, redactor)
	return rc
}

// decode runs one direction's decoder loop, feeding events to the tracker.
func (c *recordConn) decode(dir direction, r io.Reader, redactor headerRedactor) {
	defer c.wg.Done()
	if dir == dirClientToServer {
		// The client preface precedes the first frame and is not a frame;
		// the Framer would reject it. Transport noise, dropped by design.
		if _, err := io.ReadFull(r, make([]byte, len(http2.ClientPreface))); err != nil {
			return
		}
	}
	dec := newConnDecoder(dir, r, redactor)
	for {
		ev, err := dec.next()
		if err != nil {
			if !errors.Is(err, io.EOF) && !errors.Is(err, io.ErrClosedPipe) {
				c.noteErr(err)
			}
			return
		}
		if err := c.tracker.handle(dir, ev); err != nil {
			c.noteErr(err)
			return
		}
	}
}

func (c *recordConn) noteErr(err error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.firstErr == nil {
		c.firstErr = err
	}
}

// ── per-stream recording state ─────────────────────────────────────────────

// recEventKind labels an entry in a stream's observed-order event log.
type recEventKind int

const (
	evSend recEventKind = iota
	evRecv
	evHalfClose
)

// recEvent is one observed event, in arrival order.
type recEvent struct {
	kind recEventKind
	msg  []byte
}

// recStream is the recording state of one HTTP/2 stream (one RPC).
//
// Events accumulate in one interleaved log rather than per-direction
// buffers: the cassette's global seq numbers must reflect the order events
// were actually observed, and the two directions decode concurrently. The
// log is appended under the tracker's mutex, so it is the single serialized
// record of arrival order.
//
// Nothing is written to the cassette until the terminal, because the
// stream's type — and therefore its fingerprint — is only decidable once
// the full shape is known (see inferStreamType).
type recStream struct {
	service string
	method  string

	// events is the observed-order log replayed into the recording at
	// terminal.
	events []recEvent
	// sends holds client message bytes in order; sends[0] is the
	// content-addressing input for a server stream.
	sends [][]byte

	sentCount int
	recvCount int
	// sawHalfClose records that the client closed its send side.
	sawHalfClose bool
	// halfCloseAfterFirstSend records that the half-close arrived with (or
	// immediately after) the first and only client message — the
	// server-streaming signature.
	halfCloseAfterFirstSend bool

	compressed bool
	finished   bool
}

// streamTracker demultiplexes wireEvents into per-RPC recordings.
//
// Stream type inference is the subtle part. The transport does not label an
// RPC as server/client/bidi — that is a property of the service definition,
// invisible on the wire. What IS observable is the message counts and the
// half-close position, which is exactly what the cassette format keys on.
// The type is therefore decided at TERMINAL, when the full shape is known,
// rather than guessed at open.
type streamTracker struct {
	session *xrr.FileSession
	mu      sync.Mutex
	streams map[uint32]*recStream
}

func newStreamTracker(session *xrr.FileSession) *streamTracker {
	return &streamTracker{session: session, streams: make(map[uint32]*recStream)}
}

func (t *streamTracker) get(id uint32) *recStream {
	s := t.streams[id]
	if s == nil {
		s = &recStream{}
		t.streams[id] = s
	}
	return s
}

// handle folds one decoded event into the recording state.
func (t *streamTracker) handle(dir direction, ev *wireEvent) error {
	if ev.kind == wireGoAway || ev.streamID == 0 {
		return nil
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	s := t.get(ev.streamID)

	switch {
	case ev.kind == wireReset:
		t.finish(ev.streamID, s, codes.Canceled, "stream reset")
		return nil

	case dir == dirClientToServer && ev.kind == wireHeaders:
		service, method, ok := splitPath(headerValue(ev.headers, ":path"))
		if !ok {
			return nil // not a gRPC request; ignore this stream
		}
		s.service, s.method = service, method
		if isCompressed(ev.headers) {
			s.compressed = true
			return fmt.Errorf("grpctransport: %s/%s negotiates grpc-encoding %q; transport capture records decoded message bytes and cannot record compressed streams",
				service, method, headerValue(ev.headers, "grpc-encoding"))
		}
		if ev.endStream {
			s.noteHalfClose()
		}
		return nil

	case dir == dirClientToServer && ev.kind == wireMessage:
		if s.service == "" {
			return nil
		}
		if ev.message != nil {
			s.sends = append(s.sends, ev.message)
			s.sentCount++
			s.events = append(s.events, recEvent{kind: evSend, msg: ev.message})
		}
		if ev.endStream {
			s.noteHalfClose()
		}
		return nil

	case dir == dirServerToClient && ev.kind == wireHeaders:
		if s.service == "" {
			return nil
		}
		if isCompressed(ev.headers) {
			return fmt.Errorf("grpctransport: %s/%s response negotiates grpc-encoding %q; cannot record compressed streams",
				s.service, s.method, headerValue(ev.headers, "grpc-encoding"))
		}
		if code, msg, ok := grpcStatus(ev.headers); ok {
			t.finish(ev.streamID, s, code, msg)
		}
		return nil

	case dir == dirServerToClient && ev.kind == wireMessage:
		if s.service == "" || ev.message == nil {
			return nil
		}
		s.recvCount++
		s.events = append(s.events, recEvent{kind: evRecv, msg: ev.message})
		return nil
	}
	return nil
}

// noteHalfClose records the client closing its send side, once, in observed
// order. Whether it landed on the first and only client message is the
// server-streaming signature that inferStreamType keys on.
func (s *recStream) noteHalfClose() {
	if s.sawHalfClose {
		return
	}
	s.sawHalfClose = true
	s.halfCloseAfterFirstSend = s.sentCount == 1
	s.events = append(s.events, recEvent{kind: evHalfClose})
}

// finish decides the stream type from the observed shape, opens the
// recording, replays the buffered events into it in their observed order,
// and persists the pair.
//
// Ordering note: the cassette's global seq numbers must reflect observed
// order. Because both directions decode concurrently, the tracker records
// arrival order in one interleaved log (events) rather than reconstructing
// it from per-direction buffers.
func (t *streamTracker) finish(id uint32, s *recStream, code codes.Code, msg string) {
	defer delete(t.streams, id)
	if s.finished || s.service == "" || s.compressed {
		return
	}
	s.finished = true

	typ := inferStreamType(s)
	open := streamOpenFor(t.session, typ, s.service, s.method, firstOrNil(s.sends))
	rec, err := t.session.OpenStreamRecord(open)
	if err != nil {
		return
	}
	for _, ev := range s.events {
		switch ev.kind {
		case evSend:
			rec.RecordSend(ev.msg)
		case evRecv:
			rec.RecordRecv(ev.msg)
		case evHalfClose:
			rec.RecordHalfClose()
		}
	}
	var termErr error
	if code != codes.OK {
		termErr = status.Error(code, msg)
	}
	_ = rec.Finish(map[string]any{"status_code": int(code)}, termErr)
}

// abandonAll drops streams still open when the connection closed. A stream
// that never reached a terminal produces no cassette, per the format spec.
func (t *streamTracker) abandonAll() {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.streams = make(map[uint32]*recStream)
}

func firstOrNil(msgs [][]byte) []byte {
	if len(msgs) == 0 {
		return nil
	}
	return msgs[0]
}

// inferStreamType derives the cassette stream type from the observed
// message shape, since the wire does not carry the RPC's declared kind.
//
//   - exactly one client message followed immediately by half-close, with
//     any number of server messages ⇒ server-streaming (the generated
//     client half-closes with the request).
//   - at most one server message ⇒ client-streaming.
//   - otherwise ⇒ bidi.
//
// The inference matters only for cassette addressing: server streams are
// content-addressed by their request message, client/bidi by the occurrence
// counter. A misclassification would change the fingerprint, so replay
// applies the SAME inference from the same evidence.
func inferStreamType(s *recStream) xrr.StreamType {
	switch {
	case s.sentCount == 1 && s.sawHalfClose && s.halfCloseAfterFirstSend:
		return xrr.StreamServer
	case s.recvCount <= 1:
		return xrr.StreamClient
	default:
		return xrr.StreamBidi
	}
}

// streamOpenFor builds the core open value, mirroring the interceptor-based
// adapter's gRPC mapping exactly: same adapter id, same canonical identity
// fields, same content- vs counter-addressing rule. Cassettes recorded at
// the transport are therefore indistinguishable from interceptor-recorded
// ones — same fingerprints, same files, replayable by either path.
func streamOpenFor(session *xrr.FileSession, typ xrr.StreamType, service, method string, openMsg []byte) xrr.StreamOpen {
	open := xrr.StreamOpen{
		AdapterID: adapterID,
		Type:      typ,
		Identity:  map[string]any{"service": service, "method": method},
		Payload:   map[string]any{"service": service, "method": method},
	}
	if typ == xrr.StreamServer {
		scrubbed := session.ScrubStreamFrame(xrr.StreamSend,
			xrr.StreamScrubInfo{AdapterID: adapterID, Type: typ}, openMsg)
		open.Identity["msg_hash"] = msgHash(scrubbed)
	} else {
		open.Counter = true
	}
	return open
}
