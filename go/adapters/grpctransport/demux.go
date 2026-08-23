package grpctransport

import (
	"encoding/binary"
	"fmt"
	"io"

	"golang.org/x/net/http2"
	"golang.org/x/net/http2/hpack"
)

// HTTP/2 wire decoding for the transport-level gRPC adapter.
//
// Everything here operates one level below the gRPC library: a byte stream
// carrying an HTTP/2 connection is decoded into per-stream gRPC message
// events. That is the whole point of this adapter — the messages are
// reconstructed from frames, not read out of an interceptor, so a client
// library that exposes no interceptor hooks is still recordable.
//
// Two decisions are load-bearing and both follow from what the wire
// actually carries (verified against grpc-go 1.83 on a live connection):
//
//   - Granularity is RECONSTRUCTED gRPC MESSAGES, not raw bytes and not
//     raw frames. DATA frame boundaries are a flow-control artifact — the
//     same logical message splits differently depending on window sizes,
//     MTU, and timing — so recording them would make cassettes replay-
//     fragile and unreadable. gRPC's own length-prefixed framing (a 5-byte
//     header per message) is the stable unit, and it is exactly the unit
//     the existing streaming cassette format stores. Reconstructing it
//     means transport-recorded cassettes are ordinary streaming cassettes,
//     byte-identical in shape to interceptor-recorded ones.
//   - Transport noise is dropped, never recorded. SETTINGS, WINDOW_UPDATE,
//     PING, PRIORITY, and the connection preface vary run to run (window
//     sizes, keepalive timing, ack interleaving) and carry no application
//     information. They never enter a cassette and never enter a
//     fingerprint.

// grpcMsgPrefixLen is the gRPC message header: 1 compression byte plus a
// 4-byte big-endian length. See the gRPC HTTP/2 wire protocol.
const grpcMsgPrefixLen = 5

// maxGRPCMessageSize caps a single reconstructed message so a corrupt or
// hostile length prefix cannot drive an unbounded allocation. 64 MiB is far
// above gRPC's own 4 MiB default receive limit.
const maxGRPCMessageSize = 64 << 20

// hpackTableSize is the HPACK dynamic-table size used by our decoders. It
// only has to be large enough to follow the peer's encoder; grpc-go uses
// 4096, the HTTP/2 default.
const hpackTableSize = 4096

// direction identifies which half of the connection a decoder is reading.
type direction int

const (
	// dirClientToServer decodes bytes the client wrote (requests).
	dirClientToServer direction = iota
	// dirServerToClient decodes bytes the client read (responses).
	dirServerToClient
)

// wireEvent is one decoded, application-meaningful event on one HTTP/2
// stream. Transport noise never produces a wireEvent.
type wireEvent struct {
	streamID uint32
	kind     wireEventKind
	// headers is set for wireHeaders: the decoded header fields, already
	// sanitized (see sanitizeHeaders).
	headers []hpack.HeaderField
	// message is set for wireMessage: one complete gRPC message's bytes,
	// with the 5-byte length prefix stripped.
	message []byte
	// endStream reports whether this event carried the END_STREAM flag.
	// On a client HEADERS/DATA it is the half-close; on a server HEADERS
	// it is the trailers terminal.
	endStream bool
}

type wireEventKind int

const (
	wireHeaders wireEventKind = iota
	wireMessage
	wireReset
	wireGoAway
)

// pendingMsgs holds messages decoded from one DATA frame that have not
// been returned yet. One DATA frame may pack several complete gRPC
// messages; each surfaces as its own event, drained one at a time.
type pendingMsgs struct {
	streamID  uint32
	msgs      [][]byte
	endStream bool
}

// streamDecoder reassembles gRPC messages for one HTTP/2 stream ID. DATA
// frame payloads are appended to buf and drained into whole messages;
// a message split across any number of DATA frames reassembles here, and
// several messages packed into one DATA frame all emerge.
type streamDecoder struct {
	buf []byte
}

// feed appends a DATA payload and returns every complete gRPC message it
// now holds. Trailing partial bytes stay buffered for the next frame.
func (d *streamDecoder) feed(data []byte) ([][]byte, error) {
	d.buf = append(d.buf, data...)
	var out [][]byte
	for len(d.buf) >= grpcMsgPrefixLen {
		n := binary.BigEndian.Uint32(d.buf[1:grpcMsgPrefixLen])
		if n > maxGRPCMessageSize {
			return nil, fmt.Errorf("grpctransport: gRPC message length %d exceeds cap %d", n, maxGRPCMessageSize)
		}
		total := grpcMsgPrefixLen + int(n)
		if len(d.buf) < total {
			break
		}
		// The compression byte is deliberately not preserved: the cassette
		// format stores decoded message bytes, and the recorder rejects
		// compressed connections outright (see connDecoder.feedData).
		msg := make([]byte, n)
		copy(msg, d.buf[grpcMsgPrefixLen:total])
		out = append(out, msg)
		d.buf = d.buf[total:]
	}
	// Reclaim the backing array once drained, so a long-lived stream that
	// carried one huge message does not pin it.
	if len(d.buf) == 0 {
		d.buf = nil
	}
	return out, nil
}

// connDecoder decodes one direction of one HTTP/2 connection into
// wireEvents, demultiplexing by stream ID.
//
// Multiplexing is the main correctness risk of transport capture: one TCP
// connection interleaves frames from many concurrent RPCs, and the
// interleaving is arbitrary. It is handled by keeping reassembly state
// per stream ID (streams) and never assuming frames for one stream arrive
// contiguously.
type connDecoder struct {
	framer  *http2.Framer
	streams map[uint32]*streamDecoder
	redact  headerRedactor
	// queued holds messages decoded from one DATA frame beyond the first.
	// One DATA frame can carry several complete gRPC messages, and each
	// must surface as its own event.
	queued pendingMsgs
}

func newConnDecoder(r io.Reader, redact headerRedactor) *connDecoder {
	fr := http2.NewFramer(io.Discard, r)
	// ReadMetaHeaders makes the Framer merge HEADERS + CONTINUATION and
	// HPACK-decode them for us. Decoding is mandatory, not a convenience:
	// the :path pseudo-header is the only place service and method appear
	// on the wire, and HPACK is stateful, so the whole header stream must
	// be decoded in order even for streams we ignore.
	fr.ReadMetaHeaders = hpack.NewDecoder(hpackTableSize, nil)
	fr.SetMaxReadFrameSize(maxFrameSize)
	return &connDecoder{
		framer:  fr,
		streams: make(map[uint32]*streamDecoder),
		redact:  redact,
	}
}

// maxFrameSize is the largest HTTP/2 frame we will read. The protocol
// maximum is 2^24-1; grpc-go negotiates far smaller frames, but a peer may
// legitimately raise SETTINGS_MAX_FRAME_SIZE, so we accept the protocol max
// rather than guessing.
const maxFrameSize = 1<<24 - 1

// next returns the next application-meaningful event, skipping transport
// noise. It returns io.EOF when the direction is exhausted.
func (c *connDecoder) next() (*wireEvent, error) {
	for {
		// Messages left over from a multi-message DATA frame come first:
		// they precede anything read from the wire next.
		if ev := c.drainQueued(); ev != nil {
			return ev, nil
		}
		f, err := c.framer.ReadFrame()
		if err != nil {
			return nil, err
		}
		switch v := f.(type) {
		case *http2.MetaHeadersFrame:
			return &wireEvent{
				streamID:  v.StreamID,
				kind:      wireHeaders,
				headers:   sanitizeHeaders(c.redact, v.Fields),
				endStream: v.StreamEnded(),
			}, nil
		case *http2.DataFrame:
			ev, err := c.feedData(v)
			if err != nil {
				return nil, err
			}
			if ev != nil {
				return ev, nil
			}
			// A DATA frame that completed no message (a partial message, or
			// an empty END_STREAM frame) still may carry the half-close.
			if v.StreamEnded() {
				return &wireEvent{streamID: v.StreamID, kind: wireMessage, endStream: true, message: nil}, nil
			}
		case *http2.RSTStreamFrame:
			delete(c.streams, v.StreamID)
			return &wireEvent{streamID: v.StreamID, kind: wireReset}, nil
		case *http2.GoAwayFrame:
			return &wireEvent{kind: wireGoAway}, nil
		default:
			// SETTINGS, WINDOW_UPDATE, PING, PRIORITY, PUSH_PROMISE and any
			// unknown frame type: transport noise. Dropped by design — see
			// the package comment. Never recorded, never fingerprinted.
			continue
		}
	}
}

// feedData reassembles a DATA frame into whole gRPC messages. Multiple
// completed messages are queued and returned one event at a time.
func (c *connDecoder) feedData(f *http2.DataFrame) (*wireEvent, error) {
	sd := c.streams[f.StreamID]
	if sd == nil {
		sd = &streamDecoder{}
		c.streams[f.StreamID] = sd
	}
	msgs, err := sd.feed(f.Data())
	if err != nil {
		return nil, err
	}
	if len(msgs) == 0 {
		return nil, nil
	}
	c.queued = pendingMsgs{streamID: f.StreamID, msgs: msgs[1:], endStream: f.StreamEnded()}
	return &wireEvent{
		streamID: f.StreamID,
		kind:     wireMessage,
		message:  msgs[0],
		// END_STREAM belongs to the LAST message carried by the frame; if
		// more are queued, this one is not the last.
		endStream: len(msgs) == 1 && f.StreamEnded(),
	}, nil
}

// drainQueued returns the next queued message event, if any.
func (c *connDecoder) drainQueued() *wireEvent {
	if len(c.queued.msgs) == 0 {
		return nil
	}
	msg := c.queued.msgs[0]
	c.queued.msgs = c.queued.msgs[1:]
	last := len(c.queued.msgs) == 0
	return &wireEvent{
		streamID:  c.queued.streamID,
		kind:      wireMessage,
		message:   msg,
		endStream: last && c.queued.endStream,
	}
}
