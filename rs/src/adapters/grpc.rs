//! Streaming gRPC adapter: records and replays server-, client-, and
//! bidi-streamed RPCs on top of the core stream session API. See
//! `spec/cassette-format-streaming.md` (gRPC Adapter Mapping) for the
//! normative semantics.
//!
//! # The seam
//!
//! grpc-go hands interceptors a `ClientStream` whose `SendMsg`/`RecvMsg`
//! take typed messages, so the Go port wraps that object. tonic has no
//! equivalent per-call interceptor: its `tower` layers sit *below* the
//! codec, where a whole RPC is one opaque HTTP body, and its `Codec` trait
//! cannot be wrapped to observe wire bytes (`EncodeBuf` is write-only —
//! there is no way to read back what an inner encoder produced).
//!
//! The seam that does work is the codec's *message type*. [`BytesCodec`]
//! is a `tonic::codec::Codec` whose `Encode`/`Decode` are both `Vec<u8>`:
//! the protobuf wire bytes of exactly one gRPC message, which is precisely
//! the `message_b64` payload the spec records. tonic still owns framing,
//! compression, HTTP/2, and trailers; this adapter only chooses the
//! representation crossing the codec boundary, then tees it.
//!
//! [`GrpcStream`] is the call-level wrapper built on that codec. It
//! dispatches on session mode — passthrough calls straight through, record
//! tees each message into a cassette pair, replay serves the recording with
//! no network — mirroring the Go interceptor's structure. Callers marshal
//! their prost messages with [`to_wire`] / [`from_wire`], which is where
//! the spec's deterministic-serialization requirement is met.

use std::sync::Mutex;

use bytes::{Buf, BufMut};
use prost::Message;
use serde_yaml::{Mapping, Value};
use sha2::{Digest, Sha256};
use tonic::codec::{Codec, DecodeBuf, Decoder, EncodeBuf, Encoder};
use tonic::Status;

use crate::{
    error::XrrError,
    session::{Mode, Session},
    stream::{StreamOpen, StreamType},
    stream_scrub::{StreamDirection, StreamScrubInfo},
    stream_session::{StreamRecording, StreamReplay},
};

/// The adapter id written to cassette envelopes and filenames.
pub const ADAPTER_ID: &str = "grpc";

// ── wire-bytes codec ─────────────────────────────────────────────────────────

/// A `tonic` codec whose message type is the raw protobuf wire bytes of one
/// gRPC message.
///
/// tonic handles gRPC framing itself, so `Decoder::decode` is handed
/// "exactly the bytes of a full message" and `Encoder::encode` writes one
/// message's bytes. That makes `Vec<u8>` a faithful message type and gives
/// the adapter the exact bytes the spec stores in `message_b64`, without
/// the adapter ever knowing the caller's concrete prost type.
#[derive(Debug, Clone, Copy, Default)]
pub struct BytesCodec;

#[derive(Debug, Clone, Copy, Default)]
pub struct BytesEncoder;

#[derive(Debug, Clone, Copy, Default)]
pub struct BytesDecoder;

impl Encoder for BytesEncoder {
    type Item = Vec<u8>;
    type Error = Status;

    fn encode(&mut self, item: Self::Item, dst: &mut EncodeBuf<'_>) -> Result<(), Self::Error> {
        dst.reserve(item.len());
        dst.put_slice(&item);
        Ok(())
    }
}

impl Decoder for BytesDecoder {
    type Item = Vec<u8>;
    type Error = Status;

    fn decode(&mut self, src: &mut DecodeBuf<'_>) -> Result<Option<Self::Item>, Self::Error> {
        let n = src.remaining();
        let mut out = vec![0u8; n];
        src.copy_to_slice(&mut out);
        Ok(Some(out))
    }
}

impl Codec for BytesCodec {
    type Encode = Vec<u8>;
    type Decode = Vec<u8>;
    type Encoder = BytesEncoder;
    type Decoder = BytesDecoder;

    fn encoder(&mut self) -> Self::Encoder {
        BytesEncoder
    }

    fn decoder(&mut self) -> Self::Decoder {
        BytesDecoder
    }
}

// ── message marshalling ──────────────────────────────────────────────────────

/// Protobuf wire bytes of a message crossing the adapter boundary.
///
/// Marshalling MUST be deterministic: the format's byte-level contracts
/// (content-addressed server-stream fingerprints, client/bidi send
/// validation) presume the same message always marshals to the same bytes.
/// prost encodes map fields in sorted key order, so `prost::Message::encode`
/// is already deterministic — unlike Go's `proto.Marshal`, which follows
/// randomized map iteration and forces the Go port to opt into
/// `Deterministic: true`.
pub fn to_wire<M: Message>(msg: &M) -> Vec<u8> {
    let mut buf = Vec::with_capacity(msg.encoded_len());
    msg.encode(&mut buf).expect("Vec<u8> grows to fit");
    buf
}

/// Decode recorded wire bytes into a caller message type. Replay never has
/// a typed value on hand — only the recorded raw bytes — so the caller's
/// message is populated here, exactly as a live stream's codec would.
pub fn from_wire<M: Message + Default>(bytes: &[u8]) -> Result<M, Status> {
    M::decode(bytes).map_err(|e| Status::internal(format!("grpc: decode: {e}")))
}

// ── open construction ────────────────────────────────────────────────────────

/// Split `/pkg.Service/Method` into its service and method identifiers.
pub fn split_full_method(full: &str) -> Result<(&str, &str), XrrError> {
    let s = full.strip_prefix('/').unwrap_or(full);
    match s.rsplit_once('/') {
        Some((service, method)) if !service.is_empty() && !method.is_empty() => {
            Ok((service, method))
        }
        _ => Err(XrrError::Usage(format!(
            "grpc: malformed full method {full:?} (want /service/method)"
        ))),
    }
}

fn payload_of(service: &str, method: &str) -> Mapping {
    let mut m = Mapping::new();
    m.insert(Value::from("service"), Value::from(service));
    m.insert(Value::from("method"), Value::from(method));
    m
}

/// Build the core open value for a gRPC streamed RPC per the spec's gRPC
/// mapping: canonical inputs service + method, req payload
/// `{service, method}`. Server streams are content-addressed via
/// `msg_hash = sha256(message_bytes)[:8]` (the wire bytes of the single
/// request message); client/bidi opens are counter-addressed.
///
/// The `msg_hash` is the one identity input derived from message bytes, and
/// it is computed here, before the core's frame seam — so it is derived
/// from the session-scrubbed bytes. Record and replay both pass through
/// this path, which keeps a scrubbed recording and a scrubbed replay of the
/// same live traffic addressing the same cassette. The raw message itself
/// is handed to the core untouched: the core scrubs frames exactly once.
fn stream_open(
    session: &Session,
    stream_type: StreamType,
    service: &str,
    method: &str,
    open_msg: Option<&[u8]>,
) -> StreamOpen {
    let mut identity = std::collections::BTreeMap::new();
    identity.insert("service".to_string(), serde_json::Value::from(service));
    identity.insert("method".to_string(), serde_json::Value::from(method));

    let mut counter = true;
    if stream_type == StreamType::Server {
        counter = false;
        let raw = open_msg.unwrap_or(&[]);
        let info = StreamScrubInfo {
            adapter_id: ADAPTER_ID.to_string(),
            stream_type,
        };
        let scrubbed = session.scrub_stream_frame(StreamDirection::Send, &info, raw);
        let hash = hex::encode(&Sha256::digest(&scrubbed)[..4]);
        identity.insert("msg_hash".to_string(), serde_json::Value::from(hash));
    }

    StreamOpen {
        adapter_id: ADAPTER_ID.to_string(),
        stream_type,
        identity,
        counter,
        payload: payload_of(service, method),
    }
}

/// Terminal response payload: the gRPC status code (spec: required).
fn resp_payload(status_code: i32) -> Mapping {
    let mut m = Mapping::new();
    m.insert(Value::from("status_code"), Value::from(status_code));
    m
}

/// Extract the recorded `status_code`, tolerating the integer types YAML
/// decoding can produce. Absent or malformed ⇒ `Unknown` (2): the spec
/// requires the field, and replay must still fail loudly rather than
/// succeed with a fabricated OK.
fn status_code_from(payload: &Value) -> i32 {
    payload
        .get("status_code")
        .and_then(|v| v.as_i64())
        .map(|v| v as i32)
        .unwrap_or(UNKNOWN_CODE)
}

const OK_CODE: i32 = 0;
const UNKNOWN_CODE: i32 = 2;

/// Reconstruct the terminal gRPC status from the resp payload
/// `status_code`, treating the recorded error string as the status text
/// (spec). When the string is the standard client rendering
/// (`rpc error: code = X desc = ...`), the description is extracted so the
/// reconstructed error renders like the live one instead of nesting.
fn recorded_status(code: i32, msg: &str) -> Status {
    let code = if code == OK_CODE {
        // An error terminal can never be OK; guard hand-authored cassettes
        // violating the spec invariant.
        UNKNOWN_CODE
    } else {
        code
    };
    let code = tonic::Code::from_i32(code);
    let prefix = format!("rpc error: code = {code:?} desc = ");
    let desc = msg.strip_prefix(&prefix).unwrap_or(msg);
    Status::new(code, desc)
}

/// Map a core stream error into what a live tonic stream surfaces.
/// `StreamEnd` is the caller's end-of-stream signal and is represented as
/// `Ok(None)` by the callers of this helper, so it never reaches here.
fn map_core_err(err: XrrError, resp_payload: &Value) -> Status {
    match err {
        XrrError::StreamRecordedError(msg) => recorded_status(status_code_from(resp_payload), &msg),
        XrrError::StreamMismatch { .. } => Status::failed_precondition(err.to_string()),
        other => Status::internal(other.to_string()),
    }
}

// ── the call wrapper ─────────────────────────────────────────────────────────

/// One streamed gRPC interaction, dispatched on the session mode.
///
/// Construct with [`GrpcStream::open`], then drive it exactly as a live
/// tonic stream: [`send`](Self::send) each outgoing message,
/// [`close_send`](Self::close_send) at half-close, [`recv`](Self::recv)
/// until it yields `Ok(None)` (end-of-stream) or a status error, then
/// [`finish`](Self::finish) to persist a recording.
///
/// Messages cross this boundary as protobuf wire bytes; use [`to_wire`] and
/// [`from_wire`] at the caller's typed edge.
// One handle exists per RPC, and callers pattern-match the variants
// directly — boxing the replay handle would push an allocation and a
// deref into every call site to shrink a short-lived value that is never
// stored in bulk.
#[allow(clippy::large_enum_variant)]
pub enum GrpcStream<'s> {
    /// Transparent: the caller drives the live stream itself.
    Passthrough,
    Record(RecordStream<'s>),
    Replay(ReplayStream),
}

impl std::fmt::Debug for GrpcStream<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            GrpcStream::Passthrough => f.write_str("GrpcStream::Passthrough"),
            GrpcStream::Record(r) => f
                .debug_struct("GrpcStream::Record")
                .field("fingerprint", &r.fingerprint())
                .finish(),
            GrpcStream::Replay(r) => f
                .debug_struct("GrpcStream::Replay")
                .field("fingerprint", &r.fingerprint())
                .finish(),
        }
    }
}

impl<'s> GrpcStream<'s> {
    /// Open a streamed interaction. `full_method` is the gRPC path
    /// (`/pkg.Service/Method`); `open_msg` is the wire bytes of the single
    /// request message for server streams (server-stream cassettes are
    /// content-addressed, so their fingerprint needs it at open) and `None`
    /// for client/bidi streams, which are counter-addressed.
    ///
    /// Client/bidi opens consume the session's occurrence counter here —
    /// identically in record and replay mode — so a cassette miss or shape
    /// mismatch surfaces from this call. Server-stream misses surface here
    /// too, because the request message is already available.
    pub fn open(
        session: &'s Session,
        stream_type: StreamType,
        full_method: &str,
        open_msg: Option<&[u8]>,
    ) -> Result<Self, XrrError> {
        let (service, method) = split_full_method(full_method)?;
        if stream_type == StreamType::Server && open_msg.is_none() {
            return Err(XrrError::Usage(
                "grpc: server streams are content-addressed; the open message is required".into(),
            ));
        }
        match session.mode() {
            Mode::Passthrough => Ok(GrpcStream::Passthrough),
            Mode::Record => {
                let open = stream_open(session, stream_type, service, method, open_msg);
                let rec = session.open_stream_record(open)?;
                Ok(GrpcStream::Record(RecordStream {
                    rec: Mutex::new(rec),
                    server_streams: stream_type != StreamType::Client,
                }))
            }
            Mode::Replay => {
                let open = stream_open(session, stream_type, service, method, open_msg);
                let rp = session.open_stream_replay(open)?;
                Ok(GrpcStream::Replay(ReplayStream { rp: Mutex::new(rp) }))
            }
        }
    }

    /// True when this stream serves a recording and must never touch the
    /// network.
    pub fn is_replay(&self) -> bool {
        matches!(self, GrpcStream::Replay(_))
    }
}

// ── record ───────────────────────────────────────────────────────────────────

/// Tees every observed message of a live stream into a cassette pair.
pub struct RecordStream<'s> {
    rec: Mutex<StreamRecording<'s>>,
    /// Client-streaming RPCs complete on their single response message;
    /// server/bidi RPCs complete at end-of-stream.
    server_streams: bool,
}

impl RecordStream<'_> {
    /// The open-time fingerprint of this interaction.
    pub fn fingerprint(&self) -> String {
        self.rec
            .lock()
            .expect("recording lock")
            .fingerprint()
            .to_string()
    }

    /// Log one client→server message, observed on the live stream.
    pub fn send(&self, message: &[u8]) {
        self.rec
            .lock()
            .expect("recording lock")
            .record_send(message);
    }

    /// Log one server→client message, observed on the live stream.
    pub fn recv(&self, message: &[u8]) {
        self.rec
            .lock()
            .expect("recording lock")
            .record_recv(message);
    }

    /// Log the client closing its send side.
    pub fn close_send(&self) {
        self.rec.lock().expect("recording lock").record_half_close();
    }

    /// Whether this RPC shape completes at end-of-stream rather than on its
    /// single response message.
    pub fn server_streams(&self) -> bool {
        self.server_streams
    }

    /// Record an OK terminal and persist the pair.
    pub fn finish_ok(&self) -> Result<(), XrrError> {
        self.finish(OK_CODE, None)
    }

    /// Record an error terminal and persist the pair. The spec invariant —
    /// envelope `error` non-empty iff `status_code != 0` — is enforced
    /// here: an error terminal reported as OK is rewritten to `Unknown`,
    /// and an empty status message is synthesized into a non-empty string.
    pub fn finish_err(&self, status: &Status) -> Result<(), XrrError> {
        let code = status.code() as i32;
        let rendered = format!(
            "rpc error: code = {:?} desc = {}",
            status.code(),
            status.message()
        );
        self.finish(code, Some(rendered))
    }

    fn finish(&self, code: i32, err: Option<String>) -> Result<(), XrrError> {
        let (code, err) = match err {
            Some(e) => {
                let code = if code == OK_CODE { UNKNOWN_CODE } else { code };
                let e = if e.is_empty() {
                    "grpc: stream failed".to_string()
                } else {
                    e
                };
                (code, Some(e))
            }
            None => (code, None),
        };
        self.rec
            .lock()
            .expect("recording lock")
            .finish(resp_payload(code), err.as_deref())
    }
}

// ── replay ───────────────────────────────────────────────────────────────────

/// Serves one recorded streamed interaction: no network, no live stream.
pub struct ReplayStream {
    rp: Mutex<StreamReplay>,
}

impl ReplayStream {
    /// The open-time fingerprint of this interaction.
    pub fn fingerprint(&self) -> String {
        self.rp
            .lock()
            .expect("replay lock")
            .fingerprint()
            .to_string()
    }

    /// Validate the i-th client message against recorded send frame i.
    ///
    /// Divergent bytes at `i < S` are a stream mismatch. At `i >= S` the
    /// recording was already past its last observed send: with an OK
    /// terminal this returns `Ok(())` (tonic's post-completion sends are
    /// silently dropped rather than surfaced, and the recorder drops
    /// post-terminal sends too, so treating them as mismatches would fail
    /// correct flow-controlled producers); with an error terminal the
    /// recorded error is returned, because the real stream was dead too.
    pub fn send(&self, message: &[u8]) -> Result<(), Status> {
        let mut rp = self.rp.lock().expect("replay lock");
        match rp.send(message) {
            Ok(()) => Ok(()),
            // Post-completion send against an OK terminal: stream-done, not
            // a failure, and it must not poison the recv side.
            Err(XrrError::StreamEnd) => Ok(()),
            Err(e) => Err(map_core_err(e, rp.resp_payload())),
        }
    }

    /// Validate the client closing its send side: accepted once all
    /// recorded sends were observed, a stream mismatch after fewer.
    pub fn close_send(&self) -> Result<(), Status> {
        let mut rp = self.rp.lock().expect("replay lock");
        match rp.half_close() {
            Ok(()) => Ok(()),
            Err(e) => Err(map_core_err(e, rp.resp_payload())),
        }
    }

    /// Deliver the next recorded recv frame's decoded bytes. `Ok(None)` is
    /// end-of-stream; it repeats indefinitely, as does a recorded error.
    /// Never blocks on send-side progress.
    pub fn recv(&self) -> Result<Option<Vec<u8>>, Status> {
        let mut rp = self.rp.lock().expect("replay lock");
        match rp.recv() {
            Ok(bytes) => Ok(Some(bytes)),
            Err(XrrError::StreamEnd) => Ok(None),
            Err(e) => Err(map_core_err(e, rp.resp_payload())),
        }
    }

    /// The recorded terminal status code.
    pub fn status_code(&self) -> i32 {
        status_code_from(self.rp.lock().expect("replay lock").resp_payload())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn splits_full_method() {
        assert_eq!(
            split_full_method("/files.FileService/Download").unwrap(),
            ("files.FileService", "Download")
        );
        assert_eq!(
            split_full_method("files.FileService/Download").unwrap(),
            ("files.FileService", "Download")
        );
    }

    #[test]
    fn rejects_malformed_full_method() {
        for bad in ["/", "/Download", "files.FileService/", "nomethod"] {
            assert!(split_full_method(bad).is_err(), "{bad:?} must be rejected");
        }
    }

    #[test]
    fn wire_round_trip_is_deterministic() {
        let msg = "hello".to_string();
        let a = to_wire(&msg);
        let b = to_wire(&msg);
        assert_eq!(a, b, "prost encoding must be byte-stable");
        assert_eq!(from_wire::<String>(&a).unwrap(), "hello");
    }

    #[test]
    fn status_code_absent_is_unknown() {
        let payload: Value = serde_yaml::from_str("{}").unwrap();
        assert_eq!(status_code_from(&payload), UNKNOWN_CODE);
    }

    #[test]
    fn recorded_status_unwraps_standard_rendering() {
        let s = recorded_status(14, "rpc error: code = Unavailable desc = connection reset");
        assert_eq!(s.code(), tonic::Code::Unavailable);
        assert_eq!(s.message(), "connection reset");
    }

    #[test]
    fn recorded_status_never_ok() {
        // A hand-authored cassette claiming an OK error terminal violates
        // the spec invariant; it must still surface as a failure.
        let s = recorded_status(OK_CODE, "something went wrong");
        assert_ne!(s.code(), tonic::Code::Ok);
        assert_eq!(s.code(), tonic::Code::Unknown);
    }
}
