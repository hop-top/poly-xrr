//! Session-level streamed record/replay machinery
//! (spec/cassette-format-streaming.md, Record Semantics + Matching and
//! Replay Semantics). Adapters build their stream wrappers on top of these
//! handles.
//!
//! Ownership model: `StreamRecording` borrows the session's cassette for
//! the write at finish, and both handles mutate through `&mut self` — the
//! aliasing rules give the single-writer guarantee the Go port needs a
//! mutex for. Callers that split send/recv across threads wrap a handle in
//! their own lock; the common single-threaded path stays lock-free.

use std::time::Instant;

use chrono::Utc;
use serde_yaml::{Mapping, Value};
use sha2::{Digest, Sha256};

use crate::{
    cassette::FileCassette,
    error::XrrError,
    session::{Mode, Session},
    stream::{
        stream_fingerprint, Frame, MessageEncoding, ReqStream, RespStream, StreamEvent,
        StreamOpen, StreamType, StreamedPair, StreamedReq, StreamedResp,
    },
    stream_scrub::{scrub_frame, StreamDirection, StreamScrub, StreamScrubInfo},
};

impl Session {
    fn check_stream_open(&self, open: &StreamOpen, want: Mode, verb: &str) -> Result<(), XrrError> {
        if self.mode() != want {
            return Err(XrrError::Usage(format!(
                "{verb} requires {want:?} mode (session is {:?})",
                self.mode()
            )));
        }
        if open.adapter_id.is_empty() {
            return Err(XrrError::Usage(format!("{verb} requires an adapter id")));
        }
        Ok(())
    }

    /// Open-time fingerprint, consuming the session's occurrence counter
    /// for counter-addressed opens — hit or miss, record and replay alike.
    fn stream_open_fingerprint(&self, open: &StreamOpen) -> Result<(String, Option<u64>), XrrError> {
        let n = if open.counter {
            Some(self.stream_counters().next_open(open)?)
        } else {
            None
        };
        Ok((stream_fingerprint(open, n)?, n))
    }

    /// Open a streamed interaction for recording. The adapter observes the
    /// live stream and mirrors it into the returned recording:
    /// `record_send`/`record_recv` per message, `record_half_close` when
    /// the client closes its send side, then `finish` exactly once when the
    /// terminal is observed — only `finish` persists the pair, so a stream
    /// that never reaches terminal produces no cassette.
    pub fn open_stream_record(&self, open: StreamOpen) -> Result<StreamRecording<'_>, XrrError> {
        self.check_stream_open(&open, Mode::Record, "open_stream_record")?;
        let (fingerprint, n) = self.stream_open_fingerprint(&open)?;
        let mut req_payload = open.payload;
        if let Some(n) = n {
            // Informational occurrence ordinal: recoverable from disk,
            // never read back to drive matching.
            req_payload.insert(Value::String("n".into()), Value::Number(n.into()));
        }
        let scrub_info = StreamScrubInfo {
            adapter_id: open.adapter_id.clone(),
            stream_type: open.stream_type,
        };
        Ok(StreamRecording {
            cassette: self.cassette(),
            scrub: self.stream_scrub().cloned(),
            scrub_info,
            adapter_id: open.adapter_id,
            fingerprint,
            stream_type: open.stream_type,
            req_payload,
            opened: Instant::now(),
            seq: 0,
            sends: Vec::new(),
            recvs: Vec::new(),
            half_close: None,
            finished: false,
        })
    }

    /// Locate the cassette pair for a streamed open and return a replay
    /// handle. No pair ⇒ `CassetteMiss`; a unary pair or a recorded stream
    /// type diverging from the requested one ⇒ `ShapeMismatch`.
    pub fn open_stream_replay(&self, open: StreamOpen) -> Result<StreamReplay, XrrError> {
        self.check_stream_open(&open, Mode::Replay, "open_stream_replay")?;
        let (fingerprint, _n) = self.stream_open_fingerprint(&open)?;
        let pair = self.cassette().load_stream(&open.adapter_id, &fingerprint)?;
        if pair.req.stream.stream_type != open.stream_type {
            return Err(XrrError::ShapeMismatch(format!(
                "recorded stream type {}, requested {}",
                pair.req.stream.stream_type.as_str(),
                open.stream_type.as_str()
            )));
        }
        Ok(StreamReplay {
            fingerprint,
            pair,
            send_idx: 0,
            recv_idx: 0,
            mismatch: None,
            scrub: self.stream_scrub().cloned(),
            scrub_info: StreamScrubInfo {
                adapter_id: open.adapter_id,
                stream_type: open.stream_type,
            },
        })
    }
}

// ── record path ──────────────────────────────────────────────────────────────

/// Accumulates the event log of one live stream and writes the cassette
/// pair at terminal. Events are stamped with `at_ms` (monotonic
/// milliseconds since open) and sequenced by one per-interaction counter in
/// arrival order.
pub struct StreamRecording<'s> {
    cassette: &'s FileCassette,
    /// Frame scrub hook: applied to every frame, both directions, before
    /// the bytes are persisted.
    scrub: Option<StreamScrub>,
    scrub_info: StreamScrubInfo,
    adapter_id: String,
    fingerprint: String,
    stream_type: StreamType,
    req_payload: Mapping,
    opened: Instant,

    seq: u64,
    sends: Vec<Frame>,
    recvs: Vec<Frame>,
    half_close: Option<StreamEvent>,
    finished: bool,
}

impl StreamRecording<'_> {
    /// The open-time fingerprint of this interaction.
    pub fn fingerprint(&self) -> &str {
        &self.fingerprint
    }

    fn elapsed_ms(&self) -> u64 {
        u64::try_from(self.opened.elapsed().as_millis()).unwrap_or(u64::MAX)
    }

    fn frame(&mut self, dir: StreamDirection, message: &[u8]) -> Frame {
        let frame = Frame {
            seq: self.seq,
            bytes: scrub_frame(self.scrub.as_ref(), dir, &self.scrub_info, message),
            encoding: MessageEncoding::B64,
            at_ms: Some(self.elapsed_ms()),
        };
        self.seq += 1;
        frame
    }

    /// Log one client→server message. Dropped after `finish`.
    pub fn record_send(&mut self, message: &[u8]) {
        if self.finished {
            return;
        }
        let frame = self.frame(StreamDirection::Send, message);
        self.sends.push(frame);
    }

    /// Log one server→client message. Dropped after `finish`.
    pub fn record_recv(&mut self, message: &[u8]) {
        if self.finished {
            return;
        }
        let frame = self.frame(StreamDirection::Recv, message);
        self.recvs.push(frame);
    }

    /// Log the client closing its send side. It occurs at most once;
    /// repeats and post-terminal calls are dropped, matching their
    /// real-world no-op.
    pub fn record_half_close(&mut self) {
        if self.finished || self.half_close.is_some() {
            return;
        }
        self.half_close = Some(StreamEvent { seq: self.seq, at_ms: Some(self.elapsed_ms()) });
        self.seq += 1;
    }

    /// Record the terminal event and persist the pair. `terminal_error` is
    /// `None` for an OK terminal; a non-empty message is persisted as the
    /// resp envelope `error` field so replay re-emits it. No events are
    /// recorded after `finish`, and calling it twice is an error.
    pub fn finish(
        &mut self,
        resp_payload: Mapping,
        terminal_error: Option<&str>,
    ) -> Result<(), XrrError> {
        if self.finished {
            return Err(XrrError::Usage("stream already finished".into()));
        }
        self.finished = true;

        let end = StreamEvent { seq: self.seq, at_ms: Some(self.elapsed_ms()) };
        self.seq += 1;

        let recorded_at = Utc::now().format("%Y-%m-%dT%H:%M:%SZ").to_string();
        let pair = StreamedPair {
            req: StreamedReq {
                adapter: self.adapter_id.clone(),
                fingerprint: self.fingerprint.clone(),
                recorded_at: recorded_at.clone(),
                payload: Value::Mapping(std::mem::take(&mut self.req_payload)),
                stream: ReqStream {
                    stream_type: self.stream_type,
                    frames: std::mem::take(&mut self.sends),
                    half_close: self.half_close.take(),
                },
            },
            resp: StreamedResp {
                adapter: self.adapter_id.clone(),
                fingerprint: self.fingerprint.clone(),
                recorded_at,
                error: terminal_error.filter(|e| !e.is_empty()).map(str::to_string),
                payload: Value::Mapping(resp_payload),
                stream: RespStream { frames: std::mem::take(&mut self.recvs), end },
            },
        };
        pair.validate_seqs()?;
        self.cassette.save_stream(&pair)
    }
}

// ── replay path ──────────────────────────────────────────────────────────────

#[derive(Debug)]
struct Mismatch {
    op: &'static str,
    ordinal: usize,
    detail: String,
}

/// Serves one recorded streamed interaction. Send-side events are validated
/// against the recording (order and bytes); recv-side frames are delivered
/// in `seq` order, never gated on send progress. Timing is ignored: frames
/// are delivered as fast as the client consumes them (`at_ms` stays
/// available on the loaded pair for a future opt-in replay-timing mode).
pub struct StreamReplay {
    fingerprint: String,
    pair: StreamedPair,

    send_idx: usize,
    recv_idx: usize,
    mismatch: Option<Mismatch>,
    /// Frame scrub hook: applied to LIVE send bytes before comparison.
    /// Recorded frames were already scrubbed at record time and are
    /// delivered verbatim — never re-scrubbed.
    scrub: Option<StreamScrub>,
    scrub_info: StreamScrubInfo,
}

// Hand-written: the scrub hook is a closure and cannot derive Debug.
impl std::fmt::Debug for StreamReplay {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StreamReplay")
            .field("fingerprint", &self.fingerprint)
            .field("pair", &self.pair)
            .field("send_idx", &self.send_idx)
            .field("recv_idx", &self.recv_idx)
            .field("mismatch", &self.mismatch)
            .field("scrub", &self.scrub.as_ref().map(|_| "<hook>"))
            .field("scrub_info", &self.scrub_info)
            .finish()
    }
}

impl StreamReplay {
    /// The open-time fingerprint of this interaction.
    pub fn fingerprint(&self) -> &str {
        &self.fingerprint
    }

    /// The recorded stream type.
    pub fn stream_type(&self) -> StreamType {
        self.pair.req.stream.stream_type
    }

    /// The recorded open-request payload.
    pub fn req_payload(&self) -> &Value {
        &self.pair.req.payload
    }

    /// The recorded terminal-response payload (for gRPC: the status code).
    /// Available from open — adapters typically read it at terminal
    /// delivery.
    pub fn resp_payload(&self) -> &Value {
        &self.pair.resp.payload
    }

    /// Terminal result: the recorded error when the resp envelope `error`
    /// is non-empty, the end-of-stream signal otherwise.
    fn terminal(&self) -> XrrError {
        match self.pair.resp.error.as_deref().filter(|e| !e.is_empty()) {
            Some(err) => XrrError::StreamRecordedError(err.to_string()),
            None => XrrError::StreamEnd,
        }
    }

    fn poisoned(&self) -> Option<XrrError> {
        self.mismatch.as_ref().map(|m| XrrError::StreamMismatch {
            op: m.op,
            ordinal: m.ordinal,
            detail: m.detail.clone(),
        })
    }

    fn fail(&mut self, op: &'static str, ordinal: usize, detail: String) -> XrrError {
        self.mismatch = Some(Mismatch { op, ordinal, detail });
        self.poisoned().expect("mismatch just set")
    }

    /// Validate the i-th client message against recorded send frame i:
    /// - `i < S`, equal bytes: accepted (the message is discarded).
    /// - `i < S`, divergent bytes: stream mismatch — terminal for the
    ///   handle.
    /// - `i ≥ S`: the recording was already past its last observed send.
    ///   With an OK terminal this returns `StreamEnd` (the post-completion
    ///   stream-done signal) and does NOT poison the recv side; with an
    ///   error terminal it returns the recorded error. Bytes at `i ≥ S` are
    ///   never compared.
    pub fn send(&mut self, message: &[u8]) -> Result<(), XrrError> {
        if let Some(err) = self.poisoned() {
            return Err(err);
        }
        let i = self.send_idx;
        if i >= self.pair.req.stream.frames.len() {
            return Err(self.terminal());
        }
        // Live send bytes pass through the same hook the recording was
        // written with, so a scrubbed cassette matches a scrubbed replay.
        let message = &scrub_frame(
            self.scrub.as_ref(),
            StreamDirection::Send,
            &self.scrub_info,
            message,
        )[..];
        if message != self.pair.req.stream.frames[i].bytes.as_slice() {
            let want = hex::encode(Sha256::digest(&self.pair.req.stream.frames[i].bytes));
            let got = hex::encode(Sha256::digest(message));
            return Err(self.fail(
                "send",
                i,
                format!("expected sha256 {want}, got sha256 {got}"),
            ));
        }
        self.send_idx += 1;
        Ok(())
    }

    /// Validate the client closing its send side: always accepted after all
    /// recorded sends were observed (whether or not the recording has
    /// `half_close`), a stream mismatch after fewer.
    pub fn half_close(&mut self) -> Result<(), XrrError> {
        if let Some(err) = self.poisoned() {
            return Err(err);
        }
        let s = self.pair.req.stream.frames.len();
        if self.send_idx < s {
            let detail = format!("half-close after {} sends, recording has {s}", self.send_idx);
            return Err(self.fail("half_close", self.send_idx, detail));
        }
        Ok(())
    }

    /// Deliver the j-th recorded recv frame's decoded bytes. At `j = R` it
    /// returns the terminal — the recorded error or `StreamEnd` — and
    /// repeats it for every later read. Never blocks on send-side progress.
    pub fn recv(&mut self) -> Result<Vec<u8>, XrrError> {
        if let Some(err) = self.poisoned() {
            return Err(err);
        }
        let frames = &self.pair.resp.stream.frames;
        if self.recv_idx >= frames.len() {
            return Err(self.terminal());
        }
        let message = frames[self.recv_idx].bytes.clone();
        self.recv_idx += 1;
        Ok(message)
    }
}
