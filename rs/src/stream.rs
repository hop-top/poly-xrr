//! Streamed-interaction format layer (spec/cassette-format-streaming.md).
//!
//! Parse, validate, and emit the v1 `stream` envelope extension. Format
//! layer only — adapter replay semantics (gRPC) are out of scope for this
//! port. Serde stays permissive on unknown fields per the v1 forward-compat
//! guarantee: parsing extracts known keys from a `serde_yaml::Value` and
//! ignores the rest.

use std::collections::HashSet;
use std::path::Path;

use serde_yaml::Value;

use crate::{b64, error::XrrError};

// ── model ────────────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StreamType {
    Server,
    Client,
    Bidi,
}

impl StreamType {
    pub fn as_str(&self) -> &'static str {
        match self {
            StreamType::Server => "server",
            StreamType::Client => "client",
            StreamType::Bidi => "bidi",
        }
    }

    fn parse(s: &str) -> Option<Self> {
        match s {
            "server" => Some(StreamType::Server),
            "client" => Some(StreamType::Client),
            "bidi" => Some(StreamType::Bidi),
            _ => None,
        }
    }
}

/// Which wire encoding a frame's message used (or should use on emit).
/// Fingerprints and comparisons always operate on decoded bytes; the
/// encoding choice is free on re-emit.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MessageEncoding {
    B64,
    Text,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Frame {
    pub seq: u64,
    /// Decoded message bytes — the canonical form for hashing and equality.
    pub bytes: Vec<u8>,
    pub encoding: MessageEncoding,
    pub at_ms: Option<u64>,
}

/// A positioned scalar event (`half_close`, `end`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StreamEvent {
    pub seq: u64,
    pub at_ms: Option<u64>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ReqStream {
    pub stream_type: StreamType,
    pub frames: Vec<Frame>,
    pub half_close: Option<StreamEvent>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct RespStream {
    pub frames: Vec<Frame>,
    pub end: StreamEvent,
}

#[derive(Debug, Clone, PartialEq)]
pub struct StreamedReq {
    pub adapter: String,
    pub fingerprint: String,
    pub recorded_at: String,
    pub payload: Value,
    pub stream: ReqStream,
}

#[derive(Debug, Clone, PartialEq)]
pub struct StreamedResp {
    pub adapter: String,
    pub fingerprint: String,
    pub recorded_at: String,
    /// v1 envelope `error` field: non-empty ⇔ the stream terminated with an
    /// error (mid-stream errors are N recv frames, then `end` + this field).
    pub error: Option<String>,
    pub payload: Value,
    pub stream: RespStream,
}

/// One streamed interaction: the `.req.yaml` / `.resp.yaml` pair, loaded
/// and validated together.
#[derive(Debug, Clone, PartialEq)]
pub struct StreamedPair {
    pub req: StreamedReq,
    pub resp: StreamedResp,
}

// ── parse ────────────────────────────────────────────────────────────────────

fn invalid(msg: impl Into<String>) -> XrrError {
    XrrError::InvalidStream(msg.into())
}

fn get_str(map: &Value, key: &str, ctx: &str) -> Result<String, XrrError> {
    match map.get(key) {
        Some(Value::String(s)) => Ok(s.clone()),
        Some(_) => Err(invalid(format!("{ctx}: {key} must be a string"))),
        None => Err(invalid(format!("{ctx}: missing {key}"))),
    }
}

fn get_seq(map: &Value, key: &str, ctx: &str) -> Result<u64, XrrError> {
    map.get(key)
        .ok_or_else(|| invalid(format!("{ctx}: missing {key}")))?
        .as_u64()
        .ok_or_else(|| invalid(format!("{ctx}: {key} must be a non-negative integer")))
}

fn get_at_ms(map: &Value, ctx: &str) -> Result<Option<u64>, XrrError> {
    match map.get("at_ms") {
        None => Ok(None),
        Some(v) => v
            .as_u64()
            .map(Some)
            .ok_or_else(|| invalid(format!("{ctx}: at_ms must be a non-negative integer"))),
    }
}

fn parse_event(v: &Value, ctx: &str) -> Result<StreamEvent, XrrError> {
    Ok(StreamEvent {
        seq: get_seq(v, "seq", ctx)?,
        at_ms: get_at_ms(v, ctx)?,
    })
}

/// Decode `message_text` as a string regardless of YAML scalar resolution:
/// quoted scalars arrive as strings; a non-normative unquoted scalar that
/// resolved to a bool/number is coerced back to its textual form.
fn message_text_string(v: &Value, ctx: &str) -> Result<String, XrrError> {
    match v {
        Value::String(s) => Ok(s.clone()),
        Value::Bool(b) => Ok(b.to_string()),
        Value::Number(n) => Ok(n.to_string()),
        _ => Err(invalid(format!("{ctx}: message_text must be a string"))),
    }
}

fn parse_frame(v: &Value, ctx: &str) -> Result<Frame, XrrError> {
    let seq = get_seq(v, "seq", ctx)?;
    let b64_val = v.get("message_b64");
    let text_val = v.get("message_text");
    let (bytes, encoding) = match (b64_val, text_val) {
        (Some(b), None) => {
            let s = match b {
                Value::String(s) => s,
                _ => return Err(invalid(format!("{ctx}: message_b64 must be a string"))),
            };
            let bytes = b64::decode(s)
                .map_err(|e| invalid(format!("{ctx}: invalid base64: {e}")))?;
            (bytes, MessageEncoding::B64)
        }
        (None, Some(t)) => {
            let s = message_text_string(t, ctx)?;
            (s.into_bytes(), MessageEncoding::Text)
        }
        (Some(_), Some(_)) => {
            return Err(invalid(format!(
                "{ctx}: exactly one of message_b64/message_text required, both present"
            )))
        }
        (None, None) => {
            return Err(invalid(format!(
                "{ctx}: exactly one of message_b64/message_text required, neither present"
            )))
        }
    };
    Ok(Frame { seq, bytes, encoding, at_ms: get_at_ms(v, ctx)? })
}

/// Absent `frames` key reads as `[]` per spec; the list must be strictly
/// ascending in `seq`.
fn parse_frames(stream: &Value, ctx: &str) -> Result<Vec<Frame>, XrrError> {
    let list = match stream.get("frames") {
        None | Some(Value::Null) => return Ok(Vec::new()),
        Some(Value::Sequence(list)) => list,
        Some(_) => return Err(invalid(format!("{ctx}: frames must be a list"))),
    };
    let mut frames = Vec::with_capacity(list.len());
    for (i, item) in list.iter().enumerate() {
        frames.push(parse_frame(item, &format!("{ctx} frame {i}"))?);
    }
    for pair in frames.windows(2) {
        if pair[1].seq <= pair[0].seq {
            return Err(invalid(format!(
                "{ctx}: frames not strictly ascending in seq ({} then {})",
                pair[0].seq, pair[1].seq
            )));
        }
    }
    Ok(frames)
}

struct RawFile {
    adapter: String,
    fingerprint: String,
    recorded_at: String,
    error: Option<String>,
    payload: Value,
    stream: Option<Value>,
}

fn parse_file(yaml: &str, kind: &str) -> Result<RawFile, XrrError> {
    let root: Value = serde_yaml::from_str(yaml)?;
    let xrr = get_str(&root, "xrr", kind)?;
    if xrr != "1" {
        return Err(invalid(format!("{kind}: unsupported xrr version {xrr:?}")));
    }
    let payload = match root.get("payload") {
        Some(p @ Value::Mapping(_)) => p.clone(),
        Some(_) => return Err(invalid(format!("{kind}: payload must be an object"))),
        None => return Err(invalid(format!("{kind}: missing payload"))),
    };
    let error = match root.get("error") {
        Some(Value::String(s)) if kind == "resp" => Some(s.clone()),
        _ => None, // absent, or misplaced on req (writer bug — v1 says ignore)
    };
    Ok(RawFile {
        adapter: get_str(&root, "adapter", kind)?,
        fingerprint: get_str(&root, "fingerprint", kind)?,
        recorded_at: get_str(&root, "recorded_at", kind)?,
        error,
        payload,
        stream: root.get("stream").cloned(),
    })
}

fn parse_req_stream(v: &Value) -> Result<ReqStream, XrrError> {
    let type_str = get_str(v, "type", "req stream")?;
    let stream_type = StreamType::parse(&type_str)
        .ok_or_else(|| invalid(format!("req stream: type must be server/client/bidi, got {type_str:?}")))?;
    let half_close = match v.get("half_close") {
        None | Some(Value::Null) => None,
        Some(hc) => Some(parse_event(hc, "req half_close")?),
    };
    Ok(ReqStream { stream_type, frames: parse_frames(v, "req")?, half_close })
}

fn parse_resp_stream(v: &Value) -> Result<RespStream, XrrError> {
    let end = match v.get("end") {
        None | Some(Value::Null) => return Err(invalid("resp stream: missing end event")),
        Some(e) => parse_event(e, "resp end")?,
    };
    Ok(RespStream { frames: parse_frames(v, "resp")?, end })
}

impl StreamedPair {
    /// Parse and validate a streamed pair from raw YAML. A pair where
    /// neither file carries `stream` is a shape mismatch (unary cassette
    /// loaded through the streaming path); one-sided `stream` is malformed.
    pub fn parse(req_yaml: &str, resp_yaml: &str) -> Result<Self, XrrError> {
        let req_raw = parse_file(req_yaml, "req")?;
        let resp_raw = parse_file(resp_yaml, "resp")?;
        let (req_stream, resp_stream) = match (&req_raw.stream, &resp_raw.stream) {
            (Some(r), Some(p)) => (parse_req_stream(r)?, parse_resp_stream(p)?),
            (None, None) => {
                return Err(XrrError::ShapeMismatch(
                    "unary pair (no stream field) loaded through the streaming path".into(),
                ))
            }
            _ => {
                return Err(invalid(
                    "stream present on one file of the pair but not the other",
                ))
            }
        };
        let pair = StreamedPair {
            req: StreamedReq {
                adapter: req_raw.adapter,
                fingerprint: req_raw.fingerprint,
                recorded_at: req_raw.recorded_at,
                payload: req_raw.payload,
                stream: req_stream,
            },
            resp: StreamedResp {
                adapter: resp_raw.adapter,
                fingerprint: resp_raw.fingerprint,
                recorded_at: resp_raw.recorded_at,
                error: resp_raw.error,
                payload: resp_raw.payload,
                stream: resp_stream,
            },
        };
        pair.validate_seqs()?;
        Ok(pair)
    }

    /// Pair-level `seq` rules: no duplicates across the pair, `end.seq`
    /// is the interaction maximum. (Per-list ascending order is enforced
    /// at parse; sparse numbering is reader-accepted per spec.)
    fn validate_seqs(&self) -> Result<(), XrrError> {
        let mut all: Vec<u64> = Vec::new();
        all.extend(self.req.stream.frames.iter().map(|f| f.seq));
        all.extend(self.resp.stream.frames.iter().map(|f| f.seq));
        if let Some(hc) = &self.req.stream.half_close {
            all.push(hc.seq);
        }
        all.push(self.resp.stream.end.seq);

        let mut seen = HashSet::new();
        for seq in &all {
            if !seen.insert(seq) {
                return Err(invalid(format!("duplicate seq {seq} across the pair")));
            }
        }
        let max = *all.iter().max().expect("end always present");
        if self.resp.stream.end.seq != max {
            return Err(invalid(format!(
                "end.seq {} is not the maximum seq in the pair ({max})",
                self.resp.stream.end.seq
            )));
        }
        Ok(())
    }

    /// Load `<adapter>-<fingerprint>.{req,resp}.yaml` from `dir`, parse and
    /// validate. A missing file is a cassette miss, exactly as v1.
    pub fn load(dir: &Path, adapter: &str, fingerprint: &str) -> Result<Self, XrrError> {
        let read = |kind: &str| -> Result<String, XrrError> {
            let path = dir.join(format!("{adapter}-{fingerprint}.{kind}.yaml"));
            std::fs::read_to_string(&path).map_err(|e| {
                if e.kind() == std::io::ErrorKind::NotFound {
                    XrrError::CassetteMiss {
                        adapter: adapter.into(),
                        fingerprint: fingerprint.into(),
                    }
                } else {
                    XrrError::Io(e)
                }
            })
        };
        Self::parse(&read("req")?, &read("resp")?)
    }

    /// Write both files of the pair into `dir` (v1 naming, last-write-wins).
    pub fn save(&self, dir: &Path) -> Result<(), XrrError> {
        let base = format!("{}-{}", self.req.adapter, self.req.fingerprint);
        std::fs::write(dir.join(format!("{base}.req.yaml")), self.emit_req())?;
        std::fs::write(dir.join(format!("{base}.resp.yaml")), self.emit_resp())?;
        Ok(())
    }

    /// Serialize the req side per the normative YAML rules: quoted
    /// fingerprint, quoted `message_text`, whitespace-free `message_b64`.
    pub fn emit_req(&self) -> String {
        let mut out = emit_envelope_head(
            &self.req.adapter,
            &self.req.fingerprint,
            &self.req.recorded_at,
            None,
            &self.req.payload,
        );
        out.push_str("stream:\n");
        out.push_str(&format!("  type: {}\n", self.req.stream.stream_type.as_str()));
        emit_frames(&mut out, &self.req.stream.frames);
        if let Some(hc) = &self.req.stream.half_close {
            out.push_str("  half_close:\n");
            emit_event(&mut out, hc);
        }
        out
    }

    /// Serialize the resp side, including the v1 envelope `error` field
    /// when the stream terminated with an error.
    pub fn emit_resp(&self) -> String {
        let mut out = emit_envelope_head(
            &self.resp.adapter,
            &self.resp.fingerprint,
            &self.resp.recorded_at,
            self.resp.error.as_deref(),
            &self.resp.payload,
        );
        out.push_str("stream:\n");
        emit_frames(&mut out, &self.resp.stream.frames);
        out.push_str("  end:\n");
        emit_event(&mut out, &self.resp.stream.end);
        out
    }
}

// ── emit helpers ─────────────────────────────────────────────────────────────

/// YAML double-quoted scalar — the mandatory style for `message_text` (and
/// used for every envelope string we own), immune to 1.1 scalar resolution.
fn yaml_dquote(s: &str) -> String {
    let mut out = String::with_capacity(s.len() + 2);
    out.push('"');
    for c in s.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\t' => out.push_str("\\t"),
            '\r' => out.push_str("\\r"),
            c if (c as u32) < 0x20 => out.push_str(&format!("\\u{:04x}", c as u32)),
            c => out.push(c),
        }
    }
    out.push('"');
    out
}

fn emit_envelope_head(
    adapter: &str,
    fingerprint: &str,
    recorded_at: &str,
    error: Option<&str>,
    payload: &Value,
) -> String {
    let mut out = String::new();
    out.push_str("xrr: \"1\"\n");
    out.push_str(&format!("adapter: {adapter}\n"));
    // Always quoted: all-digit fingerprints parse as int, leading-zero as
    // octal under YAML 1.1 readers — either way replay misses spuriously.
    out.push_str(&format!("fingerprint: {}\n", yaml_dquote(fingerprint)));
    out.push_str(&format!("recorded_at: {}\n", yaml_dquote(recorded_at)));
    if let Some(err) = error {
        out.push_str(&format!("error: {}\n", yaml_dquote(err)));
    }
    // Payload is adapter-defined; delegate its body to serde_yaml.
    let body = serde_yaml::to_string(payload).expect("payload serializes");
    if body.trim() == "{}" {
        out.push_str("payload: {}\n");
    } else {
        out.push_str("payload:\n");
        for line in body.lines() {
            out.push_str(&format!("  {line}\n"));
        }
    }
    out
}

fn emit_frames(out: &mut String, frames: &[Frame]) {
    if frames.is_empty() {
        out.push_str("  frames: []\n");
        return;
    }
    out.push_str("  frames:\n");
    for f in frames {
        out.push_str(&format!("    - seq: {}\n", f.seq));
        match f.encoding {
            // message_text only when the bytes are valid UTF-8; else b64.
            MessageEncoding::Text => match std::str::from_utf8(&f.bytes) {
                Ok(s) => out.push_str(&format!("      message_text: {}\n", yaml_dquote(s))),
                Err(_) => {
                    out.push_str(&format!("      message_b64: \"{}\"\n", b64::encode(&f.bytes)))
                }
            },
            MessageEncoding::B64 => {
                out.push_str(&format!("      message_b64: \"{}\"\n", b64::encode(&f.bytes)))
            }
        }
        if let Some(at_ms) = f.at_ms {
            out.push_str(&format!("      at_ms: {at_ms}\n"));
        }
    }
}

fn emit_event(out: &mut String, ev: &StreamEvent) {
    out.push_str(&format!("    seq: {}\n", ev.seq));
    if let Some(at_ms) = ev.at_ms {
        out.push_str(&format!("    at_ms: {at_ms}\n"));
    }
}

// ── fingerprinting + occurrence counters ─────────────────────────────────────

pub use crate::stream_fingerprint::{
    grpc_counter_fingerprint, grpc_server_fingerprint, msg_hash, StreamCounters,
};
