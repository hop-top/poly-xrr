// Streamed-interaction format conformance (spec/cassette-format-streaming.md).
// Format layer only: parse / validate / emit — no gRPC adapter involved.
//
// Fixture obligations covered:
// - round-trip every streamed fixture pair (load → re-emit → reload → equal)
// - fingerprint recomputation matches filenames (grpc pairs)
// - malformed-b64 fixture rejected by path (not in manifest)
// - scalar-hazard message_text decoded to exact characters
// - empty frames lists, absent at_ms tolerated
// - mid-stream error surfaced from the v1 envelope error field
// - n=0/n=1 scripted two-open case via per-session occurrence counters

use std::path::{Path, PathBuf};

use serde::Deserialize;
use tempfile::TempDir;
use hop_top_xrr::{
    stream::{
        grpc_counter_fingerprint, grpc_server_fingerprint, MessageEncoding,
        StreamCounters, StreamType, StreamedPair,
    },
    FileCassette, Mode, Session, XrrError,
};

#[derive(Deserialize)]
struct Manifest {
    interactions: Vec<Interaction>,
}

#[derive(Deserialize)]
struct Interaction {
    adapter: String,
    fingerprint: String,
    #[serde(default)]
    streamed: bool,
}

fn fixtures_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../spec/fixtures")
}

fn load_manifest(dir: &Path) -> Manifest {
    let raw = std::fs::read_to_string(dir.join("manifest.yaml")).expect("read manifest");
    serde_yaml::from_str(&raw).expect("parse manifest")
}

fn streamed_dirs() -> Vec<PathBuf> {
    let mut dirs: Vec<PathBuf> = std::fs::read_dir(fixtures_root())
        .expect("read fixtures dir")
        .filter_map(|e| {
            let p = e.expect("dir entry").path();
            (p.is_dir() && p.join("manifest.yaml").exists()).then_some(p)
        })
        .filter(|p| load_manifest(p).interactions.iter().any(|i| i.streamed))
        .collect();
    dirs.sort();
    dirs
}

/// Field-for-field pair equality; messages compared over decoded bytes
/// (encoding choice is free on re-emit per spec).
fn assert_pair_eq(a: &StreamedPair, b: &StreamedPair, ctx: &str) {
    assert_eq!(a.req.adapter, b.req.adapter, "{ctx}: req adapter");
    assert_eq!(a.req.fingerprint, b.req.fingerprint, "{ctx}: req fingerprint");
    assert_eq!(a.req.recorded_at, b.req.recorded_at, "{ctx}: req recorded_at");
    assert_eq!(a.req.payload, b.req.payload, "{ctx}: req payload");
    assert_eq!(a.req.stream.stream_type, b.req.stream.stream_type, "{ctx}: type");
    assert_eq!(a.req.stream.half_close, b.req.stream.half_close, "{ctx}: half_close");
    assert_eq!(a.resp.adapter, b.resp.adapter, "{ctx}: resp adapter");
    assert_eq!(a.resp.fingerprint, b.resp.fingerprint, "{ctx}: resp fingerprint");
    assert_eq!(a.resp.recorded_at, b.resp.recorded_at, "{ctx}: resp recorded_at");
    assert_eq!(a.resp.payload, b.resp.payload, "{ctx}: resp payload");
    assert_eq!(a.resp.error, b.resp.error, "{ctx}: resp error");
    assert_eq!(a.resp.stream.end, b.resp.stream.end, "{ctx}: end");
    for (side, fa, fb) in [
        ("req", &a.req.stream.frames, &b.req.stream.frames),
        ("resp", &a.resp.stream.frames, &b.resp.stream.frames),
    ] {
        assert_eq!(fa.len(), fb.len(), "{ctx}: {side} frame count");
        for (x, y) in fa.iter().zip(fb.iter()) {
            assert_eq!(x.seq, y.seq, "{ctx}: {side} frame seq");
            assert_eq!(x.bytes, y.bytes, "{ctx}: {side} frame bytes (seq {})", x.seq);
            assert_eq!(x.at_ms, y.at_ms, "{ctx}: {side} frame at_ms (seq {})", x.seq);
        }
    }
}

// ── round-trip ───────────────────────────────────────────────────────────────

#[test]
fn streamed_fixtures_round_trip() {
    let mut total = 0usize;
    for dir in streamed_dirs() {
        for i in load_manifest(&dir).interactions.iter().filter(|i| i.streamed) {
            let ctx = format!("{}/{}-{}", dir.display(), i.adapter, i.fingerprint);
            let pair = StreamedPair::load(&dir, &i.adapter, &i.fingerprint)
                .unwrap_or_else(|e| panic!("{ctx}: load failed: {e}"));

            let tmp = TempDir::new().unwrap();
            pair.save(tmp.path()).unwrap_or_else(|e| panic!("{ctx}: save failed: {e}"));
            let reloaded = StreamedPair::load(tmp.path(), &i.adapter, &i.fingerprint)
                .unwrap_or_else(|e| panic!("{ctx}: reload failed: {e}"));

            assert_pair_eq(&pair, &reloaded, &ctx);
            total += 1;
        }
    }
    assert_eq!(total, 10, "expected 10 streamed pairs across fixtures");
}

// ── fingerprint recomputation ────────────────────────────────────────────────

#[test]
fn streamed_fingerprints_recompute_from_content() {
    for dir in streamed_dirs() {
        let manifest = load_manifest(&dir);
        let grpc: Vec<&Interaction> = manifest
            .interactions
            .iter()
            .filter(|i| i.streamed && i.adapter == "grpc")
            .collect();
        // Counter domain: one per simulated session (= one fixture dir here).
        let counters = StreamCounters::new();
        let mut pairs: Vec<StreamedPair> = grpc
            .iter()
            .map(|i| StreamedPair::load(&dir, &i.adapter, &i.fingerprint).unwrap())
            .collect();
        // Scripted open order for multi-pair dirs: ascending payload n.
        pairs.sort_by_key(|p| p.req.payload.get("n").and_then(|v| v.as_u64()).unwrap_or(0));

        for pair in &pairs {
            let service = pair.req.payload.get("service").unwrap().as_str().unwrap();
            let method = pair.req.payload.get("method").unwrap().as_str().unwrap();
            let recomputed = match pair.req.stream.stream_type {
                StreamType::Server => {
                    let msg = &pair.req.stream.frames[0].bytes;
                    grpc_server_fingerprint(service, method, msg)
                }
                t @ (StreamType::Client | StreamType::Bidi) => {
                    let n = counters.next(service, method, t);
                    grpc_counter_fingerprint(service, method, t, n).unwrap()
                }
            };
            assert_eq!(
                recomputed,
                pair.req.fingerprint,
                "{}: fingerprint mismatch",
                dir.display()
            );
        }
    }
}

// ── malformed base64 (must-reject, targeted by path) ─────────────────────────

#[test]
fn malformed_b64_fixture_rejected() {
    let dir = fixtures_root().join("grpc-stream-malformed-b64");
    let result = StreamedPair::load(&dir, "grpc", "8dbfb222");
    match result {
        Err(XrrError::InvalidStream(msg)) => {
            assert!(msg.contains("base64"), "reason should cite base64: {msg}")
        }
        other => panic!("expected InvalidStream, got {other:?}"),
    }
}

// ── scalar hazards ───────────────────────────────────────────────────────────

#[test]
fn scalar_hazard_text_frames_decode_exactly() {
    let dir = fixtures_root().join("sse-text-scalars");
    let pair = StreamedPair::load(&dir, "sse", "66ecc77a").unwrap();
    let want: [&[u8]; 6] = [
        b"on",
        b"12:30",
        b"null",
        b" leading",
        b"trailing ",
        b"  padded  ",
    ];
    let frames = &pair.resp.stream.frames;
    assert_eq!(frames.len(), want.len());
    for (frame, expected) in frames.iter().zip(want) {
        assert_eq!(frame.bytes, expected, "seq {}", frame.seq);
        assert_eq!(frame.encoding, MessageEncoding::Text, "seq {}", frame.seq);
    }
    // Re-emit MUST quote message_text scalars.
    let emitted = pair.emit_resp();
    for s in ["\"on\"", "\"12:30\"", "\"null\"", "\" leading\"", "\"trailing \"", "\"  padded  \""] {
        assert!(emitted.contains(s), "emitted resp must quote {s}:\n{emitted}");
    }
}

// ── empty frames / absent fields ─────────────────────────────────────────────

#[test]
fn empty_frames_parse() {
    let dir = fixtures_root().join("grpc-stream-empty");
    let bidi = StreamedPair::load(&dir, "grpc", "ebbd3938").unwrap();
    assert!(bidi.req.stream.frames.is_empty());
    assert!(bidi.resp.stream.frames.is_empty());
    assert_eq!(bidi.req.stream.half_close.as_ref().unwrap().seq, 0);
    assert_eq!(bidi.resp.stream.end.seq, 1);

    let client = StreamedPair::load(&dir, "grpc", "fbdff683").unwrap();
    assert!(client.req.stream.frames.is_empty());
    assert_eq!(client.resp.stream.frames.len(), 1);

    let server = StreamedPair::load(&dir, "grpc", "ffbc4bac").unwrap();
    assert!(server.resp.stream.frames.is_empty());
}

#[test]
fn absent_frames_treated_as_empty_and_absent_at_ms_tolerated() {
    let req = r#"
xrr: "1"
adapter: grpc
fingerprint: "ebbd3938"
recorded_at: "2026-08-23T12:00:00Z"
payload:
  service: chat.ChatService
  method: Ping
  n: 0
stream:
  type: bidi
  half_close:
    seq: 0
"#;
    let resp = r#"
xrr: "1"
adapter: grpc
fingerprint: "ebbd3938"
recorded_at: "2026-08-23T12:00:00Z"
payload:
  status_code: 0
stream:
  end:
    seq: 1
"#;
    let pair = StreamedPair::parse(req, resp).unwrap();
    assert!(pair.req.stream.frames.is_empty());
    assert!(pair.resp.stream.frames.is_empty());
    assert_eq!(pair.req.stream.half_close.as_ref().unwrap().at_ms, None);
    assert_eq!(pair.resp.stream.end.at_ms, None);
}

// ── mid-stream error (v1 envelope error field) ───────────────────────────────

#[test]
fn mid_stream_error_surfaces_envelope_error() {
    let dir = fixtures_root().join("grpc-stream-error");
    let pair = StreamedPair::load(&dir, "grpc", "9e8c4d4c").unwrap();
    assert_eq!(
        pair.resp.error.as_deref(),
        Some("rpc error: code = Unavailable desc = connection reset")
    );
    assert_eq!(
        pair.resp.payload.get("status_code").and_then(|v| v.as_u64()),
        Some(14)
    );
    assert_eq!(pair.resp.stream.frames.len(), 2);
    assert_eq!(pair.resp.stream.end.seq, 4);
    // Round-trip preserves the error field.
    let tmp = TempDir::new().unwrap();
    pair.save(tmp.path()).unwrap();
    let reloaded = StreamedPair::load(tmp.path(), "grpc", "9e8c4d4c").unwrap();
    assert_eq!(reloaded.resp.error, pair.resp.error);
}

// ── scripted two-open case (n=0 then n=1, one counter domain) ────────────────

#[test]
fn client_stream_repeat_two_open_scripted() {
    let dir = fixtures_root().join("grpc-client-stream-repeat");
    // One session object = one counter domain.
    let session = Session::new(Mode::Replay, FileCassette::new(&dir));
    let counters = session.stream_counters();

    let mut opened = Vec::new();
    for _ in 0..2 {
        let n = counters.next("files.FileService", "Upload", StreamType::Client);
        let fp =
            grpc_counter_fingerprint("files.FileService", "Upload", StreamType::Client, n)
                .unwrap();
        let pair = StreamedPair::load(&dir, "grpc", &fp)
            .unwrap_or_else(|e| panic!("open n={n} (fp {fp}) failed: {e}"));
        assert_eq!(
            pair.req.payload.get("n").and_then(|v| v.as_u64()),
            Some(n),
            "informational payload n should match occurrence"
        );
        opened.push(fp);
    }
    assert_eq!(opened, ["2bebfd6f", "b27b5fe1"]);

    // A different tuple in the same session counts independently.
    assert_eq!(counters.next("chat.ChatService", "Converse", StreamType::Bidi), 0);
    // Same tuple keeps counting.
    assert_eq!(counters.next("files.FileService", "Upload", StreamType::Client), 2);
}

// ── validation rules (spec: Validation Rules 1-7) ────────────────────────────

const REQ_OK: &str = r#"
xrr: "1"
adapter: grpc
fingerprint: "c6233d2e"
recorded_at: "2026-08-23T12:00:00Z"
payload:
  service: chat.ChatService
  method: Converse
  n: 0
stream:
  type: bidi
  frames:
    - seq: 0
      message_b64: "cGluZy0x"
      at_ms: 0
  half_close:
    seq: 2
    at_ms: 45
"#;

const RESP_OK: &str = r#"
xrr: "1"
adapter: grpc
fingerprint: "c6233d2e"
recorded_at: "2026-08-23T12:00:00Z"
payload:
  status_code: 0
stream:
  frames:
    - seq: 1
      message_b64: "cG9uZy0x"
      at_ms: 3
  end:
    seq: 3
    at_ms: 47
"#;

fn assert_invalid(req: &str, resp: &str, needle: &str) {
    match StreamedPair::parse(req, resp) {
        Err(XrrError::InvalidStream(msg)) => assert!(
            msg.contains(needle),
            "expected reason containing {needle:?}, got {msg:?}"
        ),
        other => panic!("expected InvalidStream({needle:?}), got {other:?}"),
    }
}

#[test]
fn valid_pair_parses() {
    StreamedPair::parse(REQ_OK, RESP_OK).unwrap();
}

#[test]
fn rejects_one_sided_stream() {
    let resp_no_stream = r#"
xrr: "1"
adapter: grpc
fingerprint: "c6233d2e"
recorded_at: "2026-08-23T12:00:00Z"
payload:
  status_code: 0
"#;
    assert_invalid(REQ_OK, resp_no_stream, "stream");
}

#[test]
fn unary_pair_is_shape_mismatch() {
    let req = r#"
xrr: "1"
adapter: exec
fingerprint: "a3f9c1b2"
recorded_at: "2026-08-23T12:00:00Z"
payload:
  argv: ["true"]
"#;
    let resp = r#"
xrr: "1"
adapter: exec
fingerprint: "a3f9c1b2"
recorded_at: "2026-08-23T12:00:00Z"
payload:
  exit_code: 0
"#;
    assert!(matches!(
        StreamedPair::parse(req, resp),
        Err(XrrError::ShapeMismatch(_))
    ));
}

#[test]
fn rejects_bad_stream_type() {
    let req = REQ_OK.replace("type: bidi", "type: duplex");
    assert_invalid(&req, RESP_OK, "type");
    let req = REQ_OK.replace("  type: bidi\n", "");
    assert_invalid(&req, RESP_OK, "type");
}

#[test]
fn rejects_frame_without_seq() {
    let req = REQ_OK.replace("    - seq: 0\n", "    - at_s: 0\n");
    assert_invalid(&req, RESP_OK, "seq");
}

#[test]
fn rejects_dual_and_absent_message_encoding() {
    let dual = RESP_OK.replace(
        "message_b64: \"cG9uZy0x\"",
        "message_b64: \"cG9uZy0x\"\n      message_text: \"pong-1\"",
    );
    assert_invalid(REQ_OK, &dual, "message");
    let neither = RESP_OK.replace("      message_b64: \"cG9uZy0x\"\n", "");
    assert_invalid(REQ_OK, &neither, "message");
}

#[test]
fn rejects_non_ascending_frames() {
    let req = r#"
xrr: "1"
adapter: grpc
fingerprint: "c6233d2e"
recorded_at: "2026-08-23T12:00:00Z"
payload: {}
stream:
  type: bidi
  frames:
    - seq: 2
      message_b64: "cGluZy0x"
    - seq: 0
      message_b64: "cGluZy0y"
"#;
    let resp = r#"
xrr: "1"
adapter: grpc
fingerprint: "c6233d2e"
recorded_at: "2026-08-23T12:00:00Z"
payload: {}
stream:
  frames: []
  end:
    seq: 3
"#;
    assert_invalid(req, resp, "ascending");
}

#[test]
fn rejects_duplicate_seq_across_pair() {
    // resp frame seq 1 collides with... make half_close collide with resp frame.
    let req = REQ_OK.replace("    seq: 2\n    at_ms: 45", "    seq: 1\n    at_ms: 45");
    assert_invalid(&req, RESP_OK, "duplicate");
}

#[test]
fn rejects_missing_end_and_non_maximal_end() {
    let no_end = r#"
xrr: "1"
adapter: grpc
fingerprint: "c6233d2e"
recorded_at: "2026-08-23T12:00:00Z"
payload: {}
stream:
  frames: []
"#;
    assert_invalid(REQ_OK, no_end, "end");

    let low_end = RESP_OK.replace("    seq: 3\n    at_ms: 47", "    seq: 1\n    at_ms: 47");
    // seq 1 duplicates the resp frame — use a resp without frames instead.
    let _ = low_end;
    let resp = r#"
xrr: "1"
adapter: grpc
fingerprint: "c6233d2e"
recorded_at: "2026-08-23T12:00:00Z"
payload: {}
stream:
  frames: []
  end:
    seq: 1
"#;
    // req has frames seq 0 + half_close seq 2 > end seq 1.
    assert_invalid(REQ_OK, resp, "end.seq");
}

#[test]
fn rejects_invalid_base64_variants() {
    // Length-valid strings with whitespace / out-of-alphabet bytes (the
    // strict-engine cases), plus a bad-length one.
    for bad in ["YmxvYi1jaHV ayAy", "cG9uZy0\t", "cG9uZy0!", "cG9uZy0"] {
        let resp = RESP_OK.replace("cG9uZy0x", bad);
        assert_invalid(REQ_OK, &resp, "base64");
    }
}

#[test]
fn ignores_unknown_fields_everywhere() {
    let req = REQ_OK
        .replace("stream:\n", "future_top: 1\nstream:\n  vendor_ext: true\n")
        .replace("      at_ms: 0\n", "      at_ms: 0\n      sse_event: tick\n")
        .replace("    at_ms: 45", "    at_ms: 45\n    reason: done");
    let resp = RESP_OK.replace("    at_ms: 47", "    at_ms: 47\n    grpc_status: 0");
    StreamedPair::parse(&req, &resp).unwrap();
}

// ── emit normative rules ─────────────────────────────────────────────────────

#[test]
fn emit_quotes_fingerprint_even_when_all_digits() {
    let req = REQ_OK.replace("\"c6233d2e\"", "\"12345678\"");
    let resp = RESP_OK.replace("\"c6233d2e\"", "\"12345678\"");
    let pair = StreamedPair::parse(&req, &resp).unwrap();
    assert!(pair.emit_req().contains("fingerprint: \"12345678\""));
    assert!(pair.emit_resp().contains("fingerprint: \"12345678\""));
}

#[test]
fn emit_base64_has_no_whitespace_and_events_keep_at_ms() {
    let pair = StreamedPair::parse(REQ_OK, RESP_OK).unwrap();
    let req = pair.emit_req();
    let resp = pair.emit_resp();
    assert!(req.contains("message_b64: \"cGluZy0x\""));
    assert!(resp.contains("message_b64: \"cG9uZy0x\""));
    assert!(req.contains("at_ms: 45"));
    assert!(resp.contains("at_ms: 47"));
    // Round-trip through parse to prove emitted text is valid per validation rules.
    StreamedPair::parse(&req, &resp).unwrap();
}
