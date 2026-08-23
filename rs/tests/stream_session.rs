// Session-level stream record/replay machinery
// (spec/cassette-format-streaming.md, Record Semantics + Matching and
// Replay Semantics). Mirrors go/stream_session_test.go.
//
// Coverage:
// - record round-trip: synthetic conversation → valid pair on disk, correct
//   filename, dense seq, monotonic at_ms, informational payload n
// - record: error terminal, post-finish no-op, double finish error
// - replay: ALL streamed fixture dirs driven through the session API
// - scripted n=0/n=1 two-open via one session (record and replay)
// - send-mismatch poisoning, short half-close, non-poisoning
//   post-completion send (both terminal kinds), terminal repeats,
// - mid-stream error, empty streams, miss vs shape mismatch, mode gates
// - sse acceptance: url-keyed identity reproduces 66ecc77a

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use hop_top_xrr::{
    stream::{msg_hash, stream_fingerprint, StreamOpen, StreamType, StreamedPair},
    FileCassette, Mode, Session, XrrError,
};
use serde_json::json;
use serde_yaml::Value;
use tempfile::TempDir;

// ── helpers ──────────────────────────────────────────────────────────────────

/// Mirrors the gRPC adapter's open definition: canonical inputs service +
/// method (+ msg_hash for content-addressed server streams),
/// counter-addressed client/bidi, req payload {service, method}.
fn grpc_open(t: StreamType, service: &str, method: &str, msg: Option<&[u8]>) -> StreamOpen {
    let mut identity = BTreeMap::new();
    identity.insert("service".to_string(), json!(service));
    identity.insert("method".to_string(), json!(method));
    let mut payload = serde_yaml::Mapping::new();
    payload.insert(Value::String("service".into()), Value::String(service.into()));
    payload.insert(Value::String("method".into()), Value::String(method.into()));
    let counter = t != StreamType::Server;
    if t == StreamType::Server {
        identity.insert(
            "msg_hash".to_string(),
            json!(msg_hash(msg.expect("server open carries its message"))),
        );
    }
    StreamOpen { adapter_id: "grpc".into(), stream_type: t, identity, counter, payload }
}

fn sse_open(url: &str) -> StreamOpen {
    let mut identity = BTreeMap::new();
    identity.insert("url".to_string(), json!(url));
    let mut payload = serde_yaml::Mapping::new();
    payload.insert(Value::String("url".into()), Value::String(url.into()));
    StreamOpen {
        adapter_id: "sse".into(),
        stream_type: StreamType::Server,
        identity,
        counter: false,
        payload,
    }
}

fn status_payload(code: i64) -> serde_yaml::Mapping {
    let mut payload = serde_yaml::Mapping::new();
    payload.insert(Value::String("status_code".into()), Value::Number(code.into()));
    payload
}

fn fixtures_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../spec/fixtures")
}

fn fixture_session(dir: &str) -> Session {
    Session::new(Mode::Replay, FileCassette::new(fixtures_root().join(dir)))
}

fn payload_n(pair: &StreamedPair) -> Option<u64> {
    pair.req.payload.get("n").and_then(|v| v.as_u64())
}

fn is_mismatch(e: &XrrError) -> bool {
    matches!(e, XrrError::StreamMismatch { .. })
}

// ── record path ──────────────────────────────────────────────────────────────

#[test]
fn record_server_stream_round_trip() {
    let tmp = TempDir::new().unwrap();
    let session = Session::new(Mode::Record, FileCassette::new(tmp.path()));
    let msg = br#"{"path":"/etc/hosts"}"#;
    let mut rec = session
        .open_stream_record(grpc_open(StreamType::Server, "files.FileService", "Download", Some(msg)))
        .unwrap();
    assert_eq!(rec.fingerprint(), "58a4bf3f");

    rec.record_send(msg);
    rec.record_half_close();
    rec.record_recv(b"chunk-one\n");
    rec.record_recv(b"chunk-two\n");
    rec.finish(status_payload(0), None).unwrap();

    // Correct v1 filenames on disk.
    assert!(tmp.path().join("grpc-58a4bf3f.req.yaml").exists());
    assert!(tmp.path().join("grpc-58a4bf3f.resp.yaml").exists());

    // load_stream validates: the pair on disk satisfies the spec rules.
    let pair = FileCassette::new(tmp.path()).load_stream("grpc", "58a4bf3f").unwrap();
    assert_eq!(pair.req.stream.stream_type, StreamType::Server);

    // Dense seq 0..N-1 counting all events in arrival order.
    assert_eq!(pair.req.stream.frames.len(), 1);
    assert_eq!(pair.req.stream.frames[0].seq, 0);
    assert_eq!(pair.req.stream.half_close.as_ref().unwrap().seq, 1);
    assert_eq!(pair.resp.stream.frames.len(), 2);
    assert_eq!(pair.resp.stream.frames[0].seq, 2);
    assert_eq!(pair.resp.stream.frames[1].seq, 3);
    assert_eq!(pair.resp.stream.end.seq, 4);
    assert_eq!(pair.resp.stream.frames[0].bytes, b"chunk-one\n");

    // at_ms stamped on every event, monotonic (non-decreasing) from open.
    let mut prev = 0u64;
    for f in pair.req.stream.frames.iter().chain(&pair.resp.stream.frames) {
        let at = f.at_ms.expect("recorder stamps at_ms on every frame");
        assert!(at >= prev, "at_ms not monotonic: {at} after {prev}");
        prev = at;
    }
    assert!(pair.req.stream.half_close.as_ref().unwrap().at_ms.is_some());
    assert!(pair.resp.stream.end.at_ms.is_some());

    // Server-stream payload carries no occurrence ordinal.
    assert_eq!(pair.req.payload.get("service").and_then(|v| v.as_str()), Some("files.FileService"));
    assert_eq!(pair.req.payload.get("method").and_then(|v| v.as_str()), Some("Download"));
    assert!(pair.req.payload.get("n").is_none());
}

// One Session object is one counter domain: two opens of the same
// (service, method, type) tuple record n=0 then n=1, matching the
// grpc-client-stream-repeat fixture fingerprints.
#[test]
fn record_counter_n_scripted_two_open() {
    let tmp = TempDir::new().unwrap();
    let session = Session::new(Mode::Record, FileCassette::new(tmp.path()));
    let open = || grpc_open(StreamType::Client, "files.FileService", "Upload", None);

    let mut rec1 = session.open_stream_record(open()).unwrap();
    assert_eq!(rec1.fingerprint(), "2bebfd6f");
    rec1.record_send(b"alpha\n");
    rec1.record_half_close();
    rec1.record_recv(br#"{"received_bytes":6}"#);
    rec1.finish(status_payload(0), None).unwrap();

    let mut rec2 = session.open_stream_record(open()).unwrap();
    assert_eq!(rec2.fingerprint(), "b27b5fe1");
    rec2.record_half_close();
    rec2.finish(status_payload(0), None).unwrap();

    // Informational occurrence ordinal recoverable from disk.
    let c = FileCassette::new(tmp.path());
    assert_eq!(payload_n(&c.load_stream("grpc", "2bebfd6f").unwrap()), Some(0));
    assert_eq!(payload_n(&c.load_stream("grpc", "b27b5fe1").unwrap()), Some(1));

    // A different tuple starts its own count.
    let rec3 = session
        .open_stream_record(grpc_open(StreamType::Bidi, "chat.ChatService", "Converse", None))
        .unwrap();
    assert_eq!(rec3.fingerprint(), "c6233d2e");
}

// No events are recorded after the terminal; a second finish is an error.
#[test]
fn record_post_finish_calls_no_op() {
    let tmp = TempDir::new().unwrap();
    let session = Session::new(Mode::Record, FileCassette::new(tmp.path()));
    let mut rec = session
        .open_stream_record(grpc_open(StreamType::Bidi, "chat.ChatService", "Converse", None))
        .unwrap();
    rec.record_send(b"ping-1");
    rec.record_recv(b"pong-1");
    rec.finish(status_payload(0), None).unwrap();

    // Dropped, matching the real-world no-op.
    rec.record_send(b"late");
    rec.record_recv(b"late");
    rec.record_half_close();
    assert!(rec.finish(status_payload(0), None).is_err(), "double finish");

    let pair = FileCassette::new(tmp.path()).load_stream("grpc", rec.fingerprint()).unwrap();
    assert_eq!(pair.req.stream.frames.len(), 1);
    assert_eq!(pair.resp.stream.frames.len(), 1);
    assert!(pair.req.stream.half_close.is_none());
    assert_eq!(pair.resp.stream.end.seq, 2);
}

#[test]
fn record_error_terminal() {
    let tmp = TempDir::new().unwrap();
    let session = Session::new(Mode::Record, FileCassette::new(tmp.path()));
    let msg = br#"{"path":"/var/log/big.log"}"#;
    let mut rec = session
        .open_stream_record(grpc_open(StreamType::Server, "files.FileService", "Download", Some(msg)))
        .unwrap();
    rec.record_send(msg);
    rec.record_half_close();
    rec.record_recv(b"log-chunk-1\n");
    rec.finish(
        status_payload(14),
        Some("rpc error: code = Unavailable desc = connection reset"),
    )
    .unwrap();

    let pair = FileCassette::new(tmp.path()).load_stream("grpc", "9e8c4d4c").unwrap();
    assert_eq!(
        pair.resp.error.as_deref(),
        Some("rpc error: code = Unavailable desc = connection reset")
    );
    assert_eq!(pair.resp.payload.get("status_code").and_then(|v| v.as_u64()), Some(14));
}

// ── replay path: full fixture corpus through the session API ─────────────────

#[derive(serde::Deserialize)]
struct Manifest {
    interactions: Vec<Interaction>,
}

#[derive(serde::Deserialize)]
struct Interaction {
    adapter: String,
    fingerprint: String,
    #[serde(default)]
    streamed: bool,
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

/// Rebuild the adapter-shaped open from a recorded pair.
fn open_for(pair: &StreamedPair) -> StreamOpen {
    let p = &pair.req.payload;
    match pair.req.adapter.as_str() {
        "grpc" => {
            let service = p.get("service").and_then(|v| v.as_str()).expect("service");
            let method = p.get("method").and_then(|v| v.as_str()).expect("method");
            let msg = (pair.req.stream.stream_type == StreamType::Server)
                .then(|| pair.req.stream.frames[0].bytes.clone());
            grpc_open(pair.req.stream.stream_type, service, method, msg.as_deref())
        }
        "sse" => sse_open(p.get("url").and_then(|v| v.as_str()).expect("url")),
        other => panic!("no open builder for adapter {other}"),
    }
}

// Every streamed fixture pair replays through the session API: recorded
// sends accepted in order, half-close after S sends accepted, recv frames
// delivered in seq order, then the terminal (recorded error or
// end-of-stream) which repeats indefinitely.
#[test]
fn replay_all_streamed_fixture_dirs() {
    let mut replayed = 0usize;
    for dir in streamed_dirs() {
        // One session per dir = one counter domain per scripted run.
        let session = Session::new(Mode::Replay, FileCassette::new(&dir));
        let mut pairs: Vec<(String, StreamedPair)> = load_manifest(&dir)
            .interactions
            .iter()
            .filter(|i| i.streamed)
            .map(|i| {
                (i.fingerprint.clone(), StreamedPair::load(&dir, &i.adapter, &i.fingerprint).unwrap())
            })
            .collect();
        // Scripted open order for same-tuple repeats: ascending payload n.
        pairs.sort_by_key(|(_, p)| payload_n(p).unwrap_or(0));

        for (fp, pair) in &pairs {
            let ctx = format!("{}/{fp}", dir.display());
            let mut rep = session
                .open_stream_replay(open_for(pair))
                .unwrap_or_else(|e| panic!("{ctx}: open failed: {e}"));
            assert_eq!(rep.fingerprint(), fp, "{ctx}: fingerprint");
            assert_eq!(rep.stream_type(), pair.req.stream.stream_type, "{ctx}: type");

            for f in &pair.req.stream.frames {
                rep.send(&f.bytes).unwrap_or_else(|e| panic!("{ctx}: send seq {}: {e}", f.seq));
            }
            rep.half_close().unwrap_or_else(|e| panic!("{ctx}: half_close: {e}"));
            for f in &pair.resp.stream.frames {
                let got = rep.recv().unwrap_or_else(|e| panic!("{ctx}: recv seq {}: {e}", f.seq));
                assert_eq!(got, f.bytes, "{ctx}: recv bytes at seq {}", f.seq);
            }
            // Terminal repeats for every read past R.
            let recorded_err = pair.resp.error.as_deref().filter(|e| !e.is_empty());
            for round in 0..2 {
                match (rep.recv(), recorded_err) {
                    (Err(XrrError::StreamEnd), None) => {}
                    (Err(XrrError::StreamRecordedError(msg)), Some(want)) => {
                        assert_eq!(msg, want, "{ctx}: recorded error text")
                    }
                    (other, want) => {
                        panic!("{ctx}: terminal round {round}: got {other:?}, want {want:?}")
                    }
                }
            }
            replayed += 1;
        }
    }
    assert_eq!(replayed, 10, "expected 10 streamed pairs across fixture dirs");
}

#[test]
fn sse_url_keyed_identity_reproduces_66ecc77a() {
    let open = sse_open("https://example.test/events");
    assert_eq!(stream_fingerprint(&open, None).unwrap(), "66ecc77a");

    let session = fixture_session("sse-text-scalars");
    let mut rep = session.open_stream_replay(open).unwrap();
    assert_eq!(rep.fingerprint(), "66ecc77a");
    assert_eq!(rep.recv().unwrap(), b"on");
    assert_eq!(rep.recv().unwrap(), b"12:30");
}

// ── replay semantics ─────────────────────────────────────────────────────────

// Reads never gate on send progress: drain both pongs before any send.
#[test]
fn replay_bidi_reads_never_block_on_sends() {
    let session = fixture_session("grpc-bidi-stream");
    let mut rep = session
        .open_stream_replay(grpc_open(StreamType::Bidi, "chat.ChatService", "Converse", None))
        .unwrap();
    assert_eq!(rep.fingerprint(), "c6233d2e");

    assert_eq!(rep.recv().unwrap(), b"pong-1");
    assert_eq!(rep.recv().unwrap(), b"pong-2");

    rep.send(b"ping-1").unwrap();
    rep.send(b"ping-2").unwrap();
    rep.half_close().unwrap();

    assert!(matches!(rep.recv(), Err(XrrError::StreamEnd)));
    assert!(matches!(rep.recv(), Err(XrrError::StreamEnd)), "terminal repeats for j > R");
}

#[test]
fn replay_send_mismatch_is_terminal() {
    let session = fixture_session("grpc-bidi-stream");
    let mut rep = session
        .open_stream_replay(grpc_open(StreamType::Bidi, "chat.ChatService", "Converse", None))
        .unwrap();

    rep.send(b"ping-1").unwrap();
    let err = rep.send(b"ping-DIVERGED").unwrap_err();
    match &err {
        XrrError::StreamMismatch { op, ordinal, detail } => {
            assert_eq!(*op, "send");
            assert_eq!(*ordinal, 1);
            assert!(detail.contains("sha256"), "detail identifies content: {detail}");
        }
        other => panic!("want StreamMismatch, got {other:?}"),
    }

    // Mismatch poisons every subsequent operation.
    assert!(is_mismatch(&rep.recv().unwrap_err()));
    assert!(is_mismatch(&rep.half_close().unwrap_err()));
    assert!(is_mismatch(&rep.send(b"ping-2").unwrap_err()));
}

#[test]
fn replay_short_half_close_is_mismatch() {
    let session = fixture_session("grpc-client-stream");
    let mut rep = session
        .open_stream_replay(grpc_open(StreamType::Client, "files.FileService", "Upload", None))
        .unwrap();

    rep.send(b"part-one\n").unwrap();
    let err = rep.half_close().unwrap_err();
    assert!(is_mismatch(&err), "half-close after 1 of 2 sends: {err}");
}

// Send at i >= S with an OK terminal is the non-poisoning stream-done
// signal; the recv side is unaffected.
#[test]
fn replay_post_completion_send_ok_terminal() {
    let session = fixture_session("grpc-client-stream");
    let mut rep = session
        .open_stream_replay(grpc_open(StreamType::Client, "files.FileService", "Upload", None))
        .unwrap();

    rep.send(b"part-one\n").unwrap();
    rep.send(b"part-two\n").unwrap();
    assert!(matches!(rep.send(b"part-three\n"), Err(XrrError::StreamEnd)));
    rep.half_close().expect("half-close after all recorded sends is always accepted");

    assert_eq!(rep.recv().unwrap(), br#"{"received_bytes":18}"#);
    assert!(matches!(rep.recv(), Err(XrrError::StreamEnd)));
}

// Recorded frames delivered, then the recorded error in place of
// end-of-stream; post-completion sends surface the same recorded error and
// do not poison the recv side.
#[test]
fn replay_mid_stream_error() {
    let session = fixture_session("grpc-stream-error");
    let msg = br#"{"path":"/var/log/big.log"}"#;
    let mut rep = session
        .open_stream_replay(grpc_open(StreamType::Server, "files.FileService", "Download", Some(msg)))
        .unwrap();
    assert_eq!(rep.fingerprint(), "9e8c4d4c");
    assert_eq!(rep.resp_payload().get("status_code").and_then(|v| v.as_u64()), Some(14));

    rep.send(msg).unwrap();
    rep.half_close().unwrap();

    // The recorded stream was already dead past its last send: the recorded
    // error returns from post-completion sends without poisoning recv.
    let want = "rpc error: code = Unavailable desc = connection reset";
    match rep.send(b"extra") {
        Err(XrrError::StreamRecordedError(msg)) => assert_eq!(msg, want),
        other => panic!("want recorded error, got {other:?}"),
    }

    assert_eq!(rep.recv().unwrap(), b"log-chunk-1\n");
    assert_eq!(rep.recv().unwrap(), b"log-chunk-2\n");
    for _ in 0..2 {
        match rep.recv() {
            Err(e @ XrrError::StreamRecordedError(_)) => {
                assert_eq!(e.to_string(), want, "recorded error re-emitted verbatim");
                assert!(!is_mismatch(&e));
            }
            other => panic!("want recorded error, got {other:?}"),
        }
    }
}

#[test]
fn replay_empty_streams() {
    // Server stream whose server sent nothing before OK.
    let session = fixture_session("grpc-stream-empty");
    let mut rep = session
        .open_stream_replay(grpc_open(
            StreamType::Server,
            "files.FileService",
            "Download",
            Some(br#"{"path":"/etc/empty"}"#),
        ))
        .unwrap();
    assert!(matches!(rep.recv(), Err(XrrError::StreamEnd)), "first read yields end-of-stream");

    // Client stream where the client half-closed immediately (S = 0).
    let session = fixture_session("grpc-stream-empty");
    let mut rep = session
        .open_stream_replay(grpc_open(StreamType::Client, "telemetry.MetricsService", "Push", None))
        .unwrap();
    rep.half_close().expect("S=0: immediate half-close accepted");
    assert_eq!(rep.recv().unwrap(), br#"{"count":0}"#);
    assert!(matches!(rep.recv(), Err(XrrError::StreamEnd)));

    // Bidi with no traffic at all.
    let session = fixture_session("grpc-stream-empty");
    let mut rep = session
        .open_stream_replay(grpc_open(StreamType::Bidi, "chat.ChatService", "Ping", None))
        .unwrap();
    rep.half_close().unwrap();
    assert!(matches!(rep.recv(), Err(XrrError::StreamEnd)));
}

// ── open-time errors ─────────────────────────────────────────────────────────

#[test]
fn replay_miss_vs_shape_mismatch() {
    let tmp = TempDir::new().unwrap();
    let session = Session::new(Mode::Replay, FileCassette::new(tmp.path()));

    // No pair on disk ⇒ cassette miss (consumes n=0).
    let err = session
        .open_stream_replay(grpc_open(StreamType::Bidi, "s", "m", None))
        .unwrap_err();
    assert!(matches!(err, XrrError::CassetteMiss { .. }), "got {err:?}");

    // A unary pair at the next streamed fingerprint (n=1) ⇒ shape mismatch,
    // not a miss.
    let fp = stream_fingerprint(&grpc_open(StreamType::Bidi, "s", "m", None), Some(1)).unwrap();
    FileCassette::new(tmp.path())
        .save("grpc", &fp, &json!({"service": "s", "method": "m"}), &json!({"status_code": 0}))
        .unwrap();
    let err = session
        .open_stream_replay(grpc_open(StreamType::Bidi, "s", "m", None))
        .unwrap_err();
    assert!(matches!(err, XrrError::ShapeMismatch(_)), "got {err:?}");
}

#[test]
fn replay_recorded_type_divergence_is_shape_mismatch() {
    // Hand-authored lie: the pair stored under the bidi fingerprint
    // declares type client.
    let dir = fixtures_root().join("grpc-bidi-stream");
    let mut pair = StreamedPair::load(&dir, "grpc", "c6233d2e").unwrap();
    pair.req.stream.stream_type = StreamType::Client;
    let tmp = TempDir::new().unwrap();
    pair.save(tmp.path()).unwrap();

    let session = Session::new(Mode::Replay, FileCassette::new(tmp.path()));
    let err = session
        .open_stream_replay(grpc_open(StreamType::Bidi, "chat.ChatService", "Converse", None))
        .unwrap_err();
    assert!(matches!(err, XrrError::ShapeMismatch(_)), "got {err:?}");
}

#[test]
fn open_mode_and_adapter_enforcement() {
    let tmp = TempDir::new().unwrap();
    let open = || grpc_open(StreamType::Bidi, "s", "m", None);

    let replay_session = Session::new(Mode::Replay, FileCassette::new(tmp.path()));
    assert_eq!(replay_session.mode(), Mode::Replay);
    assert!(matches!(replay_session.open_stream_record(open()), Err(XrrError::Usage(_))));

    let record_session = Session::new(Mode::Record, FileCassette::new(tmp.path()));
    assert!(matches!(record_session.open_stream_replay(open()), Err(XrrError::Usage(_))));

    let mut anonymous = open();
    anonymous.adapter_id = String::new();
    assert!(matches!(record_session.open_stream_record(anonymous), Err(XrrError::Usage(_))));
}

// Scripted two-open replay: one session object is one counter domain.
#[test]
fn replay_scripted_two_open_one_session() {
    let session = fixture_session("grpc-client-stream-repeat");
    let open = || grpc_open(StreamType::Client, "files.FileService", "Upload", None);

    let rep1 = session.open_stream_replay(open()).unwrap();
    assert_eq!(rep1.fingerprint(), "2bebfd6f");
    assert_eq!(rep1.req_payload().get("n").and_then(|v| v.as_u64()), Some(0));

    let rep2 = session.open_stream_replay(open()).unwrap();
    assert_eq!(rep2.fingerprint(), "b27b5fe1");
    assert_eq!(rep2.req_payload().get("n").and_then(|v| v.as_u64()), Some(1));
}
