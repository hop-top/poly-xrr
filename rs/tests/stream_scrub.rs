// Frame-level scrub hook — the normative contract in
// spec/cassette-format-streaming.md "Frame Scrub Hook".
//
// Secrets are rewritten on the DECODED bytes, identically at record and
// replay time. Symmetry is the correctness invariant: a cassette recorded
// through a scrub only replays green when the same scrub is active on the
// replaying session. Mirrors go/stream_scrub_test.go — these run without
// the `grpc` feature, so the core contract is covered by the default suite.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use hop_top_xrr::{
    stream::{msg_hash, stream_fingerprint, StreamOpen, StreamType},
    FileCassette, Mode, Session, StreamDirection, StreamScrub, StreamScrubInfo, XrrError,
};
use serde_json::json;
use serde_yaml::Value;
use tempfile::TempDir;

const SECRET: &str = "hunter2-FAKE-TOKEN-0123456789";
const MASK: &str = "<scrubbed>";

/// Deterministic scrub replacing the fake token wherever it appears.
fn mask_secret() -> StreamScrub {
    Arc::new(|_dir, _info, data: &[u8]| {
        String::from_utf8_lossy(data).replace(SECRET, MASK).into_bytes()
    })
}

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

/// Mirrors the gRPC adapter under the scrub contract: msg_hash is derived
/// from the SCRUBBED open-message bytes (spec clause 3).
fn scrubbed_server_open(s: &Session, service: &str, method: &str, msg: &[u8]) -> StreamOpen {
    let info = StreamScrubInfo {
        adapter_id: "grpc".to_string(),
        stream_type: StreamType::Server,
    };
    let scrubbed = s.scrub_stream_frame(StreamDirection::Send, &info, msg);
    grpc_open(StreamType::Server, service, method, Some(&scrubbed))
}

fn status_payload(code: i64) -> serde_yaml::Mapping {
    let mut m = serde_yaml::Mapping::new();
    m.insert(Value::String("status_code".into()), Value::Number(code.into()));
    m
}

fn is_mismatch(e: &XrrError) -> bool {
    matches!(e, XrrError::StreamMismatch { .. })
}

/// Clause 1 + 2: both directions scrubbed on the DECODED bytes before
/// persistence, so the secret reaches disk in no encoding.
#[test]
fn record_scrubs_both_directions() {
    let dir = TempDir::new().unwrap();
    let s = Session::with_stream_scrub(
        Mode::Record,
        FileCassette::new(dir.path()),
        mask_secret(),
    );
    let open = grpc_open(StreamType::Bidi, "chat.ChatService", "Converse", None);
    let mut rec = s.open_stream_record(open).unwrap();
    rec.record_send(format!("ping {SECRET}").as_bytes());
    rec.record_recv(format!("pong {SECRET}").as_bytes());
    rec.record_half_close();
    let fp = rec.fingerprint().to_string();
    rec.finish(status_payload(0), None).unwrap();

    let pair = FileCassette::new(dir.path()).load_stream("grpc", &fp).unwrap();
    assert_eq!(pair.req.stream.frames[0].bytes, format!("ping {MASK}").into_bytes());
    assert_eq!(pair.resp.stream.frames[0].bytes, format!("pong {MASK}").into_bytes());

    // Base64 hides the secret from a text scan, so the decoded check above
    // is the real gate; this guards the payload side.
    for kind in ["req", "resp"] {
        let raw =
            std::fs::read_to_string(dir.path().join(format!("grpc-{fp}.{kind}.yaml"))).unwrap();
        assert!(!raw.contains(SECRET), "{kind} envelope must not carry the secret");
    }
}

/// Clause 3: content-derived identity is computed over scrubbed bytes on
/// both sides, so a scrubbed replay resolves to the scrubbed recording.
#[test]
fn server_stream_identity_from_scrubbed_bytes() {
    let dir = TempDir::new().unwrap();
    let msg = format!(r#"{{"cmd":"deploy","token":"{SECRET}"}}"#).into_bytes();

    let rec_s = Session::with_stream_scrub(
        Mode::Record,
        FileCassette::new(dir.path()),
        mask_secret(),
    );
    let open = scrubbed_server_open(&rec_s, "ops.Deploy", "Run", &msg);
    let mut rec = rec_s.open_stream_record(open).unwrap();
    rec.record_send(&msg);
    rec.record_half_close();
    rec.record_recv(b"deployed");
    let fp = rec.fingerprint().to_string();
    rec.finish(status_payload(0), None).unwrap();

    // Self-consistency: recomputing the fingerprint from the persisted
    // (scrubbed) open frame reproduces the filename.
    let pair = FileCassette::new(dir.path()).load_stream("grpc", &fp).unwrap();
    let on_disk = &pair.req.stream.frames[0].bytes;
    assert!(!String::from_utf8_lossy(on_disk).contains(SECRET));
    let from_disk = stream_fingerprint(
        &grpc_open(StreamType::Server, "ops.Deploy", "Run", Some(on_disk)),
        None,
    )
    .unwrap();
    assert_eq!(from_disk, fp);

    let rep_s = Session::with_stream_scrub(
        Mode::Replay,
        FileCassette::new(dir.path()),
        mask_secret(),
    );
    let replay_open = scrubbed_server_open(&rep_s, "ops.Deploy", "Run", &msg);
    let mut rep = rep_s.open_stream_replay(replay_open).unwrap();
    assert_eq!(rep.fingerprint(), fp);
    rep.send(&msg).expect("live secret-bearing open matches after symmetric scrub");
    rep.half_close().unwrap();
    assert_eq!(rep.recv().unwrap(), b"deployed");
}

/// Clause 5: the same hook replays green; replaying without it fails loudly
/// rather than silently succeeding.
#[test]
fn replay_symmetry_is_required() {
    let dir = TempDir::new().unwrap();
    let open = || grpc_open(StreamType::Client, "vault.Vault", "Put", None);

    let rec_s = Session::with_stream_scrub(
        Mode::Record,
        FileCassette::new(dir.path()),
        mask_secret(),
    );
    let mut rec = rec_s.open_stream_record(open()).unwrap();
    rec.record_send(format!("put {SECRET}").as_bytes());
    rec.record_half_close();
    rec.finish(status_payload(0), None).unwrap();

    let ok_s = Session::with_stream_scrub(
        Mode::Replay,
        FileCassette::new(dir.path()),
        mask_secret(),
    );
    ok_s.open_stream_replay(open())
        .unwrap()
        .send(format!("put {SECRET}").as_bytes())
        .expect("symmetric scrub replays green");

    let bad_s = Session::new(Mode::Replay, FileCassette::new(dir.path()));
    let err = bad_s
        .open_stream_replay(open())
        .unwrap()
        .send(format!("put {SECRET}").as_bytes())
        .expect_err("replay without the hook must fail loudly");
    assert!(is_mismatch(&err));
}

/// Clause 5: recorded frames are delivered verbatim, never re-scrubbed. A
/// deliberately non-idempotent hook pins single application per phase.
#[test]
fn applied_exactly_once_and_never_rescrubbed() {
    let marker: StreamScrub = Arc::new(|_d, _i, data: &[u8]| {
        let mut v = data.to_vec();
        v.push(b'#');
        v
    });
    let dir = TempDir::new().unwrap();
    let open = || grpc_open(StreamType::Bidi, "chat.ChatService", "Converse", None);

    let rec_s =
        Session::with_stream_scrub(Mode::Record, FileCassette::new(dir.path()), marker.clone());
    let mut rec = rec_s.open_stream_record(open()).unwrap();
    rec.record_send(b"ping");
    rec.record_recv(b"pong");
    rec.record_half_close();
    let fp = rec.fingerprint().to_string();
    rec.finish(status_payload(0), None).unwrap();

    let pair = FileCassette::new(dir.path()).load_stream("grpc", &fp).unwrap();
    assert_eq!(pair.req.stream.frames[0].bytes, b"ping#");
    assert_eq!(pair.resp.stream.frames[0].bytes, b"pong#");

    let rep_s =
        Session::with_stream_scrub(Mode::Replay, FileCassette::new(dir.path()), marker);
    let mut rep = rep_s.open_stream_replay(open()).unwrap();
    rep.send(b"ping").expect("live send scrubbed once matches the once-scrubbed frame");
    assert_eq!(rep.recv().unwrap(), b"pong#", "recorded frames delivered verbatim");
}

/// Clause 2: the hook runs at exactly the specified points and nowhere
/// else. Half-close and the terminal carry no payload; recorded recv frames
/// are never re-scrubbed; bytes past the last recorded send are never
/// compared, so they are never scrubbed.
#[test]
fn invocation_points() {
    let seen: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
    let sink = Arc::clone(&seen);
    let trace: StreamScrub = Arc::new(move |dir: StreamDirection, _i, data: &[u8]| {
        sink.lock()
            .unwrap()
            .push(format!("{}:{}", dir.as_str(), String::from_utf8_lossy(data)));
        data.to_vec()
    });
    let dir = TempDir::new().unwrap();
    let open = || grpc_open(StreamType::Bidi, "chat.ChatService", "Converse", None);

    let rec_s =
        Session::with_stream_scrub(Mode::Record, FileCassette::new(dir.path()), trace.clone());
    let mut rec = rec_s.open_stream_record(open()).unwrap();
    rec.record_send(b"a");
    rec.record_recv(b"b");
    rec.record_half_close(); // no payload — not scrubbed
    rec.finish(status_payload(0), None).unwrap(); // terminal — not scrubbed
    assert_eq!(*seen.lock().unwrap(), vec!["send:a", "recv:b"]);

    seen.lock().unwrap().clear();
    let rep_s =
        Session::with_stream_scrub(Mode::Replay, FileCassette::new(dir.path()), trace);
    let mut rep = rep_s.open_stream_replay(open()).unwrap();
    rep.send(b"a").unwrap();
    rep.recv().unwrap(); // recorded frame — never re-scrubbed
    rep.half_close().unwrap();
    assert_eq!(*seen.lock().unwrap(), vec!["send:a"]);

    // Past the last recorded send: never compared, so never scrubbed.
    seen.lock().unwrap().clear();
    let _ = rep.send(b"overrun");
    assert!(seen.lock().unwrap().is_empty());
}

/// Clause 6: the hook MAY change a frame's length; neither side assumes
/// byte-count preservation.
#[test]
fn length_changing_hook() {
    const LONG: &str = "[REDACTED-MUCH-LONGER-PLACEHOLDER]";
    let expand: StreamScrub = Arc::new(|_d, _i, data: &[u8]| {
        String::from_utf8_lossy(data).replace(SECRET, LONG).into_bytes()
    });
    let dir = TempDir::new().unwrap();
    let open = || grpc_open(StreamType::Bidi, "chat.ChatService", "Converse", None);

    let rec_s =
        Session::with_stream_scrub(Mode::Record, FileCassette::new(dir.path()), expand.clone());
    let mut rec = rec_s.open_stream_record(open()).unwrap();
    rec.record_send(format!("k={SECRET}").as_bytes());
    rec.record_recv(format!("v={SECRET}").as_bytes());
    rec.record_half_close();
    let fp = rec.fingerprint().to_string();
    rec.finish(status_payload(0), None).unwrap();

    let pair = FileCassette::new(dir.path()).load_stream("grpc", &fp).unwrap();
    assert_eq!(pair.req.stream.frames[0].bytes, format!("k={LONG}").into_bytes());

    let rep_s =
        Session::with_stream_scrub(Mode::Replay, FileCassette::new(dir.path()), expand);
    let mut rep = rep_s.open_stream_replay(open()).unwrap();
    rep.send(format!("k={SECRET}").as_bytes())
        .expect("green despite the length change");
    assert_eq!(rep.recv().unwrap(), format!("v={LONG}").into_bytes());
}

/// Clause 7: no hook installed is identical to the feature not existing.
#[test]
fn absent_hook_records_verbatim() {
    let dir = TempDir::new().unwrap();
    let s = Session::new(Mode::Record, FileCassette::new(dir.path()));
    let open = grpc_open(StreamType::Bidi, "chat.ChatService", "Converse", None);
    let mut rec = s.open_stream_record(open).unwrap();
    rec.record_send(format!("ping {SECRET}").as_bytes());
    rec.record_half_close();
    let fp = rec.fingerprint().to_string();
    rec.finish(status_payload(0), None).unwrap();

    let pair = FileCassette::new(dir.path()).load_stream("grpc", &fp).unwrap();
    assert_eq!(pair.req.stream.frames[0].bytes, format!("ping {SECRET}").into_bytes());
}
