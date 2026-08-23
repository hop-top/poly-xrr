// Identity-hook conformance — spec "Scrub Hook Obligations — Identity-Hook
// Matrix" (M1..M7).
//
// The scrub hook's contract is WHEN it runs and WHAT it receives, never
// what it computes; xrr defines no scrub algorithm. Two byte-neutral hooks
// generate the whole matrix:
//
//   - identity: returns its input. Installed and invoked but byte-neutral,
//     so any divergence from a no-hook session is a mechanics defect —
//     clause 7 fixes no-hook behaviour as the reference.
//   - counting: identity plus a call log. Reveals invocation points,
//     multiplicity, and — the part fixtures cannot see — non-invocation.
//
// Mirrors go/stream_scrub_identity_test.go. These run without the `grpc`
// feature, so the core contract is covered by the default suite.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use hop_top_xrr::{
    stream::{msg_hash, StreamOpen, StreamType},
    FileCassette, Mode, Session, StreamDirection, StreamScrub, StreamScrubInfo, XrrError,
};
use serde_json::json;
use serde_yaml::Value;
use tempfile::TempDir;

const OPEN_MSG: &[u8] = br#"{"room":"ops"}"#;

/// Clause 6's "MAY return the input unchanged": observable, byte-neutral.
fn identity_scrub() -> StreamScrub {
    Arc::new(|_dir, _info, data: &[u8]| data.to_vec())
}

type CallLog = Arc<Mutex<Vec<(String, String)>>>;

/// Identity plus a call log. The bookkeeping is test scaffolding, not scrub
/// state — the bytes returned are the input, so clause 4's determinism holds.
fn counting_scrub(log: &CallLog) -> StreamScrub {
    let sink = Arc::clone(log);
    Arc::new(move |dir: StreamDirection, _i, data: &[u8]| {
        sink.lock()
            .unwrap()
            .push((dir.as_str().to_string(), String::from_utf8_lossy(data).into_owned()));
        data.to_vec()
    })
}

fn new_log() -> CallLog {
    Arc::new(Mutex::new(Vec::new()))
}

fn calls(log: &CallLog) -> Vec<(String, String)> {
    log.lock().unwrap().clone()
}

fn expect_calls(pairs: &[(&str, &str)]) -> Vec<(String, String)> {
    pairs.iter().map(|(d, b)| (d.to_string(), b.to_string())).collect()
}

fn fixed_open(t: StreamType) -> StreamOpen {
    let mut identity = BTreeMap::new();
    identity.insert("service".to_string(), json!("chat.ChatService"));
    identity.insert("method".to_string(), json!("Converse"));
    if t == StreamType::Server {
        identity.insert("msg_hash".to_string(), json!(msg_hash(OPEN_MSG)));
    }
    let mut payload = serde_yaml::Mapping::new();
    payload.insert(Value::String("service".into()), Value::String("chat.ChatService".into()));
    payload.insert(Value::String("method".into()), Value::String("Converse".into()));
    StreamOpen {
        adapter_id: "grpc".into(),
        stream_type: t,
        identity,
        counter: t != StreamType::Server,
        payload,
    }
}

fn status_payload(code: i64) -> serde_yaml::Mapping {
    let mut m = serde_yaml::Mapping::new();
    m.insert(Value::String("status_code".into()), Value::Number(code.into()));
    m
}

/// gRPC mapping: server streams record exactly one send frame.
fn fixed_sends(t: StreamType) -> Vec<&'static [u8]> {
    if t == StreamType::Server {
        vec![OPEN_MSG]
    } else {
        vec![b"alpha".as_slice(), b"beta".as_slice()]
    }
}

/// gRPC mapping: client streams record at most one recv frame.
fn fixed_recvs(t: StreamType) -> Vec<&'static [u8]> {
    if t == StreamType::Client {
        vec![b"ack".as_slice()]
    } else {
        vec![b"one".as_slice(), b"two".as_slice()]
    }
}

/// One identical scripted stream through a record session, so two sessions
/// differing only in hook installation are byte-comparable.
fn record_fixed(dir: &std::path::Path, t: StreamType, scrub: Option<StreamScrub>) -> String {
    let cassette = FileCassette::new(dir);
    let s = match scrub {
        Some(hook) => Session::with_stream_scrub(Mode::Record, cassette, hook),
        None => Session::new(Mode::Record, cassette),
    };
    let mut rec = s.open_stream_record(fixed_open(t)).unwrap();
    for f in fixed_sends(t) {
        rec.record_send(f);
    }
    rec.record_half_close();
    for f in fixed_recvs(t) {
        rec.record_recv(f);
    }
    let fp = rec.fingerprint().to_string();
    rec.finish(status_payload(0), None).unwrap();
    fp
}

fn replay_fixed(dir: &std::path::Path, t: StreamType, scrub: Option<StreamScrub>) {
    let cassette = FileCassette::new(dir);
    let s = match scrub {
        Some(hook) => Session::with_stream_scrub(Mode::Replay, cassette, hook),
        None => Session::new(Mode::Replay, cassette),
    };
    let mut rep = s.open_stream_replay(fixed_open(t)).unwrap();
    for f in fixed_sends(t) {
        rep.send(f).unwrap();
    }
    rep.half_close().unwrap();
    for want in fixed_recvs(t) {
        assert_eq!(rep.recv().unwrap(), want.to_vec());
    }
    assert!(matches!(rep.recv(), Err(XrrError::StreamEnd)));
}

/// The call log a byte-neutral hook must produce for one `record_fixed` of
/// this type: one send call per send frame, one recv call per recv frame,
/// in frame order.
///
/// M1..M3 pair their byte-identity assertions with this expectation. Byte
/// identity alone is vacuous for a byte-neutral hook — with the hook
/// dispatch removed the hooked and unhooked branches become the same
/// computation, so the comparison holds by construction whether or not the
/// hook ever ran. The call log supplies the missing half: positive
/// evidence of invocation.
fn fixed_frame_calls(t: StreamType) -> Vec<(String, String)> {
    fixed_sends(t)
        .into_iter()
        .map(|f| ("send".to_string(), String::from_utf8_lossy(f).into_owned()))
        .chain(
            fixed_recvs(t)
                .into_iter()
                .map(|f| ("recv".to_string(), String::from_utf8_lossy(f).into_owned())),
        )
        .collect()
}

/// Replay scrubs live sends only (M5); recorded recv frames are delivered
/// verbatim.
fn fixed_send_calls(t: StreamType) -> Vec<(String, String)> {
    fixed_sends(t)
        .into_iter()
        .map(|f| ("send".to_string(), String::from_utf8_lossy(f).into_owned()))
        .collect()
}

fn pair_bytes(dir: &std::path::Path, fp: &str) -> (String, String) {
    (
        std::fs::read_to_string(dir.join(format!("grpc-{fp}.req.yaml"))).unwrap(),
        std::fs::read_to_string(dir.join(format!("grpc-{fp}.resp.yaml"))).unwrap(),
    )
}

const TYPES: [StreamType; 3] = [StreamType::Server, StreamType::Client, StreamType::Bidi];

/// M1: an installed identity hook is byte-indistinguishable from no hook.
/// Any divergence is a mechanics defect — an extra scrub site, a missed
/// one, or an identity input derived from the wrong bytes.
///
/// The hooked branch runs a COUNTING identity hook, and the call log is
/// asserted alongside the bytes. Byte equality on its own proves only that
/// the two sessions agree, which a hook that never ran also satisfies; the
/// log proves the hook was installed AND invoked while agreeing.
#[test]
fn identity_hook_matches_no_hook() {
    for t in TYPES {
        let bare = TempDir::new().unwrap();
        let hooked = TempDir::new().unwrap();

        let log = new_log();
        let bare_fp = record_fixed(bare.path(), t, None);
        let hooked_fp = record_fixed(hooked.path(), t, Some(counting_scrub(&log)));
        assert_eq!(bare_fp, hooked_fp, "{t:?}: identity hook must not move the fingerprint");
        assert_eq!(
            pair_bytes(bare.path(), &bare_fp),
            pair_bytes(hooked.path(), &hooked_fp),
            "{t:?}: cassette bytes must be identical"
        );
        assert_eq!(
            calls(&log),
            fixed_frame_calls(t),
            "{t:?}: the identity hook must actually run — byte equality alone is \
             satisfied by a hook that never fired"
        );
    }
}

/// M2: because the identity hook changes no bytes, a cassette crosses the
/// hook boundary both ways. The one legitimate exception to clause 5's
/// "same hook both sides" — it holds precisely because the two agree
/// byte-for-byte.
///
/// Each direction installs a COUNTING identity hook on its hooked side and
/// asserts the log. A green cross-hook replay is otherwise equally
/// consistent with a hook that was never dispatched — the exception is
/// only meaningful if the hook is genuinely present on one side and absent
/// on the other.
#[test]
fn identity_hook_replays_across_the_hook_boundary() {
    for t in TYPES {
        let with_hook = TempDir::new().unwrap();
        let rec_log = new_log();
        record_fixed(with_hook.path(), t, Some(counting_scrub(&rec_log)));
        assert_eq!(
            calls(&rec_log),
            fixed_frame_calls(t),
            "{t:?}: the recording side's hook must actually run"
        );
        replay_fixed(with_hook.path(), t, None);

        let without = TempDir::new().unwrap();
        record_fixed(without.path(), t, None);
        let rep_log = new_log();
        replay_fixed(without.path(), t, Some(counting_scrub(&rep_log)));
        assert_eq!(
            calls(&rep_log),
            fixed_send_calls(t),
            "{t:?}: the replaying side's hook must actually run"
        );
    }
}

/// M3: clause 3 routes content-derived identity through the hook. Under
/// identity it must land on the raw msg_hash in both modes — otherwise the
/// hook is applied to the wrong buffer, or applied twice.
///
/// A COUNTING identity hook supplies the routing evidence. `msg_hash` of
/// identity-scrubbed bytes equalling `msg_hash` of the raw bytes is a
/// tautology for any byte-neutral hook, and holds even if
/// `scrub_stream_frame` never dispatches — the assertion that clause 3's
/// route exists is the call log, exactly one call carrying the raw bytes.
#[test]
fn identity_derived_identity_equals_raw() {
    for mode in [Mode::Record, Mode::Replay] {
        let dir = TempDir::new().unwrap();
        let log = new_log();
        let s =
            Session::with_stream_scrub(mode, FileCassette::new(dir.path()), counting_scrub(&log));
        let info = StreamScrubInfo {
            adapter_id: "grpc".to_string(),
            stream_type: StreamType::Server,
        };
        let scrubbed = s.scrub_stream_frame(StreamDirection::Send, &info, OPEN_MSG);
        assert_eq!(msg_hash(&scrubbed), msg_hash(OPEN_MSG), "{mode:?}");
        assert_eq!(
            calls(&log),
            expect_calls(&[("send", r#"{"room":"ops"}"#)]),
            "{mode:?}: identity derivation must route through the hook exactly once"
        );
    }
}

/// M4: exactly one call per frame per direction, in frame order, carrying
/// that frame's bytes. Half-close and the terminal carry no payload and
/// contribute no call.
#[test]
fn counting_hook_record_invocations() {
    let log = new_log();
    let dir = TempDir::new().unwrap();
    record_fixed(dir.path(), StreamType::Bidi, Some(counting_scrub(&log)));

    assert_eq!(
        calls(&log),
        expect_calls(&[
            ("send", "alpha"),
            ("send", "beta"),
            ("recv", "one"),
            ("recv", "two"),
        ])
    );
}

/// M5: replay scrubs live sends only, once each, and never touches recorded
/// frames. The trailing case caught a real cross-port divergence: two ports
/// ran the hook BEFORE the bound check that rejects a send past the end of
/// the recording, two after. Only a counting hook sees that.
#[test]
fn counting_hook_replay_invocations() {
    let dir = TempDir::new().unwrap();
    record_fixed(dir.path(), StreamType::Bidi, Some(identity_scrub()));

    let log = new_log();
    let s = Session::with_stream_scrub(
        Mode::Replay,
        FileCassette::new(dir.path()),
        counting_scrub(&log),
    );
    let mut rep = s.open_stream_replay(fixed_open(StreamType::Bidi)).unwrap();
    rep.send(b"alpha").unwrap();
    rep.send(b"beta").unwrap();
    rep.half_close().unwrap();
    assert_eq!(rep.recv().unwrap(), b"one".to_vec());
    assert_eq!(rep.recv().unwrap(), b"two".to_vec());
    assert_eq!(calls(&log), expect_calls(&[("send", "alpha"), ("send", "beta")]));

    log.lock().unwrap().clear();
    let _ = rep.send(b"overrun");
    assert!(
        calls(&log).is_empty(),
        "a send past the last recorded frame is never compared, so never scrubbed"
    );
}

/// M6: clause 3's no-pre-scrub rule. The gRPC server-stream open message is
/// both an identity input and a persisted frame — two distinct invocation
/// points, one call each. An adapter that pre-scrubbed the message it also
/// hands the core would show two calls for the persist point.
#[test]
fn counting_hook_no_double_scrub() {
    let log = new_log();
    let dir = TempDir::new().unwrap();
    let msg = br#"{"cmd":"deploy"}"#;
    let s = Session::with_stream_scrub(
        Mode::Record,
        FileCassette::new(dir.path()),
        counting_scrub(&log),
    );

    // Identity point: the adapter derives msg_hash over the scrubbed bytes.
    let info = StreamScrubInfo {
        adapter_id: "grpc".to_string(),
        stream_type: StreamType::Server,
    };
    let scrubbed = s.scrub_stream_frame(StreamDirection::Send, &info, msg);
    assert_eq!(calls(&log).len(), 1, "identity derivation is exactly one call");

    let mut identity = BTreeMap::new();
    identity.insert("service".to_string(), json!("ops.Deploy"));
    identity.insert("method".to_string(), json!("Run"));
    identity.insert("msg_hash".to_string(), json!(msg_hash(&scrubbed)));
    let mut payload = serde_yaml::Mapping::new();
    payload.insert(Value::String("service".into()), Value::String("ops.Deploy".into()));
    payload.insert(Value::String("method".into()), Value::String("Run".into()));
    let open = StreamOpen {
        adapter_id: "grpc".into(),
        stream_type: StreamType::Server,
        identity,
        counter: false,
        payload,
    };
    let mut rec = s.open_stream_record(open).unwrap();

    // Persist point: the adapter passes the message RAW. The core scrubs.
    rec.record_send(msg);
    rec.record_half_close();
    rec.record_recv(b"deployed");
    rec.finish(status_payload(0), None).unwrap();

    assert_eq!(
        calls(&log),
        expect_calls(&[
            ("send", r#"{"cmd":"deploy"}"#), // identity derivation
            ("send", r#"{"cmd":"deploy"}"#), // persist — one call, not two
            ("recv", "deployed"),
        ])
    );
}

/// M7: clause 6 permits a length change; neither the record nor the replay
/// path may assume byte-count preservation.
#[test]
fn length_changing_hook_round_trips() {
    let grow: StreamScrub = Arc::new(|_d, _i, data: &[u8]| {
        let mut out = data.to_vec();
        out.extend_from_slice(b"-PADDED-LONGER");
        out
    });
    let dir = TempDir::new().unwrap();

    let rec_s =
        Session::with_stream_scrub(Mode::Record, FileCassette::new(dir.path()), grow.clone());
    let mut rec = rec_s.open_stream_record(fixed_open(StreamType::Bidi)).unwrap();
    rec.record_send(b"alpha");
    rec.record_half_close();
    rec.record_recv(b"one");
    let fp = rec.fingerprint().to_string();
    rec.finish(status_payload(0), None).unwrap();

    let pair = FileCassette::new(dir.path()).load_stream("grpc", &fp).unwrap();
    assert_eq!(pair.req.stream.frames[0].bytes, b"alpha-PADDED-LONGER".to_vec());

    let rep_s = Session::with_stream_scrub(Mode::Replay, FileCassette::new(dir.path()), grow);
    let mut rep = rep_s.open_stream_replay(fixed_open(StreamType::Bidi)).unwrap();
    rep.send(b"alpha").expect("green despite the length change");
    rep.half_close().unwrap();
    assert_eq!(rep.recv().unwrap(), b"one-PADDED-LONGER".to_vec());
}
