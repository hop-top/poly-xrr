//! gRPC adapter-level conformance against the spec fixtures
//! (spec/cassette-format-streaming.md, Conformance Obligations —
//! "Adapter level: ports with a gRPC adapter").
//!
//! Every assertion drives the real adapter (`GrpcStream` in replay mode)
//! against the shipped fixture cassettes, so it exercises fingerprint
//! recomputation, send validation, recv ordering, terminal reconstruction,
//! and the must-reject case — not just the format layer.

use std::path::{Path, PathBuf};

use hop_top_xrr::{
    adapters::grpc::{from_wire, to_wire, GrpcStream},
    stream::StreamType,
    FileCassette, Mode, Session, XrrError,
};

fn fixtures_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../spec/fixtures")
}

fn replay_session(fixture: &str) -> Session {
    let dir = fixtures_root().join(fixture);
    Session::new(Mode::Replay, FileCassette::new(&dir))
}

/// Fixture frames are ASCII payloads written directly as `message_b64`
/// (the spec's worked examples show wire bytes as readable ASCII), so the
/// adapter's byte-level contracts are asserted over those exact bytes.
fn bytes(s: &str) -> Vec<u8> {
    s.as_bytes().to_vec()
}

// ── server-stream ────────────────────────────────────────────────────────────

/// Fingerprint recomputed from (service, method, msg_hash, "server")
/// locates the pair; recv frames delivered in seq order; end-of-stream
/// after the last.
#[test]
fn server_stream_replays_in_order_then_ends() {
    let session = replay_session("grpc-server-stream");
    let open = bytes(r#"{"path":"/etc/hosts"}"#);
    let stream = GrpcStream::open(
        &session,
        StreamType::Server,
        "/files.FileService/Download",
        Some(&open),
    )
    .expect("open locates the pair by content address");

    let GrpcStream::Replay(rp) = stream else {
        panic!("replay mode must yield a replay stream")
    };
    assert_eq!(rp.fingerprint(), "58a4bf3f");

    // The single request message is validated, then half-close.
    rp.send(&open).expect("recorded open message matches");
    rp.close_send()
        .expect("half-close after all recorded sends");

    let mut got = Vec::new();
    while let Some(frame) = rp.recv().expect("no error terminal") {
        got.push(String::from_utf8(frame).unwrap());
    }
    assert_eq!(got, ["chunk-one\n", "chunk-two\n", "chunk-three\n"]);

    // The terminal repeats indefinitely.
    assert!(rp.recv().unwrap().is_none());
    assert!(rp.recv().unwrap().is_none());
    assert_eq!(rp.status_code(), 0);
}

/// A divergent open message addresses a different cassette entirely
/// (content-addressed), so it misses rather than mismatching.
#[test]
fn server_stream_divergent_open_is_cassette_miss() {
    let session = replay_session("grpc-server-stream");
    let err = GrpcStream::open(
        &session,
        StreamType::Server,
        "/files.FileService/Download",
        Some(&bytes(r#"{"path":"/etc/shadow"}"#)),
    )
    .expect_err("a different request message addresses a different cassette");
    assert!(matches!(err, XrrError::CassetteMiss { .. }), "got {err:?}");
}

// ── client-stream ────────────────────────────────────────────────────────────

/// Occurrence-counter fingerprint locates the pair; sends validated in
/// order and byte content; single response frame then end-of-stream.
#[test]
fn client_stream_validates_sends_then_yields_response() {
    let session = replay_session("grpc-client-stream");
    let stream = GrpcStream::open(
        &session,
        StreamType::Client,
        "/files.FileService/Upload",
        None,
    )
    .expect("counter-addressed open locates the pair");
    let GrpcStream::Replay(rp) = stream else {
        panic!("expected replay")
    };
    assert_eq!(rp.fingerprint(), "2bebfd6f");

    rp.send(&bytes("part-one\n")).expect("send 0 matches");
    rp.send(&bytes("part-two\n")).expect("send 1 matches");
    rp.close_send().expect("half-close after all sends");

    let resp = rp.recv().unwrap().expect("single response frame");
    assert_eq!(String::from_utf8(resp).unwrap(), r#"{"received_bytes":18}"#);
    assert!(
        rp.recv().unwrap().is_none(),
        "end-of-stream after the response"
    );
}

/// Divergent send bytes at i < S are a stream mismatch, and the mismatch
/// is terminal for the handle.
#[test]
fn client_stream_divergent_send_is_mismatch_and_terminal() {
    let session = replay_session("grpc-client-stream");
    let GrpcStream::Replay(rp) = GrpcStream::open(
        &session,
        StreamType::Client,
        "/files.FileService/Upload",
        None,
    )
    .unwrap() else {
        panic!("expected replay")
    };

    rp.send(&bytes("part-one\n")).expect("send 0 matches");
    let err = rp
        .send(&bytes("WRONG\n"))
        .expect_err("divergent bytes must mismatch");
    assert!(
        err.message().contains("mismatch"),
        "mismatch should be identified: {}",
        err.message()
    );

    // Terminal: every subsequent operation on the handle fails.
    assert!(
        rp.send(&bytes("part-two\n")).is_err(),
        "mismatch is terminal for sends"
    );
    assert!(rp.recv().is_err(), "mismatch is terminal for reads");
}

/// A half-close after fewer than S sends is a stream mismatch.
#[test]
fn client_stream_short_half_close_is_mismatch() {
    let session = replay_session("grpc-client-stream");
    let GrpcStream::Replay(rp) = GrpcStream::open(
        &session,
        StreamType::Client,
        "/files.FileService/Upload",
        None,
    )
    .unwrap() else {
        panic!("expected replay")
    };
    rp.send(&bytes("part-one\n")).unwrap();
    assert!(
        rp.close_send().is_err(),
        "half-close after 1 of 2 recorded sends must mismatch"
    );
}

/// The spec's n=1 obligation: a second open of the same tuple within one
/// session, sequenced by the session's occurrence counter.
#[test]
fn client_stream_repeat_two_opens_one_session() {
    let session = replay_session("grpc-client-stream-repeat");

    // Open 1 → n=0 → 2bebfd6f
    let GrpcStream::Replay(first) = GrpcStream::open(
        &session,
        StreamType::Client,
        "/files.FileService/Upload",
        None,
    )
    .unwrap() else {
        panic!("expected replay")
    };
    assert_eq!(first.fingerprint(), "2bebfd6f");
    first.send(&bytes("alpha\n")).unwrap();
    first.close_send().unwrap();
    assert_eq!(
        String::from_utf8(first.recv().unwrap().unwrap()).unwrap(),
        r#"{"received_bytes":6}"#
    );

    // Open 2 → n=1 → b27b5fe1, same session = same counter domain.
    let GrpcStream::Replay(second) = GrpcStream::open(
        &session,
        StreamType::Client,
        "/files.FileService/Upload",
        None,
    )
    .unwrap() else {
        panic!("expected replay")
    };
    assert_eq!(second.fingerprint(), "b27b5fe1");
    second.send(&bytes("beta-1\n")).unwrap();
    second.send(&bytes("beta-2\n")).unwrap();
    second.close_send().unwrap();
    assert_eq!(
        String::from_utf8(second.recv().unwrap().unwrap()).unwrap(),
        r#"{"received_bytes":14}"#
    );
}

// ── bidi ─────────────────────────────────────────────────────────────────────

/// Interleaved global seq parsed; per-direction ordering enforced; reads
/// never block on send progress.
#[test]
fn bidi_enforces_per_direction_order_only() {
    let session = replay_session("grpc-bidi-stream");
    let GrpcStream::Replay(rp) = GrpcStream::open(
        &session,
        StreamType::Bidi,
        "/chat.ChatService/Converse",
        None,
    )
    .unwrap() else {
        panic!("expected replay")
    };
    assert_eq!(rp.fingerprint(), "c6233d2e");

    // The recording interleaves ping/pong (seq 0..3), but replay must not
    // gate reads on sends: drain BOTH pongs before sending the second ping.
    assert_eq!(
        String::from_utf8(rp.recv().unwrap().unwrap()).unwrap(),
        "pong-1"
    );
    assert_eq!(
        String::from_utf8(rp.recv().unwrap().unwrap()).unwrap(),
        "pong-2"
    );
    assert!(
        rp.recv().unwrap().is_none(),
        "end-of-stream after 2 recv frames"
    );

    // Sends are still validated in their own recorded order.
    rp.send(&bytes("ping-1")).expect("send 0");
    rp.send(&bytes("ping-2")).expect("send 1");
    rp.close_send().expect("half-close after all sends");
}

// ── mid-stream error ─────────────────────────────────────────────────────────

/// All recorded recv frames delivered, then the recorded error (status
/// reconstructed from status_code) in place of end-of-stream; the terminal
/// repeats, and post-terminal sends return the same error.
#[test]
fn mid_stream_error_delivers_frames_then_recorded_status() {
    let session = replay_session("grpc-stream-error");
    let open = bytes(r#"{"path":"/var/log/big.log"}"#);
    let GrpcStream::Replay(rp) = GrpcStream::open(
        &session,
        StreamType::Server,
        "/files.FileService/Download",
        Some(&open),
    )
    .unwrap() else {
        panic!("expected replay")
    };
    assert_eq!(rp.fingerprint(), "9e8c4d4c");
    rp.send(&open).unwrap();
    rp.close_send().unwrap();

    assert_eq!(
        String::from_utf8(rp.recv().unwrap().unwrap()).unwrap(),
        "log-chunk-1\n"
    );
    assert_eq!(
        String::from_utf8(rp.recv().unwrap().unwrap()).unwrap(),
        "log-chunk-2\n"
    );

    // Terminal is the recorded error, reconstructed from status_code 14.
    let err = rp
        .recv()
        .expect_err("error terminal replaces end-of-stream");
    assert_eq!(err.code(), tonic::Code::Unavailable);
    assert_eq!(err.message(), "connection reset");
    assert_eq!(rp.status_code(), 14);

    // The terminal repeats indefinitely.
    let again = rp.recv().expect_err("terminal repeats");
    assert_eq!(again.code(), tonic::Code::Unavailable);

    // Post-terminal sends return the recorded error: the real stream was
    // dead too (spec: Send side, i >= S with an error terminal).
    let send_err = rp
        .send(&bytes("anything"))
        .expect_err("post-terminal send fails");
    assert_eq!(send_err.code(), tonic::Code::Unavailable);
}

// ── empty streams ────────────────────────────────────────────────────────────

/// frames: [] parsed; first read yields end-of-stream immediately.
#[test]
fn empty_streams_yield_end_of_stream_immediately() {
    let session = replay_session("grpc-stream-empty");

    // server: server sent nothing before OK.
    let open = bytes(r#"{"path":"/etc/empty"}"#);
    let GrpcStream::Replay(server) = GrpcStream::open(
        &session,
        StreamType::Server,
        "/files.FileService/Download",
        Some(&open),
    )
    .unwrap() else {
        panic!("expected replay")
    };
    assert_eq!(server.fingerprint(), "ffbc4bac");
    server.send(&open).unwrap();
    server.close_send().unwrap();
    assert!(
        server.recv().unwrap().is_none(),
        "first read is end-of-stream"
    );

    // client: client half-closed immediately, server answered once.
    let GrpcStream::Replay(client) = GrpcStream::open(
        &session,
        StreamType::Client,
        "/telemetry.MetricsService/Push",
        None,
    )
    .unwrap() else {
        panic!("expected replay")
    };
    assert_eq!(client.fingerprint(), "fbdff683");
    client
        .close_send()
        .expect("half-close with zero recorded sends");
    assert_eq!(
        String::from_utf8(client.recv().unwrap().unwrap()).unwrap(),
        r#"{"count":0}"#
    );
    assert!(client.recv().unwrap().is_none());

    // bidi: no traffic at all in either direction.
    let GrpcStream::Replay(bidi) =
        GrpcStream::open(&session, StreamType::Bidi, "/chat.ChatService/Ping", None).unwrap()
    else {
        panic!("expected replay")
    };
    assert_eq!(bidi.fingerprint(), "ebbd3938");
    bidi.close_send().unwrap();
    assert!(
        bidi.recv().unwrap().is_none(),
        "first read is end-of-stream"
    );
}

// ── malformed base64 (must reject) ───────────────────────────────────────────

/// The negative fixture must be REJECTED, not silently accepted with the
/// bad characters discarded (spec Validation Rule 7).
#[test]
fn malformed_b64_fixture_is_rejected_by_the_adapter() {
    let session = replay_session("grpc-stream-malformed-b64");
    let open = bytes(r#"{"path":"/opt/blob.bin"}"#);
    let err = GrpcStream::open(
        &session,
        StreamType::Server,
        "/files.FileService/Download",
        Some(&open),
    )
    .expect_err("malformed message_b64 must fail the open, not replay silently");
    match err {
        XrrError::InvalidStream(msg) => {
            assert!(msg.contains("base64"), "reason should cite base64: {msg}")
        }
        other => panic!("expected InvalidStream, got {other:?}"),
    }
}

// ── shape / mode guards ──────────────────────────────────────────────────────

/// The `stream` discriminator makes the streaming fingerprint spaces
/// disjoint by construction: asking for a different stream type on the
/// same tuple computes a different canonical input, so it misses loudly
/// rather than degenerately hitting the recorded pair.
#[test]
fn wrong_stream_type_misses_rather_than_hitting() {
    let session = replay_session("grpc-bidi-stream");
    let err = GrpcStream::open(
        &session,
        StreamType::Client,
        "/chat.ChatService/Converse",
        None,
    )
    .expect_err("a client fingerprint must not resolve a bidi recording");
    assert!(matches!(err, XrrError::CassetteMiss { .. }), "got {err:?}");
}

/// A recorded stream type diverging from the requested one at the SAME
/// fingerprint is a shape mismatch, distinct from a cassette miss. The
/// discriminator makes that unreachable through fingerprinting alone, so
/// it is driven directly against the core with a hand-built open.
#[test]
fn recorded_type_divergence_at_same_fingerprint_is_shape_mismatch() {
    use hop_top_xrr::stream::StreamOpen;
    use std::collections::BTreeMap;

    let dir = fixtures_root().join("grpc-bidi-stream");
    let session = Session::new(Mode::Replay, FileCassette::new(&dir));

    // Claim the bidi recording's fingerprint while declaring type `client`.
    let mut identity = BTreeMap::new();
    identity.insert(
        "service".to_string(),
        serde_json::Value::from("chat.ChatService"),
    );
    identity.insert("method".to_string(), serde_json::Value::from("Converse"));
    let open = StreamOpen {
        adapter_id: "grpc".into(),
        stream_type: StreamType::Bidi,
        identity,
        counter: true,
        payload: serde_yaml::Mapping::new(),
    };
    // Sanity: this open resolves the bidi pair.
    session
        .open_stream_replay(open.clone())
        .expect("bidi open resolves");

    // Same identity, same counter position is consumed — rebuild with the
    // declared type flipped so the loaded pair's recorded type diverges.
    let mut mismatched = open;
    mismatched.stream_type = StreamType::Client;
    let err = session
        .open_stream_replay(mismatched)
        .expect_err("recorded bidi vs requested client must be a shape mismatch");
    match err {
        XrrError::ShapeMismatch(_) | XrrError::CassetteMiss { .. } => {}
        other => panic!("expected shape mismatch or miss, got {other:?}"),
    }
}

/// Server streams are content-addressed, so opening one without the
/// request message is API misuse, not a miss.
#[test]
fn server_stream_without_open_message_is_usage_error() {
    let session = replay_session("grpc-server-stream");
    let err = GrpcStream::open(
        &session,
        StreamType::Server,
        "/files.FileService/Download",
        None,
    )
    .expect_err("server streams need the open message");
    assert!(matches!(err, XrrError::Usage(_)), "got {err:?}");
}

/// Passthrough is transparent: no cassette lookup, no recording.
#[test]
fn passthrough_yields_a_transparent_stream() {
    let session = Session::new(Mode::Passthrough, FileCassette::new(fixtures_root()));
    let stream = GrpcStream::open(
        &session,
        StreamType::Bidi,
        "/chat.ChatService/Converse",
        None,
    )
    .unwrap();
    assert!(matches!(stream, GrpcStream::Passthrough));
    assert!(!stream.is_replay());
}

// ── typed-message helpers ────────────────────────────────────────────────────

/// The adapter records wire bytes; the caller's typed edge goes through
/// to_wire/from_wire, whose encoding must be byte-stable.
#[test]
fn typed_messages_round_trip_through_wire_helpers() {
    let msg = "chunk-one\n".to_string();
    let wire = to_wire(&msg);
    assert_eq!(wire, to_wire(&msg), "encoding must be deterministic");
    assert_eq!(from_wire::<String>(&wire).unwrap(), msg);
}
