//! gRPC streaming e2e: record against a REAL tonic server over a real TCP
//! socket, stop the server and close the port, then replay from cassettes
//! only and assert the client-observed transcripts match byte-for-byte
//! while nothing is dialled.
//!
//! The server is hand-rolled on `tonic::server::Grpc` with the adapter's
//! own [`BytesCodec`], so no protoc/build.rs is needed: request and
//! response messages are prost-encoded `String`s, which is exactly what a
//! generated client would put on the wire.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use bytes::Bytes;
use futures::{future::BoxFuture, stream::BoxStream, StreamExt};
use hop_top_xrr::{
    adapters::grpc::{from_wire, to_wire, BytesCodec, GrpcStream},
    stream::{StreamType, StreamedPair},
    FileCassette, Mode, Session, StreamDirection, XrrError,
};
use tonic::body::Body;
use tonic::{Request, Response, Status, Streaming};

const SERVICE: &str = "xrrtest.StreamService";
const SECRET: &str = "hunter2-FAKE-TOKEN-0123456789";
const MASK: &str = "<scrubbed-tok-0123456789>____"; // same length as SECRET

// ── hand-rolled streaming service over BytesCodec ────────────────────────────

fn enc(s: &str) -> Vec<u8> {
    to_wire(&s.to_string())
}

fn dec(b: &[u8]) -> String {
    from_wire::<String>(b).expect("prost String decodes")
}

#[derive(Clone)]
struct StreamService;

impl tonic::server::NamedService for StreamService {
    const NAME: &'static str = SERVICE;
}

/// Download: server-streaming. "empty" streams nothing; "boom" fails
/// mid-stream after two chunks; anything else streams three chunks.
struct Download;

impl tonic::server::ServerStreamingService<Vec<u8>> for Download {
    type Response = Vec<u8>;
    type ResponseStream = BoxStream<'static, Result<Vec<u8>, Status>>;
    type Future = BoxFuture<'static, Result<Response<Self::ResponseStream>, Status>>;

    fn call(&mut self, req: Request<Vec<u8>>) -> Self::Future {
        Box::pin(async move {
            let name = dec(&req.into_inner());
            let out: Vec<Result<Vec<u8>, Status>> = match name.as_str() {
                "empty" => vec![],
                "boom" => vec![
                    Ok(enc("log-chunk-1")),
                    Ok(enc("log-chunk-2")),
                    Err(Status::unavailable("connection reset")),
                ],
                // Secret-bearing traffic: the scrub hook must keep the token
                // out of the cassette in both directions.
                n if n.starts_with("token:") => {
                    vec![Ok(enc(&format!("granted:{SECRET}"))), Ok(enc("done"))]
                }
                n => (1..=3)
                    .map(|i| Ok(enc(&format!("{n}-chunk-{i}"))))
                    .collect(),
            };
            Ok(Response::new(futures::stream::iter(out).boxed()))
        })
    }
}

/// Upload: client-streaming, answers with the total byte count.
struct Upload;

impl tonic::server::ClientStreamingService<Vec<u8>> for Upload {
    type Response = Vec<u8>;
    type Future = BoxFuture<'static, Result<Response<Vec<u8>>, Status>>;

    fn call(&mut self, req: Request<Streaming<Vec<u8>>>) -> Self::Future {
        Box::pin(async move {
            let mut s = req.into_inner();
            let mut total = 0usize;
            while let Some(m) = s.next().await {
                total += dec(&m?).len();
            }
            Ok(Response::new(enc(&format!("received:{total}"))))
        })
    }
}

/// Converse: bidi, pongs every ping until the client half-closes.
struct Converse;

impl tonic::server::StreamingService<Vec<u8>> for Converse {
    type Response = Vec<u8>;
    type ResponseStream = BoxStream<'static, Result<Vec<u8>, Status>>;
    type Future = BoxFuture<'static, Result<Response<Self::ResponseStream>, Status>>;

    fn call(&mut self, req: Request<Streaming<Vec<u8>>>) -> Self::Future {
        Box::pin(async move {
            let s = req
                .into_inner()
                .map(|r| r.map(|b| enc(&format!("pong:{}", dec(&b)))));
            Ok(Response::new(s.boxed()))
        })
    }
}

impl tower_service::Service<http::Request<Body>> for StreamService {
    type Response = http::Response<Body>;
    type Error = std::convert::Infallible;
    type Future = BoxFuture<'static, Result<Self::Response, Self::Error>>;

    fn poll_ready(
        &mut self,
        _: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        std::task::Poll::Ready(Ok(()))
    }

    fn call(&mut self, req: http::Request<Body>) -> Self::Future {
        Box::pin(async move {
            let mut g = tonic::server::Grpc::new(BytesCodec);
            Ok(match req.uri().path() {
                p if p.ends_with("/Download") => g.server_streaming(Download, req).await,
                p if p.ends_with("/Upload") => g.client_streaming(Upload, req).await,
                p if p.ends_with("/Converse") => g.streaming(Converse, req).await,
                _ => http::Response::builder()
                    .status(200)
                    .header("grpc-status", "12")
                    .body(Body::empty())
                    .unwrap(),
            })
        })
    }
}

// ── live client over the real socket ─────────────────────────────────────────

/// A live tonic client bound to one TCP endpoint, driving `BytesCodec` so
/// every message crosses the boundary as the wire bytes the adapter records.
struct LiveClient {
    grpc: tonic::client::Grpc<tonic::transport::Channel>,
}

impl LiveClient {
    async fn connect(addr: std::net::SocketAddr) -> Self {
        let channel = tonic::transport::Channel::from_shared(format!("http://{addr}"))
            .unwrap()
            .connect()
            .await
            .expect("connect to the live server");
        Self {
            grpc: tonic::client::Grpc::new(channel),
        }
    }

    fn path(method: &str) -> http::uri::PathAndQuery {
        format!("/{SERVICE}/{method}").parse().unwrap()
    }

    async fn server_streaming(
        &mut self,
        method: &str,
        msg: Vec<u8>,
    ) -> Result<Streaming<Vec<u8>>, Status> {
        self.grpc
            .ready()
            .await
            .map_err(|e| Status::unavailable(e.to_string()))?;
        Ok(self
            .grpc
            .server_streaming(Request::new(msg), Self::path(method), BytesCodec)
            .await?
            .into_inner())
    }

    async fn client_streaming(
        &mut self,
        method: &str,
        msgs: Vec<Vec<u8>>,
    ) -> Result<Vec<u8>, Status> {
        self.grpc
            .ready()
            .await
            .map_err(|e| Status::unavailable(e.to_string()))?;
        Ok(self
            .grpc
            .client_streaming(
                Request::new(tokio_stream::iter(msgs)),
                Self::path(method),
                BytesCodec,
            )
            .await?
            .into_inner())
    }

    async fn streaming(
        &mut self,
        method: &str,
        msgs: Vec<Vec<u8>>,
    ) -> Result<Streaming<Vec<u8>>, Status> {
        self.grpc
            .ready()
            .await
            .map_err(|e| Status::unavailable(e.to_string()))?;
        Ok(self
            .grpc
            .streaming(
                Request::new(tokio_stream::iter(msgs)),
                Self::path(method),
                BytesCodec,
            )
            .await?
            .into_inner())
    }
}

/// Everything a client observes on one stream: the decoded messages in
/// order, then the terminal rendered as a stable string.
#[derive(Debug, PartialEq, Eq)]
struct Transcript {
    msgs: Vec<String>,
    terminal: String,
}

fn ok_terminal() -> String {
    "EOF".to_string()
}

fn err_terminal(s: &Status) -> String {
    format!("{:?}/{}", s.code(), s.message())
}

// ── server lifecycle ─────────────────────────────────────────────────────────

struct LiveServer {
    addr: std::net::SocketAddr,
    shutdown: tokio::sync::oneshot::Sender<()>,
    joined: tokio::task::JoinHandle<()>,
}

async fn start_server() -> LiveServer {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let (tx, rx) = tokio::sync::oneshot::channel();
    let joined = tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(StreamService)
            .serve_with_incoming_shutdown(
                tokio_stream::wrappers::TcpListenerStream::new(listener),
                async {
                    let _ = rx.await;
                },
            )
            .await
            .unwrap();
    });
    LiveServer {
        addr,
        shutdown: tx,
        joined,
    }
}

impl LiveServer {
    /// Stop serving and wait for the task to finish, so the port is truly
    /// closed before the replay phase runs.
    async fn stop(self) {
        let _ = self.shutdown.send(());
        let _ = self.joined.await;
    }
}

/// Prove the port is dead: a fresh connection attempt must fail.
async fn assert_port_dead(addr: std::net::SocketAddr) {
    let attempt = tokio::time::timeout(
        std::time::Duration::from_millis(500),
        tokio::net::TcpStream::connect(addr),
    )
    .await;
    match attempt {
        Ok(Ok(_)) => panic!("port {addr} still accepts connections; replay would not be proven"),
        _ => { /* refused or timed out: the server is gone */ }
    }
}

// ── phase 1 helpers: drive live traffic THROUGH the recording adapter ────────

/// Record a server-streaming RPC: open with the request message (server
/// streams are content-addressed), tee every observed event, finish at the
/// terminal.
async fn record_server_stream(
    session: &Session,
    client: &mut LiveClient,
    method: &str,
    msg: &str,
) -> Transcript {
    let open = enc(msg);
    let GrpcStream::Record(rec) = GrpcStream::open(
        session,
        StreamType::Server,
        &format!("/{SERVICE}/{method}"),
        Some(&open),
    )
    .unwrap() else {
        panic!("record mode must yield a record stream")
    };

    let mut tr = Transcript {
        msgs: vec![],
        terminal: ok_terminal(),
    };
    match client.server_streaming(method, open.clone()).await {
        Ok(mut live) => {
            // The single request message and the implicit half-close.
            rec.send(&open);
            rec.close_send();
            loop {
                match live.message().await {
                    Ok(Some(b)) => {
                        rec.recv(&b);
                        tr.msgs.push(dec(&b));
                    }
                    Ok(None) => {
                        rec.finish_ok().unwrap();
                        break;
                    }
                    Err(s) => {
                        rec.finish_err(&s).unwrap();
                        tr.terminal = err_terminal(&s);
                        break;
                    }
                }
            }
        }
        Err(s) => {
            rec.send(&open);
            rec.close_send();
            rec.finish_err(&s).unwrap();
            tr.terminal = err_terminal(&s);
        }
    }
    tr
}

async fn record_client_stream(
    session: &Session,
    client: &mut LiveClient,
    method: &str,
    parts: &[&str],
) -> Transcript {
    let GrpcStream::Record(rec) = GrpcStream::open(
        session,
        StreamType::Client,
        &format!("/{SERVICE}/{method}"),
        None,
    )
    .unwrap() else {
        panic!("expected record")
    };
    let msgs: Vec<Vec<u8>> = parts.iter().map(|p| enc(p)).collect();
    for m in &msgs {
        rec.send(m);
    }
    rec.close_send();

    let mut tr = Transcript {
        msgs: vec![],
        terminal: ok_terminal(),
    };
    match client.client_streaming(method, msgs).await {
        Ok(resp) => {
            rec.recv(&resp);
            tr.msgs.push(dec(&resp));
            rec.finish_ok().unwrap();
        }
        Err(s) => {
            rec.finish_err(&s).unwrap();
            tr.terminal = err_terminal(&s);
        }
    }
    tr
}

async fn record_bidi(
    session: &Session,
    client: &mut LiveClient,
    method: &str,
    pings: &[&str],
) -> Transcript {
    let GrpcStream::Record(rec) = GrpcStream::open(
        session,
        StreamType::Bidi,
        &format!("/{SERVICE}/{method}"),
        None,
    )
    .unwrap() else {
        panic!("expected record")
    };
    let msgs: Vec<Vec<u8>> = pings.iter().map(|p| enc(p)).collect();
    for m in &msgs {
        rec.send(m);
    }
    rec.close_send();

    let mut tr = Transcript {
        msgs: vec![],
        terminal: ok_terminal(),
    };
    match client.streaming(method, msgs).await {
        Ok(mut live) => loop {
            match live.message().await {
                Ok(Some(b)) => {
                    rec.recv(&b);
                    tr.msgs.push(dec(&b));
                }
                Ok(None) => {
                    rec.finish_ok().unwrap();
                    break;
                }
                Err(s) => {
                    rec.finish_err(&s).unwrap();
                    tr.terminal = err_terminal(&s);
                    break;
                }
            }
        },
        Err(s) => {
            rec.finish_err(&s).unwrap();
            tr.terminal = err_terminal(&s);
        }
    }
    tr
}

// ── phase 2 helpers: replay the SAME drivers with no server ──────────────────

fn replay_server_stream(session: &Session, method: &str, msg: &str) -> Transcript {
    let open = enc(msg);
    let GrpcStream::Replay(rp) = GrpcStream::open(
        session,
        StreamType::Server,
        &format!("/{SERVICE}/{method}"),
        Some(&open),
    )
    .expect("replay open must locate the recorded pair") else {
        panic!("expected replay")
    };
    rp.send(&open).expect("recorded open message");
    rp.close_send().expect("half-close");

    let mut tr = Transcript {
        msgs: vec![],
        terminal: ok_terminal(),
    };
    loop {
        match rp.recv() {
            Ok(Some(b)) => tr.msgs.push(dec(&b)),
            Ok(None) => break,
            Err(s) => {
                tr.terminal = err_terminal(&s);
                break;
            }
        }
    }
    tr
}

fn replay_client_stream(session: &Session, method: &str, parts: &[&str]) -> Transcript {
    let GrpcStream::Replay(rp) = GrpcStream::open(
        session,
        StreamType::Client,
        &format!("/{SERVICE}/{method}"),
        None,
    )
    .expect("replay open") else {
        panic!("expected replay")
    };
    for p in parts {
        rp.send(&enc(p)).expect("recorded send");
    }
    rp.close_send().expect("half-close");

    let mut tr = Transcript {
        msgs: vec![],
        terminal: ok_terminal(),
    };
    loop {
        match rp.recv() {
            Ok(Some(b)) => tr.msgs.push(dec(&b)),
            Ok(None) => break,
            Err(s) => {
                tr.terminal = err_terminal(&s);
                break;
            }
        }
    }
    tr
}

fn replay_bidi(session: &Session, method: &str, pings: &[&str]) -> Transcript {
    let GrpcStream::Replay(rp) = GrpcStream::open(
        session,
        StreamType::Bidi,
        &format!("/{SERVICE}/{method}"),
        None,
    )
    .expect("replay open") else {
        panic!("expected replay")
    };
    for p in pings {
        rp.send(&enc(p)).expect("recorded send");
    }
    rp.close_send().expect("half-close");

    let mut tr = Transcript {
        msgs: vec![],
        terminal: ok_terminal(),
    };
    loop {
        match rp.recv() {
            Ok(Some(b)) => tr.msgs.push(dec(&b)),
            Ok(None) => break,
            Err(s) => {
                tr.terminal = err_terminal(&s);
                break;
            }
        }
    }
    tr
}

// ── the round-trip test ──────────────────────────────────────────────────────

/// Record every streaming shape against a live tonic server, stop the
/// server, verify the port is dead, then replay the same drivers from
/// cassettes only and assert the transcripts match.
#[tokio::test]
async fn record_live_then_replay_with_server_stopped() {
    let dir = tempfile::tempdir().unwrap();

    // ── phase 1: record against the live server ──────────────────────────
    let server = start_server().await;
    let addr = server.addr;
    let mut client = LiveClient::connect(addr).await;
    let rec_session = Session::new(Mode::Record, FileCassette::new(dir.path()));

    let live_server_stream =
        record_server_stream(&rec_session, &mut client, "Download", "file").await;
    let live_empty = record_server_stream(&rec_session, &mut client, "Download", "empty").await;
    let live_error = record_server_stream(&rec_session, &mut client, "Download", "boom").await;
    let live_client_stream = record_client_stream(
        &rec_session,
        &mut client,
        "Upload",
        &["part-one", "part-two", "part-three"],
    )
    .await;
    let live_bidi = record_bidi(&rec_session, &mut client, "Converse", &["ping-1", "ping-2"]).await;

    // Live sanity: the interesting shapes actually happened over the wire.
    assert_eq!(
        live_server_stream.msgs,
        ["file-chunk-1", "file-chunk-2", "file-chunk-3"]
    );
    assert_eq!(live_server_stream.terminal, ok_terminal());
    assert!(live_empty.msgs.is_empty(), "empty stream sent no messages");
    assert_eq!(live_error.msgs, ["log-chunk-1", "log-chunk-2"]);
    assert_eq!(live_error.terminal, "Unavailable/connection reset");
    assert_eq!(live_client_stream.msgs, ["received:26"]);
    assert_eq!(live_bidi.msgs, ["pong:ping-1", "pong:ping-2"]);

    // One req/resp pair per recorded interaction.
    let files: Vec<_> = std::fs::read_dir(dir.path()).unwrap().collect();
    assert_eq!(files.len(), 10, "5 interactions × 2 files");

    drop(client);

    // ── phase 2: server STOPPED, port closed ─────────────────────────────
    server.stop().await;
    assert_port_dead(addr).await;

    // Replay runs with NO client, NO channel, NO socket: a dial is not
    // merely unused, it is impossible from this code path.
    let rep_session = Session::new(Mode::Replay, FileCassette::new(dir.path()));

    assert_eq!(
        replay_server_stream(&rep_session, "Download", "file"),
        live_server_stream,
        "server-stream replay must match the live transcript"
    );
    assert_eq!(
        replay_server_stream(&rep_session, "Download", "empty"),
        live_empty,
        "empty-stream replay must match"
    );
    assert_eq!(
        replay_server_stream(&rep_session, "Download", "boom"),
        live_error,
        "mid-stream-error replay must reconstruct the status"
    );
    assert_eq!(
        replay_client_stream(
            &rep_session,
            "Upload",
            &["part-one", "part-two", "part-three"]
        ),
        live_client_stream,
        "client-stream replay must match"
    );
    assert_eq!(
        replay_bidi(&rep_session, "Converse", &["ping-1", "ping-2"]),
        live_bidi,
        "bidi replay must match"
    );
}

/// Replaying a stream that was never recorded fails loudly with a cassette
/// miss — not a hang, and not a dial.
#[tokio::test]
async fn replay_cassette_miss_without_a_server() {
    let dir = tempfile::tempdir().unwrap();
    let session = Session::new(Mode::Replay, FileCassette::new(dir.path()));

    let err = GrpcStream::open(
        &session,
        StreamType::Client,
        &format!("/{SERVICE}/Upload"),
        None,
    )
    .expect_err("nothing was recorded");
    assert!(matches!(err, XrrError::CassetteMiss { .. }), "got {err:?}");

    let err = GrpcStream::open(
        &session,
        StreamType::Server,
        &format!("/{SERVICE}/Download"),
        Some(&enc("file")),
    )
    .expect_err("nothing was recorded");
    assert!(matches!(err, XrrError::CassetteMiss { .. }), "got {err:?}");
}

/// Passthrough is transparent: live traffic works and the cassette dir
/// stays empty.
#[tokio::test]
async fn passthrough_touches_no_cassette() {
    let dir = tempfile::tempdir().unwrap();
    let server = start_server().await;
    let mut client = LiveClient::connect(server.addr).await;

    let session = Session::new(Mode::Passthrough, FileCassette::new(dir.path()));
    let stream = GrpcStream::open(
        &session,
        StreamType::Server,
        &format!("/{SERVICE}/Download"),
        Some(&enc("file")),
    )
    .unwrap();
    assert!(matches!(stream, GrpcStream::Passthrough));

    let mut live = client
        .server_streaming("Download", enc("file"))
        .await
        .unwrap();
    let mut msgs = vec![];
    while let Some(b) = live.message().await.unwrap() {
        msgs.push(dec(&b));
    }
    assert_eq!(msgs, ["file-chunk-1", "file-chunk-2", "file-chunk-3"]);

    assert_eq!(
        std::fs::read_dir(dir.path()).unwrap().count(),
        0,
        "passthrough must not write cassettes"
    );

    drop(client);
    server.stop().await;
}

// ── frame scrub over real traffic ────────────────────────────────────────────

/// A server stream whose traffic carries a fake token in BOTH directions —
/// the hazard the spec's redaction warning describes. Recording through the
/// frame scrub hook must keep the token out of the cassette in any
/// encoding, and replaying the same secret-bearing traffic through the same
/// hook must be green: the scrubbed open message addresses the same
/// fingerprint on both sides, and the scrubbed live sends match the
/// scrubbed recorded frames.
#[tokio::test]
async fn scrub_hook_keeps_secrets_out_of_cassettes_and_replays_green() {
    let dir = tempfile::tempdir().unwrap();

    // Equal-length mask: frames are protobuf wire bytes, so the scrub must
    // preserve the encoding's structure (spec: structure-preserving).
    assert_eq!(SECRET.len(), MASK.len(), "mask must preserve length");
    let calls = Arc::new(AtomicUsize::new(0));
    let seen = calls.clone();
    let scrub: hop_top_xrr::StreamScrub = Arc::new(move |_dir, _info, data: &[u8]| {
        seen.fetch_add(1, Ordering::SeqCst);
        let needle = SECRET.as_bytes();
        let hay = Bytes::copy_from_slice(data);
        let mut out = Vec::with_capacity(hay.len());
        let mut i = 0;
        while i < hay.len() {
            if hay.len() - i >= needle.len() && &hay[i..i + needle.len()] == needle {
                out.extend_from_slice(MASK.as_bytes());
                i += needle.len();
            } else {
                out.push(hay[i]);
                i += 1;
            }
        }
        out
    });

    // ── record through the scrub against the live server
    let server = start_server().await;
    let addr = server.addr;
    let mut client = LiveClient::connect(addr).await;
    let rec_session =
        Session::with_stream_scrub(Mode::Record, FileCassette::new(dir.path()), scrub.clone());

    let request = format!("token:{SECRET}");
    let live = record_server_stream(&rec_session, &mut client, "Download", &request).await;

    // The live run sees the REAL bytes; only the cassette is scrubbed.
    assert_eq!(
        live.msgs,
        [format!("granted:{SECRET}"), "done".to_string()],
        "the live server response is untouched"
    );
    assert!(calls.load(Ordering::SeqCst) > 0, "the hook must have run");

    drop(client);
    server.stop().await;
    assert_port_dead(addr).await;

    // ── the cassette must not contain the token, in any encoding
    for entry in std::fs::read_dir(dir.path()).unwrap() {
        let path = entry.unwrap().path();
        let raw = std::fs::read_to_string(&path).unwrap();
        assert!(!raw.contains(SECRET), "{path:?} leaks the secret verbatim");
    }

    // Decoded frame bytes are scrubbed too — base64 hides a plain scan.
    let scrubbed_open = {
        let info = hop_top_xrr::StreamScrubInfo {
            adapter_id: "grpc".into(),
            stream_type: StreamType::Server,
        };
        rec_session.scrub_stream_frame(StreamDirection::Send, &info, &enc(&request))
    };
    let fp = {
        use sha2::{Digest, Sha256};
        let msg_hash = hex::encode(&Sha256::digest(&scrubbed_open)[..4]);
        let canonical = format!(
            r#"{{"method":"Download","msg_hash":"{msg_hash}","service":"{SERVICE}","stream":"server"}}"#
        );
        hex::encode(&Sha256::digest(canonical.as_bytes())[..4])
    };
    let pair = StreamedPair::load(dir.path(), "grpc", &fp).expect("scrubbed pair loads");
    for frame in pair
        .req
        .stream
        .frames
        .iter()
        .chain(&pair.resp.stream.frames)
    {
        let text = String::from_utf8_lossy(&frame.bytes);
        assert!(
            !text.contains(SECRET),
            "decoded frame bytes at seq {} leak the secret",
            frame.seq
        );
    }
    assert!(
        String::from_utf8_lossy(&pair.resp.stream.frames[0].bytes).contains(MASK),
        "the mask should be visible where the secret was"
    );

    // ── replay the SAME secret-bearing traffic through the SAME hook
    let rep_session =
        Session::with_stream_scrub(Mode::Replay, FileCassette::new(dir.path()), scrub.clone());
    let replayed = replay_server_stream(&rep_session, "Download", &request);
    assert_eq!(
        replayed.msgs,
        [format!("granted:{MASK}"), "done".to_string()],
        "replay delivers the scrubbed recording"
    );
    assert_eq!(
        replayed.terminal,
        ok_terminal(),
        "symmetric scrub replays green"
    );

    // ── asymmetry is loud: no hook on the replaying session ⇒ the open
    // message hashes differently, so the cassette is not even found.
    let bare = Session::new(Mode::Replay, FileCassette::new(dir.path()));
    let err = GrpcStream::open(
        &bare,
        StreamType::Server,
        &format!("/{SERVICE}/Download"),
        Some(&enc(&request)),
    )
    .expect_err("replaying a scrubbed cassette without the hook must fail loudly");
    assert!(matches!(err, XrrError::CassetteMiss { .. }), "got {err:?}");
}
