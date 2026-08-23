"""Round-trip tests against a REAL grpcio server.

Each test records live traffic through the interceptor, STOPS the server,
and replays with no server listening — so a replay that reached the
network would fail to connect rather than pass. Transcripts are compared
byte-for-byte, and the cassette files on disk are asserted against the
spec's stream schema.
"""
from __future__ import annotations

from concurrent import futures

import grpc
import pytest

from grpcfixture import xrrtest_pb2, xrrtest_pb2_grpc
from xrr.adapters.grpc import GrpcStreamInterceptor, deterministic_serializer
from xrr.cassette import FileCassette
from xrr.session import PASSTHROUGH, RECORD, REPLAY, Session

_CHUNK = xrrtest_pb2.Chunk


class _Streamer(xrrtest_pb2_grpc.StreamerServicer):
    """Deterministic echo-ish server covering all three stream kinds."""

    def Download(self, request, context):
        for i in range(3):
            yield _CHUNK(body=f"{request.body}-chunk-{i}")

    def Upload(self, request_iterator, context):
        bodies = [r.body for r in request_iterator]
        return _CHUNK(body="+".join(bodies))

    def Converse(self, request_iterator, context):
        for r in request_iterator:
            yield _CHUNK(body=f"pong:{r.body}")


class _Server:
    """A real grpcio server on a real loopback port, in-process."""

    def __init__(self) -> None:
        self._server = grpc.server(futures.ThreadPoolExecutor(max_workers=4))
        xrrtest_pb2_grpc.add_StreamerServicer_to_server(_Streamer(), self._server)
        self.port = self._server.add_insecure_port("127.0.0.1:0")
        self._server.start()

    @property
    def target(self) -> str:
        return f"127.0.0.1:{self.port}"

    def stop(self) -> None:
        self._server.stop(grace=None).wait()


@pytest.fixture
def server():
    s = _Server()
    yield s
    s.stop()


def _interceptor(session: Session) -> GrpcStreamInterceptor:
    """Register the wire codecs for every method under test. A Python
    client interceptor cannot see the serializers bound at
    channel.stream_stream(), so they are supplied here."""
    ser = deterministic_serializer(_CHUNK)
    interceptor = GrpcStreamInterceptor(session)
    for method in ("Download", "Upload", "Converse"):
        interceptor.register(
            f"/xrrtest.Streamer/{method}", ser, _CHUNK.FromString
        )
    return interceptor


def _stub(target: str, session: Session):
    channel = grpc.insecure_channel(target)
    return xrrtest_pb2_grpc.StreamerStub(
        grpc.intercept_channel(channel, _interceptor(session))
    ), channel


# Replaying against this target must never connect: nothing listens on
# the discard port, so any real network attempt fails instead of passing.
_DEAD_TARGET = "127.0.0.1:9"


def _session(mode: str, tmp_path, **kw) -> Session:
    return Session(mode, FileCassette(str(tmp_path)), **kw)


# ── server-streaming ─────────────────────────────────────────────────────────


def test_server_stream_record_then_replay_offline(server, tmp_path):
    stub, chan = _stub(server.target, _session(RECORD, tmp_path))
    live = [c.body for c in stub.Download(_CHUNK(body="req"))]
    chan.close()
    assert live == ["req-chunk-0", "req-chunk-1", "req-chunk-2"]

    server.stop()  # no server from here on

    stub, chan = _stub(_DEAD_TARGET, _session(REPLAY, tmp_path))
    replayed = [c.body for c in stub.Download(_CHUNK(body="req"))]
    chan.close()
    assert replayed == live


def test_server_stream_cassette_shape(server, tmp_path):
    stub, chan = _stub(server.target, _session(RECORD, tmp_path))
    list(stub.Download(_CHUNK(body="req")))
    chan.close()

    pair = _sole_pair(tmp_path)
    assert pair.req_stream.type == "server"
    assert pair.req_payload == {
        "service": "xrrtest.Streamer",
        "method": "Download",
    }
    assert pair.resp_payload == {"status_code": 0}
    assert pair.error == ""
    # Exactly one send frame, half_close immediately after it.
    assert len(pair.req_stream.frames) == 1
    assert pair.req_stream.half_close is not None
    assert pair.req_stream.half_close.seq == pair.req_stream.frames[0].seq + 1
    # Frames carry protobuf wire bytes, never text (spec: gRPC writers
    # MUST use message_b64).
    assert all(not f.text for f in pair.resp_stream.frames)
    assert [
        _CHUNK.FromString(f.message).body for f in pair.resp_stream.frames
    ] == ["req-chunk-0", "req-chunk-1", "req-chunk-2"]
    # Dense 0..N-1: send(0), half_close(1), 3 recv(2,3,4), end(5) —
    # the same shape as the spec's worked server-stream example.
    assert pair.resp_stream.end.seq == 5


# ── client-streaming ─────────────────────────────────────────────────────────


def test_client_stream_record_then_replay_offline(server, tmp_path):
    bodies = ["a", "b", "c"]
    stub, chan = _stub(server.target, _session(RECORD, tmp_path))
    live = stub.Upload(iter(_CHUNK(body=b) for b in bodies)).body
    chan.close()
    assert live == "a+b+c"

    server.stop()

    stub, chan = _stub(_DEAD_TARGET, _session(REPLAY, tmp_path))
    replayed = stub.Upload(iter(_CHUNK(body=b) for b in bodies)).body
    chan.close()
    assert replayed == live


def test_client_stream_cassette_shape(server, tmp_path):
    stub, chan = _stub(server.target, _session(RECORD, tmp_path))
    stub.Upload(iter(_CHUNK(body=b) for b in ("a", "b")))
    chan.close()

    pair = _sole_pair(tmp_path)
    assert pair.req_stream.type == "client"
    # Counter-addressed opens record the informational ordinal.
    assert pair.req_payload["n"] == 0
    assert len(pair.req_stream.frames) == 2
    assert pair.req_stream.half_close is not None
    # At most one recv frame for a client stream.
    assert len(pair.resp_stream.frames) == 1
    assert pair.resp_payload == {"status_code": 0}


def test_client_stream_divergent_send_is_mismatch(server, tmp_path):
    stub, chan = _stub(server.target, _session(RECORD, tmp_path))
    stub.Upload(iter(_CHUNK(body=b) for b in ("a", "b")))
    chan.close()

    server.stop()

    stub, chan = _stub(_DEAD_TARGET, _session(REPLAY, tmp_path))
    with pytest.raises(grpc.RpcError) as excinfo:
        stub.Upload(iter(_CHUNK(body=b) for b in ("a", "DIVERGENT")))
    chan.close()
    assert excinfo.value.code() == grpc.StatusCode.FAILED_PRECONDITION
    assert "stream mismatch" in excinfo.value.details()


def test_client_stream_short_half_close_is_mismatch(server, tmp_path):
    stub, chan = _stub(server.target, _session(RECORD, tmp_path))
    stub.Upload(iter(_CHUNK(body=b) for b in ("a", "b")))
    chan.close()

    server.stop()

    # One send then end-of-iterator: a half-close after fewer than S sends.
    stub, chan = _stub(_DEAD_TARGET, _session(REPLAY, tmp_path))
    with pytest.raises(grpc.RpcError) as excinfo:
        stub.Upload(iter([_CHUNK(body="a")]))
    chan.close()
    assert excinfo.value.code() == grpc.StatusCode.FAILED_PRECONDITION
    assert "half_close" in excinfo.value.details()


def test_client_stream_repeat_opens_distinct_cassettes(server, tmp_path):
    """Two opens of the same tuple in one session get n=0 and n=1, so
    they address distinct cassettes rather than overwriting."""
    session = _session(RECORD, tmp_path)
    stub, chan = _stub(server.target, session)
    first = stub.Upload(iter([_CHUNK(body="first")])).body
    second = stub.Upload(iter([_CHUNK(body="second")])).body
    chan.close()
    assert (first, second) == ("first", "second")

    server.stop()

    replay_session = _session(REPLAY, tmp_path)
    stub, chan = _stub(_DEAD_TARGET, replay_session)
    assert stub.Upload(iter([_CHUNK(body="first")])).body == "first"
    assert stub.Upload(iter([_CHUNK(body="second")])).body == "second"
    chan.close()


# ── bidi ─────────────────────────────────────────────────────────────────────


def test_bidi_record_then_replay_offline(server, tmp_path):
    bodies = ["p1", "p2", "p3"]
    stub, chan = _stub(server.target, _session(RECORD, tmp_path))
    live = [c.body for c in stub.Converse(iter(_CHUNK(body=b) for b in bodies))]
    chan.close()
    assert live == ["pong:p1", "pong:p2", "pong:p3"]

    server.stop()

    stub, chan = _stub(_DEAD_TARGET, _session(REPLAY, tmp_path))
    replayed = [
        c.body for c in stub.Converse(iter(_CHUNK(body=b) for b in bodies))
    ]
    chan.close()
    assert replayed == live


def test_bidi_cassette_interleaves_seq(server, tmp_path):
    stub, chan = _stub(server.target, _session(RECORD, tmp_path))
    list(stub.Converse(iter(_CHUNK(body=b) for b in ("p1", "p2"))))
    chan.close()

    pair = _sole_pair(tmp_path)
    assert pair.req_stream.type == "bidi"
    assert pair.req_payload["n"] == 0
    seqs = (
        [f.seq for f in pair.req_stream.frames]
        + [f.seq for f in pair.resp_stream.frames]
        + [pair.req_stream.half_close.seq, pair.resp_stream.end.seq]
    )
    # Dense 0..N-1 with no duplicates, end last.
    assert sorted(seqs) == list(range(len(seqs)))
    assert pair.resp_stream.end.seq == max(seqs)


def test_bidi_reads_do_not_block_on_send_progress(server, tmp_path):
    """A replaying client that reads every response before its request
    iterator would have produced them still completes: replay never gates
    recv delivery on send position (spec: Ordering)."""
    stub, chan = _stub(server.target, _session(RECORD, tmp_path))
    list(stub.Converse(iter(_CHUNK(body=b) for b in ("p1", "p2"))))
    chan.close()

    server.stop()

    stub, chan = _stub(_DEAD_TARGET, _session(REPLAY, tmp_path))
    # A generator that yields lazily: a send-gated replay would deadlock.
    def lazy():
        yield _CHUNK(body="p1")
        yield _CHUNK(body="p2")

    got = [c.body for c in stub.Converse(lazy())]
    chan.close()
    assert got == ["pong:p1", "pong:p2"]


# ── mid-stream error ─────────────────────────────────────────────────────────


class _FailingStreamer(xrrtest_pb2_grpc.StreamerServicer):
    """Delivers two chunks, then aborts with a non-OK status."""

    def Download(self, request, context):
        yield _CHUNK(body="log-chunk-1")
        yield _CHUNK(body="log-chunk-2")
        context.abort(grpc.StatusCode.UNAVAILABLE, "connection reset")


def test_mid_stream_error_records_and_replays(tmp_path):
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=2))
    xrrtest_pb2_grpc.add_StreamerServicer_to_server(_FailingStreamer(), server)
    port = server.add_insecure_port("127.0.0.1:0")
    server.start()

    stub, chan = _stub(f"127.0.0.1:{port}", _session(RECORD, tmp_path))
    got, live_err = _drain(stub.Download(_CHUNK(body="big.log")))
    chan.close()
    server.stop(grace=None).wait()

    assert got == ["log-chunk-1", "log-chunk-2"]
    assert live_err is not None
    assert live_err.code() == grpc.StatusCode.UNAVAILABLE

    pair = _sole_pair(tmp_path)
    # All recorded recv frames, then end, plus the non-empty envelope
    # error and the status code in the payload.
    assert len(pair.resp_stream.frames) == 2
    assert pair.resp_payload == {"status_code": grpc.StatusCode.UNAVAILABLE.value[0]}
    assert pair.error != ""

    stub, chan = _stub(_DEAD_TARGET, _session(REPLAY, tmp_path))
    replayed, replay_err = _drain(stub.Download(_CHUNK(body="big.log")))
    chan.close()
    assert replayed == got
    assert replay_err is not None
    # Status reconstructed from status_code (spec: Response Payload and
    # Errors); the recorded string is the description text.
    assert replay_err.code() == grpc.StatusCode.UNAVAILABLE
    assert replay_err.details() == live_err.details()


# ── empty stream ─────────────────────────────────────────────────────────────


class _SilentStreamer(xrrtest_pb2_grpc.StreamerServicer):
    """Finishes OK having sent nothing."""

    def Download(self, request, context):
        return iter(())


def test_empty_server_stream(tmp_path):
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=2))
    xrrtest_pb2_grpc.add_StreamerServicer_to_server(_SilentStreamer(), server)
    port = server.add_insecure_port("127.0.0.1:0")
    server.start()

    stub, chan = _stub(f"127.0.0.1:{port}", _session(RECORD, tmp_path))
    assert [c.body for c in stub.Download(_CHUNK(body="x"))] == []
    chan.close()
    server.stop(grace=None).wait()

    pair = _sole_pair(tmp_path)
    assert pair.resp_stream.frames == []

    stub, chan = _stub(_DEAD_TARGET, _session(REPLAY, tmp_path))
    # First read yields end-of-stream immediately.
    assert [c.body for c in stub.Download(_CHUNK(body="x"))] == []
    chan.close()


# ── passthrough ──────────────────────────────────────────────────────────────


def test_passthrough_writes_no_cassette(server, tmp_path):
    stub, chan = _stub(server.target, _session(PASSTHROUGH, tmp_path))
    got = [c.body for c in stub.Download(_CHUNK(body="p"))]
    chan.close()
    assert got == ["p-chunk-0", "p-chunk-1", "p-chunk-2"]
    assert list(tmp_path.iterdir()) == []


# ── helpers ──────────────────────────────────────────────────────────────────


def _drain(call) -> tuple[list[str], grpc.RpcError | None]:
    """Read a call-iterator to its terminal, returning the bodies and the
    terminal RpcError (None for an OK terminal)."""
    bodies = []
    try:
        for chunk in call:
            bodies.append(chunk.body)
    except grpc.RpcError as exc:
        return bodies, exc
    return bodies, None


def _sole_pair(tmp_path):
    """Load the single streamed pair written into tmp_path."""
    reqs = sorted(tmp_path.glob("*.req.yaml"))
    assert len(reqs) == 1, f"expected 1 cassette pair, got {[p.name for p in reqs]}"
    adapter, fingerprint = reqs[0].name.split(".")[0].split("-", 1)
    return FileCassette(str(tmp_path)).load_stream(adapter, fingerprint)
