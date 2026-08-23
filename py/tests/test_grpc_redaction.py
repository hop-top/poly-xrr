"""Frame-level redaction for the gRPC adapter.

Field-name redaction (redact.py) covers structured payload fields — the
adapter's open-request and terminal-response objects. It cannot reach
frame payloads: those are opaque bytes, base64-encoded on disk, so no
field name exists to match and the encoding defeats scrubbing the
cassette text afterwards. Frames are covered by the session's symmetric
scrub hook instead (spec: REDACTION WARNING).

These tests pin both halves of that split, and the symmetry invariant
that makes a scrubbed cassette replayable.
"""
from __future__ import annotations

import base64
from concurrent import futures

import grpc
import pytest

from grpcfixture import xrrtest_pb2, xrrtest_pb2_grpc
from xrr.adapters.grpc import GrpcStreamInterceptor, deterministic_serializer
from xrr.cassette import FileCassette
from xrr.session import RECORD, REPLAY, Session

_CHUNK = xrrtest_pb2.Chunk
_TOKEN = "ghp_0123456789abcdefghijklmnopqrstuvwx"
# Derived, not spelled out: the replacement MUST be the same length as
# the token, or the length-delimited protobuf field it sits in stops
# decoding.
_PLACEHOLDER = "ghp_" + "x" * (len(_TOKEN) - 4)


def _scrub(_direction, _info, data: bytes) -> bytes:
    """Deterministic, length-preserving frame scrub.

    Equal-length replacement keeps the protobuf wire encoding valid, as
    the hook contract requires — a length-changing edit would corrupt the
    length-delimited field the token sits in.
    """
    return data.replace(_TOKEN.encode(), _PLACEHOLDER.encode())


class _EchoStreamer(xrrtest_pb2_grpc.StreamerServicer):
    def Download(self, request, context):
        # The server echoes the credential back, so it appears on BOTH
        # the send and the recv side.
        yield _CHUNK(body=f"received {request.body}")


@pytest.fixture
def server():
    s = grpc.server(futures.ThreadPoolExecutor(max_workers=2))
    xrrtest_pb2_grpc.add_StreamerServicer_to_server(_EchoStreamer(), s)
    port = s.add_insecure_port("127.0.0.1:0")
    s.start()
    yield f"127.0.0.1:{port}"
    s.stop(grace=None).wait()


def _stub(target: str, session: Session):
    interceptor = GrpcStreamInterceptor(session).register(
        "/xrrtest.Streamer/Download",
        deterministic_serializer(_CHUNK),
        _CHUNK.FromString,
    )
    channel = grpc.insecure_channel(target)
    return (
        xrrtest_pb2_grpc.StreamerStub(grpc.intercept_channel(channel, interceptor)),
        channel,
    )


def _cassette_text(tmp_path) -> str:
    return "".join(p.read_text() for p in sorted(tmp_path.glob("*.yaml")))


def _decoded_frames(tmp_path) -> list[bytes]:
    """Every frame's decoded bytes — what a reader actually gets back,
    as opposed to the base64 that hides it in the raw file."""
    reqs = sorted(tmp_path.glob("*.req.yaml"))
    adapter, fingerprint = reqs[0].name.split(".")[0].split("-", 1)
    pair = FileCassette(str(tmp_path)).load_stream(adapter, fingerprint)
    return [f.message for f in pair.req_stream.frames + pair.resp_stream.frames]


def test_frames_are_verbatim_without_the_hook(server, tmp_path):
    """Baseline: no hook means frames record verbatim, so the credential
    survives into the cassette. This is the gap the hook closes — and the
    reason a port without it must not record streams carrying secrets."""
    stub, chan = _stub(server, Session(RECORD, FileCassette(str(tmp_path))))
    list(stub.Download(_CHUNK(body=_TOKEN)))
    chan.close()

    assert any(_TOKEN.encode() in f for f in _decoded_frames(tmp_path))
    # And base64 is exactly why scrubbing the file text cannot find it.
    assert _TOKEN not in _cassette_text(tmp_path)


def test_scrub_hook_removes_credential_from_frames(server, tmp_path):
    """With the hook installed the credential reaches neither direction's
    frames, in decoded or encoded form."""
    session = Session(RECORD, FileCassette(str(tmp_path)), stream_scrub=_scrub)
    stub, chan = _stub(server, session)
    list(stub.Download(_CHUNK(body=_TOKEN)))
    chan.close()

    frames = _decoded_frames(tmp_path)
    assert frames, "expected recorded frames"
    assert not any(_TOKEN.encode() in f for f in frames)
    assert any(_PLACEHOLDER.encode() in f for f in frames)
    # Neither the raw token nor its base64 form appears anywhere on disk.
    text = _cassette_text(tmp_path)
    assert _TOKEN not in text
    assert base64.b64encode(_TOKEN.encode()).decode().rstrip("=") not in text


def test_scrubbed_cassette_replays_with_the_same_hook(server, tmp_path):
    """Symmetry: live send bytes are scrubbed before comparison, and the
    server-stream fingerprint is derived from scrubbed bytes, so the same
    live traffic addresses the same cassette in both modes."""
    stub, chan = _stub(
        server, Session(RECORD, FileCassette(str(tmp_path)), stream_scrub=_scrub)
    )
    live = [c.body for c in stub.Download(_CHUNK(body=_TOKEN))]
    chan.close()

    stub, chan = _stub(
        "127.0.0.1:9", Session(REPLAY, FileCassette(str(tmp_path)), stream_scrub=_scrub)
    )
    replayed = [c.body for c in stub.Download(_CHUNK(body=_TOKEN))]
    chan.close()
    # The replayed body carries the placeholder — the recording never
    # held the real credential.
    assert replayed == [b.replace(_TOKEN, _PLACEHOLDER) for b in live]


def test_replay_without_the_hook_fails_loudly(server, tmp_path):
    """The hook must be installed on record AND replay. Without it the
    server-stream fingerprint is derived from unscrubbed bytes, so the
    cassette is not found — loud, not silently wrong."""
    from xrr.cassette import CassetteMiss

    stub, chan = _stub(
        server, Session(RECORD, FileCassette(str(tmp_path)), stream_scrub=_scrub)
    )
    list(stub.Download(_CHUNK(body=_TOKEN)))
    chan.close()

    stub, chan = _stub("127.0.0.1:9", Session(REPLAY, FileCassette(str(tmp_path))))
    with pytest.raises(CassetteMiss):
        list(stub.Download(_CHUNK(body=_TOKEN)))
    chan.close()


def test_payload_redaction_still_covers_named_fields(tmp_path):
    """The two mechanisms are complementary: the scrub hook owns frame
    bytes, record-time field-name redaction owns the structured payloads.
    A credential-named field in a unary payload is still placeholdered."""
    cassette = FileCassette(str(tmp_path))
    cassette.save("grpc", "deadbeef", {"api_token": _TOKEN}, {"status_code": 0})
    text = (tmp_path / "grpc-deadbeef.req.yaml").read_text()
    assert _TOKEN not in text
    assert "<redacted:API_TOKEN>" in text
