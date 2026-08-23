"""Adapter-level conformance: replay every gRPC stream fixture in
spec/fixtures/ THROUGH the gRPC adapter.

tests/test_conformance.py covers the format layer (parse / re-emit /
validate). This file covers the obligations the spec puts on ports that
ship a gRPC adapter (cassette-format-streaming.md, Conformance
Obligations → "Adapter level"): the fingerprint recomputed at open
locates the pair, recv frames arrive in seq order, sends are validated,
terminals are reconstructed from status_code, and a malformed cassette
is rejected rather than silently accepted.

Fixture frames carry JSON/text bytes rather than protobuf wire bytes, so
these tests register identity codecs — the adapter takes its serializer
pair from the caller precisely so the frame layer stays codec-agnostic.
No channel is ever created: replay serves the recording with no network,
so the interceptor's replay path is driven directly.
"""
from __future__ import annotations

from pathlib import Path
from typing import Any

import grpc
import pytest

from xrr.adapters.grpc import GrpcStreamInterceptor
from xrr.cassette import FileCassette
from xrr.session import REPLAY, Session
from xrr.stream import StreamFormatError

_FIXTURES_DIR = Path(__file__).resolve().parent.parent.parent / "spec" / "fixtures"


def _identity(data: Any) -> Any:  # noqa: ANN401 - codec seam is bytes-in/bytes-out
    """Identity codec: fixture frames are raw bytes, not protobuf."""
    return data


class _CallDetails:
    """Minimal grpc.ClientCallDetails: the adapter reads only `method`."""

    def __init__(self, method: str) -> None:
        self.method = method
        self.timeout = None
        self.metadata = None
        self.credentials = None
        self.wait_for_ready = None
        self.compression = None


def _no_continuation(*_args, **_kwargs):
    """Replay must never call the continuation — doing so would mean the
    adapter tried to perform a real RPC."""
    raise AssertionError("xrr: replay invoked the live continuation")


def _interceptor(fixture: str, full_method: str) -> GrpcStreamInterceptor:
    session = Session(REPLAY, FileCassette(str(_FIXTURES_DIR / fixture)))
    return GrpcStreamInterceptor(session).register(
        full_method, _identity, _identity, response_serializer=_identity
    )


def _server_stream(fixture: str, full_method: str, request: bytes):
    return _interceptor(fixture, full_method).intercept_unary_stream(
        _no_continuation, _CallDetails(full_method), request
    )


def _client_stream(fixture: str, full_method: str, requests: list[bytes]):
    return _interceptor(fixture, full_method).intercept_stream_unary(
        _no_continuation, _CallDetails(full_method), iter(requests)
    )


def _bidi_stream(fixture: str, full_method: str, requests: list[bytes]):
    return _interceptor(fixture, full_method).intercept_stream_stream(
        _no_continuation, _CallDetails(full_method), iter(requests)
    )


# ── server-stream ────────────────────────────────────────────────────────────


def test_server_stream_fixture():
    """Fingerprint recomputed from (service, method, msg_hash, "server")
    locates the pair; recv frames delivered in seq order; end-of-stream
    after the last."""
    call = _server_stream(
        "grpc-server-stream", "/files.FileService/Download", b'{"path":"/etc/hosts"}'
    )
    assert list(call) == [b"chunk-one\n", b"chunk-two\n", b"chunk-three\n"]
    # The terminal repeats and is replayable indefinitely.
    assert list(call) == []
    assert call.code() == grpc.StatusCode.OK


def test_server_stream_wrong_request_misses():
    """A different request message is a different fingerprint, so it
    misses rather than replaying the recorded stream."""
    from xrr.cassette import CassetteMiss

    with pytest.raises(CassetteMiss):
        _server_stream(
            "grpc-server-stream", "/files.FileService/Download", b'{"path":"/other"}'
        )


# ── client-stream ────────────────────────────────────────────────────────────


def test_client_stream_fixture():
    """Occurrence-counter fingerprint (n=0) locates the pair; sends
    validated in order; single response frame then end-of-stream."""
    call = _client_stream(
        "grpc-client-stream",
        "/files.FileService/Upload",
        [b"part-one\n", b"part-two\n"],
    )
    assert call.result() == b'{"received_bytes":18}'


def test_client_stream_divergent_send_is_mismatch():
    call = _client_stream(
        "grpc-client-stream",
        "/files.FileService/Upload",
        [b"part-one\n", b"WRONG\n"],
    )
    with pytest.raises(grpc.RpcError) as excinfo:
        call.result()
    assert excinfo.value.code() == grpc.StatusCode.FAILED_PRECONDITION
    assert "send 1" in excinfo.value.details()


def test_client_stream_short_half_close_is_mismatch():
    call = _client_stream(
        "grpc-client-stream", "/files.FileService/Upload", [b"part-one\n"]
    )
    with pytest.raises(grpc.RpcError) as excinfo:
        call.result()
    assert excinfo.value.code() == grpc.StatusCode.FAILED_PRECONDITION
    assert "half_close" in excinfo.value.details()


def test_client_stream_repeat_n0_then_n1():
    """The spec's n=1 obligation: a second open of the same tuple within
    ONE session addresses the second cassette. One interceptor, one
    session, two sequential opens."""
    full_method = "/files.FileService/Upload"
    interceptor = _interceptor("grpc-client-stream-repeat", full_method)

    first = interceptor.intercept_stream_unary(
        _no_continuation, _CallDetails(full_method), iter([b"alpha\n"])
    )
    assert first.result() == b'{"received_bytes":6}'

    second = interceptor.intercept_stream_unary(
        _no_continuation, _CallDetails(full_method), iter([b"beta-1\n", b"beta-2\n"])
    )
    assert second.result() == b'{"received_bytes":14}'


# ── bidi ─────────────────────────────────────────────────────────────────────


def test_bidi_fixture():
    """Interleaved global seq parsed; per-direction ordering enforced;
    reads never block on send progress."""
    call = _bidi_stream(
        "grpc-bidi-stream", "/chat.ChatService/Converse", [b"ping-1", b"ping-2"]
    )
    assert list(call) == [b"pong-1", b"pong-2"]


def test_bidi_reads_do_not_block_on_send_progress():
    """The recording interleaves sends and recvs (seq 0..5), but replay
    must not gate recv delivery on send position — a client that reads
    both pongs before its second ping would deadlock otherwise."""
    full_method = "/chat.ChatService/Converse"

    def lazy_requests():
        yield b"ping-1"
        yield b"ping-2"

    call = _interceptor("grpc-bidi-stream", full_method).intercept_stream_stream(
        _no_continuation, _CallDetails(full_method), lazy_requests()
    )
    assert list(call) == [b"pong-1", b"pong-2"]


# ── mid-stream error ─────────────────────────────────────────────────────────


def test_mid_stream_error_fixture():
    """All recorded recv frames delivered, then the recorded error
    (status reconstructed from status_code) in place of end-of-stream."""
    call = _server_stream(
        "grpc-stream-error",
        "/files.FileService/Download",
        b'{"path":"/var/log/big.log"}',
    )
    got = []
    with pytest.raises(grpc.RpcError) as excinfo:
        for chunk in call:
            got.append(chunk)
    assert got == [b"log-chunk-1\n", b"log-chunk-2\n"]
    # status_code 14 == UNAVAILABLE; the recorded string is the
    # description, with the standard client rendering stripped.
    assert excinfo.value.code() == grpc.StatusCode.UNAVAILABLE
    assert excinfo.value.details() == "connection reset"


# ── empty streams ────────────────────────────────────────────────────────────


def test_empty_server_stream_fixture():
    """frames: [] parsed; first read yields end-of-stream immediately."""
    call = _server_stream(
        "grpc-stream-empty", "/files.FileService/Download", b'{"path":"/etc/empty"}'
    )
    assert list(call) == []
    assert call.code() == grpc.StatusCode.OK


def test_empty_client_stream_fixture():
    """Client half-closed immediately: req frames: [], one response."""
    call = _client_stream("grpc-stream-empty", "/telemetry.MetricsService/Push", [])
    assert call.result() == b'{"count":0}'


def test_empty_bidi_stream_fixture():
    """No traffic at all in either direction."""
    call = _bidi_stream("grpc-stream-empty", "/chat.ChatService/Ping", [])
    assert list(call) == []


# ── malformed base64 ─────────────────────────────────────────────────────────


def test_malformed_b64_rejected_by_adapter():
    """The negative fixture must be REJECTED at open, not silently
    accepted with the bad characters discarded."""
    with pytest.raises(StreamFormatError):
        _server_stream(
            "grpc-stream-malformed-b64",
            "/files.FileService/Download",
            b'{"path":"/opt/blob.bin"}',
        )
