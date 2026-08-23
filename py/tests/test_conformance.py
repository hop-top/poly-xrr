"""Conformance tests: load all fixture cassettes from spec/fixtures/."""
from __future__ import annotations

from pathlib import Path

import pytest
import yaml

from xrr.cassette import FileCassette
from xrr.session import REPLAY, Session
from xrr.stream import (
    StreamedPair,
    StreamFormatError,
    counter_stream_fingerprint,
    server_stream_fingerprint,
)

# Resolve path relative to this file: tests/ -> py/ -> spec/fixtures/
_FIXTURES_DIR = Path(__file__).resolve().parent.parent.parent / "spec" / "fixtures"


def _fixture_dirs() -> list[Path]:
    if not _FIXTURES_DIR.exists():
        return []
    return [p for p in _FIXTURES_DIR.iterdir() if p.is_dir()]


def _recompute_grpc_fingerprint(
    pair: StreamedPair, counters: dict[tuple[str, str, str], int]
) -> str | None:
    """Recompute a grpc streamed fingerprint per the spec's algorithms.

    Non-grpc adapters (sse) have no specified algorithm — their
    fingerprint is opaque, return None. Counter-fingerprinted tuples
    share one counter domain per fixture dir; manifest order is open
    order.
    """
    if pair.adapter != "grpc":
        return None
    service = pair.req_payload["service"]
    method = pair.req_payload["method"]
    stype = pair.req_stream.type
    if stype == "server":
        # Exactly one send frame; its bytes are the fingerprint input.
        return server_stream_fingerprint(
            service, method, pair.req_stream.frames[0].message
        )
    key = (service, method, stype)
    n = counters.get(key, 0)
    counters[key] = n + 1
    # Payload n is informational and must agree with the recomputation.
    assert pair.req_payload.get("n") == n
    return counter_stream_fingerprint(service, method, stype, n)


def _assert_pairs_equal(a: StreamedPair, b: StreamedPair) -> None:
    """Field-for-field equality with messages compared over decoded bytes.

    The message-encoding choice (message_b64 vs message_text) is free on
    re-emit, so the frame `text` flag is deliberately not compared.
    """
    assert b.adapter == a.adapter
    assert b.fingerprint == a.fingerprint
    assert b.req_recorded_at == a.req_recorded_at
    assert b.resp_recorded_at == a.resp_recorded_at
    assert b.req_payload == a.req_payload
    assert b.resp_payload == a.resp_payload
    assert b.error == a.error
    assert b.req_stream.type == a.req_stream.type
    assert b.req_stream.half_close == a.req_stream.half_close
    assert b.resp_stream.end == a.resp_stream.end
    for got, want in zip(
        b.req_stream.frames + b.resp_stream.frames,
        a.req_stream.frames + a.resp_stream.frames,
        strict=True,
    ):
        assert got.seq == want.seq
        assert got.at_ms == want.at_ms
        assert got.message == want.message


@pytest.mark.parametrize("fixture_dir", _fixture_dirs(), ids=lambda p: p.name)
def test_conformance_fixture(fixture_dir: Path, tmp_path):
    manifest_path = fixture_dir / "manifest.yaml"
    assert manifest_path.exists(), f"missing manifest.yaml in {fixture_dir}"

    manifest = yaml.safe_load(manifest_path.read_text())
    # interactions may be [] — expected-rejection dirs (see
    # grpc-stream-malformed-b64/README.md) list no replayable pairs.
    assert "interactions" in manifest, f"no interactions key in {manifest_path}"
    interactions = manifest.get("interactions") or []

    cassette = FileCassette(str(fixture_dir))
    counters: dict[tuple[str, str, str], int] = {}
    for item in interactions:
        adapter = item["adapter"]
        fingerprint = item["fingerprint"]
        if not item.get("streamed", False):
            # Must not raise CassetteMiss
            req, resp = cassette.load(adapter, fingerprint)
            assert req is not None
            assert resp is not None
            continue

        pair = cassette.load_stream(adapter, fingerprint)
        assert pair.fingerprint == fingerprint

        recomputed = _recompute_grpc_fingerprint(pair, counters)
        if recomputed is not None:
            assert recomputed == fingerprint

        # Round-trip: re-emit, reload, compare fields + decoded bytes.
        out = FileCassette(str(tmp_path))
        out.save_stream(pair)
        _assert_pairs_equal(pair, out.load_stream(adapter, fingerprint))


def test_malformed_b64_rejected():
    """Negative fixture, targeted by path per its README: strict readers
    reject invalid base64 instead of discarding the bad characters."""
    d = _FIXTURES_DIR / "grpc-stream-malformed-b64"
    cassette = FileCassette(str(d))
    with pytest.raises(StreamFormatError):
        cassette.load_stream("grpc", "8dbfb222")


def test_sse_text_scalars_survive():
    """Scalar-hazard payloads decode to exactly their characters."""
    d = _FIXTURES_DIR / "sse-text-scalars"
    pair = FileCassette(str(d)).load_stream("sse", "66ecc77a")
    texts = [f.message.decode("utf-8") for f in pair.resp_stream.frames]
    assert texts == ["on", "12:30", "null", " leading", "trailing ", "  padded  "]


def test_client_stream_repeat_two_opens():
    """Scripted n=0/n=1 case: two sequential opens of one tuple within
    one session locate distinct cassettes (grpc-client-stream-repeat)."""
    d = _FIXTURES_DIR / "grpc-client-stream-repeat"
    cassette = FileCassette(str(d))
    session = Session(REPLAY, cassette)
    for want in ("2bebfd6f", "b27b5fe1"):
        n = session.next_stream_n("files.FileService", "Upload", "client")
        fp = counter_stream_fingerprint("files.FileService", "Upload", "client", n)
        assert fp == want
        pair = cassette.load_stream("grpc", fp)
        # Payload n is informational only; replay recomputes its own.
        assert pair.req_payload.get("n") == n
