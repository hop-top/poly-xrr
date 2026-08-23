"""Tests for the stream record/replay session machinery (stream_session.py).

Mirrors go/stream_session_test.go: record path (event log -> pair on disk
at finish), replay path (send validation, frame delivery, terminal
semantics), and the StreamOpen identity seam shared by both.
"""
from __future__ import annotations

from pathlib import Path

import pytest
import yaml

from xrr.cassette import CassetteMiss, FileCassette
from xrr.session import PASSTHROUGH, RECORD, REPLAY, Session
from xrr.stream import (
    BIDI,
    CLIENT,
    SERVER,
    ReqStream,
    RespStream,
    ShapeMismatch,
    StreamedPair,
    StreamEvent,
    StreamOpen,
    counter_stream_fingerprint,
    msg_hash,
    server_stream_fingerprint,
    stream_fingerprint,
)
from xrr.stream_session import (
    RecordedStreamError,
    StreamDone,
    StreamMismatchError,
)

_FIXTURES = Path(__file__).resolve().parent.parent.parent / "spec" / "fixtures"


def grpc_open(
    stype: str, service: str, method: str, msg: bytes | None = None
) -> StreamOpen:
    """Mirror of the gRPC adapter's open definition: canonical inputs
    service + method (+ msg_hash for content-addressed server streams),
    counter-addressed client/bidi, req payload {service, method}."""
    identity: dict = {"service": service, "method": method}
    if stype == SERVER:
        identity["msg_hash"] = msg_hash(msg or b"")
    return StreamOpen(
        adapter_id="grpc",
        type=stype,
        identity=identity,
        counter=stype != SERVER,
        payload={"service": service, "method": method},
    )


def fixture_session(name: str) -> Session:
    return Session(REPLAY, FileCassette(str(_FIXTURES / name)))


# ── identity seam / fingerprints ─────────────────────────────────────────────


def test_stream_fingerprint_grpc_vectors_via_seam():
    """Spec vector table reproduced through StreamOpen; byte-identical to
    the legacy gRPC-shaped helpers."""
    server1 = grpc_open(SERVER, "files.FileService", "Download", b'{"path":"/etc/hosts"}')
    server2 = grpc_open(
        SERVER, "files.FileService", "Download", b'{"path":"/var/log/big.log"}'
    )
    client = grpc_open(CLIENT, "files.FileService", "Upload")
    bidi = grpc_open(BIDI, "chat.ChatService", "Converse")

    assert stream_fingerprint(server1) == "58a4bf3f"
    assert stream_fingerprint(server2) == "9e8c4d4c"
    assert stream_fingerprint(client, 0) == "2bebfd6f"
    assert stream_fingerprint(client, 1) == "b27b5fe1"
    assert stream_fingerprint(bidi, 0) == "c6233d2e"

    assert stream_fingerprint(server1) == server_stream_fingerprint(
        "files.FileService", "Download", b'{"path":"/etc/hosts"}'
    )
    assert stream_fingerprint(client, 0) == counter_stream_fingerprint(
        "files.FileService", "Upload", CLIENT, 0
    )
    assert stream_fingerprint(bidi, 0) == counter_stream_fingerprint(
        "chat.ChatService", "Converse", BIDI, 0
    )


def test_stream_fingerprint_sse_url_identity():
    """SSE acceptance: url-keyed identity reproduces the sse-text-scalars
    fixture fingerprint (spec/fixtures/sse-text-scalars/README.md)."""
    open_ = StreamOpen(
        adapter_id="sse",
        type=SERVER,
        identity={"url": "https://example.test/events"},
    )
    assert stream_fingerprint(open_) == "66ecc77a"


def test_stream_fingerprint_rejects_reserved_identity_keys():
    for key in ("stream", "n"):
        open_ = StreamOpen(adapter_id="x", type=BIDI, identity={key: 1})
        with pytest.raises(ValueError):
            stream_fingerprint(open_, 0)


def test_stream_fingerprint_rejects_invalid_type():
    with pytest.raises(ValueError):
        stream_fingerprint(StreamOpen(adapter_id="x", type="duplex", identity={}))


def test_stream_fingerprint_counter_requires_n():
    open_ = grpc_open(CLIENT, "s", "m")
    with pytest.raises(ValueError):
        stream_fingerprint(open_)


# ── record path ──────────────────────────────────────────────────────────────


def test_record_server_round_trip(tmp_path):
    session = Session(RECORD, FileCassette(str(tmp_path)))
    msg = b'{"path":"/etc/hosts"}'
    rec = session.open_stream_record(
        grpc_open(SERVER, "files.FileService", "Download", msg)
    )
    assert rec.fingerprint == "58a4bf3f"

    rec.record_send(msg)
    rec.record_half_close()
    rec.record_recv(b"chunk-one\n")
    rec.record_recv(b"chunk-two\n")
    # Pair is written only at finish.
    assert not (tmp_path / "grpc-58a4bf3f.req.yaml").exists()
    rec.finish({"status_code": 0})

    assert (tmp_path / "grpc-58a4bf3f.req.yaml").exists()
    assert (tmp_path / "grpc-58a4bf3f.resp.yaml").exists()

    pair = FileCassette(str(tmp_path)).load_stream("grpc", "58a4bf3f")
    assert pair.req_stream.type == SERVER
    # Dense seq 0..N-1 counting all events in arrival order.
    assert [f.seq for f in pair.req_stream.frames] == [0]
    assert pair.req_stream.frames[0].message == msg
    assert pair.req_stream.half_close == StreamEvent(
        seq=1, at_ms=pair.req_stream.half_close.at_ms
    )
    assert [f.seq for f in pair.resp_stream.frames] == [2, 3]
    assert [f.message for f in pair.resp_stream.frames] == [
        b"chunk-one\n",
        b"chunk-two\n",
    ]
    assert pair.resp_stream.end.seq == 4
    assert pair.error == ""
    assert pair.resp_payload == {"status_code": 0}
    # Server-stream payload carries no occurrence ordinal.
    assert pair.req_payload == {"service": "files.FileService", "method": "Download"}

    # at_ms stamped on every event, >= 0 and non-decreasing in seq order.
    events = [(f.seq, f.at_ms) for f in pair.req_stream.frames]
    events += [(f.seq, f.at_ms) for f in pair.resp_stream.frames]
    events.append((pair.req_stream.half_close.seq, pair.req_stream.half_close.at_ms))
    events.append((pair.resp_stream.end.seq, pair.resp_stream.end.at_ms))
    events.sort()
    prev = 0
    for _, at_ms in events:
        assert at_ms is not None
        assert at_ms >= prev
        prev = at_ms


def test_record_counter_n_two_opens(tmp_path):
    """One session object is one counter domain: two opens of the same
    (service, method, type) tuple record n=0 then n=1, matching the
    grpc-client-stream-repeat fixture fingerprints."""
    session = Session(RECORD, FileCassette(str(tmp_path)))
    open_ = grpc_open(CLIENT, "files.FileService", "Upload")

    rec1 = session.open_stream_record(open_)
    assert rec1.fingerprint == "2bebfd6f"
    rec1.record_send(b"alpha\n")
    rec1.record_half_close()
    rec1.record_recv(b'{"received_bytes":6}')
    rec1.finish({"status_code": 0})

    rec2 = session.open_stream_record(open_)
    assert rec2.fingerprint == "b27b5fe1"
    rec2.record_half_close()
    rec2.finish({"status_code": 0})

    cassette = FileCassette(str(tmp_path))
    # Informational occurrence ordinal injected into the req payload.
    assert cassette.load_stream("grpc", "2bebfd6f").req_payload["n"] == 0
    assert cassette.load_stream("grpc", "b27b5fe1").req_payload["n"] == 1

    # A different tuple starts its own count.
    rec3 = session.open_stream_record(grpc_open(BIDI, "chat.ChatService", "Converse"))
    assert rec3.fingerprint == "c6233d2e"


def test_record_terminal_is_final(tmp_path):
    """No events are recorded after the terminal; a second finish raises."""
    session = Session(RECORD, FileCassette(str(tmp_path)))
    rec = session.open_stream_record(grpc_open(BIDI, "chat.ChatService", "Converse"))
    rec.record_send(b"ping-1")
    rec.record_recv(b"pong-1")
    rec.finish({"status_code": 0})

    # Dropped, matching the real-world no-op.
    rec.record_send(b"late")
    rec.record_recv(b"late")
    rec.record_half_close()
    with pytest.raises(RuntimeError):
        rec.finish({"status_code": 0})

    pair = FileCassette(str(tmp_path)).load_stream("grpc", rec.fingerprint)
    assert len(pair.req_stream.frames) == 1
    assert len(pair.resp_stream.frames) == 1
    assert pair.req_stream.half_close is None
    assert pair.resp_stream.end.seq == 2


def test_record_half_close_repeats_dropped(tmp_path):
    session = Session(RECORD, FileCassette(str(tmp_path)))
    rec = session.open_stream_record(grpc_open(BIDI, "chat.ChatService", "Converse"))
    rec.record_send(b"ping-1")
    rec.record_half_close()
    rec.record_half_close()  # at most once; repeat dropped
    rec.finish({"status_code": 0})

    pair = FileCassette(str(tmp_path)).load_stream("grpc", rec.fingerprint)
    assert pair.req_stream.half_close.seq == 1
    assert pair.resp_stream.end.seq == 2


def test_record_error_terminal(tmp_path):
    session = Session(RECORD, FileCassette(str(tmp_path)))
    msg = b'{"path":"/var/log/big.log"}'
    rec = session.open_stream_record(
        grpc_open(SERVER, "files.FileService", "Download", msg)
    )
    rec.record_send(msg)
    rec.record_half_close()
    rec.record_recv(b"log-chunk-1\n")
    rec.finish(
        {"status_code": 14},
        Exception("rpc error: code = Unavailable desc = connection reset"),
    )

    pair = FileCassette(str(tmp_path)).load_stream("grpc", "9e8c4d4c")
    assert pair.error == "rpc error: code = Unavailable desc = connection reset"
    assert pair.resp_payload == {"status_code": 14}


def test_record_then_replay_round_trip(tmp_path):
    rec_session = Session(RECORD, FileCassette(str(tmp_path)))
    rec = rec_session.open_stream_record(
        grpc_open(BIDI, "chat.ChatService", "Converse")
    )
    rec.record_send(b"ping-1")
    rec.record_recv(b"pong-1")
    rec.record_send(b"ping-2")
    rec.record_recv(b"pong-2")
    rec.record_half_close()
    rec.finish({"status_code": 0})

    rep_session = Session(REPLAY, FileCassette(str(tmp_path)))
    rep = rep_session.open_stream_replay(
        grpc_open(BIDI, "chat.ChatService", "Converse")
    )
    assert rep.fingerprint == rec.fingerprint
    rep.send(b"ping-1")
    assert rep.recv() == b"pong-1"
    rep.send(b"ping-2")
    assert rep.recv() == b"pong-2"
    rep.half_close()
    with pytest.raises(StreamDone):
        rep.recv()


# ── replay path ──────────────────────────────────────────────────────────────


def test_replay_bidi_reads_never_gate_on_sends():
    session = fixture_session("grpc-bidi-stream")
    rep = session.open_stream_replay(grpc_open(BIDI, "chat.ChatService", "Converse"))
    assert rep.fingerprint == "c6233d2e"
    assert rep.type == BIDI

    # Reads never gate on send progress: drain both pongs first.
    assert rep.recv() == b"pong-1"
    assert rep.recv() == b"pong-2"

    # Sends validated in order and bytes afterwards.
    rep.send(b"ping-1")
    rep.send(b"ping-2")
    rep.half_close()

    with pytest.raises(StreamDone):
        rep.recv()
    with pytest.raises(StreamDone):
        rep.recv()  # terminal repeats for j > R


def test_replay_send_mismatch_is_terminal():
    session = fixture_session("grpc-bidi-stream")
    rep = session.open_stream_replay(grpc_open(BIDI, "chat.ChatService", "Converse"))

    rep.send(b"ping-1")
    with pytest.raises(StreamMismatchError) as exc_info:
        rep.send(b"ping-DIVERGED")
    assert exc_info.value.op == "send"
    assert exc_info.value.ordinal == 1
    assert "sha256" in str(exc_info.value)

    # Mismatch poisons every subsequent operation.
    with pytest.raises(StreamMismatchError):
        rep.recv()
    with pytest.raises(StreamMismatchError):
        rep.half_close()
    with pytest.raises(StreamMismatchError):
        rep.send(b"ping-2")


def test_replay_short_half_close_is_mismatch():
    session = fixture_session("grpc-client-stream")
    rep = session.open_stream_replay(grpc_open(CLIENT, "files.FileService", "Upload"))

    rep.send(b"part-one\n")
    # Half-close after 1 of 2 recorded sends.
    with pytest.raises(StreamMismatchError) as exc_info:
        rep.half_close()
    assert exc_info.value.op == "half_close"
    with pytest.raises(StreamMismatchError):
        rep.recv()


def test_replay_post_completion_send_is_non_poisoning():
    """Send at i >= S with an OK terminal raises the end-of-stream signal
    and does NOT poison the recv side."""
    session = fixture_session("grpc-client-stream")
    rep = session.open_stream_replay(grpc_open(CLIENT, "files.FileService", "Upload"))

    rep.send(b"part-one\n")
    rep.send(b"part-two\n")
    with pytest.raises(StreamDone):
        rep.send(b"part-three\n")
    # Half-close after all recorded sends is always accepted.
    rep.half_close()

    assert rep.recv() == b'{"received_bytes":18}'
    with pytest.raises(StreamDone):
        rep.recv()


def test_replay_mid_stream_error():
    """Recorded frames delivered, then the recorded error in place of
    end-of-stream; post-completion sends surface the same error."""
    session = fixture_session("grpc-stream-error")
    rep = session.open_stream_replay(
        grpc_open(SERVER, "files.FileService", "Download", b'{"path":"/var/log/big.log"}')
    )
    assert rep.fingerprint == "9e8c4d4c"
    assert rep.resp_payload == {"status_code": 14}

    rep.send(b'{"path":"/var/log/big.log"}')
    rep.half_close()

    assert rep.recv() == b"log-chunk-1\n"
    assert rep.recv() == b"log-chunk-2\n"

    want = "rpc error: code = Unavailable desc = connection reset"
    with pytest.raises(RecordedStreamError) as exc_info:
        rep.recv()
    assert str(exc_info.value) == want
    with pytest.raises(RecordedStreamError):
        rep.recv()  # recorded error repeats for j > R

    # The recorded stream was already dead: post-completion send returns it.
    with pytest.raises(RecordedStreamError) as exc_info:
        rep.send(b"extra")
    assert str(exc_info.value) == want
    # Recorded error is not a mismatch: nothing is poisoned.
    with pytest.raises(RecordedStreamError):
        rep.recv()


def test_replay_empty_server_stream():
    session = fixture_session("grpc-stream-empty")
    rep = session.open_stream_replay(
        grpc_open(SERVER, "files.FileService", "Download", b'{"path":"/etc/empty"}')
    )
    # First read yields end-of-stream immediately; never gated on sends.
    with pytest.raises(StreamDone):
        rep.recv()


def test_replay_empty_client_immediate_half_close():
    session = fixture_session("grpc-stream-empty")
    rep = session.open_stream_replay(
        grpc_open(CLIENT, "telemetry.MetricsService", "Push")
    )
    rep.half_close()  # S=0: immediate half-close accepted
    assert rep.recv() == b'{"count":0}'
    with pytest.raises(StreamDone):
        rep.recv()


def test_replay_empty_bidi_no_traffic():
    session = fixture_session("grpc-stream-empty")
    rep = session.open_stream_replay(grpc_open(BIDI, "chat.ChatService", "Ping"))
    rep.half_close()
    with pytest.raises(StreamDone):
        rep.recv()


def test_replay_miss_vs_shape_mismatch(tmp_path):
    cassette = FileCassette(str(tmp_path))
    session = Session(REPLAY, cassette)

    # No pair on disk -> cassette miss.
    with pytest.raises(CassetteMiss):
        session.open_stream_replay(grpc_open(BIDI, "s", "m"))

    # A unary pair at the streamed fingerprint -> shape mismatch, not a miss.
    fp = stream_fingerprint(grpc_open(BIDI, "s", "m"), 1)
    cassette.save("grpc", fp, {"service": "s", "method": "m"}, {"status_code": 0})
    with pytest.raises(ShapeMismatch):
        session.open_stream_replay(grpc_open(BIDI, "s", "m"))


def test_replay_type_mismatch_is_shape_mismatch(tmp_path):
    # A recorded pair of a different stream type at the open's fingerprint.
    fp = stream_fingerprint(grpc_open(BIDI, "chat.ChatService", "Converse"), 0)
    FileCassette(str(tmp_path)).save_stream(
        StreamedPair(
            adapter="grpc",
            fingerprint=fp,
            req_recorded_at="2026-08-23T12:00:00Z",
            resp_recorded_at="2026-08-23T12:00:00Z",
            req_payload={"service": "chat.ChatService", "method": "Converse", "n": 0},
            resp_payload={"status_code": 0},
            req_stream=ReqStream(
                type=CLIENT, frames=[], half_close=StreamEvent(seq=0)
            ),
            resp_stream=RespStream(frames=[], end=StreamEvent(seq=1)),
        )
    )
    session = Session(REPLAY, FileCassette(str(tmp_path)))
    with pytest.raises(ShapeMismatch):
        session.open_stream_replay(grpc_open(BIDI, "chat.ChatService", "Converse"))


def test_replay_counter_consumed_on_miss(tmp_path):
    """The occurrence counter advances exactly as in record mode, hit or
    miss: after a missed n=0 open, the next open locates the n=1 pair."""
    pair = FileCassette(str(_FIXTURES / "grpc-client-stream-repeat")).load_stream(
        "grpc", "b27b5fe1"
    )
    FileCassette(str(tmp_path)).save_stream(pair)

    session = Session(REPLAY, FileCassette(str(tmp_path)))
    open_ = grpc_open(CLIENT, "files.FileService", "Upload")
    with pytest.raises(CassetteMiss):
        session.open_stream_replay(open_)  # n=0 -> 2bebfd6f absent
    rep = session.open_stream_replay(open_)  # n=1 -> b27b5fe1 present
    assert rep.fingerprint == "b27b5fe1"


def test_scripted_two_open_n0_n1_via_session():
    """Spec conformance: the scripted n=0/n=1 two-open case — sequenced
    opens of one tuple driven through one session (grpc-client-stream-repeat)."""
    session = fixture_session("grpc-client-stream-repeat")
    open_ = grpc_open(CLIENT, "files.FileService", "Upload")

    rep1 = session.open_stream_replay(open_)
    assert rep1.fingerprint == "2bebfd6f"
    rep1.send(b"alpha\n")
    rep1.half_close()
    assert rep1.recv() == b'{"received_bytes":6}'
    with pytest.raises(StreamDone):
        rep1.recv()

    rep2 = session.open_stream_replay(open_)
    assert rep2.fingerprint == "b27b5fe1"
    rep2.send(b"beta-1\n")
    rep2.send(b"beta-2\n")
    rep2.half_close()
    assert rep2.recv() == b'{"received_bytes":14}'
    with pytest.raises(StreamDone):
        rep2.recv()


def test_open_stream_mode_enforcement(tmp_path):
    open_ = grpc_open(BIDI, "s", "m")

    replay_session = Session(REPLAY, FileCassette(str(tmp_path)))
    with pytest.raises(RuntimeError):
        replay_session.open_stream_record(open_)

    record_session = Session(RECORD, FileCassette(str(tmp_path)))
    with pytest.raises(RuntimeError):
        record_session.open_stream_replay(open_)

    passthrough_session = Session(PASSTHROUGH, FileCassette(str(tmp_path)))
    with pytest.raises(RuntimeError):
        passthrough_session.open_stream_record(open_)
    with pytest.raises(RuntimeError):
        passthrough_session.open_stream_replay(open_)


# ── all streamed fixture dirs through the session API ────────────────────────


def _streamed_entries(d: Path) -> list[dict]:
    """Streamed entries in a spec-conforming open order.

    `interactions` is an unordered set (cassette-format-streaming.md,
    Manifest Extension), so file order carries no scheduling meaning.
    Entries sharing a counter domain — the `(service, method, stream type)`
    tuple of a `client`/`bidi` open — must be opened ascending by the req
    payload's `n`; everything else is order-independent. Sorting the whole
    list by `n` within its domain satisfies that: `n` is unique inside a
    domain, and comparisons across domains are inconsequential.
    """
    manifest = yaml.safe_load((d / "manifest.yaml").read_text())
    entries = [i for i in (manifest.get("interactions") or []) if i.get("streamed")]

    def domain_order(entry: dict) -> tuple:
        req = yaml.safe_load(
            (d / f"{entry['adapter']}-{entry['fingerprint']}.req.yaml").read_text()
        )
        stype = req["stream"]["type"]
        payload = req["payload"]
        # Server streams use no counter and join no domain; key them apart so
        # they never interleave into a domain's ascending-n run.
        if stype == "server":
            return ("", "", "", 0)
        return (
            payload.get("service", ""),
            payload.get("method", ""),
            stype,
            payload["n"],
        )

    return sorted(entries, key=domain_order)


_STREAMED_DIRS = sorted(
    d.name for d in _FIXTURES.iterdir() if d.is_dir() and _streamed_entries(d)
)


def _open_for_pair(pair: StreamedPair) -> StreamOpen:
    """Rebuild the adapter open from a recorded pair (test scaffolding)."""
    if pair.adapter == "grpc":
        msg = None
        if pair.req_stream.type == SERVER:
            msg = pair.req_stream.frames[0].message
        return grpc_open(
            pair.req_stream.type,
            pair.req_payload["service"],
            pair.req_payload["method"],
            msg,
        )
    if pair.adapter == "sse":
        return StreamOpen(
            adapter_id="sse",
            type=pair.req_stream.type,
            identity={"url": pair.req_payload["url"]},
            payload=dict(pair.req_payload),
        )
    raise AssertionError(f"no open builder for adapter {pair.adapter!r}")


@pytest.mark.parametrize("dirname", _STREAMED_DIRS)
def test_replay_streamed_fixture_dir(dirname: str):
    """Replay every streamed fixture through the session API: fingerprint
    located at open, sends validated, recv frames in order, then the
    recorded terminal.

    One session — hence one counter domain per `(service, method, type)`
    tuple — per fixture dir. Manifest order is NOT open order: `interactions`
    is an unordered set, so `_streamed_entries` establishes the open order
    itself, ascending by req payload `n` within a counter domain, per
    cassette-format-streaming.md (Manifest Extension)."""
    d = _FIXTURES / dirname
    cassette = FileCassette(str(d))
    session = Session(REPLAY, cassette)
    for entry in _streamed_entries(d):
        pair = cassette.load_stream(entry["adapter"], entry["fingerprint"])
        rep = session.open_stream_replay(_open_for_pair(pair))
        assert rep.fingerprint == entry["fingerprint"]
        assert rep.type == pair.req_stream.type
        assert rep.req_payload == pair.req_payload
        assert rep.resp_payload == pair.resp_payload

        for frame in pair.req_stream.frames:
            rep.send(frame.message)
        rep.half_close()
        for frame in pair.resp_stream.frames:
            assert rep.recv() == frame.message
        if pair.error:
            with pytest.raises(RecordedStreamError) as exc_info:
                rep.recv()
            assert str(exc_info.value) == pair.error
        else:
            with pytest.raises(StreamDone):
                rep.recv()
