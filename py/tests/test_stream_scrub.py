"""Frame-level scrub hook — the normative contract in
cassette-format-streaming.md "Frame Scrub Hook".

Secrets are rewritten on the DECODED bytes, identically at record and
replay time. Symmetry is the correctness invariant: a cassette recorded
through a scrub only replays green when the same scrub is active on the
replaying session. Mirrors go/stream_scrub_test.go.
"""
from __future__ import annotations

from pathlib import Path

import pytest

from xrr.cassette import FileCassette
from xrr.session import RECORD, REPLAY, Session
from xrr.stream import BIDI, CLIENT, SERVER, StreamOpen, msg_hash, stream_fingerprint
from xrr.stream_scrub import SEND, StreamScrubInfo
from xrr.stream_session import StreamDone, StreamMismatchError

SECRET = "hunter2-FAKE-TOKEN-0123456789"
MASK = "<scrubbed>"


def mask_secret(_dir: str, _info: StreamScrubInfo, data: bytes) -> bytes:
    """Deterministic scrub replacing the fake token wherever it appears."""
    return data.replace(SECRET.encode(), MASK.encode())


def grpc_open(stype: str, service: str, method: str, msg: bytes | None = None) -> StreamOpen:
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


def scrubbed_server_open(s: Session, service: str, method: str, msg: bytes) -> StreamOpen:
    """Mirrors the gRPC adapter under the scrub contract: msg_hash is
    derived from the SCRUBBED open-message bytes (spec clause 3)."""
    scrubbed = s.scrub_stream_frame(
        SEND, StreamScrubInfo(adapter_id="grpc", type=SERVER), msg
    )
    return StreamOpen(
        adapter_id="grpc",
        type=SERVER,
        identity={"service": service, "method": method, "msg_hash": msg_hash(scrubbed)},
        counter=False,
        payload={"service": service, "method": method},
    )


def test_record_scrubs_both_directions(tmp_path: Path):
    """Clause 1 + 2: both directions scrubbed on the DECODED bytes before
    persistence, so the secret reaches disk in no encoding."""
    s = Session(RECORD, FileCassette(str(tmp_path)), stream_scrub=mask_secret)
    rec = s.open_stream_record(grpc_open(BIDI, "chat.ChatService", "Converse"))
    rec.record_send(f"ping {SECRET}".encode())
    rec.record_recv(f"pong {SECRET}".encode())
    rec.record_half_close()
    rec.finish({"status_code": 0})

    pair = FileCassette(str(tmp_path)).load_stream("grpc", rec.fingerprint)
    assert pair.req_stream.frames[0].message == f"ping {MASK}".encode()
    assert pair.resp_stream.frames[0].message == f"pong {MASK}".encode()

    # Base64 hides the secret from a text scan, so the decoded check above
    # is the real gate; this guards the payload side.
    for kind in ("req", "resp"):
        raw = (tmp_path / f"grpc-{rec.fingerprint}.{kind}.yaml").read_text()
        assert SECRET not in raw


def test_server_stream_identity_from_scrubbed_bytes(tmp_path: Path):
    """Clause 3: content-derived identity is computed over scrubbed bytes
    on both sides, so a scrubbed replay resolves to the scrubbed
    recording."""
    msg = f'{{"cmd":"deploy","token":"{SECRET}"}}'.encode()

    rec_s = Session(RECORD, FileCassette(str(tmp_path)), stream_scrub=mask_secret)
    rec = rec_s.open_stream_record(scrubbed_server_open(rec_s, "ops.Deploy", "Run", msg))
    rec.record_send(msg)
    rec.record_half_close()
    rec.record_recv(b"deployed")
    rec.finish({"status_code": 0})

    # Self-consistency: recomputing the fingerprint from the persisted
    # (scrubbed) open frame reproduces the filename.
    pair = FileCassette(str(tmp_path)).load_stream("grpc", rec.fingerprint)
    assert SECRET.encode() not in pair.req_stream.frames[0].message
    from_disk = stream_fingerprint(
        grpc_open(SERVER, "ops.Deploy", "Run", pair.req_stream.frames[0].message)
    )
    assert from_disk == rec.fingerprint

    rep_s = Session(REPLAY, FileCassette(str(tmp_path)), stream_scrub=mask_secret)
    rep = rep_s.open_stream_replay(scrubbed_server_open(rep_s, "ops.Deploy", "Run", msg))
    assert rep.fingerprint == rec.fingerprint
    rep.send(msg)  # live secret-bearing open matches after symmetric scrub
    rep.half_close()
    assert rep.recv() == b"deployed"
    with pytest.raises(StreamDone):
        rep.recv()


def test_replay_symmetry_required(tmp_path: Path):
    """Clause 5: the same hook replays green; replaying without it fails
    loudly rather than silently succeeding."""
    open_ = grpc_open(CLIENT, "vault.Vault", "Put")

    rec_s = Session(RECORD, FileCassette(str(tmp_path)), stream_scrub=mask_secret)
    rec = rec_s.open_stream_record(open_)
    rec.record_send(f"put {SECRET}".encode())
    rec.record_half_close()
    rec.finish({"status_code": 0})

    ok_s = Session(REPLAY, FileCassette(str(tmp_path)), stream_scrub=mask_secret)
    ok_s.open_stream_replay(open_).send(f"put {SECRET}".encode())  # green

    bad_s = Session(REPLAY, FileCassette(str(tmp_path)))
    with pytest.raises(StreamMismatchError):
        bad_s.open_stream_replay(open_).send(f"put {SECRET}".encode())


def test_applied_exactly_once_and_never_rescrubbed(tmp_path: Path):
    """Clause 5: recorded frames are delivered verbatim, never re-scrubbed.
    A deliberately non-idempotent hook pins single application."""

    def marker(_d: str, _i: StreamScrubInfo, data: bytes) -> bytes:
        return data + b"#"

    open_ = grpc_open(BIDI, "chat.ChatService", "Converse")
    rec_s = Session(RECORD, FileCassette(str(tmp_path)), stream_scrub=marker)
    rec = rec_s.open_stream_record(open_)
    rec.record_send(b"ping")
    rec.record_recv(b"pong")
    rec.record_half_close()
    rec.finish({"status_code": 0})

    pair = FileCassette(str(tmp_path)).load_stream("grpc", rec.fingerprint)
    assert pair.req_stream.frames[0].message == b"ping#"
    assert pair.resp_stream.frames[0].message == b"pong#"

    rep_s = Session(REPLAY, FileCassette(str(tmp_path)), stream_scrub=marker)
    rep = rep_s.open_stream_replay(open_)
    rep.send(b"ping")  # live send scrubbed once, matches once-scrubbed frame
    assert rep.recv() == b"pong#"  # delivered verbatim


def test_invocation_points(tmp_path: Path):
    """Clause 2: the hook runs at exactly the specified points and nowhere
    else. Half-close and terminal carry no payload; recorded recv frames
    are never re-scrubbed; bytes past the last recorded send are never
    compared, so they are never scrubbed."""
    seen: list[str] = []

    def trace(direction: str, _i: StreamScrubInfo, data: bytes) -> bytes:
        seen.append(f"{direction}:{data.decode()}")
        return data

    open_ = grpc_open(BIDI, "chat.ChatService", "Converse")
    rec_s = Session(RECORD, FileCassette(str(tmp_path)), stream_scrub=trace)
    rec = rec_s.open_stream_record(open_)
    rec.record_send(b"a")
    rec.record_recv(b"b")
    rec.record_half_close()  # no payload — not scrubbed
    rec.finish({"status_code": 0})  # terminal — not scrubbed
    assert seen == ["send:a", "recv:b"]

    seen.clear()
    rep_s = Session(REPLAY, FileCassette(str(tmp_path)), stream_scrub=trace)
    rep = rep_s.open_stream_replay(open_)
    rep.send(b"a")
    rep.recv()  # recorded frame — never re-scrubbed
    rep.half_close()
    assert seen == ["send:a"]

    seen.clear()
    with pytest.raises((StreamDone, StreamMismatchError)):
        rep.send(b"overrun")  # past the last recorded send
    assert seen == []


def test_length_changing_hook(tmp_path: Path):
    """Clause 6: the hook MAY change a frame's length; neither side
    assumes byte-count preservation."""
    long = "[REDACTED-MUCH-LONGER-PLACEHOLDER]"

    def expand(_d: str, _i: StreamScrubInfo, data: bytes) -> bytes:
        return data.replace(SECRET.encode(), long.encode())

    open_ = grpc_open(BIDI, "chat.ChatService", "Converse")
    rec_s = Session(RECORD, FileCassette(str(tmp_path)), stream_scrub=expand)
    rec = rec_s.open_stream_record(open_)
    rec.record_send(f"k={SECRET}".encode())
    rec.record_recv(f"v={SECRET}".encode())
    rec.record_half_close()
    rec.finish({"status_code": 0})

    pair = FileCassette(str(tmp_path)).load_stream("grpc", rec.fingerprint)
    assert pair.req_stream.frames[0].message == f"k={long}".encode()

    rep_s = Session(REPLAY, FileCassette(str(tmp_path)), stream_scrub=expand)
    rep = rep_s.open_stream_replay(open_)
    rep.send(f"k={SECRET}".encode())  # green despite the length change
    assert rep.recv() == f"v={long}".encode()


def test_absent_hook_records_verbatim(tmp_path: Path):
    """Clause 7: no hook installed is identical to the feature not
    existing."""
    open_ = grpc_open(BIDI, "chat.ChatService", "Converse")
    rec_s = Session(RECORD, FileCassette(str(tmp_path)))
    rec = rec_s.open_stream_record(open_)
    rec.record_send(f"ping {SECRET}".encode())
    rec.record_half_close()
    rec.finish({"status_code": 0})

    pair = FileCassette(str(tmp_path)).load_stream("grpc", rec.fingerprint)
    assert pair.req_stream.frames[0].message == f"ping {SECRET}".encode()


def test_no_aliasing_of_caller_buffers(tmp_path: Path):
    """Clause 8: a caller mutating the buffer it handed over cannot reach
    stored frames.

    Python is the only port where this is expressible — ``bytearray`` is a
    mutable buffer accepted where ``bytes`` is expected. Rust's ``&[u8]``
    and PHP's value-type strings make the bug structurally impossible, so
    those ports pin nothing here; go and ts pin it with a slice mutation.
    """

    def passthrough(_d: str, _i: StreamScrubInfo, data: bytes) -> bytes:
        return data

    open_ = grpc_open(BIDI, "chat.ChatService", "Converse")
    rec_s = Session(RECORD, FileCassette(str(tmp_path)), stream_scrub=passthrough)
    rec = rec_s.open_stream_record(open_)
    live = bytearray(b"ping")
    rec.record_send(live)
    live[0] = ord("X")  # mutate after handing it over — must not reach disk
    rec.record_half_close()
    rec.finish({"status_code": 0})

    pair = FileCassette(str(tmp_path)).load_stream("grpc", rec.fingerprint)
    assert pair.req_stream.frames[0].message == b"ping"
