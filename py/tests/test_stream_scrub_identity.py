"""Identity-hook conformance — spec "Scrub Hook Obligations —
Identity-Hook Matrix" (M1..M7).

The scrub hook's contract is WHEN it runs and WHAT it receives, never what
it computes; xrr defines no scrub algorithm. Two byte-neutral hooks
generate the whole matrix:

- identity: returns its input. Installed and invoked but byte-neutral, so
  any divergence from a no-hook session is a mechanics defect — clause 7
  fixes no-hook behaviour as the reference.
- counting: identity plus a call log. Reveals invocation points,
  multiplicity, and — the part fixtures cannot see — non-invocation.

Mirrors go/stream_scrub_identity_test.go.
"""
from __future__ import annotations

from pathlib import Path

import pytest

from xrr.cassette import FileCassette
from xrr.session import RECORD, REPLAY, Session
from xrr.stream import BIDI, CLIENT, SERVER, StreamOpen, msg_hash
from xrr.stream_scrub import RECV, SEND, StreamScrubInfo
from xrr.stream_session import StreamDone, StreamMismatchError

OPEN_MSG = b'{"room":"ops"}'


def identity_scrub(_dir: str, _info: StreamScrubInfo, data: bytes) -> bytes:
    """Clause 6's "MAY return the input unchanged": observable, byte-neutral."""
    return data


def counting_scrub(log: list[tuple[str, bytes]]):
    """Identity plus a call log. The bookkeeping is test scaffolding, not
    scrub state — the bytes returned are the input, so clause 4's
    determinism holds."""

    def hook(direction: str, _info: StreamScrubInfo, data: bytes) -> bytes:
        log.append((direction, data))
        return data

    return hook


def grpc_open(stype: str) -> StreamOpen:
    identity: dict = {"service": "chat.ChatService", "method": "Converse"}
    if stype == SERVER:
        identity["msg_hash"] = msg_hash(OPEN_MSG)
    return StreamOpen(
        adapter_id="grpc",
        type=stype,
        identity=identity,
        counter=stype != SERVER,
        payload={"service": "chat.ChatService", "method": "Converse"},
    )


def fixed_sends(stype: str) -> list[bytes]:
    """gRPC mapping: server streams record exactly one send frame."""
    return [OPEN_MSG] if stype == SERVER else [b"alpha", b"beta"]


def fixed_recvs(stype: str) -> list[bytes]:
    """gRPC mapping: client streams record at most one recv frame."""
    return [b"ack"] if stype == CLIENT else [b"one", b"two"]


def record_fixed(tmp: Path, stype: str, scrub) -> str:
    """One identical scripted stream through a record session, so two
    sessions differing only in hook installation are byte-comparable."""
    s = Session(RECORD, FileCassette(str(tmp)), stream_scrub=scrub)
    rec = s.open_stream_record(grpc_open(stype))
    for f in fixed_sends(stype):
        rec.record_send(f)
    rec.record_half_close()
    for f in fixed_recvs(stype):
        rec.record_recv(f)
    fp = rec.fingerprint
    rec.finish({"status_code": 0})
    return fp


def replay_fixed(tmp: Path, stype: str, scrub) -> None:
    s = Session(REPLAY, FileCassette(str(tmp)), stream_scrub=scrub)
    rep = s.open_stream_replay(grpc_open(stype))
    for f in fixed_sends(stype):
        rep.send(f)
    rep.half_close()
    for want in fixed_recvs(stype):
        assert rep.recv() == want
    with pytest.raises(StreamDone):
        rep.recv()


def fixed_frame_calls(stype: str) -> list[tuple[str, bytes]]:
    """The call log a byte-neutral hook must produce for one ``record_fixed``
    of this type: one send call per send frame, one recv call per recv
    frame, in frame order.

    M1..M3 pair their byte-identity assertions with this expectation. Byte
    identity alone is vacuous for a byte-neutral hook — with the hook
    dispatch removed the hooked and unhooked branches become the same
    computation, so the comparison holds by construction whether or not the
    hook ever ran. The call log supplies the missing half: positive
    evidence of invocation.
    """
    return [(SEND, f) for f in fixed_sends(stype)] + [
        (RECV, f) for f in fixed_recvs(stype)
    ]


def pair_bytes(tmp: Path, fp: str) -> tuple[str, str]:
    return (
        (tmp / f"grpc-{fp}.req.yaml").read_text(),
        (tmp / f"grpc-{fp}.resp.yaml").read_text(),
    )


@pytest.mark.parametrize("stype", [SERVER, CLIENT, BIDI])
def test_identity_hook_matches_no_hook(tmp_path: Path, stype: str):
    """M1: an installed identity hook is byte-indistinguishable from no
    hook. Any divergence is a mechanics defect — an extra scrub site, a
    missed one, or an identity input derived from the wrong bytes.

    The hooked branch runs a COUNTING identity hook, and the call log is
    asserted alongside the bytes. Byte equality on its own proves only that
    the two sessions agree, which a hook that never ran also satisfies; the
    log proves the hook was installed AND invoked while agreeing."""
    bare = tmp_path / "bare"
    hooked = tmp_path / "hooked"
    bare.mkdir()
    hooked.mkdir()

    log: list[tuple[str, bytes]] = []
    bare_fp = record_fixed(bare, stype, None)
    hooked_fp = record_fixed(hooked, stype, counting_scrub(log))
    assert bare_fp == hooked_fp, "identity hook must not move the fingerprint"
    assert pair_bytes(bare, bare_fp) == pair_bytes(hooked, hooked_fp)
    assert log == fixed_frame_calls(stype), (
        "the identity hook must actually run — byte equality alone is "
        "satisfied by a hook that never fired"
    )


@pytest.mark.parametrize("stype", [SERVER, CLIENT, BIDI])
def test_identity_hook_replays_across_the_hook_boundary(tmp_path: Path, stype: str):
    """M2: because the identity hook changes no bytes, a cassette crosses
    the hook boundary both ways. The one legitimate exception to clause 5's
    "same hook both sides" — it holds precisely because the two agree
    byte-for-byte.

    Each direction installs a COUNTING identity hook on its hooked side and
    asserts the log. A green cross-hook replay is otherwise equally
    consistent with a hook that was never dispatched — the exception is
    only meaningful if the hook is genuinely present on one side and absent
    on the other."""
    with_hook = tmp_path / "with"
    without = tmp_path / "without"
    with_hook.mkdir()
    without.mkdir()

    rec_log: list[tuple[str, bytes]] = []
    record_fixed(with_hook, stype, counting_scrub(rec_log))
    assert rec_log == fixed_frame_calls(stype), "the recording side's hook must actually run"
    replay_fixed(with_hook, stype, None)

    record_fixed(without, stype, None)
    rep_log: list[tuple[str, bytes]] = []
    replay_fixed(without, stype, counting_scrub(rep_log))
    # Replay scrubs live sends only (M5); recorded recv frames are
    # delivered verbatim.
    assert rep_log == [(SEND, f) for f in fixed_sends(stype)], (
        "the replaying side's hook must actually run"
    )


@pytest.mark.parametrize("mode", [RECORD, REPLAY])
def test_identity_derived_identity_equals_raw(tmp_path: Path, mode: str):
    """M3: clause 3 routes content-derived identity through the hook. Under
    identity it must land on the raw msg_hash in both modes — otherwise the
    hook is applied to the wrong buffer, or applied twice.

    A COUNTING identity hook supplies the routing evidence. msg_hash of
    identity-scrubbed bytes equalling msg_hash of the raw bytes is a
    tautology for any byte-neutral hook, and holds even if
    ``scrub_stream_frame`` never dispatches — the assertion that clause 3's
    route exists is the call log, exactly one call carrying the raw
    bytes."""
    log: list[tuple[str, bytes]] = []
    s = Session(mode, FileCassette(str(tmp_path)), stream_scrub=counting_scrub(log))
    scrubbed = s.scrub_stream_frame(
        SEND, StreamScrubInfo(adapter_id="grpc", type=SERVER), OPEN_MSG
    )
    assert msg_hash(scrubbed) == msg_hash(OPEN_MSG)
    assert log == [(SEND, OPEN_MSG)], (
        "identity derivation must route through the hook exactly once"
    )


def test_counting_hook_record_invocations(tmp_path: Path):
    """M4: exactly one call per frame per direction, in frame order,
    carrying that frame's bytes. Half-close and the terminal carry no
    payload and contribute no call."""
    log: list[tuple[str, bytes]] = []
    record_fixed(tmp_path, BIDI, counting_scrub(log))
    assert log == [
        (SEND, b"alpha"),
        (SEND, b"beta"),
        (RECV, b"one"),
        (RECV, b"two"),
    ]


def test_counting_hook_replay_invocations(tmp_path: Path):
    """M5: replay scrubs live sends only, once each, and never touches
    recorded frames. The trailing case caught a real cross-port divergence:
    two ports ran the hook BEFORE the bound check that rejects a send past
    the end of the recording, two after. Only a counting hook sees that."""
    record_fixed(tmp_path, BIDI, identity_scrub)

    log: list[tuple[str, bytes]] = []
    s = Session(REPLAY, FileCassette(str(tmp_path)), stream_scrub=counting_scrub(log))
    rep = s.open_stream_replay(grpc_open(BIDI))
    rep.send(b"alpha")
    rep.send(b"beta")
    rep.half_close()
    assert rep.recv() == b"one"
    assert rep.recv() == b"two"
    assert log == [(SEND, b"alpha"), (SEND, b"beta")]

    log.clear()
    with pytest.raises((StreamDone, StreamMismatchError)):
        rep.send(b"overrun")
    assert log == [], "a send past the last recorded frame is never compared, so never scrubbed"


def test_counting_hook_no_double_scrub(tmp_path: Path):
    """M6: clause 3's no-pre-scrub rule. The gRPC server-stream open message
    is both an identity input and a persisted frame — two distinct
    invocation points, one call each. An adapter that pre-scrubbed the
    message it also hands the core would show two calls for the persist
    point."""
    log: list[tuple[str, bytes]] = []
    msg = b'{"cmd":"deploy"}'
    s = Session(RECORD, FileCassette(str(tmp_path)), stream_scrub=counting_scrub(log))

    # Identity point: the adapter derives msg_hash over the scrubbed bytes.
    scrubbed = s.scrub_stream_frame(
        SEND, StreamScrubInfo(adapter_id="grpc", type=SERVER), msg
    )
    assert len(log) == 1, "identity derivation is exactly one call"

    rec = s.open_stream_record(
        StreamOpen(
            adapter_id="grpc",
            type=SERVER,
            identity={
                "service": "ops.Deploy",
                "method": "Run",
                "msg_hash": msg_hash(scrubbed),
            },
            counter=False,
            payload={"service": "ops.Deploy", "method": "Run"},
        )
    )
    # Persist point: the adapter passes the message RAW. The core scrubs.
    rec.record_send(msg)
    rec.record_half_close()
    rec.record_recv(b"deployed")
    rec.finish({"status_code": 0})

    assert log == [
        (SEND, msg),  # identity derivation
        (SEND, msg),  # persist — one call, not two
        (RECV, b"deployed"),
    ]


def test_length_changing_hook_round_trips(tmp_path: Path):
    """M7: clause 6 permits a length change; neither the record nor the
    replay path may assume byte-count preservation."""

    def grow(_d: str, _i: StreamScrubInfo, data: bytes) -> bytes:
        return data + b"-PADDED-LONGER"

    rec_s = Session(RECORD, FileCassette(str(tmp_path)), stream_scrub=grow)
    rec = rec_s.open_stream_record(grpc_open(BIDI))
    rec.record_send(b"alpha")
    rec.record_half_close()
    rec.record_recv(b"one")
    fp = rec.fingerprint
    rec.finish({"status_code": 0})

    pair = FileCassette(str(tmp_path)).load_stream("grpc", fp)
    assert pair.req_stream.frames[0].message == b"alpha-PADDED-LONGER"

    rep_s = Session(REPLAY, FileCassette(str(tmp_path)), stream_scrub=grow)
    rep = rep_s.open_stream_replay(grpc_open(BIDI))
    rep.send(b"alpha")  # green despite the length change
    rep.half_close()
    assert rep.recv() == b"one-PADDED-LONGER"
