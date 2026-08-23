"""Tests for Session."""
import pytest
from xrr.cassette import CassetteMiss, FileCassette
from xrr.session import PASSTHROUGH, RECORD, REPLAY, Session
from xrr.adapters.exec import ExecAdapter, ExecRequest, ExecResponse


def _make_session(mode: str, tmp_path) -> Session:
    return Session(mode, FileCassette(str(tmp_path)))


def test_record_calls_do_and_saves(tmp_path):
    sess = _make_session(RECORD, tmp_path)
    adapter = ExecAdapter()
    req = ExecRequest(argv=["echo", "hi"])
    resp_obj = ExecResponse(stdout="hi\n")

    result = sess.record(adapter, req, lambda: resp_obj)

    assert result is resp_obj
    # cassette files should exist
    fp = adapter.fingerprint(req)
    assert (tmp_path / f"exec-{fp}.req.yaml").exists()
    assert (tmp_path / f"exec-{fp}.resp.yaml").exists()


def test_replay_returns_resp_without_calling_do(tmp_path):
    adapter = ExecAdapter()
    req = ExecRequest(argv=["echo", "hi"])
    resp_obj = ExecResponse(stdout="hi\n", exit_code=0)

    # pre-populate cassette via record
    rec_sess = _make_session(RECORD, tmp_path)
    rec_sess.record(adapter, req, lambda: resp_obj)

    called = []
    rep_sess = _make_session(REPLAY, tmp_path)
    result = rep_sess.record(adapter, req, lambda: called.append(1) or resp_obj)

    assert not called, "do() must not be called in replay mode"
    assert isinstance(result, ExecResponse)
    assert result.stdout == "hi\n"


def test_replay_miss_raises(tmp_path):
    sess = _make_session(REPLAY, tmp_path)
    adapter = ExecAdapter()
    req = ExecRequest(argv=["missing"])

    with pytest.raises(CassetteMiss):
        sess.record(adapter, req, lambda: None)


def test_passthrough_calls_do_no_cassette(tmp_path):
    sess = _make_session(PASSTHROUGH, tmp_path)
    adapter = ExecAdapter()
    req = ExecRequest(argv=["echo", "pass"])
    resp_obj = ExecResponse(stdout="pass\n")

    result = sess.record(adapter, req, lambda: resp_obj)
    assert result is resp_obj

    # no cassette files written
    fp = adapter.fingerprint(req)
    assert not (tmp_path / f"exec-{fp}.req.yaml").exists()


def test_invalid_mode_raises():
    c = FileCassette("/tmp")
    with pytest.raises(ValueError):
        Session("invalid", c)


def test_next_stream_n_increments_per_key(tmp_path):
    """Occurrence counter: 0-based, per identifying tuple, one domain
    per session object (spec: Fingerprinting Streamed Interactions)."""
    sess = _make_session(REPLAY, tmp_path)
    assert sess.next_stream_n("files.FileService", "Upload", "client") == 0
    assert sess.next_stream_n("files.FileService", "Upload", "client") == 1
    # Different tuple, independent count.
    assert sess.next_stream_n("chat.ChatService", "Converse", "bidi") == 0
    assert sess.next_stream_n("files.FileService", "Upload", "client") == 2


def test_next_stream_n_counts_same_in_record_and_replay(tmp_path):
    key = ("files.FileService", "Upload", "client")
    rec = _make_session(RECORD, tmp_path)
    rep = _make_session(REPLAY, tmp_path)
    assert [rec.next_stream_n(*key) for _ in range(3)] == [0, 1, 2]
    assert [rep.next_stream_n(*key) for _ in range(3)] == [0, 1, 2]


def test_stream_counter_domain_is_per_session(tmp_path):
    a = _make_session(REPLAY, tmp_path)
    b = _make_session(REPLAY, tmp_path)
    assert a.next_stream_n("s", "m", "bidi") == 0
    assert b.next_stream_n("s", "m", "bidi") == 0
