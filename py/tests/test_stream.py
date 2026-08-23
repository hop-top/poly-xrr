"""Tests for the streamed-interaction format layer (stream.py)."""
import base64

import pytest
import yaml

from xrr.cassette import FileCassette
from xrr.stream import (
    ReqStream,
    RespStream,
    ShapeMismatch,
    StreamedPair,
    StreamEvent,
    StreamFormatError,
    StreamFrame,
    counter_stream_fingerprint,
    emit_pair,
    msg_hash,
    parse_pair,
    server_stream_fingerprint,
)

# ── spec fingerprint vectors ─────────────────────────────────────────────────
# cassette-format-streaming.md, "Fingerprint Algorithms" test vector table.


def test_msg_hash_vectors():
    assert msg_hash(b'{"path":"/etc/hosts"}') == "f1e315a5"
    assert msg_hash(b'{"path":"/var/log/big.log"}') == "164658bd"


def test_server_stream_fingerprint_vectors():
    assert (
        server_stream_fingerprint(
            "files.FileService", "Download", b'{"path":"/etc/hosts"}'
        )
        == "58a4bf3f"
    )
    assert (
        server_stream_fingerprint(
            "files.FileService", "Download", b'{"path":"/var/log/big.log"}'
        )
        == "9e8c4d4c"
    )


def test_client_stream_fingerprint_vector():
    assert (
        counter_stream_fingerprint("files.FileService", "Upload", "client", 0)
        == "2bebfd6f"
    )


def test_bidi_stream_fingerprint_vector():
    assert (
        counter_stream_fingerprint("chat.ChatService", "Converse", "bidi", 0)
        == "c6233d2e"
    )


def test_counter_fingerprint_n1_matches_repeat_fixture():
    # Second open of the same tuple in one session (grpc-client-stream-repeat).
    assert (
        counter_stream_fingerprint("files.FileService", "Upload", "client", 1)
        == "b27b5fe1"
    )


# ── parse ────────────────────────────────────────────────────────────────────

_REQ_BIDI = """\
xrr: "1"
adapter: grpc
fingerprint: "c6233d2e"
recorded_at: "2026-08-23T12:00:00Z"
payload:
  service: chat.ChatService
  method: Converse
  n: 0
stream:
  type: bidi
  frames:
    - seq: 0
      message_b64: "cGluZy0x"
      at_ms: 0
    - seq: 2
      message_b64: "cGluZy0y"
      at_ms: 40
  half_close:
    seq: 4
    at_ms: 45
"""

_RESP_BIDI = """\
xrr: "1"
adapter: grpc
fingerprint: "c6233d2e"
recorded_at: "2026-08-23T12:00:00Z"
payload:
  status_code: 0
stream:
  frames:
    - seq: 1
      message_b64: "cG9uZy0x"
      at_ms: 3
    - seq: 3
      message_b64: "cG9uZy0y"
      at_ms: 44
  end:
    seq: 5
    at_ms: 47
"""


def test_parse_pair_bidi_worked_example():
    pair = parse_pair(_REQ_BIDI, _RESP_BIDI)
    assert pair.adapter == "grpc"
    assert pair.fingerprint == "c6233d2e"
    assert pair.error == ""
    assert pair.req_payload == {
        "service": "chat.ChatService",
        "method": "Converse",
        "n": 0,
    }
    assert pair.resp_payload == {"status_code": 0}
    assert pair.req_stream.type == "bidi"
    assert [(f.seq, f.message, f.at_ms) for f in pair.req_stream.frames] == [
        (0, b"ping-1", 0),
        (2, b"ping-2", 40),
    ]
    assert pair.req_stream.half_close == StreamEvent(seq=4, at_ms=45)
    assert [(f.seq, f.message, f.at_ms) for f in pair.resp_stream.frames] == [
        (1, b"pong-1", 3),
        (3, b"pong-2", 44),
    ]
    assert pair.resp_stream.end == StreamEvent(seq=5, at_ms=47)


def test_parse_error_terminal():
    resp = _RESP_BIDI.replace(
        "payload:\n  status_code: 0",
        'error: "rpc error: code = Unavailable"\npayload:\n  status_code: 14',
    )
    pair = parse_pair(_REQ_BIDI, resp)
    assert pair.error == "rpc error: code = Unavailable"
    assert pair.resp_payload == {"status_code": 14}


def test_absent_frames_key_treated_as_empty():
    req = """\
xrr: "1"
adapter: grpc
fingerprint: "ebbd3938"
recorded_at: "2026-08-23T12:00:00Z"
payload: {}
stream:
  type: bidi
  half_close:
    seq: 0
"""
    resp = """\
xrr: "1"
adapter: grpc
fingerprint: "ebbd3938"
recorded_at: "2026-08-23T12:00:00Z"
payload: {}
stream:
  end:
    seq: 1
"""
    pair = parse_pair(req, resp)
    assert pair.req_stream.frames == []
    assert pair.resp_stream.frames == []


def test_absent_at_ms_tolerated():
    req = _REQ_BIDI.replace("      at_ms: 0\n", "").replace(
        "      at_ms: 40\n", ""
    ).replace("    at_ms: 45\n", "")
    pair = parse_pair(req, _RESP_BIDI)
    assert [f.at_ms for f in pair.req_stream.frames] == [None, None]
    assert pair.req_stream.half_close == StreamEvent(seq=4, at_ms=None)


def test_message_text_resolution_blind():
    """Unquoted scalar-hazard values must still decode as their raw text.

    PyYAML resolves plain `on` to True and `12:30` to sexagesimal 750;
    the reader must be blind to that resolution (spec: Frame Schema).
    """
    req = """\
xrr: "1"
adapter: sse
fingerprint: "66ecc77a"
recorded_at: "2026-08-23T12:00:00Z"
payload: {}
stream:
  type: server
  frames: []
"""
    resp = """\
xrr: "1"
adapter: sse
fingerprint: "66ecc77a"
recorded_at: "2026-08-23T12:00:00Z"
payload: {}
stream:
  frames:
    - seq: 0
      message_text: on
    - seq: 1
      message_text: 12:30
    - seq: 2
      message_text: null
    - seq: 3
      message_text: 2026-08-23
  end:
    seq: 4
"""
    pair = parse_pair(req, resp)
    texts = [f.message.decode("utf-8") for f in pair.resp_stream.frames]
    assert texts == ["on", "12:30", "null", "2026-08-23"]


def test_empty_b64_is_empty_message():
    req = _REQ_BIDI.replace('message_b64: "cGluZy0x"', 'message_b64: ""')
    pair = parse_pair(req, _RESP_BIDI)
    assert pair.req_stream.frames[0].message == b""


# ── validation rejections (spec: Validation Rules) ───────────────────────────


def _strip_stream(text: str) -> str:
    return text[: text.index("stream:\n")]


def test_reject_one_sided_stream():
    # Rule 1: stream on one file of the pair only.
    with pytest.raises(StreamFormatError):
        parse_pair(_REQ_BIDI, _strip_stream(_RESP_BIDI))
    with pytest.raises(StreamFormatError):
        parse_pair(_strip_stream(_REQ_BIDI), _RESP_BIDI)


def test_unary_pair_is_shape_mismatch():
    # A pair without stream on either side is a unary cassette loaded
    # through the streaming path — shape mismatch, not a format error.
    with pytest.raises(ShapeMismatch):
        parse_pair(_strip_stream(_REQ_BIDI), _strip_stream(_RESP_BIDI))


def test_reject_missing_type():
    with pytest.raises(StreamFormatError):
        parse_pair(_REQ_BIDI.replace("  type: bidi\n", ""), _RESP_BIDI)


def test_reject_unknown_type():
    with pytest.raises(StreamFormatError):
        parse_pair(_REQ_BIDI.replace("type: bidi", "type: duplex"), _RESP_BIDI)


def test_reject_frame_missing_seq():
    req = _REQ_BIDI.replace("    - seq: 0\n      ", "    - ")
    with pytest.raises(StreamFormatError):
        parse_pair(req, _RESP_BIDI)


def test_reject_frame_both_encodings():
    req = _REQ_BIDI.replace(
        'message_b64: "cGluZy0x"',
        'message_b64: "cGluZy0x"\n      message_text: "ping-1"',
    )
    with pytest.raises(StreamFormatError):
        parse_pair(req, _RESP_BIDI)


def test_reject_frame_neither_encoding():
    req = _REQ_BIDI.replace('      message_b64: "cGluZy0x"\n', "")
    with pytest.raises(StreamFormatError):
        parse_pair(req, _RESP_BIDI)


def test_reject_non_ascending_frames():
    # Rule 4: frames list must be strictly ascending in seq.
    req = _REQ_BIDI.replace("seq: 0", "seq: 2")
    with pytest.raises(StreamFormatError):
        parse_pair(req, _RESP_BIDI)


def test_reject_duplicate_seq_across_pair():
    # Rule 5: seq 4 already taken by req half_close.
    resp = _RESP_BIDI.replace("seq: 3", "seq: 4")
    with pytest.raises(StreamFormatError):
        parse_pair(_REQ_BIDI, resp)


def test_reject_missing_end():
    resp = _RESP_BIDI[: _RESP_BIDI.index("  end:\n")]
    with pytest.raises(StreamFormatError):
        parse_pair(_REQ_BIDI, resp)


def test_reject_end_not_max():
    # Rule 6: end.seq must be the maximum seq in the pair — bump a resp
    # frame past end (no other rule trips: no dupes, still ascending).
    resp = _RESP_BIDI.replace("- seq: 3", "- seq: 6")
    assert "- seq: 6" in resp
    with pytest.raises(StreamFormatError):
        parse_pair(_REQ_BIDI, resp)


def test_reject_invalid_b64_char():
    # Rule 7: character outside the base64 alphabet.
    req = _REQ_BIDI.replace("cGluZy0x", "cGluZy0x!")
    with pytest.raises(StreamFormatError):
        parse_pair(req, _RESP_BIDI)


def test_reject_b64_whitespace():
    # Rule 7: embedded whitespace must be rejected, not discarded —
    # Python's b64decode default silently drops it.
    req = _REQ_BIDI.replace("cGluZy0x", "cGlu Zy0x")
    with pytest.raises(StreamFormatError):
        parse_pair(req, _RESP_BIDI)


def test_reject_negative_seq():
    req = _REQ_BIDI.replace("seq: 0", "seq: -1")
    with pytest.raises(StreamFormatError):
        parse_pair(req, _RESP_BIDI)


# ── emit ─────────────────────────────────────────────────────────────────────


def _text_pair() -> StreamedPair:
    return StreamedPair(
        adapter="sse",
        fingerprint="12345678",
        req_recorded_at="2026-08-23T12:00:00Z",
        resp_recorded_at="2026-08-23T12:00:00Z",
        req_payload={"url": "https://example.test/events"},
        resp_payload={},
        req_stream=ReqStream(type="server", frames=[], half_close=None),
        resp_stream=RespStream(
            frames=[
                StreamFrame(seq=0, message=b"on", text=True, at_ms=1),
                StreamFrame(seq=1, message=b"12:30", text=True, at_ms=2),
            ],
            end=StreamEvent(seq=2, at_ms=3),
        ),
    )


def test_emit_message_text_quoted():
    _, resp_text = emit_pair(_text_pair())
    # Quoted scalars per spec; a bare `on` would re-read as boolean true.
    assert 'message_text: "on"' in resp_text
    assert 'message_text: "12:30"' in resp_text
    reloaded = yaml.safe_load(resp_text)
    assert reloaded["stream"]["frames"][0]["message_text"] == "on"


def test_emit_fingerprint_quoted_all_digits():
    req_text, resp_text = emit_pair(_text_pair())
    for text in (req_text, resp_text):
        assert isinstance(yaml.safe_load(text)["fingerprint"], str)


def test_emit_empty_frames_explicit():
    req_text, _ = emit_pair(_text_pair())
    assert yaml.safe_load(req_text)["stream"]["frames"] == []
    assert "frames: []" in req_text


def test_emit_b64_no_whitespace():
    pair = _text_pair()
    pair.resp_stream.frames = [
        StreamFrame(seq=0, message=b"\x00" * 120, text=False, at_ms=1)
    ]
    pair.resp_stream.end = StreamEvent(seq=1, at_ms=2)
    _, resp_text = emit_pair(pair)
    b64 = yaml.safe_load(resp_text)["stream"]["frames"][0]["message_b64"]
    assert base64.b64decode(b64, validate=True) == b"\x00" * 120
    assert " " not in b64 and "\n" not in b64


def test_emit_error_terminal_field():
    pair = _text_pair()
    pair.error = "rpc error: code = Unavailable"
    _, resp_text = emit_pair(pair)
    assert yaml.safe_load(resp_text)["error"] == "rpc error: code = Unavailable"
    req_text, _ = emit_pair(pair)
    assert "error" not in yaml.safe_load(req_text)


def test_roundtrip_preserves_bytes_and_fields():
    pair = parse_pair(_REQ_BIDI, _RESP_BIDI)
    req_text, resp_text = emit_pair(pair)
    again = parse_pair(req_text, resp_text)
    assert again == pair


# ── cassette IO ──────────────────────────────────────────────────────────────


def test_cassette_save_load_stream_roundtrip(tmp_path):
    c = FileCassette(str(tmp_path))
    pair = parse_pair(_REQ_BIDI, _RESP_BIDI)
    c.save_stream(pair)
    again = c.load_stream("grpc", "c6233d2e")
    assert again == pair


def test_unary_load_rejects_streamed_cassette(tmp_path):
    c = FileCassette(str(tmp_path))
    c.save_stream(parse_pair(_REQ_BIDI, _RESP_BIDI))
    with pytest.raises(ShapeMismatch):
        c.load("grpc", "c6233d2e")


def test_stream_load_rejects_unary_cassette(tmp_path):
    c = FileCassette(str(tmp_path))
    c.save("grpc", "c6233d2e", {"service": "x"}, {"status_code": 0})
    with pytest.raises(ShapeMismatch):
        c.load_stream("grpc", "c6233d2e")
