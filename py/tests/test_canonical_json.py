"""Cross-port canonical-JSON escaping vectors (spec: Fingerprint Algorithm).

The hazard input covers every string-escaping class that has forked
fingerprints across ports: HTML-sensitive & < >, a slash, non-ASCII,
U+2028/U+2029, the backspace and form-feed short forms, a control byte
(U+001F) and DEL.
"""
from __future__ import annotations

from xrr.adapters.exec import ExecAdapter, ExecRequest
from xrr.adapters.fs import FsAdapter, FsRequest
from xrr.stream import StreamOpen, canonical_fingerprint, canonical_json, stream_fingerprint

HAZARD = "a&b<c>/é" + chr(0x2028) + chr(0x2029) + "\b\f\x1f\x7f"

# {"k":"a&b<c>/é<U+2028><U+2029>\b\f<U+001F escaped><DEL>","stream":"server"}
STREAM_CANONICAL_HEX = (
    "7b226b223a226126623c633e2fc3a9e280a8e280a95c625c665c75303031667f22"
    "2c2273747265616d223a22736572766572227d"
)


def test_canonical_json_hazard_vector():
    inputs = {"k": HAZARD, "stream": "server"}
    assert canonical_json(inputs).encode().hex() == STREAM_CANONICAL_HEX
    assert canonical_fingerprint(inputs) == "bcc2c6c3"


def test_canonical_json_keeps_non_ascii_and_del_raw():
    assert canonical_json("é\x7f") == '"é\x7f"'
    assert canonical_json("a&b<c>/") == '"a&b<c>/"'


def test_stream_fingerprint_hazard_vector():
    open_ = StreamOpen(adapter_id="x", type="server", identity={"k": HAZARD})
    assert stream_fingerprint(open_) == "bcc2c6c3"


def test_fs_fingerprint_hazard_vector():
    assert FsAdapter().fingerprint(FsRequest(op="write", path=HAZARD)) == "6f2fb087"


def test_exec_fingerprint_hazard_vector():
    assert ExecAdapter().fingerprint(ExecRequest(argv=["echo", HAZARD])) == "97618387"
