"""fs adapter tests — identity, fingerprint discrimination, conformance."""
from __future__ import annotations

import base64

from xrr.adapters.fs import (
    OP_REMOVE,
    OP_RENAME,
    OP_WRITE,
    FsAdapter,
    FsRequest,
    FsResponse,
)


# ---------------------------------------------------------------------------
# Identity
# ---------------------------------------------------------------------------


def test_adapter_id():
    assert FsAdapter().id == "fs"


# ---------------------------------------------------------------------------
# Fingerprint
# ---------------------------------------------------------------------------


def test_fingerprint_deterministic():
    a = FsAdapter()
    req = FsRequest(op=OP_WRITE, path="/etc/hosts", data="127.0.0.1 localhost\n")
    fp1 = a.fingerprint(req)
    fp2 = a.fingerprint(req)
    assert len(fp1) == 8
    assert fp1 == fp2


def test_fingerprint_discriminates_path():
    a = FsAdapter()
    fp_a = a.fingerprint(FsRequest(op=OP_WRITE, path="/a", data="x"))
    fp_b = a.fingerprint(FsRequest(op=OP_WRITE, path="/b", data="x"))
    assert fp_a != fp_b


def test_fingerprint_discriminates_data():
    a = FsAdapter()
    fp_a = a.fingerprint(FsRequest(op=OP_WRITE, path="/x", data="foo"))
    fp_b = a.fingerprint(FsRequest(op=OP_WRITE, path="/x", data="bar"))
    assert fp_a != fp_b


def test_fingerprint_discriminates_op():
    a = FsAdapter()
    fp_w = a.fingerprint(FsRequest(op=OP_WRITE, path="/x"))
    fp_r = a.fingerprint(FsRequest(op=OP_REMOVE, path="/x"))
    assert fp_w != fp_r


def test_fingerprint_discriminates_mode():
    a = FsAdapter()
    fp_a = a.fingerprint(FsRequest(op=OP_WRITE, path="/x", data="y", mode=0o644))
    fp_b = a.fingerprint(FsRequest(op=OP_WRITE, path="/x", data="y", mode=0o600))
    assert fp_a != fp_b


def test_fingerprint_omits_unset_optionals():
    """mode=None must hash identically to a minimal write (no mode field)."""
    a = FsAdapter()
    bare = FsRequest(op=OP_WRITE, path="/x", data="y")
    with_none = FsRequest(op=OP_WRITE, path="/x", data="y", mode=None)
    assert a.fingerprint(bare) == a.fingerprint(with_none)


def test_fingerprint_zero_mode_distinct_from_unset():
    """mode=0 must differ from mode=None (presence matters)."""
    a = FsAdapter()
    bare = FsRequest(op=OP_WRITE, path="/x", data="y")
    with_zero = FsRequest(op=OP_WRITE, path="/x", data="y", mode=0)
    assert a.fingerprint(bare) != a.fingerprint(with_zero)


# ---------------------------------------------------------------------------
# CRITICAL conformance — fingerprint MUST equal 667a7680
# ---------------------------------------------------------------------------


def test_conformance_fs_write_fingerprint():
    """Conformance fixture: spec/fixtures/fs-write/."""
    a = FsAdapter()
    req = FsRequest(
        op="write",
        path="$TMP/greeting.txt",
        data="hello, world\n",
        mode=420,
    )
    assert a.fingerprint(req) == "667a7680"


# ---------------------------------------------------------------------------
# Normalizer
# ---------------------------------------------------------------------------


def test_normalizer_applied_to_path():
    norm = FsAdapter().with_normalizer(
        lambda p: p.replace("/var/folders/abc/T/Test123", "$TMP")
    )
    raw = FsRequest(
        op=OP_WRITE, path="/var/folders/abc/T/Test123/config.yaml", data="k: v"
    )
    pre = FsRequest(op=OP_WRITE, path="$TMP/config.yaml", data="k: v")
    assert norm.fingerprint(raw) == norm.fingerprint(pre)


def test_normalizer_applied_to_dest():
    norm = FsAdapter().with_normalizer(lambda p: p.replace("/tmp", "$TMP"))
    fp1 = norm.fingerprint(FsRequest(op=OP_RENAME, path="/tmp/a", dest="/tmp/b"))
    fp2 = norm.fingerprint(FsRequest(op=OP_RENAME, path="$TMP/a", dest="$TMP/b"))
    assert fp1 == fp2


def test_normalizer_short_circuits_empty_path():
    """Empty path must not invoke the user normalizer."""
    calls = []
    a = FsAdapter().with_normalizer(lambda p: calls.append(p) or "NEVER")
    a.fingerprint(FsRequest(op="chmod", path="", mode=0o644))
    assert calls == []


# ---------------------------------------------------------------------------
# Serialize / Deserialize
# ---------------------------------------------------------------------------


def test_serialize_roundtrip():
    a = FsAdapter()
    req = FsRequest(
        op=OP_WRITE, path="/etc/hosts", data="127.0.0.1 localhost\n", mode=0o644
    )
    got = a.deserialize_req(a.serialize_req(req))
    assert got.op == req.op
    assert got.path == req.path
    assert got.data == req.data
    assert got.mode == req.mode


def test_serialize_response_roundtrip():
    a = FsAdapter()
    resp = FsResponse(duration_ms=42, bytes_written=1024)
    got = a.deserialize_resp(a.serialize_resp(resp))
    assert got.duration_ms == 42
    assert got.bytes_written == 1024


def test_serialize_omits_zero_optionals():
    """A bare write must not emit dest/uid/gid/size/flags/recursive."""
    a = FsAdapter()
    out = a.serialize_req(FsRequest(op=OP_WRITE, path="/x", data="y"))
    for forbidden in ("dest", "uid", "gid", "size", "flags", "recursive"):
        assert forbidden not in out, f"bare write must omit {forbidden!r}"


def test_serialize_base64_payload_roundtrip():
    """Binary callers base64-encode; round-trip recovers exact bytes."""
    a = FsAdapter()
    raw = bytes([0x00, 0xFF, 0xC3, 0x28, 0x80, 0x01, 0x02, 0x03])
    encoded = base64.b64encode(raw).decode("ascii")
    req = FsRequest(op=OP_WRITE, path="/bin/x", data=encoded)
    got = a.deserialize_req(a.serialize_req(req))
    assert got.data == encoded
    assert base64.b64decode(got.data) == raw
