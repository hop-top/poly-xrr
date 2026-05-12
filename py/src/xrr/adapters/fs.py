"""fs adapter — fingerprints on op + path + selected mutation fields.

Records and replays filesystem mutation operations (write, mkdir, remove,
rename, chmod, chown, symlink, hardlink, truncate). Reads are intentionally
not supported: tests should pre-seed disk state via fixtures and use xrr
only to assert on mutations.

The ``data`` field is a UTF-8 string. Binary callers MUST base64-encode
non-UTF-8 payloads themselves before passing them in (and base64-decode
on read). See spec/cassette-format-v1.md "Data Field Encoding".
"""
from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from typing import Any

# Op constants. Adopters SHOULD use these rather than literal strings.
OP_WRITE = "write"
OP_MKDIR = "mkdir"
OP_REMOVE = "remove"
OP_RENAME = "rename"
OP_CHMOD = "chmod"
OP_CHOWN = "chown"
OP_SYMLINK = "symlink"
OP_HARDLINK = "hardlink"
OP_TRUNCATE = "truncate"


@dataclass
class FsRequest:
    op: str
    path: str
    data: str = ""
    mode: int | None = None
    uid: int | None = None
    gid: int | None = None
    dest: str = ""
    size: int | None = None
    flags: int = 0
    recursive: bool = False


@dataclass
class FsResponse:
    duration_ms: int = 0
    bytes_written: int = 0


class FsAdapter:
    id = "fs"

    def __init__(self, normalizer=None) -> None:
        self._normalizer = normalizer or (lambda p: p)

    def with_normalizer(self, normalizer) -> "FsAdapter":
        return FsAdapter(normalizer=normalizer)

    def _normalize(self, p: str) -> str:
        if not p:
            return ""
        return self._normalizer(p)

    def fingerprint(self, req: FsRequest) -> str:
        fields: dict[str, Any] = {
            "op": req.op,
            "path": self._normalize(req.path),
        }
        if req.data:
            fields["data_sha256"] = hashlib.sha256(
                req.data.encode("utf-8")
            ).hexdigest()
        if req.mode is not None:
            fields["mode"] = req.mode
        if req.uid is not None:
            fields["uid"] = req.uid
        if req.gid is not None:
            fields["gid"] = req.gid
        if req.dest:
            fields["dest"] = self._normalize(req.dest)
        if req.size is not None:
            fields["size"] = req.size
        if req.flags != 0:
            fields["flags"] = req.flags
        if req.recursive:
            fields["recursive"] = True
        canonical = json.dumps(fields, sort_keys=True, separators=(",", ":"))
        return hashlib.sha256(canonical.encode()).hexdigest()[:8]

    def serialize_req(self, req: FsRequest) -> dict[str, Any]:
        out: dict[str, Any] = {"op": req.op, "path": req.path}
        if req.data:
            out["data"] = req.data
        if req.mode is not None:
            out["mode"] = req.mode
        if req.uid is not None:
            out["uid"] = req.uid
        if req.gid is not None:
            out["gid"] = req.gid
        if req.dest:
            out["dest"] = req.dest
        if req.size is not None:
            out["size"] = req.size
        if req.flags != 0:
            out["flags"] = req.flags
        if req.recursive:
            out["recursive"] = req.recursive
        return out

    def serialize_resp(self, resp: FsResponse) -> dict[str, Any]:
        out: dict[str, Any] = {}
        if resp.duration_ms:
            out["duration_ms"] = resp.duration_ms
        if resp.bytes_written:
            out["bytes_written"] = resp.bytes_written
        return out

    def deserialize_req(self, data: dict[str, Any]) -> FsRequest:
        return FsRequest(
            op=data["op"],
            path=data["path"],
            data=data.get("data", ""),
            mode=data.get("mode"),
            uid=data.get("uid"),
            gid=data.get("gid"),
            dest=data.get("dest", ""),
            size=data.get("size"),
            flags=data.get("flags", 0),
            recursive=data.get("recursive", False),
        )

    def deserialize_resp(self, data: dict[str, Any]) -> FsResponse:
        return FsResponse(
            duration_ms=data.get("duration_ms", 0),
            bytes_written=data.get("bytes_written", 0),
        )
