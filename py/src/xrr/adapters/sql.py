"""sql adapter — fingerprints on normalized query + args."""
from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Any

from ..stream import canonical_fingerprint

_WS = re.compile(r"\s+")


def _normalize(query: str) -> str:
    return _WS.sub(" ", query.lower()).strip()


@dataclass
class SqlRequest:
    query: str
    args: list[Any] = field(default_factory=list)


@dataclass
class SqlResponse:
    rows: list[dict[str, Any]] = field(default_factory=list)
    affected: int = 0


class SqlAdapter:
    id = "sql"

    def fingerprint(self, req: SqlRequest) -> str:
        key = {"args": req.args, "query": _normalize(req.query)}
        return canonical_fingerprint(key)

    def serialize_req(self, req: SqlRequest) -> dict[str, Any]:
        return {"query": req.query, "args": req.args}

    def serialize_resp(self, resp: SqlResponse) -> dict[str, Any]:
        return {"rows": resp.rows, "affected": resp.affected}

    def deserialize_req(self, data: dict[str, Any]) -> SqlRequest:
        return SqlRequest(query=data["query"], args=data.get("args", []))

    def deserialize_resp(self, data: dict[str, Any]) -> SqlResponse:
        return SqlResponse(
            rows=data.get("rows", []), affected=data.get("affected", 0)
        )
