"""redis adapter — fingerprints on command + args."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from ..stream import canonical_fingerprint


@dataclass
class RedisRequest:
    command: str
    args: list[str] = field(default_factory=list)


@dataclass
class RedisResponse:
    result: Any = None


class RedisAdapter:
    id = "redis"

    def fingerprint(self, req: RedisRequest) -> str:
        parts = [req.command.upper()] + list(req.args)
        return canonical_fingerprint(" ".join(parts))

    def serialize_req(self, req: RedisRequest) -> dict[str, Any]:
        return {"command": req.command, "args": req.args}

    def serialize_resp(self, resp: RedisResponse) -> dict[str, Any]:
        return {"result": resp.result}

    def deserialize_req(self, data: dict[str, Any]) -> RedisRequest:
        return RedisRequest(command=data["command"], args=data.get("args", []))

    def deserialize_resp(self, data: dict[str, Any]) -> RedisResponse:
        return RedisResponse(result=data.get("result"))
