"""exec adapter — fingerprints on argv + stdin."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from ..stream import canonical_fingerprint


@dataclass
class ExecRequest:
    argv: list[str]
    stdin: str = ""
    env: dict[str, str] = field(default_factory=dict)


@dataclass
class ExecResponse:
    stdout: str
    stderr: str = ""
    exit_code: int = 0
    duration_ms: int = 0


class ExecAdapter:
    id = "exec"

    def fingerprint(self, req: ExecRequest) -> str:
        key = {"argv": req.argv, "stdin": req.stdin}
        return canonical_fingerprint(key)

    def serialize_req(self, req: ExecRequest) -> dict[str, Any]:
        return {"argv": req.argv, "stdin": req.stdin, "env": req.env}

    def serialize_resp(self, resp: ExecResponse) -> dict[str, Any]:
        return {
            "stdout": resp.stdout,
            "stderr": resp.stderr,
            "exit_code": resp.exit_code,
            "duration_ms": resp.duration_ms,
        }

    def deserialize_req(self, data: dict[str, Any]) -> ExecRequest:
        return ExecRequest(
            argv=data["argv"],
            stdin=data.get("stdin", ""),
            env=data.get("env", {}),
        )

    def deserialize_resp(self, data: dict[str, Any]) -> ExecResponse:
        return ExecResponse(
            stdout=data.get("stdout", ""),
            stderr=data.get("stderr", ""),
            exit_code=data.get("exit_code", 0),
            duration_ms=data.get("duration_ms", 0),
        )
