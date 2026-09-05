"""http adapter — fingerprints on method + path+query + body hash."""
from __future__ import annotations

import hashlib
from dataclasses import dataclass, field
from typing import Any
from urllib.parse import urlparse

from ..stream import canonical_fingerprint


@dataclass
class HttpRequest:
    method: str
    url: str
    headers: dict[str, str] = field(default_factory=dict)
    body: str = ""


@dataclass
class HttpResponse:
    status: int
    headers: dict[str, str] = field(default_factory=dict)
    body: str = ""


class HttpAdapter:
    id = "http"

    def fingerprint(self, req: HttpRequest) -> str:
        parsed = urlparse(req.url)
        path_query = parsed.path
        if parsed.query:
            path_query += "?" + parsed.query
        body_hash = hashlib.sha256(req.body.encode()).hexdigest()[:8]
        key = {"method": req.method, "path": path_query, "body_hash": body_hash}
        return canonical_fingerprint(key)

    def serialize_req(self, req: HttpRequest) -> dict[str, Any]:
        return {
            "method": req.method,
            "url": req.url,
            "headers": req.headers,
            "body": req.body,
        }

    def serialize_resp(self, resp: HttpResponse) -> dict[str, Any]:
        return {"status": resp.status, "headers": resp.headers, "body": resp.body}

    def deserialize_req(self, data: dict[str, Any]) -> HttpRequest:
        return HttpRequest(
            method=data["method"],
            url=data["url"],
            headers=data.get("headers", {}),
            body=data.get("body", ""),
        )

    def deserialize_resp(self, data: dict[str, Any]) -> HttpResponse:
        return HttpResponse(
            status=data["status"],
            headers=data.get("headers", {}),
            body=data.get("body", ""),
        )
