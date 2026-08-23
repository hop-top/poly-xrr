"""Adapters package.

The gRPC streaming adapter is exported lazily: it imports grpcio, which
is a test-only dependency of this package, so importing it eagerly would
break `import xrr.adapters` for consumers who do not use gRPC. Import it
directly (`from xrr.adapters.grpc import GrpcStreamInterceptor`) or via
attribute access on this package.
"""
from typing import Any

from .exec import ExecAdapter, ExecRequest, ExecResponse
from .fs import FsAdapter, FsRequest, FsResponse
from .http import HttpAdapter, HttpRequest, HttpResponse
from .redis import RedisAdapter, RedisRequest, RedisResponse
from .sql import SqlAdapter, SqlRequest, SqlResponse

__all__ = [
    "ExecAdapter",
    "ExecRequest",
    "ExecResponse",
    "FsAdapter",
    "FsRequest",
    "FsResponse",
    "HttpAdapter",
    "HttpRequest",
    "HttpResponse",
    "RedisAdapter",
    "RedisRequest",
    "RedisResponse",
    "SqlAdapter",
    "SqlRequest",
    "SqlResponse",
    "GrpcStreamInterceptor",
    "GrpcAdapterError",
    "deterministic_serializer",
]

_GRPC_EXPORTS = frozenset(
    {"GrpcStreamInterceptor", "GrpcAdapterError", "deterministic_serializer"}
)


def __getattr__(name: str) -> Any:
    """Resolve the gRPC exports on first access, so the grpcio import
    cost — and requirement — lands only on users who ask for it."""
    if name in _GRPC_EXPORTS:
        from . import grpc as _grpc

        return getattr(_grpc, name)
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
