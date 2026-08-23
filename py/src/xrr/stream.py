"""Streamed-interaction format layer (spec/cassette-format-streaming.md).

Parses, validates, and emits the v1-additive `stream` envelope field:
frame logs with global sequence numbers, half-close and terminal events,
and the streaming fingerprint algorithms. Format only — no adapter here.

Reading is resolution-blind for `message_text`: values are taken from the
raw YAML scalar, never from PyYAML's constructed value, so unquoted
hazards (`on` -> True, `12:30` -> sexagesimal 750) survive as their exact
characters. Base64 is validated strictly: b64decode's default silently
discards invalid characters, which the spec forbids.
"""
from __future__ import annotations

import base64
import binascii
import hashlib
import json
import re
from dataclasses import dataclass, field
from typing import Any

import yaml

SERVER = "server"
CLIENT = "client"
BIDI = "bidi"
STREAM_TYPES = (SERVER, CLIENT, BIDI)

_B64_RE = re.compile(r"[A-Za-z0-9+/]*={0,2}\Z")


class StreamFormatError(Exception):
    """Raised when a streamed cassette pair violates the format spec."""


class ShapeMismatch(Exception):
    """Raised when a unary cassette meets the streaming path or vice versa."""


# ── model ────────────────────────────────────────────────────────────────────


@dataclass
class StreamFrame:
    """One message frame. `message` is always decoded bytes; `text`
    records the source encoding (message_text vs message_b64) and is the
    preferred encoding on re-emit — hashing and comparison ignore it."""

    seq: int
    message: bytes
    text: bool = False
    at_ms: int | None = None


@dataclass
class StreamEvent:
    """A positioned scalar event: `half_close` or `end`."""

    seq: int
    at_ms: int | None = None


@dataclass
class ReqStream:
    """Client→server half (`.req.yaml` `stream`)."""

    type: str
    frames: list[StreamFrame] = field(default_factory=list)
    half_close: StreamEvent | None = None


@dataclass
class RespStream:
    """Server→client half (`.resp.yaml` `stream`)."""

    frames: list[StreamFrame]
    end: StreamEvent


@dataclass
class StreamedPair:
    """One streamed interaction: both envelopes plus both stream halves."""

    adapter: str
    fingerprint: str
    req_recorded_at: str
    resp_recorded_at: str
    req_payload: dict[str, Any]
    resp_payload: dict[str, Any]
    req_stream: ReqStream
    resp_stream: RespStream
    error: str = ""


# ── fingerprinting ───────────────────────────────────────────────────────────


def canonical_fingerprint(inputs: dict[str, Any]) -> str:
    """v1 algorithm: sha256 of canonical JSON (sorted keys, no
    insignificant whitespace), truncated to 8 lowercase hex chars."""
    canonical = json.dumps(inputs, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(canonical.encode()).hexdigest()[:8]


def msg_hash(message: bytes) -> str:
    """Unary building block reused by server streams: sha256(bytes)[:8]."""
    return hashlib.sha256(message).hexdigest()[:8]


@dataclass
class StreamOpen:
    """Identifies a streamed interaction at open time — everything a
    replay needs to locate the cassette before any frames exist. The
    adapter supplies its canonical fingerprint inputs (identity), its req
    payload shape (payload), and whether the open is disambiguated by the
    session's occurrence counter (counter); the core owns canonical-JSON
    assembly, the "stream" discriminator, hashing/truncation, and the
    counter lifecycle (spec: Fingerprinting Streamed Interactions)."""

    adapter_id: str
    type: str
    # Adapter canonical fingerprint inputs (for gRPC: service, method, and
    # msg_hash for server streams; for an SSE-style adapter: url). Keys
    # "stream" and "n" are reserved for core injection. Values must
    # serialize deterministically as JSON (strings and ints in practice).
    identity: dict[str, Any] = field(default_factory=dict)
    # Counter-addressed open: the identity does not fully identify the
    # interaction, so the session's occurrence counter — keyed by
    # (adapter_id, type, identity) — supplies the 0-based ordinal n,
    # injected as canonical input "n" and informational payload field "n".
    counter: bool = False
    # Adapter-defined open-request payload persisted to the req file. The
    # core injects "n" for counter-addressed opens.
    payload: dict[str, Any] = field(default_factory=dict)


def stream_canonical(open: StreamOpen, n: int | None) -> str:
    """Canonical JSON for a streamed open: the adapter identity plus the
    injected "stream" discriminator, plus "n" when given. Sorted keys, no
    insignificant whitespace — exactly the spec's canonical JSON."""
    if open.type not in STREAM_TYPES:
        raise ValueError(
            f"xrr: stream type {open.type!r} invalid (want server|client|bidi)"
        )
    inputs = dict(open.identity)
    for key in ("stream", "n"):
        if key in inputs:
            raise ValueError(
                f"xrr: stream identity key {key!r} is reserved for core injection"
            )
    inputs["stream"] = open.type
    if n is not None:
        inputs["n"] = n
    return json.dumps(inputs, sort_keys=True, separators=(",", ":"))


def stream_fingerprint(open: StreamOpen, n: int | None = None) -> str:
    """Streaming fingerprint for an open: sha256(canonical_json)[:8] over
    the adapter's canonical inputs plus a "stream" discriminator, keeping
    the streaming fingerprint space disjoint from the unary one.
    Counter-addressed opens include the 0-based occurrence ordinal n as
    canonical input "n"; n is ignored otherwise (content-addressed
    identities, e.g. gRPC server streams, hash their content instead)."""
    if open.counter:
        if n is None or n < 0:
            raise ValueError(f"xrr: stream occurrence n must be >= 0, got {n!r}")
    else:
        n = None
    canonical = stream_canonical(open, n)
    return hashlib.sha256(canonical.encode()).hexdigest()[:8]


def server_stream_fingerprint(service: str, method: str, message: bytes) -> str:
    """Server-stream fingerprint: the single request message is available
    at open, so its hash is fingerprint input (mirrors unary).

    ``message`` must already be in the form the cassette addresses. When a
    session carries a frame scrub hook, that means the SCRUBBED bytes: the
    spec derives ``msg_hash`` over scrubbed bytes in record and replay
    alike, so passing raw bytes here on a scrubbing session computes a
    fingerprint that no cassette holds. Pass frames loaded from a cassette
    (already scrubbed at record time), or the output of
    ``session.scrub_stream_frame``. The gRPC adapter does the latter;
    prefer it over calling this directly.
    """
    return stream_fingerprint(
        StreamOpen(
            adapter_id="grpc",
            type=SERVER,
            identity={
                "service": service,
                "method": method,
                "msg_hash": msg_hash(message),
            },
        )
    )


def counter_stream_fingerprint(
    service: str, method: str, stream_type: str, n: int
) -> str:
    """Client/bidi fingerprint: no message at open; `n` is the 0-based
    occurrence of the (service, method, stream type) tuple in the session
    (see Session.next_stream_n). Always included, even when 0."""
    return stream_fingerprint(
        StreamOpen(
            adapter_id="grpc",
            type=stream_type,
            identity={"service": service, "method": method},
            counter=True,
        ),
        n,
    )


# ── YAML emission ────────────────────────────────────────────────────────────


class QuotedStr(str):
    """Marker forcing double-quoted emission. The spec mandates quoted
    scalars for `fingerprint` (all-digit forms otherwise reparse as int)
    and `message_text` (plain `on`/`12:30`/`null` corrupt under YAML 1.1)."""


class _Dumper(yaml.SafeDumper):
    pass


_Dumper.add_representer(
    QuotedStr,
    lambda dumper, value: dumper.represent_scalar(
        "tag:yaml.org,2002:str", value, style='"'
    ),
)


def dump_envelope(envelope: dict[str, Any]) -> str:
    """Serialize one cassette envelope preserving key order."""
    return yaml.dump(
        envelope,
        Dumper=_Dumper,
        default_flow_style=False,
        sort_keys=False,
        allow_unicode=True,
    )


# ── parsing (node-level, resolution-blind for message_text) ──────────────────


def _load_doc(text: str) -> tuple[dict[str, Any], yaml.Node | None]:
    """Parse one cassette document. Returns the constructed envelope and
    the raw YAML node of the top-level `stream` value (None if absent)."""
    loader = yaml.SafeLoader(text)
    try:
        node = loader.get_single_node()
        data = loader.construct_document(node) if node is not None else None
    finally:
        loader.dispose()
    if not isinstance(data, dict):
        raise StreamFormatError("xrr: cassette file is not a YAML mapping")
    stream_node = None
    if isinstance(node, yaml.MappingNode):
        for key_node, value_node in node.value:
            if isinstance(key_node, yaml.ScalarNode) and key_node.value == "stream":
                stream_node = value_node
    return data, stream_node


def _mapping_fields(node: yaml.Node, what: str) -> dict[str, yaml.Node]:
    if not isinstance(node, yaml.MappingNode):
        raise StreamFormatError(f"xrr: {what} must be a mapping")
    fields: dict[str, yaml.Node] = {}
    for key_node, value_node in node.value:
        if isinstance(key_node, yaml.ScalarNode):
            fields[key_node.value] = value_node
    return fields


def _scalar_text(node: yaml.Node, what: str) -> str:
    """Raw scalar characters, blind to tag resolution."""
    if not isinstance(node, yaml.ScalarNode):
        raise StreamFormatError(f"xrr: {what} must be a scalar")
    return node.value


def _scalar_int(node: yaml.Node, what: str) -> int:
    raw = _scalar_text(node, what)
    try:
        return int(raw)
    except ValueError:
        raise StreamFormatError(
            f"xrr: {what} must be an integer, got {raw!r}"
        ) from None


def _decode_b64(value: str, what: str) -> bytes:
    """Strict RFC 4648 decode: base64 alphabet plus trailing padding only.
    Whitespace and out-of-alphabet characters are rejected, never
    discarded (spec: Frame Schema; validation rule 7)."""
    if _B64_RE.fullmatch(value) is None:
        raise StreamFormatError(f"xrr: {what}: invalid base64 characters")
    try:
        return base64.b64decode(value, validate=True)
    except (binascii.Error, ValueError) as exc:
        raise StreamFormatError(f"xrr: {what}: invalid base64: {exc}") from None


def _parse_frame(node: yaml.Node, where: str) -> StreamFrame:
    fields = _mapping_fields(node, f"{where} frame")
    if "seq" not in fields:
        raise StreamFormatError(f"xrr: {where} frame lacks seq")
    seq = _scalar_int(fields["seq"], f"{where} frame seq")
    if seq < 0:
        raise StreamFormatError(f"xrr: {where} frame seq must be >= 0")
    has_b64 = "message_b64" in fields
    has_text = "message_text" in fields
    if has_b64 == has_text:
        raise StreamFormatError(
            f"xrr: {where} frame {seq}: exactly one of "
            "message_b64/message_text required"
        )
    if has_b64:
        raw = _scalar_text(fields["message_b64"], f"{where} frame {seq} message_b64")
        message = _decode_b64(raw, f"{where} frame {seq} message_b64")
        text = False
    else:
        raw = _scalar_text(fields["message_text"], f"{where} frame {seq} message_text")
        message = raw.encode("utf-8")
        text = True
    at_ms = None
    if "at_ms" in fields:
        at_ms = _scalar_int(fields["at_ms"], f"{where} frame {seq} at_ms")
    return StreamFrame(seq=seq, message=message, text=text, at_ms=at_ms)


def _parse_frames(node: yaml.Node | None, where: str) -> list[StreamFrame]:
    if node is None:
        return []  # readers MUST treat an absent key as []
    if isinstance(node, yaml.ScalarNode) and node.value == "":
        return []  # explicit empty value, same as absent
    if not isinstance(node, yaml.SequenceNode):
        raise StreamFormatError(f"xrr: {where} frames must be a list")
    frames = [_parse_frame(item, where) for item in node.value]
    for prev, cur in zip(frames, frames[1:]):
        if cur.seq <= prev.seq:
            raise StreamFormatError(
                f"xrr: {where} frames not strictly ascending in seq"
            )
    return frames


def _parse_event(node: yaml.Node, what: str) -> StreamEvent:
    fields = _mapping_fields(node, what)
    if "seq" not in fields:
        raise StreamFormatError(f"xrr: {what} lacks seq")
    seq = _scalar_int(fields["seq"], f"{what} seq")
    if seq < 0:
        raise StreamFormatError(f"xrr: {what} seq must be >= 0")
    at_ms = None
    if "at_ms" in fields:
        at_ms = _scalar_int(fields["at_ms"], f"{what} at_ms")
    return StreamEvent(seq=seq, at_ms=at_ms)


def _parse_req_stream(node: yaml.Node) -> ReqStream:
    fields = _mapping_fields(node, "req stream")
    if "type" not in fields:
        raise StreamFormatError("xrr: req stream lacks type")
    stype = _scalar_text(fields["type"], "req stream type")
    if stype not in STREAM_TYPES:
        raise StreamFormatError(f"xrr: unknown stream type {stype!r}")
    frames = _parse_frames(fields.get("frames"), "req")
    half_close = None
    if "half_close" in fields:
        half_close = _parse_event(fields["half_close"], "half_close")
    return ReqStream(type=stype, frames=frames, half_close=half_close)


def _parse_resp_stream(node: yaml.Node) -> RespStream:
    fields = _mapping_fields(node, "resp stream")
    frames = _parse_frames(fields.get("frames"), "resp")
    if "end" not in fields:
        raise StreamFormatError("xrr: resp stream lacks end")
    end = _parse_event(fields["end"], "end")
    return RespStream(frames=frames, end=end)


def validate_streams(req_stream: ReqStream, resp_stream: RespStream) -> None:
    """Pair-level normative checks: unique seq across the pair (frames,
    half_close, end) and end.seq as the interaction maximum."""
    seqs = [f.seq for f in req_stream.frames]
    seqs += [f.seq for f in resp_stream.frames]
    if req_stream.half_close is not None:
        seqs.append(req_stream.half_close.seq)
    seqs.append(resp_stream.end.seq)
    if len(seqs) != len(set(seqs)):
        raise StreamFormatError("xrr: duplicate seq across the pair")
    if resp_stream.end.seq != max(seqs):
        raise StreamFormatError("xrr: end.seq is not the maximum seq in the pair")


def parse_pair(req_text: str, resp_text: str) -> StreamedPair:
    """Parse and validate one streamed cassette pair.

    Raises ShapeMismatch when neither file carries `stream` (unary pair on
    the streaming path) and StreamFormatError on any validation failure.
    """
    req_data, req_snode = _load_doc(req_text)
    resp_data, resp_snode = _load_doc(resp_text)

    if req_snode is None and resp_snode is None:
        raise ShapeMismatch(
            "xrr: unary cassette pair loaded through the streaming path"
        )
    if req_snode is None or resp_snode is None:
        raise StreamFormatError(
            "xrr: stream present on one file of the pair but not the other"
        )

    req_payload = req_data.get("payload")
    resp_payload = resp_data.get("payload")
    if not isinstance(req_payload, dict) or not isinstance(resp_payload, dict):
        raise StreamFormatError("xrr: payload must be a non-null object")
    if "error" in req_data:
        raise StreamFormatError("xrr: req envelope must not carry error")

    req_stream = _parse_req_stream(req_snode)
    resp_stream = _parse_resp_stream(resp_snode)
    validate_streams(req_stream, resp_stream)

    return StreamedPair(
        adapter=str(req_data.get("adapter", "")),
        fingerprint=str(req_data.get("fingerprint", "")),
        req_recorded_at=str(req_data.get("recorded_at", "")),
        resp_recorded_at=str(resp_data.get("recorded_at", "")),
        req_payload=req_payload,
        resp_payload=resp_payload,
        req_stream=req_stream,
        resp_stream=resp_stream,
        error=str(resp_data.get("error") or ""),
    )


# ── emission ─────────────────────────────────────────────────────────────────


def _frame_dict(frame: StreamFrame) -> dict[str, Any]:
    out: dict[str, Any] = {"seq": frame.seq}
    text_value: str | None = None
    if frame.text:
        try:
            text_value = frame.message.decode("utf-8")
        except UnicodeDecodeError:
            text_value = None  # not valid UTF-8: MUST fall back to base64
    if text_value is not None:
        out["message_text"] = QuotedStr(text_value)
    else:
        out["message_b64"] = QuotedStr(
            base64.b64encode(frame.message).decode("ascii")
        )
    if frame.at_ms is not None:
        out["at_ms"] = frame.at_ms
    return out


def _event_dict(event: StreamEvent) -> dict[str, Any]:
    out: dict[str, Any] = {"seq": event.seq}
    if event.at_ms is not None:
        out["at_ms"] = event.at_ms
    return out


def emit_pair(pair: StreamedPair) -> tuple[str, str]:
    """Serialize a streamed pair to (req_text, resp_text) obeying the
    normative YAML rules: quoted fingerprint and message_text scalars,
    whitespace-free base64, explicit `frames: []` when empty."""
    if pair.req_stream.type not in STREAM_TYPES:
        raise StreamFormatError(f"xrr: unknown stream type {pair.req_stream.type!r}")
    validate_streams(pair.req_stream, pair.resp_stream)

    req_stream: dict[str, Any] = {
        "type": pair.req_stream.type,
        "frames": [_frame_dict(f) for f in pair.req_stream.frames],
    }
    if pair.req_stream.half_close is not None:
        req_stream["half_close"] = _event_dict(pair.req_stream.half_close)
    req_env: dict[str, Any] = {
        "xrr": QuotedStr("1"),
        "adapter": pair.adapter,
        "fingerprint": QuotedStr(pair.fingerprint),
        "recorded_at": QuotedStr(pair.req_recorded_at),
        "payload": pair.req_payload,
        "stream": req_stream,
    }

    resp_env: dict[str, Any] = {
        "xrr": QuotedStr("1"),
        "adapter": pair.adapter,
        "fingerprint": QuotedStr(pair.fingerprint),
        "recorded_at": QuotedStr(pair.resp_recorded_at),
    }
    if pair.error:
        resp_env["error"] = QuotedStr(pair.error)
    resp_env["payload"] = pair.resp_payload
    resp_env["stream"] = {
        "frames": [_frame_dict(f) for f in pair.resp_stream.frames],
        "end": _event_dict(pair.resp_stream.end),
    }

    return dump_envelope(req_env), dump_envelope(resp_env)
