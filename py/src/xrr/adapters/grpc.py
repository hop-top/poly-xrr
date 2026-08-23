"""Streaming gRPC adapter — records and replays server-, client-, and
bidi-streamed RPCs through grpcio's own client interceptors, on top of
the core stream session API. See spec/cassette-format-streaming.md (gRPC
Adapter Mapping) for the normative semantics.

This is a recording seam, not a gRPC implementation. grpcio provides the
interceptor points (``UnaryStreamClientInterceptor``,
``StreamUnaryClientInterceptor``, ``StreamStreamClientInterceptor``); the
interceptor here switches on session mode and delegates to the library's
own ``continuation`` — passthrough calls through, record tees each
message into a cassette pair, replay serves the recording with no
network.

**Serializers.** Unlike Go's interceptor — which sits below the codec and
sees protobuf wire bytes — a Python client interceptor sees *typed*
messages: ``ClientCallDetails`` exposes only ``method``, and the
serializer pair is bound at ``channel.stream_stream(...)``, out of the
interceptor's reach. Frames must be wire bytes (spec: gRPC writers MUST
use ``message_b64``), so the adapter is constructed with the same
``request_serializer`` / ``response_deserializer`` the stubs use. A
generated stub module exposes them on its message classes
(``Msg.SerializeToString`` / ``Msg.FromString``); ``register`` binds one
pair per full method name.

**Deterministic serialization.** Byte-level send validation and
content-addressed server-stream fingerprints presume that recording and
replaying runtimes marshal the same message to identical bytes. protobuf
map entries have no specified wire order, so serializers for messages
carrying maps MUST be deterministic; ``deterministic_serializer`` wraps a
message class's ``SerializeToString`` with ``deterministic=True``.
"""
from __future__ import annotations

import threading
from typing import Any, Callable, Iterable, Iterator

import grpc

from ..session import PASSTHROUGH, RECORD, REPLAY
from ..stream import BIDI, CLIENT, SERVER, StreamOpen, msg_hash
from ..stream_scrub import SEND, StreamScrubInfo
from ..stream_session import (
    RecordedStreamError,
    StreamDone,
    StreamMismatchError,
    StreamRecording,
    StreamReplay,
)

ADAPTER_ID = "grpc"

Serializer = Callable[[Any], bytes]
Deserializer = Callable[[bytes], Any]

# gRPC status codes referenced by the mapping. Spelled out rather than
# imported piecemeal so the mapping reads against the spec.
_OK = grpc.StatusCode.OK
_UNKNOWN = grpc.StatusCode.UNKNOWN

# grpc.StatusCode members are NOT ints and their .value is a
# (cygrpc.StatusCode, name) tuple whose first element is a Cython enum —
# so neither the member nor .value[0] is YAML-serializable, and neither
# compares equal to a plain int. int() on that first element yields the
# numeric code the spec's status_code field requires.
_OK_INT = int(_OK.value[0])
_UNKNOWN_INT = int(_UNKNOWN.value[0])


class GrpcAdapterError(Exception):
    """Raised for adapter-level misuse: an unregistered method, a
    unary-shaped RPC on the stream path, a malformed full method name."""


def deterministic_serializer(message_cls: Any) -> Serializer:
    """Serializer for ``message_cls`` with protobuf deterministic mode on.

    Map entries have no specified wire order and several runtimes
    randomize it per call, so the same message otherwise marshals to
    different bytes between record and replay — breaking both send
    validation and content-addressed server-stream fingerprints (spec:
    Deterministic serialization).
    """

    def serialize(message: Any) -> bytes:
        return message.SerializeToString(deterministic=True)

    return serialize


def split_full_method(full: str) -> tuple[str, str]:
    """Split ``/pkg.Service/Method`` into its service and method
    identifiers."""
    s = full[1:] if full.startswith("/") else full
    i = s.rfind("/")
    if i <= 0 or i == len(s) - 1:
        raise GrpcAdapterError(
            f"grpc: malformed full method {full!r} (want /service/method)"
        )
    return s[:i], s[i + 1 :]


def status_code_int(code: Any) -> int:
    """Numeric gRPC status code from a grpc.StatusCode, an int, or None.

    Always returns a plain Python int: a grpc.StatusCode member carries
    its number inside a (cygrpc.StatusCode, name) tuple, and that Cython
    enum is neither YAML-serializable nor int-comparable.
    """
    if code is None:
        return _UNKNOWN_INT
    if isinstance(code, grpc.StatusCode):
        return int(code.value[0])
    if isinstance(code, bool):
        return _UNKNOWN_INT
    if isinstance(code, int):
        return code
    return _UNKNOWN_INT


def status_code_from_payload(payload: dict[str, Any]) -> grpc.StatusCode:
    """Reconstruct the terminal status from the recorded resp payload.

    Absent or malformed ``status_code`` yields UNKNOWN: the spec requires
    the field, and replay must fail loudly rather than succeed. An error
    terminal can never be OK either (the spec's iff invariant), so OK is
    also mapped to UNKNOWN here — this function is only consulted on the
    error path, and it guards hand-authored cassettes that violate the
    invariant.
    """
    raw = payload.get("status_code")
    value = raw if isinstance(raw, int) and not isinstance(raw, bool) else None
    if value is None or value == _OK_INT:
        return _UNKNOWN
    for code in grpc.StatusCode:
        if int(code.value[0]) == value:
            return code
    return _UNKNOWN


class ReplayedRpcError(grpc.RpcError, grpc.Call):
    """A recorded error terminal, re-raised on replay as the RpcError a
    generated client expects. Metadata is empty: the cassette format
    records none (spec, matching the unary gRPC adapter)."""

    def __init__(self, code: grpc.StatusCode, details: str) -> None:
        super().__init__(details)
        self._code = code
        self._details = details

    def code(self) -> grpc.StatusCode:
        return self._code

    def details(self) -> str:
        return self._details

    def initial_metadata(self) -> tuple:
        return ()

    def trailing_metadata(self) -> tuple:
        return ()

    def is_active(self) -> bool:
        return False

    def time_remaining(self) -> None:
        return None

    def cancel(self) -> bool:
        return False

    def add_callback(self, callback: Callable[[], None]) -> bool:
        return False


def _recorded_rpc_error(replay: StreamReplay, message: str) -> ReplayedRpcError:
    """Rebuild the terminal gRPC status from the recorded status_code,
    treating the recorded error string as the status description
    (spec: Response Payload and Errors)."""
    code = status_code_from_payload(replay.resp_payload)
    prefix = f"rpc error: code = {code.name} desc = "
    details = message[len(prefix) :] if message.startswith(prefix) else message
    return ReplayedRpcError(code, details)


def _stream_open(
    session: Any,
    stream_type: str,
    service: str,
    method: str,
    message: bytes | None = None,
) -> StreamOpen:
    """Build the core open value for a gRPC streamed RPC per the spec's
    gRPC mapping: canonical inputs service + method, req payload
    {service, method}. Server streams are content-addressed via
    msg_hash = sha256(message_bytes)[:8] (the wire bytes of the single
    request message); client/bidi opens are counter-addressed.

    The msg_hash is the one identity input derived from message bytes,
    and it is computed here, before the core's frame seam — so it is
    derived from the session-scrubbed bytes. Record and replay both pass
    through this path, which keeps a scrubbed recording and a scrubbed
    replay of the same live traffic addressing the same cassette. The raw
    message itself is handed to the core untouched: the core scrubs
    frames exactly once.
    """
    identity: dict[str, Any] = {"service": service, "method": method}
    payload: dict[str, Any] = {"service": service, "method": method}
    counter = True
    if stream_type == SERVER:
        counter = False
        scrubbed = session.scrub_stream_frame(
            SEND,
            StreamScrubInfo(adapter_id=ADAPTER_ID, type=stream_type),
            b"" if message is None else message,
        )
        identity["msg_hash"] = msg_hash(scrubbed)
    return StreamOpen(
        adapter_id=ADAPTER_ID,
        type=stream_type,
        identity=identity,
        counter=counter,
        payload=payload,
    )


# ── record ───────────────────────────────────────────────────────────────────


class _RecordCall(grpc.Call, grpc.RpcContext):
    """Wraps the live call-iterator returned by ``continuation`` and tees
    every observed event into a StreamRecording.

    Iteration is the recv side: each drawn response is serialized back to
    wire bytes and logged, and the terminal (StopIteration or RpcError)
    persists the pair exactly once. Call/RpcContext members delegate to
    the live call, so a caller inspecting ``code()``/``details()`` sees
    the real RPC.
    """

    def __init__(
        self,
        live: Any,
        recording: Callable[[], StreamRecording | None],
        response_serializer: Serializer,
        server_streams: bool,
    ) -> None:
        self._live = live
        self._recording = recording
        self._serialize_response = response_serializer
        self._server_streams = server_streams
        self._lock = threading.Lock()
        self._finished = False
        self._iter: Iterator[Any] | None = None

    # ── recording lifecycle ──────────────────────────────────────────────

    def _finish(self, code: Any, terminal_error: str | None) -> None:
        """Persist the pair exactly once. A save failure propagates: a
        record run whose cassette cannot be written must not pass
        silently."""
        with self._lock:
            if self._finished:
                return
            self._finished = True
            code = status_code_int(code)
            if terminal_error is not None and code == _OK_INT:
                # Spec invariant: error non-empty iff status_code != 0.
                code = _UNKNOWN_INT
            if terminal_error is None and code != _OK_INT:
                # Mirror image of the same invariant.
                terminal_error = f"rpc error: code = {code}"
        recording = self._recording()
        if recording is None:
            # The client never produced a message and the call failed
            # before the request iterator was pulled: nothing is
            # fingerprintable, so nothing is recorded.
            return
        recording.finish({"status_code": code}, terminal_error)

    def _finish_from_rpc_error(self, exc: grpc.RpcError) -> None:
        code = status_code_int(exc.code() if hasattr(exc, "code") else None)
        details = exc.details() if hasattr(exc, "details") else None
        self._finish(code, str(exc) if not details else details)

    # ── recv side ────────────────────────────────────────────────────────

    def __iter__(self) -> Iterator[Any]:
        return self

    def __next__(self) -> Any:
        if self._iter is None:
            self._iter = iter(self._live)
        try:
            response = next(self._iter)
        except StopIteration:
            self._finish(_OK_INT, None)
            raise
        except grpc.RpcError as exc:
            self._finish_from_rpc_error(exc)
            raise
        recording = self._recording()
        if recording is not None:
            recording.record_recv(self._serialize_response(response))
        return response

    def result(self, timeout: Any = None) -> Any:
        """Response-unary result (client-streaming RPCs). The single
        response message is the OK terminal — grpcio completes the RPC on
        it and a generated client never reads again."""
        try:
            response = _call_result(self._live, timeout)
        except grpc.RpcError as exc:
            self._finish_from_rpc_error(exc)
            raise
        recording = self._recording()
        if recording is not None:
            recording.record_recv(self._serialize_response(response))
        self._finish(_OK_INT, None)
        return response

    # ── Call / RpcContext passthrough ────────────────────────────────────

    def initial_metadata(self) -> Any:
        return self._live.initial_metadata()

    def trailing_metadata(self) -> Any:
        return self._live.trailing_metadata()

    def code(self) -> Any:
        return self._live.code()

    def details(self) -> Any:
        return self._live.details()

    def is_active(self) -> bool:
        return self._live.is_active()

    def time_remaining(self) -> Any:
        return self._live.time_remaining()

    def cancel(self) -> bool:
        return self._live.cancel()

    def add_callback(self, callback: Callable[[], None]) -> bool:
        return self._live.add_callback(callback)


# ── replay ───────────────────────────────────────────────────────────────────


class _ReplayCall(grpc.Call, grpc.RpcContext):
    """Serves one recorded streamed interaction as a grpc.Call-iterator:
    no network, no live call.

    Messages cross the boundary as recorded wire bytes — sends are
    serialized for byte comparison against the recording, recv frames are
    deserialized from recorded bytes into the caller's message type, and
    error terminals are reconstructed as RpcError so generated-client
    code behaves identically to a live call.
    """

    def __init__(
        self,
        replay: StreamReplay,
        request_serializer: Serializer,
        response_deserializer: Deserializer,
        request_iterator: Iterable[Any] | None,
        drain_sends: bool,
    ) -> None:
        self._replay = replay
        self._serialize_request = request_serializer
        self._deserialize_response = response_deserializer
        self._requests = request_iterator
        self._drain_sends = drain_sends
        self._lock = threading.Lock()
        self._drained = False
        self._terminal_code: grpc.StatusCode = _OK

    # ── send side ────────────────────────────────────────────────────────

    def _send_all(self) -> None:
        """Validate the client's request sequence against the recording.

        Every message the client produced is offered to the replay in
        order: divergent bytes at i < S raise a stream mismatch, and the
        post-terminal signal at i >= S ends the drain without poisoning
        the recv side (spec: Send side). The client's own half-close —
        exhausting its request iterator — is validated once the iterator
        is drained.
        """
        with self._lock:
            if self._drained:
                return
            self._drained = True
        if self._requests is None:
            return
        for request in self._requests:
            try:
                self._replay.send(self._serialize_request(request))
            except StreamDone:
                # i >= S with an OK terminal: the recorded stream was
                # already past its last observed send. Stop offering,
                # leave the recv side intact.
                return
            except RecordedStreamError as exc:
                # i >= S with an error terminal: the real stream was dead
                # too, so the send surfaces that recorded error.
                raise _recorded_rpc_error(self._replay, str(exc)) from None
            except StreamMismatchError as exc:
                raise _mismatch_rpc_error(exc) from exc
        # Iterator exhausted: the client closed its send side.
        try:
            self._replay.half_close()
        except StreamMismatchError as exc:
            raise _mismatch_rpc_error(exc) from exc

    # ── recv side ────────────────────────────────────────────────────────

    def _terminal(self, exc: Exception) -> BaseException:
        if isinstance(exc, RecordedStreamError):
            err = _recorded_rpc_error(self._replay, str(exc))
            self._terminal_code = err.code()
            return err
        self._terminal_code = _OK
        return StopIteration()

    def __iter__(self) -> Iterator[Any]:
        return self

    def __next__(self) -> Any:
        # Sends are validated before the first read so a divergent
        # conversation fails at the mismatch rather than at end of
        # stream. Reads never block on send progress: the recording is
        # fully materialised, so this drain is non-blocking by
        # construction (spec: Recv side).
        self._send_all()
        try:
            data = self._replay.recv()
        except (StreamDone, RecordedStreamError) as exc:
            raise self._terminal(exc) from None
        except StreamMismatchError as exc:
            raise _mismatch_rpc_error(exc) from exc
        return self._deserialize_response(data)

    def result(self, timeout: Any = None) -> Any:
        """Response-unary result (client-streaming RPCs): the single
        recorded response frame, or the recorded terminal."""
        try:
            return next(self)
        except StopIteration:
            # A client stream records at most one recv frame; zero only
            # when the stream terminated in an error before the response
            # message (spec: Stream Types). An OK terminal with no
            # response frame therefore has no result to hand back.
            raise ReplayedRpcError(
                _UNKNOWN,
                "xrr: recorded client-stream has no response message",
            ) from None

    # ── Call / RpcContext ────────────────────────────────────────────────

    def initial_metadata(self) -> tuple:
        """Empty: the cassette format records no metadata (spec,
        matching the unary gRPC adapter)."""
        return ()

    def trailing_metadata(self) -> tuple:
        """Empty: the cassette format records no metadata."""
        return ()

    def code(self) -> grpc.StatusCode:
        return self._terminal_code

    def details(self) -> str:
        return ""

    def is_active(self) -> bool:
        return False

    def time_remaining(self) -> None:
        return None

    def cancel(self) -> bool:
        """A replaying client that cancels gets a local no-op: the
        cassette is read-only and unaffected (spec: Cancellation)."""
        return False

    def add_callback(self, callback: Callable[[], None]) -> bool:
        return False


def _mismatch_rpc_error(exc: StreamMismatchError) -> ReplayedRpcError:
    """Surface a stream mismatch as an RpcError so it propagates through
    generated-client code, while staying distinguishable from a recorded
    error terminal: FAILED_PRECONDITION plus the core's message. The
    originating StreamMismatchError is attached as ``__cause__`` by the
    raising site."""
    err = ReplayedRpcError(grpc.StatusCode.FAILED_PRECONDITION, str(exc))
    err.mismatch = exc  # type: ignore[attr-defined]
    return err


# ── interceptor ──────────────────────────────────────────────────────────────


class _TeeingIterator:
    """Wraps the client's request iterator, logging every message that
    reaches the wire into the recording.

    grpcio pulls from this iterator on its own send thread, so a message
    is logged at the moment the library takes it — the same seam Go's
    SendMsg occupies. Half-close is the iterator running out, which
    grpcio translates into the real half-close.
    """

    def __init__(
        self,
        requests: Iterable[Any],
        serializer: Serializer,
        on_open: Callable[[bytes], StreamRecording],
    ) -> None:
        self._requests = iter(requests)
        self._serialize = serializer
        self._on_open = on_open
        self._recording: StreamRecording | None = None

    def __iter__(self) -> Iterator[Any]:
        return self

    def __next__(self) -> Any:
        # grpcio pulls with next() rather than iterating, so this must be
        # an iterator in its own right — a generator-backed __iter__ is
        # silently rejected ("object is not an iterator").
        try:
            request = next(self._requests)
        except StopIteration:
            if self._recording is None:
                # Zero-message stream: the open still has to happen,
                # since a terminal will be observed and a cassette must
                # be written.
                self._recording = self._on_open(b"")
            self._recording.record_half_close()
            raise
        data = self._serialize(request)
        if self._recording is None:
            self._recording = self._on_open(data)
        self._recording.record_send(data)
        return request


class GrpcStreamInterceptor(
    grpc.UnaryStreamClientInterceptor,
    grpc.StreamUnaryClientInterceptor,
    grpc.StreamStreamClientInterceptor,
):
    """Records and replays streamed gRPC calls through a grpcio channel.

    Install with ``grpc.intercept_channel(channel, interceptor)``. The
    three ``intercept_*`` methods map onto the spec's stream types:

    ==========================  ===============  ==============
    grpcio interception point   RPC kind         ``stream.type``
    ==========================  ===============  ==============
    ``intercept_unary_stream``  server-streaming ``server``
    ``intercept_stream_unary``  client-streaming ``client``
    ``intercept_stream_stream`` bidirectional    ``bidi``
    ==========================  ===============  ==============

    Unary-unary RPCs are deliberately NOT intercepted: unary
    interactions keep the v1 unary format and never migrate to the
    stream path.

    Each method dispatches on the session mode — passthrough invokes
    ``continuation`` untouched, record wraps it, replay never calls it —
    so the library performs the actual RPC in every mode that has one.

    Serializers are registered per full method name (``/svc/Method``),
    because a Python client interceptor cannot reach the pair bound at
    ``channel.stream_stream(...)``; see the module docstring.
    """

    def __init__(self, session: Any) -> None:
        self._session = session
        self._codecs: dict[str, tuple[Serializer, Deserializer]] = {}
        self._response_serializers: dict[str, Serializer] = {}

    def register(
        self,
        full_method: str,
        request_serializer: Serializer,
        response_deserializer: Deserializer,
        response_serializer: Serializer | None = None,
    ) -> GrpcStreamInterceptor:
        """Bind the wire codecs for one full method name.

        ``response_serializer`` is only needed in record mode, where a
        drawn response message must be turned back into the wire bytes a
        frame stores; it defaults to the message class's own
        ``SerializeToString`` when the deserializer is a generated
        ``Msg.FromString`` (the usual case). Returns self for chaining.
        """
        if response_serializer is None:
            response_serializer = _infer_response_serializer(
                response_deserializer, full_method
            )
        self._codecs[full_method] = (
            request_serializer,
            response_deserializer,
        )
        self._response_serializers[full_method] = response_serializer
        return self

    # ── grpcio interception points ───────────────────────────────────────

    def intercept_unary_stream(
        self, continuation: Callable[..., Any], call_details: Any, request: Any
    ) -> Any:
        return self._intercept(
            SERVER, continuation, call_details, [request], drain_sends=False
        )

    def intercept_stream_unary(
        self,
        continuation: Callable[..., Any],
        call_details: Any,
        request_iterator: Iterable[Any],
    ) -> Any:
        return self._intercept(
            CLIENT, continuation, call_details, request_iterator, drain_sends=True
        )

    def intercept_stream_stream(
        self,
        continuation: Callable[..., Any],
        call_details: Any,
        request_iterator: Iterable[Any],
    ) -> Any:
        return self._intercept(
            BIDI, continuation, call_details, request_iterator, drain_sends=True
        )

    # ── dispatch ─────────────────────────────────────────────────────────

    def _codec(self, full_method: str) -> tuple[Serializer, Deserializer, Serializer]:
        try:
            request_serializer, response_deserializer = self._codecs[full_method]
        except KeyError:
            raise GrpcAdapterError(
                f"grpc: no codecs registered for {full_method!r}; call "
                "register(full_method, request_serializer, response_deserializer)"
            ) from None
        return (
            request_serializer,
            response_deserializer,
            self._response_serializers[full_method],
        )

    def _intercept(
        self,
        stream_type: str,
        continuation: Callable[..., Any],
        call_details: Any,
        request_iterator: Iterable[Any],
        drain_sends: bool,
    ) -> Any:
        mode = self._session.mode
        if mode == PASSTHROUGH:
            # Transparent: the library performs the RPC, the cassette is
            # never touched, and no codecs are required.
            return continuation(call_details, _sole(request_iterator, stream_type))
        service, method = split_full_method(call_details.method)
        request_serializer, response_deserializer, response_serializer = self._codec(
            call_details.method
        )
        if mode == RECORD:
            return self._record(
                stream_type,
                service,
                method,
                continuation,
                call_details,
                request_iterator,
                request_serializer,
                response_serializer,
            )
        if mode == REPLAY:
            return self._replay(
                stream_type,
                service,
                method,
                request_iterator,
                request_serializer,
                response_deserializer,
                drain_sends,
            )
        raise GrpcAdapterError(f"grpc: unknown session mode {mode!r}")

    def _record(
        self,
        stream_type: str,
        service: str,
        method: str,
        continuation: Callable[..., Any],
        call_details: Any,
        request_iterator: Iterable[Any],
        request_serializer: Serializer,
        response_serializer: Serializer,
    ) -> Any:
        holder: dict[str, StreamRecording] = {}

        def open_recording(first_message: bytes) -> StreamRecording:
            # Client/bidi opens are fingerprinted by the occurrence
            # counter and could open earlier, but opening at the first
            # pulled message keeps one code path for all three types and
            # still consumes the counter in call order. Server streams
            # are content-addressed by the open message, which is only
            # available here.
            if "rec" not in holder:
                holder["rec"] = self._session.open_stream_record(
                    _stream_open(
                        self._session, stream_type, service, method, first_message
                    )
                )
            return holder["rec"]

        if stream_type == SERVER:
            # Server-streaming: grpcio hands the interceptor the single
            # request object, not an iterator. Open eagerly on its bytes
            # so the content-addressed fingerprint exists before the call
            # is made, and log the send plus its implicit half-close
            # (generated clients half-close with the request).
            request = _sole(request_iterator, stream_type)
            data = request_serializer(request)
            recording = open_recording(data)
            recording.record_send(data)
            recording.record_half_close()
            live = continuation(call_details, request)
        else:
            live = continuation(
                call_details,
                _TeeingIterator(request_iterator, request_serializer, open_recording),
            )

        return _RecordCall(
            live,
            lambda: holder.get("rec"),
            response_serializer,
            server_streams=stream_type != CLIENT,
        )

    def _replay(
        self,
        stream_type: str,
        service: str,
        method: str,
        request_iterator: Iterable[Any],
        request_serializer: Serializer,
        response_deserializer: Deserializer,
        drain_sends: bool,
    ) -> Any:
        if stream_type == SERVER:
            request = _sole(request_iterator, stream_type)
            # The cassette is located by the request message, so it must
            # be serialized (and scrubbed) before the open.
            data = request_serializer(request)
            replay = self._session.open_stream_replay(
                _stream_open(self._session, stream_type, service, method, data)
            )
            requests: Iterable[Any] | None = [request]
        else:
            replay = self._session.open_stream_replay(
                _stream_open(self._session, stream_type, service, method)
            )
            requests = request_iterator
        return _ReplayCall(
            replay, request_serializer, response_deserializer, requests, drain_sends
        )


# ── helpers ──────────────────────────────────────────────────────────────────


def _call_result(call: Any, timeout: Any) -> Any:
    """Draw the response-unary result from a live call.

    grpcio hands a stream-unary interceptor either a _UnaryOutcome (whose
    result() takes no arguments) or a rendezvous (whose result() accepts
    a timeout), so the keyword is passed only when it is accepted.
    """
    if timeout is None:
        return call.result()
    return call.result(timeout=timeout)


def _sole(request_iterator: Iterable[Any], stream_type: str) -> Any:
    """The single request of a server-streaming call.

    grpcio hands ``intercept_unary_stream`` the bare request object,
    which the dispatcher boxes into a one-element list; other stream
    types pass their iterator through untouched.
    """
    if stream_type != SERVER:
        return request_iterator
    items = list(request_iterator)
    if len(items) != 1:
        raise GrpcAdapterError(
            f"grpc: server-streaming call carried {len(items)} request "
            "messages (want exactly 1)"
        )
    return items[0]


def _infer_response_serializer(
    response_deserializer: Deserializer, full_method: str
) -> Serializer:
    """Derive the record-mode response serializer from a generated
    ``Msg.FromString`` deserializer.

    Record mode has to turn a drawn response message back into the wire
    bytes a frame stores. A generated deserializer is a bound classmethod
    on the message class, so the class — and its deterministic
    ``SerializeToString`` — is recoverable from ``__self__``. Anything
    else must be supplied explicitly.
    """
    owner = getattr(response_deserializer, "__self__", None)
    if owner is not None and hasattr(owner, "SerializeToString"):
        return deterministic_serializer(owner)
    raise GrpcAdapterError(
        f"grpc: cannot infer a response serializer for {full_method!r}; "
        "pass response_serializer= to register()"
    )
