"""Session-level streamed record/replay plumbing.

Adapters build their stream wrappers on top of these handles; see
spec/cassette-format-streaming.md for the normative semantics. Handles are
created by Session.open_stream_record / Session.open_stream_replay.

Terminal signals: the end-of-stream signal is the StreamDone exception —
raised by recv() past the last recorded frame and doubling as the
post-completion send signal (the Go port's io.EOF). Error terminals raise
RecordedStreamError carrying the recorded envelope error verbatim. Stream
mismatch (byte-divergent send, short half-close) raises StreamMismatchError
and is terminal for the handle: every subsequent operation re-raises it.
"""
from __future__ import annotations

import hashlib
import threading
import time
from datetime import datetime, timezone
from typing import Any

from .cassette import FileCassette
from .stream import (
    ReqStream,
    RespStream,
    StreamedPair,
    StreamEvent,
    StreamFrame,
)
from .stream_scrub import (
    RECV,
    SEND,
    StreamScrubFunc,
    StreamScrubInfo,
    scrub_frame,
)


class StreamDone(Exception):
    """End-of-stream signal: the OK terminal on the recv side, and the
    post-completion send signal at i >= S. Never a failure — replayable
    indefinitely and never poisoning."""

    def __init__(self) -> None:
        super().__init__("xrr: end of stream")


class RecordedStreamError(Exception):
    """The recorded error terminal, re-emitted on replay in place of
    end-of-stream. Distinct from a cassette miss and a stream mismatch."""


class StreamMismatchError(Exception):
    """The replaying client diverged from the recording: byte-divergent
    send at i < S, or half-close after fewer than S sends. Terminal for
    the stream — all subsequent operations re-raise it."""

    def __init__(self, op: str, ordinal: int, detail: str) -> None:
        self.op = op
        self.ordinal = ordinal
        self.detail = detail
        super().__init__(f"xrr: stream mismatch: {op} {ordinal}: {detail}")


def _now_utc() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


# ── record path ──────────────────────────────────────────────────────────────


class StreamRecording:
    """Accumulates the event log of one live stream and writes the
    cassette pair at terminal. Events are stamped with at_ms (monotonic
    milliseconds since open) and sequenced by one per-interaction counter
    in arrival order. Safe for concurrent use — send and recv sides
    typically run on different threads."""

    def __init__(
        self,
        cassette: FileCassette,
        adapter_id: str,
        fingerprint: str,
        stream_type: str,
        req_payload: dict[str, Any],
        scrub: StreamScrubFunc | None = None,
    ) -> None:
        self._cassette = cassette
        self._adapter_id = adapter_id
        self._fingerprint = fingerprint
        self._type = stream_type
        self._req_payload = req_payload
        self._scrub = scrub
        self._scrub_info = StreamScrubInfo(adapter_id=adapter_id, type=stream_type)
        self._opened = time.monotonic()
        self._lock = threading.Lock()
        self._seq = 0
        self._sends: list[StreamFrame] = []
        self._recvs: list[StreamFrame] = []
        self._half_close: StreamEvent | None = None
        self._finished = False

    @property
    def fingerprint(self) -> str:
        """Open-time fingerprint of this interaction."""
        return self._fingerprint

    def _elapsed_ms(self) -> int:
        return max(0, int((time.monotonic() - self._opened) * 1000))

    def record_send(self, message: bytes) -> None:
        """Log one client->server message, scrubbed by the session's
        frame scrub hook before it is retained. Dropped after finish."""
        with self._lock:
            if self._finished:
                return
            msg = scrub_frame(self._scrub, SEND, self._scrub_info, message)
            self._sends.append(
                StreamFrame(seq=self._seq, message=bytes(msg), at_ms=self._elapsed_ms())
            )
            self._seq += 1

    def record_recv(self, message: bytes) -> None:
        """Log one server->client message, scrubbed by the session's
        frame scrub hook before it is retained. Dropped after finish."""
        with self._lock:
            if self._finished:
                return
            msg = scrub_frame(self._scrub, RECV, self._scrub_info, message)
            self._recvs.append(
                StreamFrame(seq=self._seq, message=bytes(msg), at_ms=self._elapsed_ms())
            )
            self._seq += 1

    def record_half_close(self) -> None:
        """Log the client closing its send side. It occurs at most once;
        repeats and post-terminal calls are dropped, matching their
        real-world no-op."""
        with self._lock:
            if self._finished or self._half_close is not None:
                return
            self._half_close = StreamEvent(seq=self._seq, at_ms=self._elapsed_ms())
            self._seq += 1

    def finish(
        self,
        resp_payload: dict[str, Any] | None,
        terminal_error: str | Exception | None = None,
    ) -> None:
        """Record the terminal event and persist the pair. terminal_error
        is None for an OK terminal; otherwise its string form is persisted
        as the resp envelope error field so replay re-emits it. No events
        are recorded after finish, and calling it twice raises."""
        with self._lock:
            if self._finished:
                raise RuntimeError("xrr: stream already finished")
            self._finished = True
            end = StreamEvent(seq=self._seq, at_ms=self._elapsed_ms())
            self._seq += 1
            now = _now_utc()
            pair = StreamedPair(
                adapter=self._adapter_id,
                fingerprint=self._fingerprint,
                req_recorded_at=now,
                resp_recorded_at=now,
                req_payload=self._req_payload,
                resp_payload=resp_payload if resp_payload is not None else {},
                req_stream=ReqStream(
                    type=self._type, frames=self._sends, half_close=self._half_close
                ),
                resp_stream=RespStream(frames=self._recvs, end=end),
                error="" if terminal_error is None else str(terminal_error),
            )
            self._cassette.save_stream(pair)


# ── replay path ──────────────────────────────────────────────────────────────


class StreamReplay:
    """Serves one recorded streamed interaction. Send-side events are
    validated against the recording (order and bytes); recv-side frames
    are delivered in seq order, never gated on send progress. Timing is
    ignored: frames are delivered as fast as the client consumes them
    (at_ms stays available on the loaded pair for a future opt-in
    replay-timing mode). Safe for concurrent use."""

    def __init__(
        self,
        fingerprint: str,
        pair: StreamedPair,
        scrub: StreamScrubFunc | None = None,
        scrub_info: StreamScrubInfo | None = None,
    ) -> None:
        self._fingerprint = fingerprint
        self._pair = pair
        self._scrub = scrub
        self._scrub_info = scrub_info or StreamScrubInfo(
            adapter_id=pair.adapter, type=pair.req_stream.type
        )
        self._lock = threading.Lock()
        self._send_idx = 0
        self._recv_idx = 0
        self._mismatch: StreamMismatchError | None = None

    @property
    def fingerprint(self) -> str:
        """Open-time fingerprint of this interaction."""
        return self._fingerprint

    @property
    def type(self) -> str:
        """Recorded stream type."""
        return self._pair.req_stream.type

    @property
    def req_payload(self) -> dict[str, Any]:
        """Recorded open-request payload."""
        return self._pair.req_payload

    @property
    def resp_payload(self) -> dict[str, Any]:
        """Recorded terminal-response payload (for gRPC: the status code).
        Available from open — adapters typically read it only at terminal
        delivery."""
        return self._pair.resp_payload

    def _terminal(self) -> Exception:
        """The terminal result: the recorded error when the resp envelope
        error is non-empty, the end-of-stream signal otherwise."""
        if self._pair.error:
            return RecordedStreamError(self._pair.error)
        return StreamDone()

    def _fail(self, mismatch: StreamMismatchError) -> StreamMismatchError:
        self._mismatch = mismatch
        return mismatch

    def send(self, message: bytes) -> None:
        """Validate the i-th client message against recorded send frame i.

        - i < S, equal bytes: accepted (the message is discarded).
        - i < S, divergent bytes: StreamMismatchError — terminal for the
          handle.
        - i >= S: the recording was already past its last observed send.
          With an OK terminal raises StreamDone (the post-completion
          stream-done signal) WITHOUT poisoning the recv side; with an
          error terminal raises the recorded error. Bytes at i >= S are
          never compared.

        The live bytes are scrubbed by the session's frame scrub hook
        before the comparison — recorded frames were scrubbed at record
        time, so symmetric scrubbing is what makes a scrubbed cassette
        match its live traffic.
        """
        with self._lock:
            if self._mismatch is not None:
                raise self._mismatch
            i = self._send_idx
            frames = self._pair.req_stream.frames
            if i >= len(frames):
                raise self._terminal()
            message = scrub_frame(self._scrub, SEND, self._scrub_info, message)
            recorded = frames[i].message
            if message != recorded:
                want = hashlib.sha256(recorded).hexdigest()
                got = hashlib.sha256(message).hexdigest()
                raise self._fail(
                    StreamMismatchError(
                        "send", i, f"expected sha256 {want}, got sha256 {got}"
                    )
                )
            self._send_idx += 1

    def half_close(self) -> None:
        """Validate the client closing its send side: always accepted
        after all recorded sends were observed (whether or not the
        recording has half_close), a stream mismatch after fewer."""
        with self._lock:
            if self._mismatch is not None:
                raise self._mismatch
            recorded_sends = len(self._pair.req_stream.frames)
            if self._send_idx < recorded_sends:
                raise self._fail(
                    StreamMismatchError(
                        "half_close",
                        self._send_idx,
                        f"half-close after {self._send_idx} sends, "
                        f"recording has {recorded_sends}",
                    )
                )

    def recv(self) -> bytes:
        """Deliver the j-th recorded recv frame's bytes. At j = R raises
        the terminal — the recorded error or StreamDone — and repeats it
        for every later read. Never blocks on send-side progress."""
        with self._lock:
            if self._mismatch is not None:
                raise self._mismatch
            frames = self._pair.resp_stream.frames
            if self._recv_idx >= len(frames):
                raise self._terminal()
            message = frames[self._recv_idx].message
            self._recv_idx += 1
            return message
