"""Session — record/replay/passthrough dispatcher."""
from __future__ import annotations

from typing import Any, Callable

from .cassette import FileCassette
from .stream import ShapeMismatch, StreamOpen, stream_canonical, stream_fingerprint
from .stream_session import StreamRecording, StreamReplay

RECORD = "record"
REPLAY = "replay"
PASSTHROUGH = "passthrough"


class Session:
    """Dispatches interactions via record, replay, or passthrough."""

    def __init__(self, mode: str, cassette: FileCassette) -> None:
        if mode not in (RECORD, REPLAY, PASSTHROUGH):
            raise ValueError(f"xrr: unknown mode {mode!r}")
        self._mode = mode
        self._cassette = cassette
        self._stream_counts: dict[tuple[Any, ...], int] = {}

    def next_stream_n(self, *key: Any) -> int:
        """Occurrence counter for streamed opens: the 0-based count of
        prior opens with the same identifying tuple in this session.

        One session object is one counter domain; the count advances at
        each open and is computed identically in record and replay modes
        (spec: Fingerprinting Streamed Interactions).
        """
        n = self._stream_counts.get(key, 0)
        self._stream_counts[key] = n + 1
        return n

    def _check_stream_open(self, open: StreamOpen, want: str, verb: str) -> None:
        if self._mode != want:
            raise RuntimeError(
                f"xrr: {verb} requires {want} mode (session is {self._mode!r})"
            )
        if not open.adapter_id:
            raise ValueError(f"xrr: {verb} requires an adapter id")

    def _stream_open_fingerprint(self, open: StreamOpen) -> tuple[str, int | None]:
        """Open-time fingerprint, consuming the occurrence counter for
        counter-addressed opens. The counter is keyed by the adapter id
        plus the canonical identity (sans "n") — the adapter's
        identifying tuple. n is None for content-addressed opens."""
        n = None
        if open.counter:
            n = self.next_stream_n(open.adapter_id, stream_canonical(open, None))
        return stream_fingerprint(open, n), n

    def open_stream_record(self, open: StreamOpen) -> StreamRecording:
        """Open a streamed interaction for recording. The adapter observes
        the live stream and mirrors it into the returned recording:
        record_send/record_recv per message, record_half_close when the
        client closes its send side, then finish exactly once when the
        terminal is observed — only finish persists the pair, so a stream
        that never reaches terminal produces no cassette."""
        self._check_stream_open(open, RECORD, "open_stream_record")
        fp, n = self._stream_open_fingerprint(open)
        payload = dict(open.payload)
        if n is not None:
            # Informational occurrence ordinal: recoverable from disk,
            # never read back to drive matching.
            payload["n"] = n
        return StreamRecording(self._cassette, open.adapter_id, fp, open.type, payload)

    def open_stream_replay(self, open: StreamOpen) -> StreamReplay:
        """Locate the cassette pair for a streamed open and return a
        replay handle. Raises CassetteMiss when no pair exists and
        ShapeMismatch when the pair is unary or of another stream type.
        The occurrence counter is consumed exactly as in record mode, hit
        or miss."""
        self._check_stream_open(open, REPLAY, "open_stream_replay")
        fp, _ = self._stream_open_fingerprint(open)
        pair = self._cassette.load_stream(open.adapter_id, fp)
        if pair.req_stream.type != open.type:
            raise ShapeMismatch(
                f"xrr: recorded stream type {pair.req_stream.type!r}, "
                f"requested {open.type!r}"
            )
        return StreamReplay(fp, pair)

    def record(self, adapter: Any, req: Any, do: Callable[[], Any]) -> Any:
        """Execute one interaction according to the session mode.

        record:      call do(), save req+resp, return resp.
        replay:      load cassette, deserialize resp, return; do() NOT called.
        passthrough: call do(), never touch cassette.
        """
        if self._mode == RECORD:
            return self._do_record(adapter, req, do)
        if self._mode == REPLAY:
            return self._do_replay(adapter, req)
        # passthrough
        return do()

    def _do_record(self, adapter: Any, req: Any, do: Callable[[], Any]) -> Any:
        resp = do()
        fp = adapter.fingerprint(req)
        self._cassette.save(
            adapter.id,
            fp,
            adapter.serialize_req(req),
            adapter.serialize_resp(resp),
        )
        return resp

    def _do_replay(self, adapter: Any, req: Any) -> Any:
        fp = adapter.fingerprint(req)
        _req_data, resp_data = self._cassette.load(adapter.id, fp)
        return adapter.deserialize_resp(resp_data)
