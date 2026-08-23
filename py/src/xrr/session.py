"""Session — record/replay/passthrough dispatcher."""
from __future__ import annotations

from typing import Any, Callable

from .cassette import FileCassette

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
