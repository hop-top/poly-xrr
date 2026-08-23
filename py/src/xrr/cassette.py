"""FileCassette — YAML-based on-disk cassette storage."""
from __future__ import annotations

import os
from datetime import datetime, timezone
from typing import Any

import yaml

from .redact import Redactor, redact_config_from_env
from .stream import (
    QuotedStr,
    ShapeMismatch,
    StreamedPair,
    dump_envelope,
    emit_pair,
    parse_pair,
)


class CassetteMiss(Exception):
    """Raised when replay finds no matching cassette file."""


class FileCassette:
    """Stores interactions as YAML files in a directory.

    Redaction is enabled by default, configured from the XRR_REDACT_*
    environment variables. Pass an explicit redactor to supply a policy
    that bypasses the environment.
    """

    def __init__(self, directory: str, redactor: Redactor | None = None) -> None:
        self._dir = directory
        self._redactor = redactor

    def _active_redactor(self) -> Redactor:
        # When none was injected, config is read from the environment on
        # each write so a test that flips XRR_REDACT_* mid-process sees
        # the change.
        return self._redactor or Redactor(redact_config_from_env())

    def save(
        self,
        adapter_id: str,
        fingerprint: str,
        req: dict[str, Any],
        resp: dict[str, Any],
    ) -> None:
        now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        self._write(adapter_id, fingerprint, "req", now, req)
        self._write(adapter_id, fingerprint, "resp", now, resp)

    def _write(
        self,
        adapter_id: str,
        fingerprint: str,
        kind: str,
        recorded_at: str,
        payload: dict[str, Any],
    ) -> None:
        # Scrub credential-bearing fields before serialization — a
        # secret never reaches the YAML string, let alone the file.
        # Envelope metadata is never scrubbed: the fingerprint in
        # particular must match the filename.
        envelope = {
            "xrr": "1",
            "adapter": adapter_id,
            # Quoted per spec: an unquoted all-digit fingerprint reparses
            # as an integer and no longer matches its filename.
            "fingerprint": QuotedStr(fingerprint),
            "recorded_at": recorded_at,
            "payload": self._active_redactor().redact_payload(payload),
        }
        with open(self._path(adapter_id, fingerprint, kind), "w", encoding="utf-8") as fh:
            fh.write(dump_envelope(envelope))

    def load(
        self, adapter_id: str, fingerprint: str
    ) -> tuple[dict[str, Any], dict[str, Any]]:
        """Return (req_payload, resp_payload). Raises CassetteMiss if not found."""
        req = self._read(adapter_id, fingerprint, "req")
        resp = self._read(adapter_id, fingerprint, "resp")
        return req, resp

    def save_stream(self, pair: StreamedPair) -> None:
        """Persist one streamed interaction as its req/resp pair."""
        req_text, resp_text = emit_pair(pair)
        for kind, text in (("req", req_text), ("resp", resp_text)):
            path = self._path(pair.adapter, pair.fingerprint, kind)
            with open(path, "w", encoding="utf-8") as fh:
                fh.write(text)

    def load_stream(self, adapter_id: str, fingerprint: str) -> StreamedPair:
        """Load and validate one streamed pair.

        Raises CassetteMiss when either file is absent, ShapeMismatch when
        the pair is unary, StreamFormatError when malformed.
        """
        req_text = self._read_text(adapter_id, fingerprint, "req")
        resp_text = self._read_text(adapter_id, fingerprint, "resp")
        return parse_pair(req_text, resp_text)

    def _path(self, adapter_id: str, fingerprint: str, kind: str) -> str:
        return os.path.join(
            self._dir, f"{adapter_id}-{fingerprint}.{kind}.yaml"
        )

    def _read_text(self, adapter_id: str, fingerprint: str, kind: str) -> str:
        path = self._path(adapter_id, fingerprint, kind)
        if not os.path.exists(path):
            raise CassetteMiss(
                f"xrr: cassette miss: {adapter_id}-{fingerprint}.{kind}.yaml"
            )
        with open(path, encoding="utf-8") as fh:
            return fh.read()

    def _read(self, adapter_id: str, fingerprint: str, kind: str) -> dict[str, Any]:
        envelope = yaml.safe_load(self._read_text(adapter_id, fingerprint, kind))
        if isinstance(envelope, dict) and "stream" in envelope:
            # Streamed cassette through the unary path — a shape-mismatch
            # error, distinct from a cassette miss (spec: Envelope Extension).
            raise ShapeMismatch(
                f"xrr: streamed cassette {kind} file loaded through unary path"
            )
        payload = envelope.get("payload")
        if payload is None:
            raise ValueError(f"xrr: missing payload in {kind} file")
        return payload
