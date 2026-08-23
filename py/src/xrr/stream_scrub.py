"""Frame-level secret scrubbing for streamed cassettes.

Stream frames are opaque decoded bytes, base64-encoded on disk — so the
record-time, field-name-based cassette redaction in ``redact`` (which
covers structured payloads) cannot see into them, and text-level
scrubbing applied to the cassette after the fact is defeated by the
encoding. The scrub hook closes that gap at the only workable seam: the
DECODED bytes, before they are fingerprinted, persisted, or compared.

Symmetry is the correctness invariant. Replay validates live send bytes
against recorded frames byte-for-byte, and server-stream fingerprints are
content-addressed by the open message, so a scrub applied only at record
time would make every scrubbed cassette unreplayable. The hook therefore
applies identically in both modes:

- record: every frame (both directions) is scrubbed before it is
  persisted, and content-derived identity inputs (gRPC server-stream
  msg_hash) are computed over scrubbed bytes — the fingerprint addresses
  the scrubbed content.
- replay: live send bytes are scrubbed before comparison, and the same
  content-derived identity is computed over scrubbed live bytes, so a
  scrubbed recording and a scrubbed replay of the same traffic meet at
  the same fingerprint and the same frame bytes. Recorded frames were
  already scrubbed at record time and are delivered verbatim — never
  re-scrubbed.

The same hook MUST be installed on the session that recorded a cassette
and on every session that replays it; replaying a scrubbed cassette
without the hook (or with a different one) fails loudly as a stream
mismatch.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Callable

# StreamDirection identifies the half of a streamed interaction a frame
# travels on.
SEND = "send"
"""Client→server frames (req side)."""

RECV = "recv"
"""Server→client frames (resp side)."""


@dataclass(frozen=True)
class StreamScrubInfo:
    """Identity context handed to a scrub hook with every frame.

    Carries only inputs known at every scrub site — including before
    content-derived identity fields (such as the gRPC server-stream
    msg_hash) exist, since those are derived FROM the scrubbed bytes — so
    one hook sees one consistent context for the same bytes everywhere.
    """

    adapter_id: str
    type: str


# StreamScrubFunc rewrites the decoded bytes of one stream frame. It may
# return the data unchanged, or new bytes.
#
# Requirements on the hook:
#
# - Deterministic and pure: the same input bytes MUST always produce the
#   same output, across calls, runs, and processes. Nondeterministic
#   scrubbing (counters, timestamps, randomized placeholders) diverges
#   content-addressed fingerprints and send validation, breaking replay.
# - Structure-preserving for encoded payloads: replayed recv frames are
#   decoded by the client (gRPC frames are protobuf wire bytes), so the
#   scrub must keep the encoding valid — replace secrets with
#   equal-length placeholders, or decode, edit, and deterministically
#   re-encode.
#
# Half-close and terminal events carry no payload and are never scrubbed.
# Adapter payload maps (the open request / terminal response objects) are
# not covered by this hook either: they are structured, named-field YAML
# — the domain of record-time cassette redaction — while this hook exists
# for the frame byte layer that field-name matching cannot reach.
StreamScrubFunc = Callable[[str, StreamScrubInfo, bytes], bytes]


def scrub_frame(
    scrub: StreamScrubFunc | None,
    direction: str,
    info: StreamScrubInfo,
    data: bytes,
) -> bytes:
    """Apply ``scrub`` to ``data`` when a hook is installed.

    Callers own the "scrub exactly once per frame" discipline: record
    scrubs before persisting, replay scrubs live send bytes before
    comparison and never re-scrubs recorded frames.
    """
    if scrub is None:
        return data
    return scrub(direction, info, data)
