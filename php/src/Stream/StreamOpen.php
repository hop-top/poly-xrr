<?php

declare(strict_types=1);

namespace HopTop\Xrr\Stream;

/**
 * Identifies a streamed interaction at open time — everything a replay
 * needs to locate the cassette before any frames exist. The adapter
 * supplies its own canonical fingerprint inputs ($identity), its req
 * payload shape ($payload), and whether the open is disambiguated by the
 * session's occurrence counter ($counter); the core owns canonical-JSON
 * assembly, the "stream" discriminator, hashing/truncation, and the
 * counter lifecycle (cassette-format-streaming.md, Fingerprinting).
 */
class StreamOpen
{
    /**
     * @param array<string, string|int> $identity canonical fingerprint
     *   inputs (for gRPC: service, method, and msg_hash for server streams;
     *   for an SSE-style adapter: url). Keys "stream" and "n" are reserved
     *   for core injection.
     * @param bool $counter marks the open as counter-addressed: the
     *   identity does not fully identify the interaction, so the session's
     *   occurrence counter — keyed by (adapterID, type, identity) —
     *   supplies the 0-based ordinal n, injected as canonical input "n"
     *   and informational payload field "n".
     * @param array<string, mixed> $payload adapter-defined open-request
     *   payload persisted to the req file. The core injects "n" for
     *   counter-addressed opens.
     */
    public function __construct(
        public readonly string $adapterID,
        public readonly StreamType $type,
        public readonly array $identity,
        public readonly bool $counter = false,
        public readonly array $payload = []
    ) {}
}
