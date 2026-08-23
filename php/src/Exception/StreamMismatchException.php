<?php

declare(strict_types=1);

namespace HopTop\Xrr\Exception;

use RuntimeException;

/**
 * A replay-time divergence between the client's send-side behaviour and
 * the recording (byte-divergent send at i < S, or short half-close).
 * Mismatch is terminal: every subsequent operation on the stream throws
 * the same exception. Distinct from a cassette miss and from a recorded
 * (replayed) error.
 */
class StreamMismatchException extends RuntimeException
{
    public function __construct(
        public readonly string $op,      // "send" or "half_close"
        public readonly int $ordinal,    // 0-based ordinal of the offending client operation
        public readonly string $detail   // expected vs actual (message content identified by sha256)
    ) {
        parent::__construct(
            sprintf('xrr: stream mismatch — %s %d: %s', $op, $ordinal, $detail)
        );
    }
}
