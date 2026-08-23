<?php

declare(strict_types=1);

namespace HopTop\Xrr\Stream;

/**
 * A positioned non-frame event in the interaction's total order:
 * the req-side `half_close` or the resp-side `end`.
 */
class StreamEvent
{
    public function __construct(
        public readonly int $seq,
        public readonly ?int $atMs = null
    ) {}
}
