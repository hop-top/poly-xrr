<?php

declare(strict_types=1);

namespace HopTop\Xrr\Stream;

/**
 * The server→client half of a streamed interaction (`.resp.yaml` `stream`).
 */
class RespStream
{
    /** @param list<Frame> $frames */
    public function __construct(
        public readonly array $frames,
        public readonly StreamEvent $end
    ) {}
}
