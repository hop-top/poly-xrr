<?php

declare(strict_types=1);

namespace HopTop\Xrr\Stream;

/**
 * The client→server half of a streamed interaction (`.req.yaml` `stream`).
 */
class ReqStream
{
    /** @param list<Frame> $frames */
    public function __construct(
        public readonly StreamType $type,
        public readonly array $frames,
        public readonly ?StreamEvent $halfClose = null
    ) {}
}
