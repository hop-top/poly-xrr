<?php

declare(strict_types=1);

namespace HopTop\Xrr\Stream;

/**
 * Direction shape of a streamed interaction (`req.stream.type`).
 * Unary interactions never use the streamed format.
 */
enum StreamType: string
{
    case Server = 'server';
    case Client = 'client';
    case Bidi   = 'bidi';
}
