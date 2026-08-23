<?php

declare(strict_types=1);

namespace HopTop\Xrr\Stream;

/**
 * The half of a streamed interaction a frame travels on.
 */
enum StreamDirection: string
{
    /** Client→server frames (req side). */
    case Send = 'send';

    /** Server→client frames (resp side). */
    case Recv = 'recv';
}
