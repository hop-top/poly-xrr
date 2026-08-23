<?php

declare(strict_types=1);

namespace HopTop\Xrr\Exception;

use RuntimeException;

/**
 * A streamed cassette was loaded through the unary code path, or a unary
 * cassette through the streaming code path. Distinct from a cassette miss.
 */
class ShapeMismatchException extends RuntimeException
{
    public function __construct(string $detail)
    {
        parent::__construct(sprintf('xrr: cassette shape mismatch — %s', $detail));
    }
}
