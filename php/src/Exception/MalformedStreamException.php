<?php

declare(strict_types=1);

namespace HopTop\Xrr\Exception;

use RuntimeException;

/**
 * A streamed cassette pair violates the streaming format's validation rules
 * (one-sided stream, bad type, bad frame encoding, seq violations, invalid
 * base64, missing end, ...).
 */
class MalformedStreamException extends RuntimeException
{
    public function __construct(string $detail)
    {
        parent::__construct(sprintf('xrr: malformed stream — %s', $detail));
    }
}
