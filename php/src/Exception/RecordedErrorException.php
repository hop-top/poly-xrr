<?php

declare(strict_types=1);

namespace HopTop\Xrr\Exception;

use RuntimeException;

/**
 * The recorded error terminal of a streamed interaction, re-emitted on
 * replay in place of end-of-stream (and as the post-completion send
 * signal when the recorded stream died in error). The message is the resp
 * envelope `error` field verbatim; adapters map it back to their own
 * error shape (for gRPC: a status reconstructed from `status_code`).
 */
class RecordedErrorException extends RuntimeException
{
    public function __construct(string $recorded)
    {
        parent::__construct($recorded);
    }
}
