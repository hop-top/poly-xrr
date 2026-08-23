<?php

declare(strict_types=1);

namespace HopTop\Xrr\Adapters\Grpc;

use HopTop\Xrr\Stream\StreamRecording;

/**
 * Terminal handling shared by the recording call classes.
 *
 * Only the terminal persists a pair, so a stream that never reaches one
 * (fatal error, process death) leaves no cassette — by design.
 */
trait RecordsStatus
{
    private bool $xrrFinished = false;

    /**
     * Persists the pair from a gRPC status object exactly once.
     *
     * The spec requires the envelope `error` to be non-empty iff
     * `status_code != 0`: a non-OK status with an empty description still
     * gets a synthesized error string, and the status text is rendered the
     * way the PHP client surfaces it.
     *
     * @param object $status gRPC status: int $code, string $details
     */
    private function finishFromStatus(?StreamRecording $rec, object $status): void
    {
        if ($rec === null || $this->xrrFinished) {
            return;
        }
        $this->xrrFinished = true;

        $code = (int) ($status->code ?? 0);
        /** @var string $details */
        $details = (string) ($status->details ?? '');

        $error = null;
        if ($code !== 0) {
            $error = $details !== ''
                ? sprintf('rpc error: code = %d desc = %s', $code, $details)
                : sprintf('rpc error: code = %d', $code);
        }

        $rec->finish(['status_code' => $code], $error);
    }
}
