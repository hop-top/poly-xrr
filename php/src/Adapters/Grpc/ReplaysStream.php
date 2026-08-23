<?php

declare(strict_types=1);

namespace HopTop\Xrr\Adapters\Grpc;

use HopTop\Xrr\Exception\RecordedErrorException;
use HopTop\Xrr\Stream\StreamReplay;

/**
 * Terminal and deserialization handling shared by the replaying call
 * classes.
 *
 * Replaying calls deliberately do NOT run {@see \Grpc\AbstractCall}'s
 * constructor: it builds a real `Grpc\Call` against a channel, which would
 * touch the network. A replaying call owns only a {@see StreamReplay}, so a
 * replay run connects to nothing — that is the property the whole mode
 * rests on.
 */
trait ReplaysStream
{
    private StreamReplay $rp;

    /**
     * The deserialize callback grpc-php would have used, in the same
     * [$className, $deserializeFunc] shape {@see \Grpc\AbstractCall} takes.
     *
     * @var array{0: class-string, 1: string}|callable|null
     */
    private $xrrDeserialize;

    /**
     * Rebuilds the caller's message object from recorded wire bytes,
     * exactly as a live call's codec would.
     */
    private function xrrDeserializeResponse(?string $value): mixed
    {
        if ($value === null) {
            return null;
        }

        $deserialize = $this->xrrDeserialize;
        if (is_array($deserialize)) {
            [$className] = $deserialize;
            $obj = new $className();
            $obj->mergeFromString($value);

            return $obj;
        }
        if (is_callable($deserialize)) {
            return $deserialize($value);
        }

        // No deserializer: hand back the raw wire bytes.
        return $value;
    }

    /**
     * Reconstructs the terminal gRPC status from the recorded resp payload.
     *
     * `status_code` is required by the spec; an absent or malformed value is
     * reported as UNKNOWN (2) rather than silently passing as OK, so a
     * malformed cassette fails loudly instead of replaying a false green.
     */
    private function xrrStatus(?RecordedErrorException $recorded = null): \stdClass
    {
        $payload = $this->rp->respPayload();
        $raw     = $payload['status_code'] ?? null;
        $code    = is_int($raw) || is_float($raw) || (is_string($raw) && is_numeric($raw))
            ? (int) $raw
            : 2; // UNKNOWN

        $details = '';
        if ($recorded !== null) {
            $details = $recorded->getMessage();
            // Unwrap the standard client rendering so the reconstructed
            // status carries the description, not a nested prefix.
            if (preg_match('/^rpc error: code = \S+ desc = (.*)$/s', $details, $m) === 1) {
                $details = $m[1];
            }
        }

        $status                    = new \stdClass();
        $status->code              = $code;
        $status->details           = $details;
        $status->metadata          = [];

        return $status;
    }
}
