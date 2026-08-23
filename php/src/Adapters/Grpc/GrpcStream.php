<?php

declare(strict_types=1);

namespace HopTop\Xrr\Adapters\Grpc;

use HopTop\Xrr\Session;
use HopTop\Xrr\Stream\StreamFingerprint;
use HopTop\Xrr\Stream\StreamOpen;
use HopTop\Xrr\Stream\StreamType;

/**
 * Shared gRPC streaming vocabulary: the adapter id, the spec's stream-open
 * construction, and full-method parsing.
 *
 * See spec/cassette-format-streaming.md (gRPC Adapter Mapping) for the
 * normative semantics this implements. The core owns canonical-JSON
 * assembly, the "stream" discriminator, hashing/truncation and the
 * occurrence-counter lifecycle; this class supplies only the gRPC identity
 * and payload shapes.
 */
final class GrpcStream
{
    public const ADAPTER_ID = 'grpc';

    /**
     * Builds the core open value for a streamed gRPC RPC.
     *
     * Identity and req payload are both {service, method}. Server streams
     * are content-addressed by `msg_hash` = sha256(message_bytes)[:8] over
     * the single request message; client and bidi opens carry no message at
     * open and are counter-addressed, so the core injects `n`.
     *
     * The msg_hash is the one identity input derived from message bytes and
     * is computed here, before the core's frame seam — so it derives from
     * the session-scrubbed bytes. Record and replay both pass through this
     * path, which keeps a scrubbed recording and a scrubbed replay of the
     * same live traffic addressing the same cassette. The raw message is
     * handed to the core untouched: the core scrubs frames exactly once.
     */
    public static function open(
        Session $session,
        StreamType $type,
        string $service,
        string $method,
        ?string $message = null
    ): StreamOpen {
        $identity = ['service' => $service, 'method' => $method];
        $counter  = true;

        if ($type === StreamType::Server) {
            $scrubbed = $session->scrubStreamFrame(
                StreamDirection::Send,
                self::ADAPTER_ID,
                $type,
                $message ?? ''
            );
            $identity['msg_hash'] = StreamFingerprint::msgHash($scrubbed);
            $counter              = false;
        }

        return new StreamOpen(
            adapterID: self::ADAPTER_ID,
            type: $type,
            identity: $identity,
            counter: $counter,
            payload: ['service' => $service, 'method' => $method]
        );
    }

    /**
     * Splits a gRPC full method ("/pkg.Service/Method") into its service and
     * method identifiers.
     *
     * @return array{string, string} [service, method]
     * @throws \InvalidArgumentException on a malformed full method
     */
    public static function splitFullMethod(string $full): array
    {
        $s = ltrim($full, '/');
        $i = strrpos($s, '/');
        if ($i === false || $i === 0 || $i === strlen($s) - 1) {
            throw new \InvalidArgumentException(sprintf(
                'xrr: malformed gRPC full method "%s" (want /service/method)',
                $full
            ));
        }

        return [substr($s, 0, $i), substr($s, $i + 1)];
    }
}
