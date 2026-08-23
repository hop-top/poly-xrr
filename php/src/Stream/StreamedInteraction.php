<?php

declare(strict_types=1);

namespace HopTop\Xrr\Stream;

/**
 * A fully parsed streamed cassette pair: v1 envelope fields plus the
 * req/resp stream halves. `$error` mirrors the v1 resp envelope field —
 * non-null ⇔ the stream terminated with an error.
 */
class StreamedInteraction
{
    /**
     * @param array<string, mixed> $reqPayload
     * @param array<string, mixed> $respPayload
     */
    public function __construct(
        public readonly string $adapter,
        public readonly string $fingerprint,
        public readonly ReqStream $req,
        public readonly RespStream $resp,
        public readonly array $reqPayload,
        public readonly array $respPayload,
        public readonly ?string $error = null,
        public readonly string $reqRecordedAt = '',
        public readonly string $respRecordedAt = ''
    ) {}
}
