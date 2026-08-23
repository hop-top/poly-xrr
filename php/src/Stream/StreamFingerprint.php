<?php

declare(strict_types=1);

namespace HopTop\Xrr\Stream;

/**
 * Streaming fingerprint algorithms (cassette-format-streaming.md).
 *
 * All variants are `sha256(canonical_json)[:8]`: 8 lowercase hex chars of
 * the sha256 of deterministic JSON with lexicographically sorted keys and
 * no insignificant whitespace. The canonical strings are constructed
 * explicitly, key by key in sorted order, for byte-for-byte control —
 * service/method names are proto identifiers (`[A-Za-z0-9_.]`), so JSON
 * string escaping never varies between ports.
 *
 * Every input set includes a `stream` discriminator, keeping streaming
 * canonical inputs disjoint from unary ones by construction.
 */
class StreamFingerprint
{
    private const JSON_FLAGS = JSON_UNESCAPED_SLASHES | JSON_UNESCAPED_UNICODE | JSON_THROW_ON_ERROR;

    /** v1 building block: `sha256(message_bytes)[:8]`. */
    public static function msgHash(string $messageBytes): string
    {
        return substr(hash('sha256', $messageBytes), 0, 8);
    }

    /**
     * Server-streaming: the single request message is available at open.
     * `{"method":M,"msg_hash":H,"service":S,"stream":"server"}`
     */
    public static function server(string $service, string $method, string $messageBytes): string
    {
        $canonical = sprintf(
            '{"method":%s,"msg_hash":%s,"service":%s,"stream":"server"}',
            self::jsonString($method),
            self::jsonString(self::msgHash($messageBytes)),
            self::jsonString($service)
        );

        return self::truncate($canonical);
    }

    /**
     * Client-streaming: no message at open; `n` is the 0-based occurrence
     * ordinal. `{"method":M,"n":N,"service":S,"stream":"client"}`
     */
    public static function client(string $service, string $method, int $n): string
    {
        return self::counted($service, $method, $n, 'client');
    }

    /**
     * Bidi: no message at open; `n` is the 0-based occurrence ordinal.
     * `{"method":M,"n":N,"service":S,"stream":"bidi"}`
     */
    public static function bidi(string $service, string $method, int $n): string
    {
        return self::counted($service, $method, $n, 'bidi');
    }

    private static function counted(string $service, string $method, int $n, string $stream): string
    {
        $canonical = sprintf(
            '{"method":%s,"n":%d,"service":%s,"stream":"%s"}',
            self::jsonString($method),
            $n,
            self::jsonString($service),
            $stream
        );

        return self::truncate($canonical);
    }

    private static function truncate(string $canonical): string
    {
        return substr(hash('sha256', $canonical), 0, 8);
    }

    private static function jsonString(string $s): string
    {
        return json_encode($s, self::JSON_FLAGS);
    }
}
