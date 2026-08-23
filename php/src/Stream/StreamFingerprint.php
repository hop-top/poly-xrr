<?php

declare(strict_types=1);

namespace HopTop\Xrr\Stream;

/**
 * Streaming fingerprint core (cassette-format-streaming.md).
 *
 * All fingerprints are `sha256(canonical_json)[:8]`: 8 lowercase hex chars
 * of the sha256 of deterministic JSON with lexicographically sorted keys
 * and no insignificant whitespace. The canonical string is constructed
 * explicitly, key by key in sorted order, for byte-for-byte control —
 * identity values are proto identifiers or URLs in practice, whose JSON
 * string escaping never varies between ports.
 *
 * The split is structural: this core owns canonical-JSON assembly, the
 * `stream` discriminator (keeping streaming canonical inputs disjoint from
 * unary ones by construction), hashing/truncation, and — via the session —
 * the occurrence-counter lifecycle. An adapter supplies only its canonical
 * identity fields through {@see StreamOpen}. The gRPC-shaped helpers
 * (server/client/bidi) are conveniences built on the same seam.
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
     * Computes the streaming fingerprint for an open:
     * `sha256(canonical(identity + "stream" discriminator [+ "n"]))[:8]`.
     * Counter-addressed opens require the 0-based occurrence ordinal
     * `$n >= 0`; `$n` is ignored otherwise (content-addressed identities,
     * e.g. gRPC server streams, carry their content hash in the identity).
     */
    public static function compute(StreamOpen $open, int $n = -1): string
    {
        if ($open->counter) {
            if ($n < 0) {
                throw new \InvalidArgumentException(
                    sprintf('xrr: stream occurrence n must be >= 0, got %d', $n)
                );
            }
        } else {
            $n = -1;
        }

        return substr(hash('sha256', self::canonical($open, $n)), 0, 8);
    }

    /**
     * Assembles the spec's canonical JSON for an open: the adapter identity
     * plus the injected `stream` discriminator, plus `n` when `$n >= 0`.
     * Keys sorted lexicographically (byte order), no insignificant
     * whitespace.
     */
    public static function canonical(StreamOpen $open, int $n = -1): string
    {
        $inputs = [];
        foreach ($open->identity as $key => $value) {
            $key = (string) $key;
            if ($key === 'stream' || $key === 'n') {
                throw new \InvalidArgumentException(
                    sprintf('xrr: stream identity key "%s" is reserved for core injection', $key)
                );
            }
            $inputs[$key] = $value;
        }
        $inputs['stream'] = $open->type->value;
        if ($n >= 0) {
            $inputs['n'] = $n;
        }
        ksort($inputs, SORT_STRING);

        $parts = [];
        foreach ($inputs as $key => $value) {
            $parts[] = self::jsonString((string) $key) . ':'
                . (is_int($value) ? (string) $value : self::jsonString($value));
        }

        return '{' . implode(',', $parts) . '}';
    }

    /**
     * gRPC server-streaming: the single request message is available at
     * open. `{"method":M,"msg_hash":H,"service":S,"stream":"server"}`
     */
    public static function server(string $service, string $method, string $messageBytes): string
    {
        return self::compute(new StreamOpen('grpc', StreamType::Server, [
            'service'  => $service,
            'method'   => $method,
            'msg_hash' => self::msgHash($messageBytes),
        ]));
    }

    /**
     * gRPC client-streaming: no message at open; `n` is the 0-based
     * occurrence ordinal. `{"method":M,"n":N,"service":S,"stream":"client"}`
     */
    public static function client(string $service, string $method, int $n): string
    {
        return self::counted($service, $method, $n, StreamType::Client);
    }

    /**
     * gRPC bidi: no message at open; `n` is the 0-based occurrence ordinal.
     * `{"method":M,"n":N,"service":S,"stream":"bidi"}`
     */
    public static function bidi(string $service, string $method, int $n): string
    {
        return self::counted($service, $method, $n, StreamType::Bidi);
    }

    private static function counted(string $service, string $method, int $n, StreamType $type): string
    {
        return self::compute(
            new StreamOpen('grpc', $type, ['service' => $service, 'method' => $method], counter: true),
            $n
        );
    }

    private static function jsonString(string $s): string
    {
        return json_encode($s, self::JSON_FLAGS);
    }
}
