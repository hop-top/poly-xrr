<?php

declare(strict_types=1);

namespace HopTop\Xrr;

/**
 * Canonical JSON shared by every fingerprint (cassette-format-v1.md,
 * "Fingerprint Algorithm"): lexicographically sorted keys, no insignificant
 * whitespace and RFC 8785 §3.2.2.2 string escaping — only `"`, `\` and
 * U+0000–U+001F are escaped; `/`, non-ASCII, U+2028/U+2029 and U+007F are
 * emitted raw. json_encode needs three flags to get there:
 * JSON_UNESCAPED_SLASHES, JSON_UNESCAPED_UNICODE and
 * JSON_UNESCAPED_LINE_TERMINATORS (PHP escapes U+2028/U+2029 even under
 * JSON_UNESCAPED_UNICODE). Anything else forks fingerprints from the other
 * ports.
 */
final class CanonicalJson
{
    public const FLAGS = JSON_UNESCAPED_SLASHES
        | JSON_UNESCAPED_UNICODE
        | JSON_UNESCAPED_LINE_TERMINATORS
        | JSON_THROW_ON_ERROR;

    /** Callers ksort() associative arrays first; scalars encode as-is. */
    public static function encode(mixed $value): string
    {
        return json_encode($value, self::FLAGS);
    }

    /** v1 fingerprint: `sha256(canonical)[:8]`, lowercase hex. */
    public static function fingerprint(mixed $value): string
    {
        return substr(hash('sha256', self::encode($value)), 0, 8);
    }
}
