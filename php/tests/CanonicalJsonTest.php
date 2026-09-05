<?php

declare(strict_types=1);

namespace HopTop\Xrr\Tests;

use HopTop\Xrr\Adapters\ExecAdapter;
use HopTop\Xrr\Adapters\FsAdapter;
use HopTop\Xrr\CanonicalJson;
use HopTop\Xrr\Stream\StreamFingerprint;
use HopTop\Xrr\Stream\StreamOpen;
use HopTop\Xrr\Stream\StreamType;
use PHPUnit\Framework\TestCase;

/**
 * Cross-port canonical-JSON escaping vectors (spec: Fingerprint Algorithm).
 *
 * The hazard input covers every string-escaping class that has forked
 * fingerprints across ports: HTML-sensitive & < >, a slash, non-ASCII,
 * U+2028/U+2029, the backspace and form-feed short forms, a control byte
 * (U+001F) and DEL.
 */
final class CanonicalJsonTest extends TestCase
{
    /** a&b<c>/é U+2028 U+2029 backspace form-feed U+001F U+007F */
    private const HAZARD = "a&b<c>/\xC3\xA9\xE2\x80\xA8\xE2\x80\xA9\x08\x0C\x1F\x7F";

    /** {"k":"a&b<c>/é<U+2028><U+2029>\b\f<U+001F escaped><DEL>","stream":"server"} */
    private const STREAM_CANONICAL_HEX = '7b226b223a226126623c633e2fc3a9e280a8e280a95c625c665c75303031667f22'
        . '2c2273747265616d223a22736572766572227d';

    public function testCanonicalJsonHazardVector(): void
    {
        $inputs = ['k' => self::HAZARD, 'stream' => 'server'];
        self::assertSame(self::STREAM_CANONICAL_HEX, bin2hex(CanonicalJson::encode($inputs)));
        self::assertSame('bcc2c6c3', CanonicalJson::fingerprint($inputs));
    }

    public function testEncodeKeepsSlashUnicodeAndLineTerminatorsRaw(): void
    {
        self::assertSame("\"a&b<c>/\xC3\xA9\x7F\"", CanonicalJson::encode("a&b<c>/\xC3\xA9\x7F"));
        self::assertSame("\"\xE2\x80\xA8\"", CanonicalJson::encode("\xE2\x80\xA8"));
    }

    public function testStreamFingerprintHazardVector(): void
    {
        $open = new StreamOpen('x', StreamType::Server, ['k' => self::HAZARD]);
        self::assertSame('bcc2c6c3', StreamFingerprint::compute($open));
    }

    public function testFsFingerprintHazardVector(): void
    {
        self::assertSame('6f2fb087', (new FsAdapter())->fingerprint(['op' => 'write', 'path' => self::HAZARD]));
    }

    public function testExecFingerprintHazardVector(): void
    {
        self::assertSame('97618387', (new ExecAdapter())->fingerprint(['argv' => ['echo', self::HAZARD]]));
    }
}
