<?php

declare(strict_types=1);

namespace HopTop\Xrr\Tests;

use HopTop\Xrr\Adapters\FsAdapter;
use PHPUnit\Framework\TestCase;

class FsAdapterTest extends TestCase
{
    public function testGetId(): void
    {
        $a = new FsAdapter();
        $this->assertSame('fs', $a->getId());
    }

    public function testFingerprintDeterministic(): void
    {
        $a   = new FsAdapter();
        $req = ['op' => 'write', 'path' => '/etc/hosts', 'data' => "127.0.0.1 localhost\n"];
        $fp1 = $a->fingerprint($req);
        $fp2 = $a->fingerprint($req);
        $this->assertSame(8, strlen($fp1), 'fingerprint must be 8 hex chars');
        $this->assertSame($fp1, $fp2, 'same request must hash identically');
    }

    public function testFingerprintDiscriminatesOp(): void
    {
        $a   = new FsAdapter();
        $fpW = $a->fingerprint(['op' => 'write',  'path' => '/x']);
        $fpR = $a->fingerprint(['op' => 'remove', 'path' => '/x']);
        $this->assertNotSame($fpW, $fpR);
    }

    public function testFingerprintDiscriminatesPath(): void
    {
        $a   = new FsAdapter();
        $fpA = $a->fingerprint(['op' => 'write', 'path' => '/a', 'data' => 'x']);
        $fpB = $a->fingerprint(['op' => 'write', 'path' => '/b', 'data' => 'x']);
        $this->assertNotSame($fpA, $fpB);
    }

    public function testFingerprintDiscriminatesData(): void
    {
        $a   = new FsAdapter();
        $fpA = $a->fingerprint(['op' => 'write', 'path' => '/x', 'data' => 'foo']);
        $fpB = $a->fingerprint(['op' => 'write', 'path' => '/x', 'data' => 'bar']);
        $this->assertNotSame($fpA, $fpB);
    }

    public function testFingerprintDiscriminatesMode(): void
    {
        $a   = new FsAdapter();
        $fpA = $a->fingerprint(['op' => 'write', 'path' => '/x', 'mode' => 420]);
        $fpB = $a->fingerprint(['op' => 'write', 'path' => '/x', 'mode' => 384]);
        $this->assertNotSame($fpA, $fpB);
    }

    public function testFingerprintOmitsUnsetFields(): void
    {
        $a    = new FsAdapter();
        $bare = ['op' => 'write', 'path' => '/x', 'data' => 'y'];
        $with = ['op' => 'write', 'path' => '/x', 'data' => 'y']; // no mode set
        $this->assertSame($a->fingerprint($bare), $a->fingerprint($with));
    }

    /**
     * Conformance: cross-runtime fingerprint MUST equal "667a7680" for the
     * canonical fs-write fixture. Locks the canonical-JSON contract with
     * the Go, TypeScript, Python, and Rust ports.
     */
    public function testConformanceFingerprintMatchesFixture(): void
    {
        $a   = new FsAdapter();
        $req = [
            'op'   => 'write',
            'path' => '$TMP/greeting.txt',
            'data' => "hello, world\n",
            'mode' => 420,
        ];
        $this->assertSame('667a7680', $a->fingerprint($req));
    }

    public function testSerializeReqRoundTrip(): void
    {
        $a   = new FsAdapter();
        $req = ['op' => 'write', 'path' => '/x', 'data' => 'hi', 'mode' => 420];
        $ser = $a->serializeReq($req);
        $this->assertSame($req, $a->deserializeReq($ser));
    }

    public function testSerializeRespRoundTrip(): void
    {
        $a    = new FsAdapter();
        $resp = ['duration_ms' => 1, 'bytes_written' => 13];
        $ser  = $a->serializeResp($resp);
        $this->assertSame($resp, $a->deserializeResp($ser));
    }
    // --- Path normalizer hook ---------------------------------------------

    /** @return callable(string): string */
    private static function stripRoot(string $root): callable
    {
        return static fn (string $p): string => str_replace($root, '$TMP', $p);
    }

    public function testDefaultNormalizerIsIdentity(): void
    {
        $a = new FsAdapter();
        $this->assertSame('/var/tmp/x', $a->normalize('/var/tmp/x'));
        $this->assertSame('', $a->normalize(''));
    }

    public function testConstructorAcceptsNormalizer(): void
    {
        $a = new FsAdapter(self::stripRoot('/var/tmp'));
        $this->assertSame('$TMP/x', $a->normalize('/var/tmp/x'));
    }

    public function testWithNormalizerReturnsCopyAndLeavesOriginalUntouched(): void
    {
        $plain = new FsAdapter();
        $norm  = $plain->withNormalizer(self::stripRoot('/var/tmp'));

        $this->assertNotSame($plain, $norm);
        $this->assertInstanceOf(FsAdapter::class, $norm);
        $this->assertSame('/var/tmp/x', $plain->normalize('/var/tmp/x'), 'original stays identity');
        $this->assertSame('$TMP/x', $norm->normalize('/var/tmp/x'));
    }

    public function testNormalizerAppliedToFingerprint(): void
    {
        $plain = new FsAdapter();
        $norm  = $plain->withNormalizer(self::stripRoot('/var/tmp'));

        $raw    = ['op' => 'write', 'path' => '/var/tmp/foo', 'data' => 'x'];
        $stored = ['op' => 'write', 'path' => '$TMP/foo',     'data' => 'x'];

        $this->assertSame($norm->fingerprint($stored), $norm->fingerprint($raw),
            'raw path must hash to the same fingerprint as its normalized form');
        $this->assertNotSame($plain->fingerprint($raw), $norm->fingerprint($raw),
            'plain adapter and normalizing adapter must differ on raw path input');
    }

    public function testTwoTempRootsFingerprintIdenticallyAndMatchConformancePin(): void
    {
        $fixture = ['data' => "hello, world\n", 'mode' => 420];
        $a = (new FsAdapter())->withNormalizer(self::stripRoot('/var/folders/ab/T/TestA1'));
        $b = (new FsAdapter())->withNormalizer(self::stripRoot('/private/tmp/TestB2'));

        $fpA = $a->fingerprint(['op' => 'write', 'path' => '/var/folders/ab/T/TestA1/greeting.txt'] + $fixture);
        $fpB = $b->fingerprint(['op' => 'write', 'path' => '/private/tmp/TestB2/greeting.txt'] + $fixture);

        $this->assertSame($fpA, $fpB, 'different tmp roots must collapse to one cassette key');
        $this->assertSame('667a7680', $fpA, 'normalized path must reproduce the cross-runtime pin');
    }

    public function testConformancePinUnchangedWithExplicitIdentityNormalizer(): void
    {
        $a   = (new FsAdapter())->withNormalizer(static fn (string $p): string => $p);
        $req = [
            'op'   => 'write',
            'path' => '$TMP/greeting.txt',
            'data' => "hello, world\n",
            'mode' => 420,
        ];
        $this->assertSame('667a7680', $a->fingerprint($req));
    }

    /** @return iterable<string, array{string}> */
    public static function destOps(): iterable
    {
        yield 'rename'   => [FsAdapter::OP_RENAME];
        yield 'symlink'  => [FsAdapter::OP_SYMLINK];
        yield 'hardlink' => [FsAdapter::OP_HARDLINK];
    }

    #[\PHPUnit\Framework\Attributes\DataProvider('destOps')]
    public function testNormalizerAppliedToDestInFingerprint(string $op): void
    {
        $norm = (new FsAdapter())->withNormalizer(self::stripRoot('/var/tmp'));

        $raw    = ['op' => $op, 'path' => '/var/tmp/a', 'dest' => '/var/tmp/b'];
        $stored = ['op' => $op, 'path' => '$TMP/a',     'dest' => '$TMP/b'];
        $half   = ['op' => $op, 'path' => '$TMP/a',     'dest' => '/var/tmp/b'];

        $this->assertSame($norm->fingerprint($stored), $norm->fingerprint($raw));
        $this->assertSame($norm->fingerprint($stored), $norm->fingerprint($half),
            'dest must be normalized independently of path');
        $this->assertNotSame((new FsAdapter())->fingerprint($raw), $norm->fingerprint($raw));
    }

    #[\PHPUnit\Framework\Attributes\DataProvider('destOps')]
    public function testSerializeReqStoresPostNormalizerPathAndDest(string $op): void
    {
        $norm = (new FsAdapter())->withNormalizer(self::stripRoot('/var/tmp'));
        $raw  = ['op' => $op, 'path' => '/var/tmp/a', 'dest' => '/var/tmp/b', 'mode' => 420];

        $ser = $norm->serializeReq($raw);

        $this->assertSame('$TMP/a', $ser['path']);
        $this->assertSame('$TMP/b', $ser['dest']);
        $this->assertSame($op, $ser['op']);
        $this->assertSame(420, $ser['mode'], 'non-path fields pass through verbatim');
        $this->assertSame($norm->fingerprint($raw), $norm->fingerprint($ser),
            'what gets stored must hash to what was recorded');
    }

    public function testSerializeReqDoesNotNormalizeData(): void
    {
        $norm = (new FsAdapter())->withNormalizer(self::stripRoot('/var/tmp'));
        $raw  = ['op' => 'write', 'path' => '/var/tmp/a', 'data' => 'see /var/tmp/a'];

        $ser = $norm->serializeReq($raw);

        $this->assertSame('$TMP/a', $ser['path']);
        $this->assertSame('see /var/tmp/a', $ser['data'], 'normalizer applies to path/dest only');
    }

    public function testSerializeReqIdentityLeavesRequestVerbatim(): void
    {
        $a   = new FsAdapter();
        $req = ['op' => 'rename', 'path' => '/x', 'dest' => '/y', 'extra' => 1];
        $this->assertSame($req, $a->serializeReq($req));
    }

    public function testEmptyPathShortCircuitsWithoutInvokingNormalizer(): void
    {
        $calls = 0;
        $a = (new FsAdapter())->withNormalizer(static function (string $p) use (&$calls): string {
            $calls++;
            return 'NEVER';
        });

        $this->assertSame('', $a->normalize(''));
        $a->fingerprint(['op' => 'chmod', 'path' => '', 'mode' => 420]);
        $ser = $a->serializeReq(['op' => 'chmod', 'path' => '', 'mode' => 420]);

        $this->assertSame(0, $calls, 'empty path must not invoke normalizer');
        $this->assertSame('', $ser['path']);
        $this->assertArrayNotHasKey('dest', $ser);
    }

    public function testDestGatedOnNormalizedValue(): void
    {
        // spec: dest participates only when non-empty AFTER normalization.
        $a      = (new FsAdapter())->withNormalizer(static fn (string $p): string => $p === '/x/drop' ? '' : $p);
        $noDest = ['op' => 'rename', 'path' => '/a'];

        $this->assertSame($a->fingerprint($noDest), $a->fingerprint($noDest + ['dest' => '/x/drop']),
            'dest normalized to "" must drop out of the fingerprint');
        $this->assertNotSame($a->fingerprint($noDest), $a->fingerprint($noDest + ['dest' => '/x/keep']));
    }

    public function testEmptyDestStaysOmittedRegardlessOfNormalizer(): void
    {
        $a      = (new FsAdapter())->withNormalizer(static fn (string $p): string => $p === '' ? '/ghost' : $p);
        $noDest = ['op' => 'rename', 'path' => '/a'];

        $this->assertSame($a->fingerprint($noDest), $a->fingerprint($noDest + ['dest' => '']));
    }

    public function testChainComposesLeftToRight(): void
    {
        $first  = static fn (string $p): string => $p . '-1';
        $second = static fn (string $p): string => $p . '-2';

        $this->assertSame('x-1-2', FsAdapter::chain($first, $second)('x'));
        $this->assertSame('x-2-1', FsAdapter::chain($second, $first)('x'));
        $this->assertSame('x', FsAdapter::chain()('x'), 'empty chain is identity');
    }

    public function testChainedNormalizerInFingerprint(): void
    {
        $tmpNorm  = static fn (string $p): string => str_replace('/tmp', '$TMP', $p);
        $homeNorm = static fn (string $p): string => str_replace('/home/u', '$HOME', $p);
        $a = (new FsAdapter())->withNormalizer(FsAdapter::chain($tmpNorm, $homeNorm));

        $this->assertSame(
            $a->fingerprint(['op' => 'write', 'path' => '$TMP/foo', 'data' => 'x']),
            $a->fingerprint(['op' => 'write', 'path' => '/tmp/foo', 'data' => 'x']),
        );
        $this->assertSame(
            $a->fingerprint(['op' => 'rename', 'path' => '$TMP/foo', 'dest' => '$HOME/bar']),
            $a->fingerprint(['op' => 'rename', 'path' => '/tmp/foo', 'dest' => '/home/u/bar']),
        );
    }
}
