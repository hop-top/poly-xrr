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
}
