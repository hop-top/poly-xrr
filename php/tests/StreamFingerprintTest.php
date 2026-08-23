<?php

declare(strict_types=1);

namespace HopTop\Xrr\Tests;

use HopTop\Xrr\FileCassette;
use HopTop\Xrr\Mode;
use HopTop\Xrr\Session;
use HopTop\Xrr\Stream\OccurrenceCounter;
use HopTop\Xrr\Stream\StreamFingerprint;
use HopTop\Xrr\Stream\StreamType;
use PHPUnit\Framework\TestCase;

/**
 * Byte-for-byte reproduction of the spec's embedded test vectors
 * (cassette-format-streaming.md, "Fingerprint Algorithms").
 */
class StreamFingerprintTest extends TestCase
{
    public function testMsgHashEtcHosts(): void
    {
        $this->assertSame('f1e315a5', StreamFingerprint::msgHash('{"path":"/etc/hosts"}'));
    }

    public function testMsgHashBigLog(): void
    {
        $this->assertSame('164658bd', StreamFingerprint::msgHash('{"path":"/var/log/big.log"}'));
    }

    public function testServerFingerprintEtcHosts(): void
    {
        $this->assertSame(
            '58a4bf3f',
            StreamFingerprint::server('files.FileService', 'Download', '{"path":"/etc/hosts"}')
        );
    }

    public function testServerFingerprintBigLog(): void
    {
        $this->assertSame(
            '9e8c4d4c',
            StreamFingerprint::server('files.FileService', 'Download', '{"path":"/var/log/big.log"}')
        );
    }

    public function testClientFingerprintN0(): void
    {
        $this->assertSame(
            '2bebfd6f',
            StreamFingerprint::client('files.FileService', 'Upload', 0)
        );
    }

    public function testBidiFingerprintN0(): void
    {
        $this->assertSame(
            'c6233d2e',
            StreamFingerprint::bidi('chat.ChatService', 'Converse', 0)
        );
    }

    public function testClientFingerprintN1(): void
    {
        // grpc-client-stream-repeat fixture: second open of the same tuple.
        $this->assertSame(
            'b27b5fe1',
            StreamFingerprint::client('files.FileService', 'Upload', 1)
        );
    }

    public function testOccurrenceCounterIncrementsPerTuple(): void
    {
        $counter = new OccurrenceCounter();

        $this->assertSame(0, $counter->next('files.FileService', 'Upload', StreamType::Client));
        $this->assertSame(1, $counter->next('files.FileService', 'Upload', StreamType::Client));
        $this->assertSame(2, $counter->next('files.FileService', 'Upload', StreamType::Client));
    }

    public function testOccurrenceCounterTuplesAreIndependent(): void
    {
        $counter = new OccurrenceCounter();

        $this->assertSame(0, $counter->next('files.FileService', 'Upload', StreamType::Client));
        $this->assertSame(0, $counter->next('files.FileService', 'Upload', StreamType::Bidi));
        $this->assertSame(0, $counter->next('chat.ChatService', 'Upload', StreamType::Client));
        $this->assertSame(1, $counter->next('files.FileService', 'Upload', StreamType::Client));
    }

    public function testSessionIsOneCounterDomain(): void
    {
        $dir = sys_get_temp_dir() . '/xrr_' . uniqid();
        mkdir($dir);

        $sessionA = new Session(Mode::Replay, new FileCassette($dir));
        $sessionB = new Session(Mode::Replay, new FileCassette($dir));

        $this->assertSame(
            $sessionA->streamOccurrences(),
            $sessionA->streamOccurrences(),
            'one session object must expose one counter domain'
        );

        $sessionA->streamOccurrences()->next('files.FileService', 'Upload', StreamType::Client);
        $this->assertSame(
            0,
            $sessionB->streamOccurrences()->next('files.FileService', 'Upload', StreamType::Client),
            'distinct session objects must own independent counter domains'
        );
    }
}
