<?php

declare(strict_types=1);

namespace HopTop\Xrr\Tests;

use HopTop\Xrr\Exception\CassetteMissException;
use HopTop\Xrr\Exception\ShapeMismatchException;
use HopTop\Xrr\FileCassette;
use HopTop\Xrr\Stream\Frame;
use HopTop\Xrr\Stream\ReqStream;
use HopTop\Xrr\Stream\RespStream;
use HopTop\Xrr\Stream\StreamedInteraction;
use HopTop\Xrr\Stream\StreamEvent;
use HopTop\Xrr\Stream\StreamType;
use PHPUnit\Framework\TestCase;

class StreamCassetteTest extends TestCase
{
    private function tempDir(): string
    {
        $dir = sys_get_temp_dir() . '/xrr_' . uniqid();
        mkdir($dir);

        return $dir;
    }

    private function bidiInteraction(): StreamedInteraction
    {
        return new StreamedInteraction(
            adapter: 'grpc',
            fingerprint: 'c6233d2e',
            req: new ReqStream(
                StreamType::Bidi,
                [new Frame(0, 'ping-1', 0), new Frame(2, 'ping-2', 40)],
                new StreamEvent(4, 45)
            ),
            resp: new RespStream(
                [new Frame(1, 'pong-1', 3), new Frame(3, 'pong-2', 44)],
                new StreamEvent(5, 47)
            ),
            reqPayload: ['service' => 'chat.ChatService', 'method' => 'Converse', 'n' => 0],
            respPayload: ['status_code' => 0],
            reqRecordedAt: '2026-08-23T12:00:00Z',
            respRecordedAt: '2026-08-23T12:00:00Z'
        );
    }

    public function testSaveLoadStreamedRoundTrip(): void
    {
        $dir = $this->tempDir();
        $c   = new FileCassette($dir);

        $c->saveStreamed($this->bidiInteraction());

        $this->assertFileExists($dir . '/grpc-c6233d2e.req.yaml');
        $this->assertFileExists($dir . '/grpc-c6233d2e.resp.yaml');

        $pair = $c->loadStreamed('grpc', 'c6233d2e');
        $this->assertSame(StreamType::Bidi, $pair->req->type);
        $this->assertSame(['ping-1', 'ping-2'], array_map(fn($f) => $f->bytes, $pair->req->frames));
        $this->assertSame(['pong-1', 'pong-2'], array_map(fn($f) => $f->bytes, $pair->resp->frames));
        $this->assertNotNull($pair->req->halfClose);
        $this->assertSame(4, $pair->req->halfClose->seq);
        $this->assertSame(5, $pair->resp->end->seq);
        $this->assertNull($pair->error);
    }

    public function testLoadStreamedMissThrows(): void
    {
        $c = new FileCassette($this->tempDir());

        $this->expectException(CassetteMissException::class);
        $c->loadStreamed('grpc', 'deadbeef');
    }

    public function testUnaryLoadOfStreamedCassetteIsShapeMismatch(): void
    {
        $c = new FileCassette($this->tempDir());
        $c->saveStreamed($this->bidiInteraction());

        $this->expectException(ShapeMismatchException::class);
        $c->load('grpc', 'c6233d2e');
    }

    public function testStreamedLoadOfUnaryCassetteIsShapeMismatch(): void
    {
        $c = new FileCassette($this->tempDir());
        $c->save('exec', 'a3f9c1b2', ['argv' => ['echo']], ['stdout' => 'ok']);

        $this->expectException(ShapeMismatchException::class);
        $c->loadStreamed('exec', 'a3f9c1b2');
    }

    public function testUnaryLoadOfUnaryCassetteUnchanged(): void
    {
        $c = new FileCassette($this->tempDir());
        $c->save('exec', 'a3f9c1b2', ['argv' => ['echo']], ['stdout' => 'ok']);

        $data = $c->load('exec', 'a3f9c1b2');
        $this->assertEquals(['argv' => ['echo']], $data['req']);
        $this->assertEquals(['stdout' => 'ok'], $data['resp']);
    }
}
