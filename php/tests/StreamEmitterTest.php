<?php

declare(strict_types=1);

namespace HopTop\Xrr\Tests;

use HopTop\Xrr\Stream\Frame;
use HopTop\Xrr\Stream\ReqStream;
use HopTop\Xrr\Stream\RespStream;
use HopTop\Xrr\Stream\StreamedInteraction;
use HopTop\Xrr\Stream\StreamEmitter;
use HopTop\Xrr\Stream\StreamEvent;
use HopTop\Xrr\Stream\StreamParser;
use HopTop\Xrr\Stream\StreamType;
use PHPUnit\Framework\TestCase;
use Symfony\Component\Yaml\Yaml;

class StreamEmitterTest extends TestCase
{
    private function serverInteraction(): StreamedInteraction
    {
        return new StreamedInteraction(
            adapter: 'grpc',
            fingerprint: '58a4bf3f',
            req: new ReqStream(
                StreamType::Server,
                [new Frame(0, '{"path":"/etc/hosts"}', 0)],
                new StreamEvent(1, 0)
            ),
            resp: new RespStream(
                [
                    new Frame(2, "chunk-one\n", 12),
                    new Frame(3, "chunk-two\n", 15),
                ],
                new StreamEvent(4, 21)
            ),
            reqPayload: ['service' => 'files.FileService', 'method' => 'Download'],
            respPayload: ['status_code' => 0],
            reqRecordedAt: '2026-08-23T12:00:00Z',
            respRecordedAt: '2026-08-23T12:00:00Z'
        );
    }

    public function testFingerprintQuotedOnBothFiles(): void
    {
        $emitter = new StreamEmitter();
        $i       = $this->serverInteraction();

        $this->assertStringContainsString('fingerprint: "58a4bf3f"', $emitter->emitReq($i));
        $this->assertStringContainsString('fingerprint: "58a4bf3f"', $emitter->emitResp($i));
    }

    public function testBase64EmittedWithoutWhitespace(): void
    {
        $i = new StreamedInteraction(
            adapter: 'grpc',
            fingerprint: 'aaaaaaaa',
            req: new ReqStream(
                StreamType::Server,
                [new Frame(0, str_repeat('x', 300), 0)],
                new StreamEvent(1, 0)
            ),
            resp: new RespStream([], new StreamEvent(2, 1)),
            reqPayload: ['service' => 's.S', 'method' => 'M'],
            respPayload: ['status_code' => 0],
            reqRecordedAt: '2026-08-23T12:00:00Z',
            respRecordedAt: '2026-08-23T12:00:00Z'
        );

        $out = (new StreamEmitter())->emitReq($i);
        $this->assertSame(1, preg_match('/message_b64: "([^"]*)"/', $out, $m));
        $this->assertSame(1, preg_match('{\A[A-Za-z0-9+/=]+\z}', $m[1]), 'b64 scalar must have no whitespace');
        $this->assertSame(str_repeat('x', 300), base64_decode($m[1], true));
    }

    public function testMessageTextAlwaysQuoted(): void
    {
        $i = new StreamedInteraction(
            adapter: 'sse',
            fingerprint: '66ecc77a',
            req: new ReqStream(StreamType::Server, []),
            resp: new RespStream(
                [
                    new Frame(0, 'on', 1, text: true),
                    new Frame(1, '12:30', 2, text: true),
                    new Frame(2, 'null', 3, text: true),
                    new Frame(3, ' leading', 4, text: true),
                    new Frame(4, 'hello', 5, text: true),
                ],
                new StreamEvent(5, 6)
            ),
            reqPayload: ['url' => 'https://example.test/events'],
            respPayload: [],
            reqRecordedAt: '2026-08-23T12:00:00Z',
            respRecordedAt: '2026-08-23T12:00:00Z'
        );

        $out = (new StreamEmitter())->emitResp($i);
        $this->assertStringContainsString('message_text: "on"', $out);
        $this->assertStringContainsString('message_text: "12:30"', $out);
        $this->assertStringContainsString('message_text: "null"', $out);
        $this->assertStringContainsString('message_text: " leading"', $out);
        // Even a harmless plain scalar must be quoted per the frame-schema rule.
        $this->assertStringContainsString('message_text: "hello"', $out);
    }

    public function testInvalidUtf8TextFallsBackToBase64(): void
    {
        $i = new StreamedInteraction(
            adapter: 'sse',
            fingerprint: 'bbbbbbbb',
            req: new ReqStream(StreamType::Server, []),
            resp: new RespStream(
                [new Frame(0, "\xFF\xFE", 1, text: true)],
                new StreamEvent(1, 2)
            ),
            reqPayload: ['url' => 'u'],
            respPayload: [],
            reqRecordedAt: '2026-08-23T12:00:00Z',
            respRecordedAt: '2026-08-23T12:00:00Z'
        );

        $out = (new StreamEmitter())->emitResp($i);
        $this->assertStringContainsString('message_b64: "//4="', $out);
        $this->assertStringNotContainsString('message_text', $out);
    }

    public function testEmptyFramesEmittedExplicitly(): void
    {
        $out = (new StreamEmitter())->emitReq(new StreamedInteraction(
            adapter: 'grpc',
            fingerprint: 'cccccccc',
            req: new ReqStream(StreamType::Bidi, [], new StreamEvent(0, 0)),
            resp: new RespStream([], new StreamEvent(1, 2)),
            reqPayload: ['service' => 'chat.ChatService', 'method' => 'Ping', 'n' => 0],
            respPayload: ['status_code' => 0],
            reqRecordedAt: '2026-08-23T12:00:00Z',
            respRecordedAt: '2026-08-23T12:00:00Z'
        ));

        $this->assertStringContainsString('frames: []', $out);
    }

    public function testErrorEmittedOnResp(): void
    {
        $i = new StreamedInteraction(
            adapter: 'grpc',
            fingerprint: '9e8c4d4c',
            req: new ReqStream(
                StreamType::Server,
                [new Frame(0, '{"path":"/var/log/big.log"}', 0)],
                new StreamEvent(1, 0)
            ),
            resp: new RespStream([], new StreamEvent(2, 30)),
            reqPayload: ['service' => 'files.FileService', 'method' => 'Download'],
            respPayload: ['status_code' => 14],
            error: 'rpc error: code = Unavailable desc = connection reset',
            reqRecordedAt: '2026-08-23T12:00:00Z',
            respRecordedAt: '2026-08-23T12:00:00Z'
        );

        $emitter = new StreamEmitter();
        $this->assertStringContainsString(
            'error: "rpc error: code = Unavailable desc = connection reset"',
            $emitter->emitResp($i)
        );
        $this->assertStringNotContainsString('error:', $emitter->emitReq($i));
    }

    public function testEmitParsesBackToEqualModel(): void
    {
        $emitter = new StreamEmitter();
        $i       = $this->serverInteraction();

        /** @var array<string, mixed> $req */
        $req = Yaml::parse($emitter->emitReq($i));
        /** @var array<string, mixed> $resp */
        $resp = Yaml::parse($emitter->emitResp($i));

        $pair = (new StreamParser())->parsePair($req, $resp);

        $this->assertSame('grpc', $pair->adapter);
        $this->assertSame('58a4bf3f', $pair->fingerprint);
        $this->assertSame('2026-08-23T12:00:00Z', $pair->reqRecordedAt);
        $this->assertSame(StreamType::Server, $pair->req->type);
        $this->assertSame('{"path":"/etc/hosts"}', $pair->req->frames[0]->bytes);
        $this->assertNotNull($pair->req->halfClose);
        $this->assertSame(1, $pair->req->halfClose->seq);
        $this->assertSame(["chunk-one\n", "chunk-two\n"], array_map(fn($f) => $f->bytes, $pair->resp->frames));
        $this->assertSame([12, 15], array_map(fn($f) => $f->atMs, $pair->resp->frames));
        $this->assertSame(4, $pair->resp->end->seq);
        $this->assertSame(21, $pair->resp->end->atMs);
        $this->assertEquals(['service' => 'files.FileService', 'method' => 'Download'], $pair->reqPayload);
        $this->assertEquals(['status_code' => 0], $pair->respPayload);
    }
}
