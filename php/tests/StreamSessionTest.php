<?php

declare(strict_types=1);

namespace HopTop\Xrr\Tests;

use HopTop\Xrr\Exception\CassetteMissException;
use HopTop\Xrr\Exception\RecordedErrorException;
use HopTop\Xrr\Exception\ShapeMismatchException;
use HopTop\Xrr\Exception\StreamMismatchException;
use HopTop\Xrr\FileCassette;
use HopTop\Xrr\Mode;
use HopTop\Xrr\Session;
use HopTop\Xrr\Stream\Frame;
use HopTop\Xrr\Stream\ReqStream;
use HopTop\Xrr\Stream\RespStream;
use HopTop\Xrr\Stream\StreamedInteraction;
use HopTop\Xrr\Stream\StreamEvent;
use HopTop\Xrr\Stream\StreamFingerprint;
use HopTop\Xrr\Stream\StreamOpen;
use HopTop\Xrr\Stream\StreamType;
use PHPUnit\Framework\TestCase;
use Psr\Clock\ClockInterface;

/**
 * Session-level stream record/replay machinery
 * (cassette-format-streaming.md, Matching and Replay Semantics + Record
 * Semantics). Mirrors the Go reference matrix in go/stream_session_test.go.
 */
class StreamSessionTest extends TestCase
{
    private function tempDir(): string
    {
        $dir = sys_get_temp_dir() . '/xrr_' . uniqid();
        mkdir($dir);

        return $dir;
    }

    private function fixtureSession(string $dir): Session
    {
        return new Session(
            Mode::Replay,
            new FileCassette(dirname(__DIR__, 2) . '/spec/fixtures/' . $dir)
        );
    }

    /**
     * Mirrors the gRPC adapter's open definition: canonical inputs
     * service + method (+ msg_hash for content-addressed server streams),
     * counter-addressed client/bidi, req payload {service, method}.
     */
    private function grpcOpen(StreamType $type, string $service, string $method, ?string $msg = null): StreamOpen
    {
        $identity = ['service' => $service, 'method' => $method];
        $counter  = true;
        if ($type === StreamType::Server) {
            $identity['msg_hash'] = StreamFingerprint::msgHash((string) $msg);
            $counter              = false;
        }

        return new StreamOpen(
            adapterID: 'grpc',
            type: $type,
            identity: $identity,
            counter: $counter,
            payload: ['service' => $service, 'method' => $method]
        );
    }

    // ── identity seam ──────────────────────────────────────────────────────

    public function testComputeReproducesSpecVectors(): void
    {
        $server = fn(string $hash) => new StreamOpen('grpc', StreamType::Server, [
            'service' => 'files.FileService', 'method' => 'Download', 'msg_hash' => $hash,
        ]);
        $this->assertSame('58a4bf3f', StreamFingerprint::compute($server('f1e315a5')));
        $this->assertSame('9e8c4d4c', StreamFingerprint::compute($server('164658bd')));

        $client = new StreamOpen('grpc', StreamType::Client, [
            'service' => 'files.FileService', 'method' => 'Upload',
        ], counter: true);
        $this->assertSame('2bebfd6f', StreamFingerprint::compute($client, 0));
        $this->assertSame('b27b5fe1', StreamFingerprint::compute($client, 1));

        $bidi = new StreamOpen('grpc', StreamType::Bidi, [
            'service' => 'chat.ChatService', 'method' => 'Converse',
        ], counter: true);
        $this->assertSame('c6233d2e', StreamFingerprint::compute($bidi, 0));
    }

    public function testComputeSseUrlKeyedIdentity(): void
    {
        // spec/fixtures/sse-text-scalars/README.md: the adapter-neutral seam
        // reproduces sha256(canonical({"stream":"server","url":...}))[:8].
        $open = new StreamOpen('sse', StreamType::Server, ['url' => 'https://example.test/events']);
        $this->assertSame('66ecc77a', StreamFingerprint::compute($open));
    }

    public function testCanonicalAssemblySortsKeys(): void
    {
        $open = new StreamOpen('grpc', StreamType::Client, [
            'service' => 'files.FileService', 'method' => 'Upload',
        ], counter: true);
        $this->assertSame(
            '{"method":"Upload","n":0,"service":"files.FileService","stream":"client"}',
            StreamFingerprint::canonical($open, 0)
        );
    }

    public function testReservedIdentityKeysRejected(): void
    {
        $open = new StreamOpen('grpc', StreamType::Bidi, ['service' => 's', 'stream' => 'x'], counter: true);

        $this->expectException(\InvalidArgumentException::class);
        $this->expectExceptionMessage('reserved');
        StreamFingerprint::compute($open, 0);
    }

    public function testCounterOpenRequiresOrdinal(): void
    {
        $open = new StreamOpen('grpc', StreamType::Bidi, ['service' => 's', 'method' => 'm'], counter: true);

        $this->expectException(\InvalidArgumentException::class);
        StreamFingerprint::compute($open);
    }

    // ── record path ────────────────────────────────────────────────────────

    public function testOpenStreamRecordServer(): void
    {
        $dir = $this->tempDir();
        $s   = new Session(Mode::Record, new FileCassette($dir));
        $msg = '{"path":"/etc/hosts"}';

        $rec = $s->openStreamRecord($this->grpcOpen(StreamType::Server, 'files.FileService', 'Download', $msg));
        $this->assertSame('58a4bf3f', $rec->fingerprint());

        usleep(5000); // at_ms measures real elapsed time from open
        $rec->recordSend($msg);
        $rec->recordHalfClose();
        $rec->recordRecv("chunk-one\n");
        $rec->recordRecv("chunk-two\n");
        $rec->finish(['status_code' => 0]);

        $this->assertFileExists($dir . '/grpc-58a4bf3f.req.yaml');
        $this->assertFileExists($dir . '/grpc-58a4bf3f.resp.yaml');

        $pair = (new FileCassette($dir))->loadStreamed('grpc', '58a4bf3f');
        $this->assertSame(StreamType::Server, $pair->req->type);

        // Dense seq 0..N-1 counting all events in arrival order.
        $this->assertCount(1, $pair->req->frames);
        $this->assertSame(0, $pair->req->frames[0]->seq);
        $this->assertNotNull($pair->req->halfClose);
        $this->assertSame(1, $pair->req->halfClose->seq);
        $this->assertCount(2, $pair->resp->frames);
        $this->assertSame(2, $pair->resp->frames[0]->seq);
        $this->assertSame(3, $pair->resp->frames[1]->seq);
        $this->assertSame(4, $pair->resp->end->seq);

        $this->assertSame("chunk-one\n", $pair->resp->frames[0]->bytes);

        // at_ms stamped on every event, monotonic from open.
        $prev = 0;
        foreach (array_merge($pair->req->frames, $pair->resp->frames) as $frame) {
            $this->assertNotNull($frame->atMs);
            $this->assertGreaterThanOrEqual($prev, $frame->atMs);
            $prev = $frame->atMs;
        }
        $this->assertNotNull($pair->req->halfClose->atMs);
        $this->assertNotNull($pair->resp->end->atMs);
        $this->assertGreaterThanOrEqual(4, $pair->req->frames[0]->atMs, 'at_ms measured from open');

        // Server-stream payload carries no occurrence ordinal.
        $this->assertSame('files.FileService', $pair->reqPayload['service']);
        $this->assertSame('Download', $pair->reqPayload['method']);
        $this->assertArrayNotHasKey('n', $pair->reqPayload);
    }

    /**
     * Every timestamp in the pair comes from the session clock: at_ms is
     * the elapsed time since the clock's reading at open, recorded_at its
     * reading at finish. A scripted clock therefore yields byte-determined
     * cassettes — the seam byte-comparing tests rely on.
     */
    public function testStreamRecordingStampsFromTheSessionClock(): void
    {
        $dir   = $this->tempDir();
        $clock = new TickingClock(new \DateTimeImmutable('2026-01-02T03:04:05Z'));
        $s     = new Session(Mode::Record, new FileCassette($dir), null, $clock);

        $rec = $s->openStreamRecord($this->grpcOpen(StreamType::Bidi, 'chat.ChatService', 'Converse'));
        $rec->recordSend('alpha');
        $rec->recordHalfClose();
        $rec->recordRecv('one');
        $rec->finish(['status_code' => 0]);

        $pair = (new FileCassette($dir))->loadStreamed('grpc', $rec->fingerprint());

        // One reading at open, then one per event, 1 ms apart.
        $this->assertSame(1, $pair->req->frames[0]->atMs);
        $this->assertNotNull($pair->req->halfClose);
        $this->assertSame(2, $pair->req->halfClose->atMs);
        $this->assertSame(3, $pair->resp->frames[0]->atMs);
        $this->assertSame(4, $pair->resp->end->atMs);

        // The envelope stamp is the reading at finish, to the second, UTC.
        $this->assertSame('2026-01-02T03:04:05Z', $pair->reqRecordedAt);
        $this->assertSame('2026-01-02T03:04:05Z', $pair->respRecordedAt);
    }

    public function testOpenStreamRecordCounterN(): void
    {
        // One session object is one counter domain: two opens of the same
        // (service, method, type) tuple record n=0 then n=1.
        $dir  = $this->tempDir();
        $s    = new Session(Mode::Record, new FileCassette($dir));
        $open = $this->grpcOpen(StreamType::Client, 'files.FileService', 'Upload');

        $rec1 = $s->openStreamRecord($open);
        $this->assertSame('2bebfd6f', $rec1->fingerprint());
        $rec1->recordSend("alpha\n");
        $rec1->recordHalfClose();
        $rec1->recordRecv('{"received_bytes":6}');
        $rec1->finish(['status_code' => 0]);

        $rec2 = $s->openStreamRecord($open);
        $this->assertSame('b27b5fe1', $rec2->fingerprint());
        $rec2->recordHalfClose();
        $rec2->finish(['status_code' => 0]);

        $c  = new FileCassette($dir);
        $p1 = $c->loadStreamed('grpc', '2bebfd6f');
        $this->assertSame(0, $p1->reqPayload['n'], 'informational n injected into req payload');
        $p2 = $c->loadStreamed('grpc', 'b27b5fe1');
        $this->assertSame(1, $p2->reqPayload['n']);

        // A different tuple starts its own count.
        $rec3 = $s->openStreamRecord($this->grpcOpen(StreamType::Bidi, 'chat.ChatService', 'Converse'));
        $this->assertSame('c6233d2e', $rec3->fingerprint());
    }

    public function testStreamRecordingTerminalIsFinal(): void
    {
        $dir = $this->tempDir();
        $s   = new Session(Mode::Record, new FileCassette($dir));
        $rec = $s->openStreamRecord($this->grpcOpen(StreamType::Bidi, 'chat.ChatService', 'Converse'));
        $rec->recordSend('ping-1');
        $rec->recordRecv('pong-1');
        $rec->finish(['status_code' => 0]);

        // Dropped, matching the real-world no-op.
        $rec->recordSend('late');
        $rec->recordRecv('late');
        $rec->recordHalfClose();
        try {
            $rec->finish(['status_code' => 0]);
            $this->fail('second finish must throw');
        } catch (\LogicException) {
        }

        $pair = (new FileCassette($dir))->loadStreamed('grpc', $rec->fingerprint());
        $this->assertCount(1, $pair->req->frames);
        $this->assertCount(1, $pair->resp->frames);
        $this->assertNull($pair->req->halfClose);
        $this->assertSame(2, $pair->resp->end->seq);
    }

    public function testStreamRecordingErrorTerminal(): void
    {
        $dir = $this->tempDir();
        $s   = new Session(Mode::Record, new FileCassette($dir));
        $msg = '{"path":"/var/log/big.log"}';
        $rec = $s->openStreamRecord($this->grpcOpen(StreamType::Server, 'files.FileService', 'Download', $msg));
        $rec->recordSend($msg);
        $rec->recordHalfClose();
        $rec->recordRecv("log-chunk-1\n");
        $rec->finish(
            ['status_code' => 14],
            'rpc error: code = Unavailable desc = connection reset'
        );

        $pair = (new FileCassette($dir))->loadStreamed('grpc', '9e8c4d4c');
        $this->assertSame('rpc error: code = Unavailable desc = connection reset', $pair->error);
        $this->assertSame(14, $pair->respPayload['status_code']);
    }

    public function testStreamRecordingThrowableTerminal(): void
    {
        $dir = $this->tempDir();
        $s   = new Session(Mode::Record, new FileCassette($dir));
        $rec = $s->openStreamRecord($this->grpcOpen(StreamType::Bidi, 'chat.ChatService', 'Converse'));
        $rec->finish(['status_code' => 13], new \RuntimeException('boom'));

        $pair = (new FileCassette($dir))->loadStreamed('grpc', $rec->fingerprint());
        $this->assertSame('boom', $pair->error);
    }

    // ── replay path ────────────────────────────────────────────────────────

    public function testOpenStreamReplayServer(): void
    {
        $s   = $this->fixtureSession('grpc-server-stream');
        $rep = $s->openStreamReplay($this->grpcOpen(
            StreamType::Server, 'files.FileService', 'Download', '{"path":"/etc/hosts"}'
        ));
        $this->assertSame('58a4bf3f', $rep->fingerprint());
        $this->assertSame(StreamType::Server, $rep->type());
        $this->assertSame(0, $rep->respPayload()['status_code']);

        $this->assertTrue($rep->send('{"path":"/etc/hosts"}'));
        $rep->halfClose();

        $this->assertSame("chunk-one\n", $rep->recv());
        $this->assertSame("chunk-two\n", $rep->recv());
        $this->assertSame("chunk-three\n", $rep->recv());
        $this->assertNull($rep->recv(), 'end-of-stream after the last frame');
        $this->assertNull($rep->recv(), 'terminal repeats for j > R');
    }

    public function testOpenStreamReplayBidi(): void
    {
        $s   = $this->fixtureSession('grpc-bidi-stream');
        $rep = $s->openStreamReplay($this->grpcOpen(StreamType::Bidi, 'chat.ChatService', 'Converse'));
        $this->assertSame('c6233d2e', $rep->fingerprint());
        $this->assertSame(StreamType::Bidi, $rep->type());

        // Reads never gate on send progress: drain both pongs first.
        $this->assertSame('pong-1', $rep->recv());
        $this->assertSame('pong-2', $rep->recv());

        // Sends validated in order and bytes afterwards.
        $this->assertTrue($rep->send('ping-1'));
        $this->assertTrue($rep->send('ping-2'));
        $rep->halfClose();

        $this->assertNull($rep->recv());
        $this->assertNull($rep->recv(), 'terminal repeats for j > R');
    }

    public function testStreamReplaySendMismatchIsTerminal(): void
    {
        $s   = $this->fixtureSession('grpc-bidi-stream');
        $rep = $s->openStreamReplay($this->grpcOpen(StreamType::Bidi, 'chat.ChatService', 'Converse'));

        $this->assertTrue($rep->send('ping-1'));
        try {
            $rep->send('ping-DIVERGED');
            $this->fail('divergent send must throw');
        } catch (StreamMismatchException $e) {
            $this->assertSame('send', $e->op);
            $this->assertSame(1, $e->ordinal);
            $this->assertStringContainsString('sha256', $e->getMessage());
        }

        // Mismatch poisons every subsequent operation.
        try {
            $rep->recv();
            $this->fail('recv after mismatch must throw');
        } catch (StreamMismatchException) {
        }
        try {
            $rep->halfClose();
            $this->fail('half-close after mismatch must throw');
        } catch (StreamMismatchException) {
        }
        try {
            $rep->send('ping-2');
            $this->fail('send after mismatch must throw');
        } catch (StreamMismatchException) {
        }
    }

    public function testStreamReplayShortHalfCloseIsMismatch(): void
    {
        $s   = $this->fixtureSession('grpc-client-stream');
        $rep = $s->openStreamReplay($this->grpcOpen(StreamType::Client, 'files.FileService', 'Upload'));

        $this->assertTrue($rep->send("part-one\n"));
        try {
            $rep->halfClose();
            $this->fail('half-close after 1 of 2 sends must throw');
        } catch (StreamMismatchException $e) {
            $this->assertSame('half_close', $e->op);
            $this->assertSame(1, $e->ordinal);
        }
    }

    public function testStreamReplayPostCompletionSend(): void
    {
        // Send at i >= S with an OK terminal is the non-poisoning
        // stream-done signal; the recv side is unaffected.
        $s   = $this->fixtureSession('grpc-client-stream');
        $rep = $s->openStreamReplay($this->grpcOpen(StreamType::Client, 'files.FileService', 'Upload'));

        $this->assertTrue($rep->send("part-one\n"));
        $this->assertTrue($rep->send("part-two\n"));
        $this->assertFalse($rep->send("part-three\n"), 'post-completion send signals stream done');
        $rep->halfClose(); // half-close after all recorded sends is always accepted

        $this->assertSame('{"received_bytes":18}', $rep->recv());
        $this->assertNull($rep->recv());
    }

    public function testStreamReplayMidStreamError(): void
    {
        $s   = $this->fixtureSession('grpc-stream-error');
        $rep = $s->openStreamReplay($this->grpcOpen(
            StreamType::Server, 'files.FileService', 'Download', '{"path":"/var/log/big.log"}'
        ));
        $this->assertSame('9e8c4d4c', $rep->fingerprint());
        $this->assertSame(14, $rep->respPayload()['status_code']);

        $this->assertTrue($rep->send('{"path":"/var/log/big.log"}'));
        $rep->halfClose();

        $this->assertSame("log-chunk-1\n", $rep->recv());
        $this->assertSame("log-chunk-2\n", $rep->recv());

        $want = 'rpc error: code = Unavailable desc = connection reset';
        for ($i = 0; $i < 2; $i++) {
            try {
                $rep->recv();
                $this->fail('recv at terminal must surface the recorded error');
            } catch (RecordedErrorException $e) {
                $this->assertSame($want, $e->getMessage());
                $this->assertNotInstanceOf(StreamMismatchException::class, $e);
            }
        }

        // The recorded stream was already dead: post-completion send returns it.
        try {
            $rep->send('extra');
            $this->fail('post-completion send with error terminal must throw');
        } catch (RecordedErrorException $e) {
            $this->assertSame($want, $e->getMessage());
        }
    }

    public function testStreamReplayEmptyStreams(): void
    {
        // server-stream whose server sent nothing before OK.
        $s   = $this->fixtureSession('grpc-stream-empty');
        $rep = $s->openStreamReplay($this->grpcOpen(
            StreamType::Server, 'files.FileService', 'Download', '{"path":"/etc/empty"}'
        ));
        $this->assertNull($rep->recv(), 'first read yields end-of-stream');

        // client-stream where the client half-closed immediately (S=0).
        $s   = $this->fixtureSession('grpc-stream-empty');
        $rep = $s->openStreamReplay($this->grpcOpen(StreamType::Client, 'telemetry.MetricsService', 'Push'));
        $rep->halfClose();
        $this->assertSame('{"count":0}', $rep->recv());
        $this->assertNull($rep->recv());

        // bidi with no traffic at all.
        $s   = $this->fixtureSession('grpc-stream-empty');
        $rep = $s->openStreamReplay($this->grpcOpen(StreamType::Bidi, 'chat.ChatService', 'Ping'));
        $rep->halfClose();
        $this->assertNull($rep->recv());
    }

    public function testStreamReplayScriptedTwoOpens(): void
    {
        // Spec's n=1 obligation: one session, two sequential opens of the
        // same tuple locate 2bebfd6f then b27b5fe1.
        $s    = $this->fixtureSession('grpc-client-stream-repeat');
        $open = $this->grpcOpen(StreamType::Client, 'files.FileService', 'Upload');

        $first = $s->openStreamReplay($open);
        $this->assertSame('2bebfd6f', $first->fingerprint());
        $this->assertTrue($first->send("alpha\n"));
        $first->halfClose();
        $this->assertSame('{"received_bytes":6}', $first->recv());
        $this->assertNull($first->recv());

        $second = $s->openStreamReplay($open);
        $this->assertSame('b27b5fe1', $second->fingerprint());
        $this->assertTrue($second->send("beta-1\n"));
        $this->assertTrue($second->send("beta-2\n"));
        $second->halfClose();
        $this->assertSame('{"received_bytes":14}', $second->recv());
        $this->assertNull($second->recv());
    }

    public function testSseReplayThroughUrlKeyedIdentity(): void
    {
        $s    = $this->fixtureSession('sse-text-scalars');
        $open = new StreamOpen(
            adapterID: 'sse',
            type: StreamType::Server,
            identity: ['url' => 'https://example.test/events'],
            payload: ['url' => 'https://example.test/events']
        );

        $rep = $s->openStreamReplay($open);
        $this->assertSame('66ecc77a', $rep->fingerprint());
        $this->assertSame(
            ['on', '12:30', 'null', ' leading', 'trailing ', '  padded  '],
            [$rep->recv(), $rep->recv(), $rep->recv(), $rep->recv(), $rep->recv(), $rep->recv()]
        );
        $this->assertNull($rep->recv());
    }

    public function testStreamReplayMissAndShapeMismatch(): void
    {
        $dir  = $this->tempDir();
        $c    = new FileCassette($dir);
        $s    = new Session(Mode::Replay, $c);
        $open = $this->grpcOpen(StreamType::Bidi, 's', 'm');

        // No pair on disk ⇒ cassette miss (consumes n=0).
        try {
            $s->openStreamReplay($open);
            $this->fail('missing pair must be a cassette miss');
        } catch (CassetteMissException $e) {
            $this->assertStringContainsString('cassette miss', $e->getMessage());
        }

        // A unary pair at the streamed fingerprint ⇒ shape mismatch, not a
        // miss. The counter was consumed by the miss above, so the next open
        // computes n=1.
        $fp = StreamFingerprint::compute($open, 1);
        $c->save('grpc', $fp, ['service' => 's', 'method' => 'm'], ['status_code' => 0]);
        try {
            $s->openStreamReplay($open);
            $this->fail('unary pair at streamed fingerprint must be a shape mismatch');
        } catch (ShapeMismatchException $e) {
            $this->assertNotInstanceOf(CassetteMissException::class, $e);
        }
    }

    public function testStreamReplayTypeDivergenceIsShapeMismatch(): void
    {
        // A pair whose on-disk stream.type contradicts the type its
        // fingerprint encodes (hand-edited cassette) is a shape mismatch.
        $dir  = $this->tempDir();
        $open = $this->grpcOpen(StreamType::Bidi, 'chat.ChatService', 'Converse');
        $fp   = StreamFingerprint::compute($open, 0);

        (new FileCassette($dir))->saveStreamed(new StreamedInteraction(
            adapter: 'grpc',
            fingerprint: $fp,
            req: new ReqStream(StreamType::Client, [], new StreamEvent(0)),
            resp: new RespStream([], new StreamEvent(1)),
            reqPayload: ['service' => 'chat.ChatService', 'method' => 'Converse', 'n' => 0],
            respPayload: ['status_code' => 0]
        ));

        $s = new Session(Mode::Replay, new FileCassette($dir));
        $this->expectException(ShapeMismatchException::class);
        $s->openStreamReplay($open);
    }

    public function testOpenStreamModeEnforcement(): void
    {
        $dir  = $this->tempDir();
        $open = $this->grpcOpen(StreamType::Bidi, 's', 'm');

        $replaySession = new Session(Mode::Replay, new FileCassette($dir));
        try {
            $replaySession->openStreamRecord($open);
            $this->fail('openStreamRecord in replay mode must throw');
        } catch (\LogicException $e) {
            $this->assertStringContainsString('requires record mode', $e->getMessage());
        }

        $recordSession = new Session(Mode::Record, new FileCassette($dir));
        try {
            $recordSession->openStreamReplay($open);
            $this->fail('openStreamReplay in record mode must throw');
        } catch (\LogicException $e) {
            $this->assertStringContainsString('requires replay mode', $e->getMessage());
        }
    }

    public function testRecordThenReplayRoundTrip(): void
    {
        // Full loop: record a synthetic bidi conversation, then walk it back
        // through a fresh replay session.
        $dir  = $this->tempDir();
        $open = $this->grpcOpen(StreamType::Bidi, 'chat.ChatService', 'Converse');

        $recordSession = new Session(Mode::Record, new FileCassette($dir));
        $rec           = $recordSession->openStreamRecord($open);
        $rec->recordSend('ping-1');
        $rec->recordRecv('pong-1');
        $rec->recordSend('ping-2');
        $rec->recordRecv('pong-2');
        $rec->recordHalfClose();
        $rec->finish(['status_code' => 0]);

        $replaySession = new Session(Mode::Replay, new FileCassette($dir));
        $rep           = $replaySession->openStreamReplay($open);
        $this->assertSame($rec->fingerprint(), $rep->fingerprint());
        $this->assertTrue($rep->send('ping-1'));
        $this->assertSame('pong-1', $rep->recv());
        $this->assertTrue($rep->send('ping-2'));
        $this->assertSame('pong-2', $rep->recv());
        $rep->halfClose();
        $this->assertNull($rep->recv());
    }
}

/** PSR-20 clock that advances one millisecond per reading. */
final class TickingClock implements ClockInterface
{
    private int $ticks = 0;

    private readonly int $startUs;

    public function __construct(\DateTimeImmutable $start)
    {
        $this->startUs = (int) $start->format('U') * 1_000_000 + (int) $start->format('u');
    }

    public function now(): \DateTimeImmutable
    {
        $us = $this->startUs + 1_000 * $this->ticks++;

        return new \DateTimeImmutable(sprintf('@%d.%06d', intdiv($us, 1_000_000), $us % 1_000_000));
    }
}
