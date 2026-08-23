<?php

declare(strict_types=1);

namespace HopTop\Xrr\Tests;

use HopTop\Xrr\Adapters\Grpc\GrpcStream;
use HopTop\Xrr\Adapters\Grpc\ReplayChannel;
use HopTop\Xrr\Adapters\Grpc\ReplayingBidiStreamingCall;
use HopTop\Xrr\Adapters\Grpc\ReplayingClientStreamingCall;
use HopTop\Xrr\Adapters\Grpc\ReplayingServerStreamingCall;
use HopTop\Xrr\Adapters\Grpc\XrrCallInvoker;
use HopTop\Xrr\Exception\CassetteMissException;
use HopTop\Xrr\Exception\MalformedStreamException;
use HopTop\Xrr\Exception\StreamMismatchException;
use HopTop\Xrr\FileCassette;
use HopTop\Xrr\Mode;
use HopTop\Xrr\Session;
use HopTop\Xrr\Stream\StreamFingerprint;
use HopTop\Xrr\Stream\StreamType;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

/**
 * The gRPC streaming adapter, exercised against the spec fixtures.
 *
 * Replay opens no channel and no call, so these run with ext-grpc absent —
 * which is exactly the property that makes a replay suite portable. The
 * record direction needs a live server and a working extension and is
 * covered separately.
 */
class GrpcStreamAdapterTest extends TestCase
{
    private function fixture(string $name): FileCassette
    {
        return new FileCassette(dirname(__DIR__, 2) . '/spec/fixtures/' . $name);
    }

    private function replaySession(string $fixture): Session
    {
        return new Session(Mode::Replay, $this->fixture($fixture));
    }

    // ── full-method parsing ────────────────────────────────────────────

    public function testSplitFullMethod(): void
    {
        $this->assertSame(
            ['files.FileService', 'Download'],
            GrpcStream::splitFullMethod('/files.FileService/Download')
        );
        // Leading slash is optional.
        $this->assertSame(
            ['chat.ChatService', 'Converse'],
            GrpcStream::splitFullMethod('chat.ChatService/Converse')
        );
    }

    /** @return list<array{string}> */
    public static function malformedMethods(): array
    {
        return [['/no-method/'], ['/'], ['bare'], ['/Method']];
    }

    #[DataProvider('malformedMethods')]
    public function testSplitFullMethodRejectsMalformed(string $full): void
    {
        $this->expectException(\InvalidArgumentException::class);
        GrpcStream::splitFullMethod($full);
    }

    // ── open identity matches the spec's fingerprints ──────────────────

    public function testServerOpenIsContentAddressed(): void
    {
        $session = $this->replaySession('grpc-server-stream');
        $open    = GrpcStream::open(
            $session,
            StreamType::Server,
            'files.FileService',
            'Download',
            '{"path":"/etc/hosts"}'
        );

        $this->assertFalse($open->counter, 'server opens are content-addressed');
        $this->assertSame('f1e315a5', $open->identity['msg_hash']);
        // Spec test vector.
        $this->assertSame('58a4bf3f', StreamFingerprint::compute($open));
        $this->assertArrayNotHasKey('n', $open->payload, 'server payloads omit n');
    }

    public function testClientAndBidiOpensAreCounterAddressed(): void
    {
        $session = $this->replaySession('grpc-client-stream');

        $client = GrpcStream::open($session, StreamType::Client, 'files.FileService', 'Upload');
        $this->assertTrue($client->counter);
        $this->assertArrayNotHasKey('msg_hash', $client->identity);
        $this->assertSame('2bebfd6f', StreamFingerprint::compute($client, 0));

        $bidi = GrpcStream::open($session, StreamType::Bidi, 'chat.ChatService', 'Converse');
        $this->assertTrue($bidi->counter);
        $this->assertSame('c6233d2e', StreamFingerprint::compute($bidi, 0));
    }

    public function testOpenPayloadCarriesServiceAndMethod(): void
    {
        $session = $this->replaySession('grpc-client-stream');
        $open    = GrpcStream::open($session, StreamType::Client, 'files.FileService', 'Upload');

        $this->assertSame(
            ['service' => 'files.FileService', 'method' => 'Upload'],
            $open->payload
        );
    }

    // ── server stream ─────────────────────────────────────────────────

    public function testServerStreamReplaysRecordedChunks(): void
    {
        $call = new ReplayingServerStreamingCall(
            $this->replaySession('grpc-server-stream'),
            'files.FileService',
            'Download',
            null
        );
        $call->start(new FakeMessage('{"path":"/etc/hosts"}'));

        $this->assertSame(
            ["chunk-one\n", "chunk-two\n", "chunk-three\n"],
            iterator_to_array($call->responses(), false)
        );
        $this->assertSame(0, $call->getStatus()->code);
        $this->assertSame([], $call->getMetadata(), 'metadata is not recorded');
    }

    public function testServerStreamRejectsDivergentRequest(): void
    {
        $call = new ReplayingServerStreamingCall(
            $this->replaySession('grpc-server-stream'),
            'files.FileService',
            'Download',
            null
        );

        // A different request message is a different content address, so
        // this misses rather than mismatching.
        $this->expectException(CassetteMissException::class);
        $call->start(new FakeMessage('{"path":"/nope"}'));
    }

    public function testServerStreamHalfClosesImplicitlyWithTheRequest(): void
    {
        // The spec's server mapping requires half_close immediately after
        // the single send frame: generated clients half-close implicitly.
        // Replay must therefore consume BOTH events at start(), so a
        // recording that lacks the half-close is caught rather than
        // silently accepted.
        $dir = sys_get_temp_dir() . '/xrr-grpc-nohc-' . bin2hex(random_bytes(6));
        mkdir($dir);

        // Same fingerprint as grpc-server-stream, but no half_close.
        file_put_contents($dir . '/grpc-58a4bf3f.req.yaml', <<<YAML
            xrr: "1"
            adapter: grpc
            fingerprint: "58a4bf3f"
            recorded_at: "2026-08-23T12:00:00Z"
            payload:
              service: files.FileService
              method: Download
            stream:
              type: server
              frames:
                - seq: 0
                  message_b64: "eyJwYXRoIjoiL2V0Yy9ob3N0cyJ9"
                  at_ms: 0
                - seq: 1
                  message_b64: "ZXh0cmE="
                  at_ms: 1
            YAML);
        file_put_contents($dir . '/grpc-58a4bf3f.resp.yaml', <<<YAML
            xrr: "1"
            adapter: grpc
            fingerprint: "58a4bf3f"
            recorded_at: "2026-08-23T12:00:00Z"
            payload:
              status_code: 0
            stream:
              frames: []
              end:
                seq: 2
                at_ms: 1
            YAML);

        try {
            $call = new ReplayingServerStreamingCall(
                new Session(Mode::Replay, new FileCassette($dir)),
                'files.FileService',
                'Download',
                null
            );

            // Half-closing with a recorded send still outstanding is a
            // mismatch — which only surfaces if start() validates it.
            $this->expectException(StreamMismatchException::class);
            $call->start(new FakeMessage('{"path":"/etc/hosts"}'));
        } finally {
            array_map('unlink', glob($dir . '/*') ?: []);
            rmdir($dir);
        }
    }

    public function testEmptyServerStreamYieldsNothingAndOk(): void
    {
        $call = new ReplayingServerStreamingCall(
            $this->replaySession('grpc-stream-empty'),
            'files.FileService',
            'Download',
            null
        );
        $call->start(new FakeMessage('{"path":"/etc/empty"}'));

        $this->assertSame([], iterator_to_array($call->responses(), false));
        $this->assertSame(0, $call->getStatus()->code);
    }

    public function testEmptyClientStreamSendsNothingAndStillGetsResponse(): void
    {
        $call = new ReplayingClientStreamingCall(
            $this->replaySession('grpc-stream-empty'),
            'telemetry.MetricsService',
            'Push',
            null
        );
        $call->start();

        // Zero send frames: half-close is immediately valid.
        [$response, $status] = $call->wait();
        $this->assertSame('{"count":0}', $response);
        $this->assertSame(0, $status->code);
    }

    public function testEmptyBidiStreamHasNoFramesInEitherDirection(): void
    {
        $call = new ReplayingBidiStreamingCall(
            $this->replaySession('grpc-stream-empty'),
            'chat.ChatService',
            'Ping',
            null
        );
        $call->start();
        $call->writesDone();

        $this->assertNull($call->read());
        $this->assertSame(0, $call->getStatus()->code);
    }

    // ── mid-stream error ──────────────────────────────────────────────

    public function testMidStreamErrorDeliversFramesThenStatus(): void
    {
        $call = new ReplayingServerStreamingCall(
            $this->replaySession('grpc-stream-error'),
            'files.FileService',
            'Download',
            null
        );
        $call->start(new FakeMessage('{"path":"/var/log/big.log"}'));

        // Frames recorded before the error are still delivered.
        $this->assertSame(
            ["log-chunk-1\n", "log-chunk-2\n"],
            iterator_to_array($call->responses(), false)
        );

        $status = $call->getStatus();
        $this->assertSame(14, $status->code, 'UNAVAILABLE from the recorded status_code');
        $this->assertSame('connection reset', $status->details, 'client rendering unwrapped');
    }

    public function testErrorStatusSurfacesWithoutDrainingTheStream(): void
    {
        $call = new ReplayingServerStreamingCall(
            $this->replaySession('grpc-stream-error'),
            'files.FileService',
            'Download',
            null
        );
        $call->start(new FakeMessage('{"path":"/var/log/big.log"}'));

        // Straight to the status, never iterating: the terminal must still
        // be observed rather than silently reported as OK.
        $this->assertSame(14, $call->getStatus()->code);
    }

    // ── client stream ─────────────────────────────────────────────────

    public function testClientStreamValidatesSendsAndReturnsResponse(): void
    {
        $call = new ReplayingClientStreamingCall(
            $this->replaySession('grpc-client-stream'),
            'files.FileService',
            'Upload',
            null
        );
        $call->start();
        $call->write(new FakeMessage("part-one\n"));
        $call->write(new FakeMessage("part-two\n"));

        [$response, $status] = $call->wait();
        $this->assertSame('{"received_bytes":18}', $response);
        $this->assertSame(0, $status->code);
    }

    public function testClientStreamMismatchOnDivergentSend(): void
    {
        $call = new ReplayingClientStreamingCall(
            $this->replaySession('grpc-client-stream'),
            'files.FileService',
            'Upload',
            null
        );
        $call->start();

        $this->expectException(StreamMismatchException::class);
        $call->write(new FakeMessage('not-what-was-recorded'));
    }

    public function testClientStreamMismatchOnShortHalfClose(): void
    {
        $call = new ReplayingClientStreamingCall(
            $this->replaySession('grpc-client-stream'),
            'files.FileService',
            'Upload',
            null
        );
        $call->start();
        $call->write(new FakeMessage("part-one\n"));

        // Half-closing after fewer sends than recorded is a mismatch.
        $this->expectException(StreamMismatchException::class);
        $call->wait();
    }

    public function testClientStreamRepeatConsumesOccurrenceCounter(): void
    {
        // One session, two sequential opens of the same tuple: n=0 then n=1.
        $session = $this->replaySession('grpc-client-stream-repeat');

        $first = new ReplayingClientStreamingCall($session, 'files.FileService', 'Upload', null);
        $first->write(new FakeMessage("alpha\n"));
        [, $firstStatus] = $first->wait();
        $this->assertSame(0, $firstStatus->code);

        $second = new ReplayingClientStreamingCall($session, 'files.FileService', 'Upload', null);
        $second->write(new FakeMessage("beta-1\n"));
        $second->write(new FakeMessage("beta-2\n"));
        [, $secondStatus] = $second->wait();
        $this->assertSame(0, $secondStatus->code);
    }

    // ── bidi ──────────────────────────────────────────────────────────

    public function testBidiStreamReplaysInterleavedConversation(): void
    {
        $call = new ReplayingBidiStreamingCall(
            $this->replaySession('grpc-bidi-stream'),
            'chat.ChatService',
            'Converse',
            null
        );
        $call->start();

        $call->write(new FakeMessage('ping-1'));
        $this->assertSame('pong-1', $call->read());

        $call->write(new FakeMessage('ping-2'));
        $this->assertSame('pong-2', $call->read());

        $call->writesDone();
        $this->assertNull($call->read(), 'end of stream');
        $this->assertSame(0, $call->getStatus()->code);
    }

    public function testBidiRecvIsNotGatedOnSendProgress(): void
    {
        // Recv frames are delivered in recorded order regardless of how the
        // client interleaves reads and writes.
        $call = new ReplayingBidiStreamingCall(
            $this->replaySession('grpc-bidi-stream'),
            'chat.ChatService',
            'Converse',
            null
        );

        $this->assertSame('pong-1', $call->read());
        $this->assertSame('pong-2', $call->read());
        $this->assertNull($call->read());
    }

    // ── malformed cassette ────────────────────────────────────────────

    public function testMalformedBase64IsRejectedNotReplayed(): void
    {
        $call = new ReplayingServerStreamingCall(
            $this->replaySession('grpc-stream-malformed-b64'),
            'files.FileService',
            'Download',
            null
        );

        // Invalid base64 in a resp frame must fail loudly at load, never
        // be silently accepted (or quietly repaired) as payload bytes.
        // The req half is well-formed, so this reaches the adapter as a
        // real open of fingerprint 8dbfb222.
        $this->expectException(MalformedStreamException::class);
        $this->expectExceptionMessage('base64');
        $call->start(new FakeMessage('{"path":"/opt/blob.bin"}'));
    }

    public function testAbsentStatusCodeReportsUnknownNotOk(): void
    {
        // The spec requires status_code; a cassette missing it (or carrying
        // a non-numeric one) must NOT replay as a success. Reporting UNKNOWN
        // keeps a malformed cassette loud instead of a false green.
        $dir = sys_get_temp_dir() . '/xrr-grpc-nostatus-' . bin2hex(random_bytes(6));
        mkdir($dir);

        file_put_contents($dir . '/grpc-58a4bf3f.req.yaml', <<<YAML
            xrr: "1"
            adapter: grpc
            fingerprint: "58a4bf3f"
            recorded_at: "2026-08-23T12:00:00Z"
            payload:
              service: files.FileService
              method: Download
            stream:
              type: server
              frames:
                - seq: 0
                  message_b64: "eyJwYXRoIjoiL2V0Yy9ob3N0cyJ9"
                  at_ms: 0
              half_close:
                seq: 1
                at_ms: 0
            YAML);
        file_put_contents($dir . '/grpc-58a4bf3f.resp.yaml', <<<YAML
            xrr: "1"
            adapter: grpc
            fingerprint: "58a4bf3f"
            recorded_at: "2026-08-23T12:00:00Z"
            payload: {}
            stream:
              frames: []
              end:
                seq: 2
                at_ms: 1
            YAML);

        try {
            $call = new ReplayingServerStreamingCall(
                new Session(Mode::Replay, new FileCassette($dir)),
                'files.FileService',
                'Download',
                null
            );
            $call->start(new FakeMessage('{"path":"/etc/hosts"}'));
            iterator_to_array($call->responses(), false);

            $this->assertSame(2, $call->getStatus()->code, 'UNKNOWN, never OK');
        } finally {
            array_map('unlink', glob($dir . '/*') ?: []);
            rmdir($dir);
        }
    }

    // ── deserialization ───────────────────────────────────────────────

    public function testRecordedBytesAreDeserializedIntoTheCallersMessage(): void
    {
        $call = new ReplayingServerStreamingCall(
            $this->replaySession('grpc-server-stream'),
            'files.FileService',
            'Download',
            [FakeMessage::class, 'decode']
        );
        $call->start(new FakeMessage('{"path":"/etc/hosts"}'));

        $first = iterator_to_array($call->responses(), false)[0];
        $this->assertInstanceOf(FakeMessage::class, $first);
        $this->assertSame("chunk-one\n", $first->serializeToString());
    }

    // ── invoker wiring ────────────────────────────────────────────────

    public function testInvokerReturnsReplayingCallsInReplayMode(): void
    {
        $invoker = new XrrCallInvoker($this->replaySession('grpc-client-stream'));
        $channel = $invoker->createChannelFactory('localhost:1', []);

        $this->assertInstanceOf(ReplayChannel::class, $channel, 'replay never dials');

        $this->assertInstanceOf(
            ReplayingClientStreamingCall::class,
            $invoker->ClientStreamingCall($channel, '/files.FileService/Upload', null, [])
        );
    }

    public function testReplayChannelReportsReadyWithoutConnecting(): void
    {
        $channel = new ReplayChannel('localhost:9999');

        $this->assertSame('localhost:9999', $channel->getTarget());
        $this->assertSame(2, $channel->getConnectivityState(true), 'READY');
        $this->assertFalse($channel->watchConnectivityState(0, null));
        $channel->close();
    }
}

/**
 * Stands in for a generated protobuf message: `serializeToString` is the
 * only surface the adapter touches on the send side, and `mergeFromString`
 * the only one on the recv side.
 */
final class FakeMessage
{
    public function __construct(private string $bytes = '') {}

    public function serializeToString(): string
    {
        return $this->bytes;
    }

    public function mergeFromString(string $bytes): void
    {
        $this->bytes = $bytes;
    }

    public static function decode(string $bytes): self
    {
        return new self($bytes);
    }
}
