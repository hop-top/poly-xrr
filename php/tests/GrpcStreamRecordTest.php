<?php

declare(strict_types=1);

namespace HopTop\Xrr\Tests;

use HopTop\Xrr\Adapters\Grpc\RecordingBidiStreamingCall;
use HopTop\Xrr\Adapters\Grpc\RecordingClientStreamingCall;
use HopTop\Xrr\Adapters\Grpc\RecordingServerStreamingCall;
use HopTop\Xrr\FileCassette;
use HopTop\Xrr\Mode;
use HopTop\Xrr\Session;
use PHPUnit\Framework\TestCase;
use Symfony\Component\Yaml\Yaml;

/**
 * The record direction of the gRPC streaming adapter.
 *
 * ext-grpc's `Grpc\Call` is a C-extension object whose `startBatch` drives
 * the wire. `AbstractCall::$call` is protected, so these tests substitute a
 * scripted batch double and assert on what reaches the cassette — pinning
 * the property that matters most: frames carry the RAW wire buffer the
 * batch produced, never a re-serialization of a decoded message.
 *
 * The live-server round-trip is a separate, extension-dependent harness.
 */
class GrpcStreamRecordTest extends TestCase
{
    private string $dir;

    protected function setUp(): void
    {
        $this->dir = sys_get_temp_dir() . '/xrr-grpc-rec-' . bin2hex(random_bytes(6));
        mkdir($this->dir);
    }

    protected function tearDown(): void
    {
        array_map('unlink', glob($this->dir . '/*') ?: []);
        if (is_dir($this->dir)) {
            rmdir($this->dir);
        }
    }


    /**
     * Builds a REAL recording call with ext-grpc's Grpc\Call swapped for a
     * scripted double. The production classes are final by design, so the
     * double is injected rather than subclassed — these tests exercise the
     * shipping code, not a stand-in for it.
     *
     * @param class-string $class
     */
    private function recordingCall(
        string $class,
        FakeBatchCall $batch,
        string $service,
        string $method,
        mixed $deserialize = null
    ): object {
        $rc   = new \ReflectionClass($class);
        $call = $rc->newInstanceWithoutConstructor();

        foreach ([
            'call'        => $batch,
            'deserialize' => $deserialize ?? [FakeRecMessage::class, 'decode'],
            'metadata'    => null,
        ] as $prop => $value) {
            $p = new \ReflectionProperty(\Grpc\AbstractCall::class, $prop);
            $p->setValue($call, $value);
        }

        return $this->recordingCallOn($this->session(), $class, $batch, $service, $method, $deserialize);
    }

    /** Same, against an explicit session (for multi-open counter tests). */
    private function recordingCallOn(
        Session $session,
        string $class,
        FakeBatchCall $batch,
        string $service,
        string $method,
        mixed $deserialize = null
    ): object {
        $rc   = new \ReflectionClass($class);
        $call = $rc->newInstanceWithoutConstructor();

        foreach ([
            'call'        => $batch,
            'deserialize' => $deserialize ?? [FakeRecMessage::class, 'decode'],
            'metadata'    => null,
        ] as $prop => $value) {
            $p = new \ReflectionProperty(\Grpc\AbstractCall::class, $prop);
            $p->setValue($call, $value);
        }

        $call->bindXrr($session, $service, $method);

        return $call;
    }

    private function session(): Session
    {
        return new Session(Mode::Record, new FileCassette($this->dir));
    }

    /** @return array{req: array<string, mixed>, resp: array<string, mixed>} */
    private function cassette(string $fingerprint): array
    {
        return [
            'req'  => Yaml::parseFile($this->dir . "/grpc-$fingerprint.req.yaml"),
            'resp' => Yaml::parseFile($this->dir . "/grpc-$fingerprint.resp.yaml"),
        ];
    }

    public function testServerStreamRecordsRequestChunksAndOkTerminal(): void
    {
        $call = $this->recordingCall(
            RecordingServerStreamingCall::class,
            new FakeBatchCall(["chunk-one\n", "chunk-two\n", null]),
            'files.FileService',
            'Download'
        );

        $call->start(new FakeRecMessage('{"path":"/etc/hosts"}'));
        $received = iterator_to_array($call->responses(), false);
        $call->getStatus();

        $this->assertSame(
            ["chunk-one\n", "chunk-two\n"],
            array_map(fn($m) => $m->serializeToString(), $received),
            'the caller still receives deserialized messages'
        );

        // Content-addressed by the request message: the spec's test vector.
        $pair = $this->cassette('58a4bf3f');

        $this->assertSame('server', $pair['req']['stream']['type']);
        $this->assertSame(
            ['service' => 'files.FileService', 'method' => 'Download'],
            $pair['req']['payload'],
            'server payloads carry service+method and omit n'
        );

        // Exactly one send frame, half-closed immediately after it.
        $this->assertCount(1, $pair['req']['stream']['frames']);
        $this->assertSame(
            base64_encode('{"path":"/etc/hosts"}'),
            $pair['req']['stream']['frames'][0]['message_b64']
        );
        $this->assertSame(1, $pair['req']['stream']['half_close']['seq']);

        $this->assertSame(
            [base64_encode("chunk-one\n"), base64_encode("chunk-two\n")],
            array_column($pair['resp']['stream']['frames'], 'message_b64')
        );
        $this->assertSame(0, $pair['resp']['payload']['status_code']);
        $this->assertArrayNotHasKey('error', $pair['resp']);
    }

    public function testRecvFramesCarryRawWireBytesNotReserialized(): void
    {
        // The batch hands back bytes that do NOT round-trip through the
        // message class (the double's decode is deliberately lossy). If the
        // adapter re-serialized a decoded message, the cassette would carry
        // the lossy form; capturing the raw buffer keeps the wire bytes.
        $call = $this->recordingCall(
            RecordingServerStreamingCall::class,
            new FakeBatchCall(["\x0a\x03raw", null]),
            'files.FileService',
            'Download',
            [LossyMessage::class, 'decode']
        );

        $call->start(new FakeRecMessage('{"path":"/etc/hosts"}'));
        iterator_to_array($call->responses(), false);
        $call->getStatus();

        $pair = $this->cassette('58a4bf3f');
        $this->assertSame(
            base64_encode("\x0a\x03raw"),
            $pair['resp']['stream']['frames'][0]['message_b64'],
            'the wire buffer is persisted verbatim'
        );
    }

    public function testServerStreamRecordsErrorTerminal(): void
    {
        $call = $this->recordingCall(
            RecordingServerStreamingCall::class,
            new FakeBatchCall(
                ["log-chunk-1\n", null],
                (object) ['code' => 14, 'details' => 'connection reset', 'metadata' => []]
            ),
            'files.FileService',
            'Download'
        );

        $call->start(new FakeRecMessage('{"path":"/etc/hosts"}'));
        iterator_to_array($call->responses(), false);
        $call->getStatus();

        $pair = $this->cassette('58a4bf3f');

        $this->assertSame(14, $pair['resp']['payload']['status_code']);
        // Spec invariant: error non-empty iff status_code != 0.
        $this->assertSame(
            'rpc error: code = 14 desc = connection reset',
            $pair['resp']['error']
        );
    }

    public function testNonOkStatusWithEmptyDetailsStillSynthesizesError(): void
    {
        $call = $this->recordingCall(
            RecordingServerStreamingCall::class,
            new FakeBatchCall(
                [null],
                (object) ['code' => 4, 'details' => '', 'metadata' => []]
            ),
            'files.FileService',
            'Download'
        );

        $call->start(new FakeRecMessage('{"path":"/etc/hosts"}'));
        iterator_to_array($call->responses(), false);
        $call->getStatus();

        $pair = $this->cassette('58a4bf3f');
        $this->assertSame(4, $pair['resp']['payload']['status_code']);
        $this->assertNotEmpty(
            $pair['resp']['error'],
            'the spec forbids an empty error alongside a non-zero status'
        );
    }

    public function testClientStreamRecordsSendsHalfCloseAndResponse(): void
    {
        $call = $this->recordingCall(
            RecordingClientStreamingCall::class,
            new FakeBatchCall(['{"received_bytes":18}']),
            'files.FileService',
            'Upload'
        );

        $call->write(new FakeRecMessage("part-one\n"));
        $call->write(new FakeRecMessage("part-two\n"));
        $call->wait();

        // Counter-addressed at n=0: the spec's test vector.
        $pair = $this->cassette('2bebfd6f');

        $this->assertSame('client', $pair['req']['stream']['type']);
        $this->assertSame(0, $pair['req']['payload']['n'], 'informational occurrence ordinal');
        $this->assertSame(
            [base64_encode("part-one\n"), base64_encode("part-two\n")],
            array_column($pair['req']['stream']['frames'], 'message_b64')
        );
        $this->assertSame(2, $pair['req']['stream']['half_close']['seq']);
        $this->assertCount(1, $pair['resp']['stream']['frames'], 'at most one recv frame');
        $this->assertSame(0, $pair['resp']['payload']['status_code']);
    }

    public function testClientStreamSecondOpenCountsUp(): void
    {
        // One session across both opens: the counter is per session.
        $session = $this->session();

        foreach ([0, 1] as $_) {
            $call = $this->recordingCallOn(
                $session,
                RecordingClientStreamingCall::class,
                new FakeBatchCall(['{}']),
                'files.FileService',
                'Upload'
            );
            $call->write(new FakeRecMessage('x'));
            $call->wait();
        }

        // n=0 and n=1 of the same tuple land in distinct cassettes.
        $this->assertSame(0, $this->cassette('2bebfd6f')['req']['payload']['n']);
        $this->assertSame(1, $this->cassette('b27b5fe1')['req']['payload']['n']);
    }

    public function testBidiRecordsInterleavedFramesInArrivalOrder(): void
    {
        $call = $this->recordingCall(
            RecordingBidiStreamingCall::class,
            new FakeBatchCall(['pong-1', 'pong-2', null]),
            'chat.ChatService',
            'Converse'
        );

        $call->write(new FakeRecMessage('ping-1'));
        $call->read();
        $call->write(new FakeRecMessage('ping-2'));
        $call->read();
        $call->writesDone();
        $call->read();
        $call->getStatus();

        $pair = $this->cassette('c6233d2e');

        $this->assertSame('bidi', $pair['req']['stream']['type']);
        // One monotonic counter across both directions preserves the
        // true interleaving: send 0, recv 1, send 2, recv 3, half_close 4.
        $this->assertSame([0, 2], array_column($pair['req']['stream']['frames'], 'seq'));
        $this->assertSame([1, 3], array_column($pair['resp']['stream']['frames'], 'seq'));
        $this->assertSame(4, $pair['req']['stream']['half_close']['seq']);
        $this->assertSame(5, $pair['resp']['stream']['end']['seq']);
    }

    public function testStreamWithoutTerminalWritesNoCassette(): void
    {
        $call = $this->recordingCall(
            RecordingClientStreamingCall::class,
            new FakeBatchCall([]),
            'files.FileService',
            'Upload'
        );

        $call->write(new FakeRecMessage('orphan'));
        // No wait() ⇒ no terminal ⇒ nothing persisted, by design.

        $this->assertSame([], glob($this->dir . '/*.yaml') ?: []);
    }
}

/** Scripted stand-in for ext-grpc's `Grpc\Call`. */
final class FakeBatchCall
{
    private int $i = 0;

    /**
     * @param list<?string> $messages recv-side wire buffers in arrival
     *   order; a trailing null marks end-of-stream
     */
    public function __construct(
        private array $messages,
        private ?object $status = null
    ) {}

    /**
     * Mimics ext-grpc: the returned event carries only what the requested
     * ops asked for, and the recv queue advances only on a recv-message op.
     */
    public function startBatch(array $batch): object
    {
        $event = new \stdClass();

        if (isset($batch[\Grpc\OP_RECV_INITIAL_METADATA])) {
            $event->metadata = [];
        }

        if (isset($batch[\Grpc\OP_RECV_MESSAGE])) {
            $event->message = $this->messages[$this->i] ?? null;
            $this->i++;
        }

        if (isset($batch[\Grpc\OP_RECV_STATUS_ON_CLIENT])) {
            $event->status = $this->status
                ?? (object) ['code' => 0, 'details' => '', 'metadata' => []];
        }

        $event->metadata ??= [];

        return $event;
    }
}

final class FakeRecMessage
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

/** Decodes lossily, so a re-serialized frame would differ from the wire. */
final class LossyMessage
{
    public function mergeFromString(string $bytes): void {}

    public function serializeToString(): string
    {
        return 'LOSSY';
    }

    public static function decode(string $bytes): self
    {
        return new self();
    }
}
