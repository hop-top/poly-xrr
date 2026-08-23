<?php

declare(strict_types=1);

namespace HopTop\Xrr\Tests;

use HopTop\Xrr\Adapters\Grpc\GrpcStream;
use HopTop\Xrr\Adapters\Grpc\ReplayingClientStreamingCall;
use HopTop\Xrr\FileCassette;
use HopTop\Xrr\Mode;
use HopTop\Xrr\Session;
use HopTop\Xrr\Redactor;
use HopTop\Xrr\Stream\StreamDirection;
use HopTop\Xrr\Stream\StreamFingerprint;
use HopTop\Xrr\Stream\StreamScrub;
use HopTop\Xrr\Stream\StreamType;
use PHPUnit\Framework\TestCase;

/**
 * Frame-level scrubbing for streamed gRPC cassettes
 * (cassette-format-streaming.md, REDACTION WARNING).
 *
 * Frame payloads are base64 on disk, so the field-name redaction that
 * covers structured payloads cannot see into them — these tests pin the
 * division of labour and the symmetry the format depends on.
 */
class GrpcStreamScrubTest extends TestCase
{
    private string $dir;

    protected function setUp(): void
    {
        $this->dir = sys_get_temp_dir() . '/xrr-grpc-scrub-' . bin2hex(random_bytes(6));
        mkdir($this->dir);
    }

    protected function tearDown(): void
    {
        array_map('unlink', glob($this->dir . '/*') ?: []);
        if (is_dir($this->dir)) {
            rmdir($this->dir);
        }
    }

    private function session(Mode $mode, ?StreamScrub $scrub): Session
    {
        return new Session($mode, new FileCassette($this->dir), $scrub);
    }

    public function testFieldNameRedactionCannotReachFramePayloads(): void
    {
        // The premise of the whole hook: a secret inside a frame is
        // base64-encoded on disk, so name-based payload redaction — which
        // only walks structured, named fields — never sees it.
        $secret = 'ghp_' . str_repeat('a', 36);

        $redactor = new Redactor();

        // A benignly-named field holding the RAW secret is still caught by
        // value-pattern matching...
        $this->assertSame(
            '<redacted:MESSAGE>',
            $redactor->redactPayload(['message' => $secret])['message']
        );

        // ...but the same secret base64-encoded, which is exactly how a
        // frame reaches disk, sails straight through.
        $encoded = base64_encode($secret);
        $this->assertSame(
            $encoded,
            $redactor->redactPayload(['message' => $encoded])['message'],
            'encoding defeats value-pattern scrubbing — hence the frame hook'
        );
    }

    public function testScrubbedFramesReachDiskScrubbedAndReplaySymmetrically(): void
    {
        $scrub  = new MaskingScrub();
        $secret = 'ghp_' . str_repeat('a', 36);

        // ── record ──
        $record = $this->session(Mode::Record, $scrub);
        $rec    = $record->openStreamRecord(GrpcStream::open(
            $record,
            StreamType::Client,
            'files.FileService',
            'Upload'
        ));
        $rec->recordSend('token=' . $secret);
        $rec->recordRecv('ok');
        $rec->recordHalfClose();
        $rec->finish(['status_code' => 0]);

        $onDisk = file_get_contents($this->dir . '/grpc-2bebfd6f.req.yaml');
        $this->assertStringNotContainsString(
            base64_encode('token=' . $secret),
            $onDisk,
            'raw secret must not reach disk in any encoding'
        );
        $this->assertStringContainsString(
            base64_encode('token=' . MaskingScrub::MASK),
            $onDisk,
            'the scrubbed form is what gets persisted'
        );

        // ── replay, same hook: live secret matches the scrubbed recording ──
        $replay = $this->session(Mode::Replay, $scrub);
        $call   = new ReplayingClientStreamingCall(
            $replay,
            'files.FileService',
            'Upload',
            null
        );
        $call->write(new FakeScrubMessage('token=' . $secret));
        [$response, $status] = $call->wait();

        $this->assertSame('ok', $response);
        $this->assertSame(0, $status->code);
    }

    public function testReplayWithoutTheHookFailsLoudly(): void
    {
        $scrub  = new MaskingScrub();
        $secret = 'ghp_' . str_repeat('b', 36);

        $record = $this->session(Mode::Record, $scrub);
        $rec    = $record->openStreamRecord(GrpcStream::open(
            $record,
            StreamType::Client,
            'files.FileService',
            'Upload'
        ));
        $rec->recordSend('token=' . $secret);
        $rec->recordHalfClose();
        $rec->finish(['status_code' => 0]);

        // Symmetry is load-bearing: replaying a scrubbed cassette without
        // the hook must fail as a mismatch, never silently diverge.
        $replay = $this->session(Mode::Replay, null);
        $call   = new ReplayingClientStreamingCall(
            $replay,
            'files.FileService',
            'Upload',
            null
        );

        $this->expectException(\HopTop\Xrr\Exception\StreamMismatchException::class);
        $call->write(new FakeScrubMessage('token=' . $secret));
    }

    public function testServerStreamIdentityIsDerivedFromScrubbedBytes(): void
    {
        // Content-addressed opens must hash the SCRUBBED bytes, so a
        // scrubbed recording and a scrubbed replay address the same file.
        $scrub   = new MaskingScrub();
        $secret  = 'ghp_' . str_repeat('c', 36);
        $session = $this->session(Mode::Record, $scrub);

        $open = GrpcStream::open(
            $session,
            StreamType::Server,
            'files.FileService',
            'Download',
            'token=' . $secret
        );

        $this->assertSame(
            StreamFingerprint::msgHash('token=' . MaskingScrub::MASK),
            $open->identity['msg_hash'],
            'msg_hash is computed over scrubbed bytes'
        );
    }

    public function testNoHookRecordsFramesVerbatim(): void
    {
        $session = $this->session(Mode::Record, null);
        $rec     = $session->openStreamRecord(GrpcStream::open(
            $session,
            StreamType::Client,
            'files.FileService',
            'Upload'
        ));
        $rec->recordSend('plain');
        $rec->recordHalfClose();
        $rec->finish(['status_code' => 0]);

        $this->assertStringContainsString(
            base64_encode('plain'),
            file_get_contents($this->dir . '/grpc-2bebfd6f.req.yaml')
        );
    }

    /**
     * Clause 2: the hook runs at exactly the specified points and nowhere
     * else. Half-close and the terminal carry no payload; recorded recv
     * frames are never re-scrubbed; and bytes past the last recorded send
     * are never compared, so they are never scrubbed.
     */
    public function testInvocationPoints(): void
    {
        $trace = new TracingScrub();
        $open  = GrpcStream::open(
            $this->session(Mode::Record, $trace),
            StreamType::Bidi,
            'chat.ChatService',
            'Converse'
        );

        $record = $this->session(Mode::Record, $trace);
        $rec    = $record->openStreamRecord($open);
        $rec->recordSend('a');
        $rec->recordRecv('b');
        $rec->recordHalfClose();          // no payload — not scrubbed
        $rec->finish(['status_code' => 0]); // terminal — not scrubbed
        $this->assertSame(['send:a', 'recv:b'], $trace->seen);

        $trace->seen = [];
        $replay      = $this->session(Mode::Replay, $trace);
        $rep         = $replay->openStreamReplay($open);
        $rep->send('a');
        $rep->recv();      // recorded frame — never re-scrubbed
        $rep->halfClose();
        $this->assertSame(['send:a'], $trace->seen);

        // Past the last recorded send: never compared, so never scrubbed.
        $trace->seen = [];
        $rep->send('overrun');
        $this->assertSame([], $trace->seen);
    }

    /**
     * Clause 6: the hook MAY change a frame's length; neither side assumes
     * byte-count preservation.
     */
    public function testLengthChangingHook(): void
    {
        $expand = new ExpandingScrub();
        $open   = GrpcStream::open(
            $this->session(Mode::Record, $expand),
            StreamType::Bidi,
            'chat.ChatService',
            'Converse'
        );

        $record = $this->session(Mode::Record, $expand);
        $rec    = $record->openStreamRecord($open);
        $rec->recordSend('k=' . ExpandingScrub::SECRET);
        $rec->recordRecv('v=' . ExpandingScrub::SECRET);
        $rec->recordHalfClose();
        $rec->finish(['status_code' => 0]);

        $replay = $this->session(Mode::Replay, $expand);
        $rep    = $replay->openStreamReplay($open);
        $rep->send('k=' . ExpandingScrub::SECRET); // green despite length change
        $this->assertSame('v=' . ExpandingScrub::LONG, $rep->recv());
    }
}

/** Records every invocation as "<dir>:<bytes>", returning data unchanged. */
final class TracingScrub implements StreamScrub
{
    /** @var list<string> */
    public array $seen = [];

    public function scrub(
        StreamDirection $dir,
        string $adapterID,
        StreamType $type,
        string $data
    ): string {
        $this->seen[] = $dir->value . ':' . $data;

        return $data;
    }
}

/** Deterministic mask that deliberately lengthens the frame. */
final class ExpandingScrub implements StreamScrub
{
    public const SECRET = 'hunter2-FAKE-TOKEN-0123456789';

    public const LONG = '[REDACTED-MUCH-LONGER-PLACEHOLDER]';

    public function scrub(
        StreamDirection $dir,
        string $adapterID,
        StreamType $type,
        string $data
    ): string {
        return str_replace(self::SECRET, self::LONG, $data);
    }
}

/** Deterministic, length-preserving mask for a GitHub-shaped token. */
final class MaskingScrub implements StreamScrub
{
    public const MASK = 'ghp_REDACTED_REDACTED_REDACTED_REDACT';

    public function scrub(
        StreamDirection $dir,
        string $adapterID,
        StreamType $type,
        string $data
    ): string {
        return preg_replace('#\bghp_[A-Za-z0-9]{20,}\b#', self::MASK, $data) ?? $data;
    }
}

final class FakeScrubMessage
{
    public function __construct(private string $bytes) {}

    public function serializeToString(): string
    {
        return $this->bytes;
    }
}
