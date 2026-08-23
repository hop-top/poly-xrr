<?php

declare(strict_types=1);

namespace HopTop\Xrr\Tests;

use HopTop\Xrr\FileCassette;
use HopTop\Xrr\Mode;
use HopTop\Xrr\Session;
use HopTop\Xrr\Stream\StreamDirection;
use HopTop\Xrr\Stream\StreamFingerprint;
use HopTop\Xrr\Stream\StreamOpen;
use HopTop\Xrr\Stream\StreamScrub;
use HopTop\Xrr\Stream\StreamType;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

/**
 * Identity-hook conformance — spec "Scrub Hook Obligations — Identity-Hook
 * Matrix" (M1..M7).
 *
 * The scrub hook's contract is WHEN it runs and WHAT it receives, never
 * what it computes; xrr defines no scrub algorithm. Two byte-neutral hooks
 * generate the whole matrix:
 *
 * - identity: returns its input. Installed and invoked but byte-neutral,
 *   so any divergence from a no-hook session is a mechanics defect —
 *   clause 7 fixes no-hook behaviour as the reference.
 * - counting: identity plus a call log. Reveals invocation points,
 *   multiplicity, and — the part fixtures cannot see — non-invocation.
 *
 * Mirrors go/stream_scrub_identity_test.go. This port's hook signature is
 * flattened (an interface taking adapterID + type rather than an info
 * struct); matrix identity is behavioural, not syntactic.
 */
class GrpcStreamScrubIdentityTest extends TestCase
{
    private const OPEN_MSG = '{"room":"ops"}';

    /** @var list<string> */
    private array $dirs = [];

    protected function tearDown(): void
    {
        foreach ($this->dirs as $dir) {
            array_map('unlink', glob($dir . '/*') ?: []);
            if (is_dir($dir)) {
                rmdir($dir);
            }
        }
        $this->dirs = [];
    }

    private function tmpDir(): string
    {
        $dir = sys_get_temp_dir() . '/xrr-scrub-identity-' . bin2hex(random_bytes(6));
        mkdir($dir);
        $this->dirs[] = $dir;

        return $dir;
    }

    private function session(string $dir, Mode $mode, ?StreamScrub $scrub): Session
    {
        return new Session($mode, new FileCassette($dir), $scrub);
    }

    private function fixedOpen(StreamType $type): StreamOpen
    {
        $identity = ['service' => 'chat.ChatService', 'method' => 'Converse'];
        if ($type === StreamType::Server) {
            $identity['msg_hash'] = StreamFingerprint::msgHash(self::OPEN_MSG);
        }

        return new StreamOpen(
            'grpc',
            $type,
            $identity,
            $type !== StreamType::Server,
            ['service' => 'chat.ChatService', 'method' => 'Converse']
        );
    }

    /** gRPC mapping: server streams record exactly one send frame. */
    private function fixedSends(StreamType $type): array
    {
        return $type === StreamType::Server ? [self::OPEN_MSG] : ['alpha', 'beta'];
    }

    /** gRPC mapping: client streams record at most one recv frame. */
    private function fixedRecvs(StreamType $type): array
    {
        return $type === StreamType::Client ? ['ack'] : ['one', 'two'];
    }

    /**
     * One identical scripted stream through a record session, so two
     * sessions differing only in hook installation are byte-comparable.
     */
    private function recordFixed(string $dir, StreamType $type, ?StreamScrub $scrub): string
    {
        $s   = $this->session($dir, Mode::Record, $scrub);
        $rec = $s->openStreamRecord($this->fixedOpen($type));
        foreach ($this->fixedSends($type) as $f) {
            $rec->recordSend($f);
        }
        $rec->recordHalfClose();
        foreach ($this->fixedRecvs($type) as $f) {
            $rec->recordRecv($f);
        }
        $fp = $rec->fingerprint();
        $rec->finish(['status_code' => 0]);

        return $fp;
    }

    private function replayFixed(string $dir, StreamType $type, ?StreamScrub $scrub): void
    {
        $s   = $this->session($dir, Mode::Replay, $scrub);
        $rep = $s->openStreamReplay($this->fixedOpen($type));
        foreach ($this->fixedSends($type) as $f) {
            $rep->send($f);
        }
        $rep->halfClose();
        foreach ($this->fixedRecvs($type) as $want) {
            $this->assertSame($want, $rep->recv());
        }
        $this->assertNull($rep->recv(), 'end of stream after the last recorded frame');
    }

    /** @return array{string, string} */
    private function pairBytes(string $dir, string $fp): array
    {
        return [
            (string) file_get_contents($dir . "/grpc-{$fp}.req.yaml"),
            (string) file_get_contents($dir . "/grpc-{$fp}.resp.yaml"),
        ];
    }

    /** @return list<StreamType> */
    public static function streamTypes(): array
    {
        return [
            'server' => [StreamType::Server],
            'client' => [StreamType::Client],
            'bidi'   => [StreamType::Bidi],
        ];
    }

    /**
     * M1: an installed identity hook is byte-indistinguishable from no
     * hook. Any divergence is a mechanics defect — an extra scrub site, a
     * missed one, or an identity input derived from the wrong bytes.
     */
    #[DataProvider('streamTypes')]
    public function testIdentityHookMatchesNoHook(StreamType $type): void
    {
        $bare   = $this->tmpDir();
        $hooked = $this->tmpDir();

        $bareFP   = $this->recordFixed($bare, $type, null);
        $hookedFP = $this->recordFixed($hooked, $type, new IdentityScrub());

        $this->assertSame($bareFP, $hookedFP, 'identity hook must not move the fingerprint');
        $this->assertSame(
            $this->pairBytes($bare, $bareFP),
            $this->pairBytes($hooked, $hookedFP),
            'cassette bytes must be identical'
        );
    }

    /**
     * M2: because the identity hook changes no bytes, a cassette crosses
     * the hook boundary both ways. The one legitimate exception to clause
     * 5's "same hook both sides" — it holds precisely because the two agree
     * byte-for-byte.
     */
    #[DataProvider('streamTypes')]
    public function testIdentityHookReplaysAcrossTheHookBoundary(StreamType $type): void
    {
        $withHook = $this->tmpDir();
        $this->recordFixed($withHook, $type, new IdentityScrub());
        $this->replayFixed($withHook, $type, null);

        $without = $this->tmpDir();
        $this->recordFixed($without, $type, null);
        $this->replayFixed($without, $type, new IdentityScrub());
    }

    /**
     * M3: clause 3 routes content-derived identity through the hook. Under
     * identity it must land on the raw msg_hash in both modes — otherwise
     * the hook is applied to the wrong buffer, or applied twice.
     */
    public function testIdentityDerivedIdentityEqualsRaw(): void
    {
        foreach ([Mode::Record, Mode::Replay] as $mode) {
            $s        = $this->session($this->tmpDir(), $mode, new IdentityScrub());
            $scrubbed = $s->scrubStreamFrame(
                StreamDirection::Send,
                'grpc',
                StreamType::Server,
                self::OPEN_MSG
            );
            $this->assertSame(
                StreamFingerprint::msgHash(self::OPEN_MSG),
                StreamFingerprint::msgHash($scrubbed),
                $mode->value
            );
        }
    }

    /**
     * M4: exactly one call per frame per direction, in frame order,
     * carrying that frame's bytes. Half-close and the terminal carry no
     * payload and contribute no call.
     */
    public function testCountingHookRecordInvocations(): void
    {
        $log = new CountingScrub();
        $this->recordFixed($this->tmpDir(), StreamType::Bidi, $log);

        $this->assertSame(
            ['send:alpha', 'send:beta', 'recv:one', 'recv:two'],
            $log->seen
        );
    }

    /**
     * M5: replay scrubs live sends only, once each, and never touches
     * recorded frames. The trailing case caught a real cross-port
     * divergence: ts and php ran the hook BEFORE the bound check that
     * rejects a send past the end of the recording; go, py and rs ran it
     * after. Only a counting hook sees that.
     */
    public function testCountingHookReplayInvocations(): void
    {
        $dir = $this->tmpDir();
        $this->recordFixed($dir, StreamType::Bidi, new IdentityScrub());

        $log = new CountingScrub();
        $s   = $this->session($dir, Mode::Replay, $log);
        $rep = $s->openStreamReplay($this->fixedOpen(StreamType::Bidi));
        $rep->send('alpha');
        $rep->send('beta');
        $rep->halfClose();
        $this->assertSame('one', $rep->recv());
        $this->assertSame('two', $rep->recv());
        $this->assertSame(['send:alpha', 'send:beta'], $log->seen);

        $log->seen = [];
        $rep->send('overrun');
        $this->assertSame(
            [],
            $log->seen,
            'a send past the last recorded frame is never compared, so never scrubbed'
        );
    }

    /**
     * M6: clause 3's no-pre-scrub rule. The gRPC server-stream open message
     * is both an identity input and a persisted frame — two distinct
     * invocation points, one call each. An adapter that pre-scrubbed the
     * message it also hands the core would show two calls for the persist
     * point.
     */
    public function testCountingHookNoDoubleScrub(): void
    {
        $log = new CountingScrub();
        $msg = '{"cmd":"deploy"}';
        $s   = $this->session($this->tmpDir(), Mode::Record, $log);

        // Identity point: the adapter derives msg_hash over scrubbed bytes.
        $scrubbed = $s->scrubStreamFrame(
            StreamDirection::Send,
            'grpc',
            StreamType::Server,
            $msg
        );
        $this->assertCount(1, $log->seen, 'identity derivation is exactly one call');

        $rec = $s->openStreamRecord(new StreamOpen(
            'grpc',
            StreamType::Server,
            [
                'service'  => 'ops.Deploy',
                'method'   => 'Run',
                'msg_hash' => StreamFingerprint::msgHash($scrubbed),
            ],
            false,
            ['service' => 'ops.Deploy', 'method' => 'Run']
        ));

        // Persist point: the adapter passes the message RAW. The core scrubs.
        $rec->recordSend($msg);
        $rec->recordHalfClose();
        $rec->recordRecv('deployed');
        $rec->finish(['status_code' => 0]);

        $this->assertSame([
            'send:' . $msg,   // identity derivation
            'send:' . $msg,   // persist — one call, not two
            'recv:deployed',
        ], $log->seen);
    }

    /**
     * M7: clause 6 permits a length change; neither the record nor the
     * replay path may assume byte-count preservation.
     */
    public function testLengthChangingHookRoundTrips(): void
    {
        $grow = new GrowingScrub();
        $dir  = $this->tmpDir();
        $open = $this->fixedOpen(StreamType::Bidi);

        $rec = $this->session($dir, Mode::Record, $grow)->openStreamRecord($open);
        $rec->recordSend('alpha');
        $rec->recordHalfClose();
        $rec->recordRecv('one');
        $fp = $rec->fingerprint();
        $rec->finish(['status_code' => 0]);

        $pair = (new FileCassette($dir))->loadStreamed('grpc', $fp);
        $this->assertSame('alpha-PADDED-LONGER', $pair->req->frames[0]->bytes);

        $rep = $this->session($dir, Mode::Replay, $grow)->openStreamReplay($open);
        $rep->send('alpha'); // green despite the length change
        $rep->halfClose();
        $this->assertSame('one-PADDED-LONGER', $rep->recv());
    }
}

/** Clause 6's "MAY return the input unchanged": observable, byte-neutral. */
final class IdentityScrub implements StreamScrub
{
    public function scrub(
        StreamDirection $dir,
        string $adapterID,
        StreamType $type,
        string $data
    ): string {
        return $data;
    }
}

/**
 * Identity plus a call log. The bookkeeping is test scaffolding, not scrub
 * state — the bytes returned are the input, so clause 4's determinism holds.
 */
final class CountingScrub implements StreamScrub
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

/** Deterministic hook that deliberately lengthens every frame. */
final class GrowingScrub implements StreamScrub
{
    public function scrub(
        StreamDirection $dir,
        string $adapterID,
        StreamType $type,
        string $data
    ): string {
        return $data . '-PADDED-LONGER';
    }
}
