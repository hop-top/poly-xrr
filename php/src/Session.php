<?php

declare(strict_types=1);

namespace HopTop\Xrr;

use HopTop\Xrr\Exception\CassetteMissException;
use HopTop\Xrr\Exception\ShapeMismatchException;
use HopTop\Xrr\Stream\OccurrenceCounter;
use HopTop\Xrr\Stream\StreamDirection;
use HopTop\Xrr\Stream\StreamFingerprint;
use HopTop\Xrr\Stream\StreamOpen;
use HopTop\Xrr\Stream\StreamRecording;
use HopTop\Xrr\Stream\StreamReplay;
use HopTop\Xrr\Stream\StreamScrub;
use HopTop\Xrr\Stream\StreamType;

class Session
{
    private ?OccurrenceCounter $streamOccurrences = null;

    /**
     * @param ?StreamScrub $streamScrub frame-level secret scrubbing for
     *   streamed interactions. Null records and replays frames verbatim.
     *   Install the SAME hook when recording and when replaying: scrubbing
     *   is symmetric by design, and a session replaying a scrubbed cassette
     *   without the hook fails with a stream mismatch
     *   (cassette-format-streaming.md, REDACTION WARNING).
     */
    public function __construct(
        private Mode $mode,
        private FileCassette $cassette,
        private ?StreamScrub $streamScrub = null
    ) {}

    /** The session's mode. Adapters dispatch their own behaviour on it. */
    public function mode(): Mode
    {
        return $this->mode;
    }

    /**
     * Applies the session's frame scrub hook to $data, returning it
     * unchanged when no hook is installed.
     *
     * Adapters whose open identity derives from message bytes (the gRPC
     * server-stream msg_hash) MUST compute that identity over this method's
     * output, in record and replay mode alike, so both modes address the
     * cassette by the scrubbed content. Frames handed to the core
     * (recordSend/recordRecv, replay send) are scrubbed by the core itself
     * — adapters pass them raw and never double-scrub.
     */
    public function scrubStreamFrame(
        StreamDirection $dir,
        string $adapterID,
        StreamType $type,
        string $data
    ): string {
        return $this->streamScrub?->scrub($dir, $adapterID, $type, $data) ?? $data;
    }

    /**
     * Occurrence counter for streamed opens whose fingerprint carries `n`.
     * One session object is one counter domain; record and replay count
     * identically (cassette-format-streaming.md, Fingerprinting).
     */
    public function streamOccurrences(): OccurrenceCounter
    {
        return $this->streamOccurrences ??= new OccurrenceCounter();
    }

    /**
     * Execute one interaction according to session mode.
     *
     * record:      calls $do(), saves to cassette, returns result.
     * replay:      loads from cassette, returns deserialized resp; never calls $do().
     * passthrough: calls $do(), never touches cassette.
     *
     * @throws CassetteMissException on replay miss
     */
    public function record(AdapterInterface $adapter, mixed $req, callable $do): mixed
    {
        return match ($this->mode) {
            Mode::Record      => $this->doRecord($adapter, $req, $do),
            Mode::Replay      => $this->doReplay($adapter, $req),
            Mode::Passthrough => $do($req),
        };
    }

    private function doRecord(AdapterInterface $adapter, mixed $req, callable $do): mixed
    {
        $resp = $do($req);

        $fp = $adapter->fingerprint($req);
        $this->cassette->save(
            $adapter->getId(),
            $fp,
            $adapter->serializeReq($req),
            $adapter->serializeResp($resp)
        );

        return $resp;
    }

    private function doReplay(AdapterInterface $adapter, mixed $req): mixed
    {
        $fp   = $adapter->fingerprint($req);
        $data = $this->cassette->load($adapter->getId(), $fp);

        return $adapter->deserializeResp($data['resp']);
    }

    /**
     * Opens a streamed interaction for recording. The adapter observes the
     * live stream and mirrors it into the returned recording:
     * recordSend/recordRecv per message, recordHalfClose when the client
     * closes its send side, then finish exactly once when the terminal is
     * observed — only finish persists the pair, so a stream that never
     * reaches terminal produces no cassette.
     *
     * @throws \LogicException when the session is not in record mode
     */
    public function openStreamRecord(StreamOpen $open): StreamRecording
    {
        $this->checkStreamOpen($open, Mode::Record, 'openStreamRecord');
        [$fp, $n] = $this->streamOpenFingerprint($open);

        $payload = $open->payload;
        if ($n >= 0) {
            // Informational occurrence ordinal: recoverable from disk,
            // never read back to drive matching.
            $payload['n'] = $n;
        }

        return new StreamRecording(
            $this->cassette,
            $open->adapterID,
            $fp,
            $open->type,
            $payload,
            $this->streamScrub
        );
    }

    /**
     * Locates the cassette pair for a streamed open and returns a replay
     * handle. The occurrence counter is consumed exactly as in record
     * mode, hit or miss.
     *
     * @throws \LogicException when the session is not in replay mode
     * @throws CassetteMissException when no pair exists
     * @throws ShapeMismatchException when the pair is unary or its recorded
     *   stream type contradicts the requested one
     * @throws Exception\MalformedStreamException on validation-rule violations
     */
    public function openStreamReplay(StreamOpen $open): StreamReplay
    {
        $this->checkStreamOpen($open, Mode::Replay, 'openStreamReplay');
        [$fp] = $this->streamOpenFingerprint($open);

        $pair = $this->cassette->loadStreamed($open->adapterID, $fp);
        if ($pair->req->type !== $open->type) {
            throw new ShapeMismatchException(sprintf(
                'recorded stream type "%s", requested "%s"',
                $pair->req->type->value,
                $open->type->value
            ));
        }

        return new StreamReplay($pair, $fp, $open->adapterID, $this->streamScrub);
    }

    /**
     * Computes the open-time fingerprint, consuming the occurrence counter
     * for counter-addressed opens. The counter is keyed by the adapter id
     * plus the canonical identity (sans `n`), i.e. the adapter's
     * identifying tuple. n is -1 for content-addressed opens.
     *
     * @return array{string, int} [fingerprint, n]
     */
    private function streamOpenFingerprint(StreamOpen $open): array
    {
        $n = -1;
        if ($open->counter) {
            $base = StreamFingerprint::canonical($open);
            $n    = $this->streamOccurrences()->nextKey($open->adapterID . "\0" . $base);
        }

        return [StreamFingerprint::compute($open, $n), $n];
    }

    private function checkStreamOpen(StreamOpen $open, Mode $want, string $verb): void
    {
        if ($this->mode !== $want) {
            throw new \LogicException(sprintf(
                'xrr: %s requires %s mode (session is "%s")',
                $verb,
                $want->value,
                $this->mode->value
            ));
        }
        if ($open->adapterID === '') {
            throw new \InvalidArgumentException(sprintf('xrr: %s requires an adapter id', $verb));
        }
    }
}
