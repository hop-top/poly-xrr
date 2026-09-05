<?php

declare(strict_types=1);

namespace HopTop\Xrr\Stream;

use HopTop\Xrr\FileCassette;
use HopTop\Xrr\MonotonicClock;
use Psr\Clock\ClockInterface;

/**
 * Accumulates the event log of one live stream and writes the cassette
 * pair at terminal. Events are stamped with at_ms (milliseconds since the
 * clock's reading at open; monotonic under the default clock) and
 * sequenced by one per-interaction counter in arrival order. Only
 * {@see finish} persists the pair — a stream that never reaches terminal
 * produces no cassette (cassette-format-streaming.md, Record mode).
 */
class StreamRecording
{
    private readonly ClockInterface $clock;

    private readonly \DateTimeImmutable $opened;

    private int $seq = 0;

    /** @var list<Frame> */
    private array $sends = [];

    /** @var list<Frame> */
    private array $recvs = [];

    private ?StreamEvent $halfClose = null;

    private bool $finished = false;

    /**
     * @param array<string, mixed> $reqPayload
     * @param ?StreamScrub $scrub frame-level scrub hook; null retains
     *   frames verbatim (cassette-format-streaming.md, REDACTION WARNING)
     * @param ?ClockInterface $clock source of every timestamp in the pair:
     *   `at_ms` is the elapsed time between its reading at open and at
     *   each event, `recorded_at` its reading at finish. Null reads a
     *   {@see MonotonicClock}.
     */
    public function __construct(
        private readonly FileCassette $cassette,
        private readonly string $adapterID,
        private readonly string $fingerprint,
        private readonly StreamType $type,
        private readonly array $reqPayload,
        private readonly ?StreamScrub $scrub = null,
        ?ClockInterface $clock = null
    ) {
        $this->clock  = $clock ?? new MonotonicClock();
        $this->opened = $this->clock->now();
    }

    /** The open-time fingerprint of this interaction. */
    public function fingerprint(): string
    {
        return $this->fingerprint;
    }

    /**
     * Logs one client→server message (decoded bytes), scrubbed by the
     * session's frame scrub hook before it is retained. Dropped after
     * finish.
     */
    public function recordSend(string $message): void
    {
        if ($this->finished) {
            return;
        }
        $this->sends[] = new Frame(
            $this->seq++,
            $this->scrubFrame(StreamDirection::Send, $message),
            $this->elapsedMs()
        );
    }

    /**
     * Logs one server→client message (decoded bytes), scrubbed by the
     * session's frame scrub hook before it is retained. Dropped after
     * finish.
     */
    public function recordRecv(string $message): void
    {
        if ($this->finished) {
            return;
        }
        $this->recvs[] = new Frame(
            $this->seq++,
            $this->scrubFrame(StreamDirection::Recv, $message),
            $this->elapsedMs()
        );
    }

    private function scrubFrame(StreamDirection $dir, string $message): string
    {
        return $this->scrub?->scrub($dir, $this->adapterID, $this->type, $message) ?? $message;
    }

    /**
     * Logs the client closing its send side. It occurs at most once;
     * repeats and post-terminal calls are dropped, matching their
     * real-world no-op.
     */
    public function recordHalfClose(): void
    {
        if ($this->finished || $this->halfClose !== null) {
            return;
        }
        $this->halfClose = new StreamEvent($this->seq++, $this->elapsedMs());
    }

    /**
     * Records the terminal event and persists the pair. $terminal is null
     * for an OK terminal; a Throwable or non-empty error string is
     * persisted as the resp envelope `error` field so replay re-emits it.
     * No events are recorded after finish, and calling it twice throws.
     *
     * @param array<string, mixed> $respPayload
     * @throws \LogicException when the stream already finished
     */
    public function finish(array $respPayload, string|\Throwable|null $terminal = null): void
    {
        if ($this->finished) {
            throw new \LogicException('xrr: stream already finished');
        }
        $this->finished = true;

        $end = new StreamEvent($this->seq++, $this->elapsedMs());
        $recordedAt = $this->clock->now()
            ->setTimezone(new \DateTimeZone('UTC'))
            ->format('Y-m-d\TH:i:s\Z');

        $error = $terminal instanceof \Throwable ? $terminal->getMessage() : $terminal;
        if ($error === '') {
            $error = null;
        }

        $this->cassette->saveStreamed(new StreamedInteraction(
            adapter: $this->adapterID,
            fingerprint: $this->fingerprint,
            req: new ReqStream($this->type, $this->sends, $this->halfClose),
            resp: new RespStream($this->recvs, $end),
            reqPayload: $this->reqPayload,
            respPayload: $respPayload,
            error: $error,
            reqRecordedAt: $recordedAt,
            respRecordedAt: $recordedAt
        ));
    }

    private function elapsedMs(): int
    {
        return max(0, intdiv(self::micros($this->clock->now()) - self::micros($this->opened), 1_000));
    }

    /** Microseconds since the epoch — the finest resolution a PSR-20 reading carries. */
    private static function micros(\DateTimeImmutable $at): int
    {
        return (int) $at->format('U') * 1_000_000 + (int) $at->format('u');
    }
}
