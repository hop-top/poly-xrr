<?php

declare(strict_types=1);

namespace HopTop\Xrr\Stream;

use HopTop\Xrr\FileCassette;

/**
 * Accumulates the event log of one live stream and writes the cassette
 * pair at terminal. Events are stamped with at_ms (monotonic milliseconds
 * since open) and sequenced by one per-interaction counter in arrival
 * order. Only {@see finish} persists the pair — a stream that never
 * reaches terminal produces no cassette
 * (cassette-format-streaming.md, Record mode).
 */
class StreamRecording
{
    private readonly int $openedNs;

    private int $seq = 0;

    /** @var list<Frame> */
    private array $sends = [];

    /** @var list<Frame> */
    private array $recvs = [];

    private ?StreamEvent $halfClose = null;

    private bool $finished = false;

    /** @param array<string, mixed> $reqPayload */
    public function __construct(
        private readonly FileCassette $cassette,
        private readonly string $adapterID,
        private readonly string $fingerprint,
        private readonly StreamType $type,
        private readonly array $reqPayload
    ) {
        $this->openedNs = (int) hrtime(true);
    }

    /** The open-time fingerprint of this interaction. */
    public function fingerprint(): string
    {
        return $this->fingerprint;
    }

    /** Logs one client→server message (decoded bytes). Dropped after finish. */
    public function recordSend(string $message): void
    {
        if ($this->finished) {
            return;
        }
        $this->sends[] = new Frame($this->seq++, $message, $this->elapsedMs());
    }

    /** Logs one server→client message (decoded bytes). Dropped after finish. */
    public function recordRecv(string $message): void
    {
        if ($this->finished) {
            return;
        }
        $this->recvs[] = new Frame($this->seq++, $message, $this->elapsedMs());
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
            error: $error
        ));
    }

    private function elapsedMs(): int
    {
        return max(0, intdiv((int) hrtime(true) - $this->openedNs, 1_000_000));
    }
}
