<?php

declare(strict_types=1);

namespace HopTop\Xrr\Stream;

use HopTop\Xrr\Exception\RecordedErrorException;
use HopTop\Xrr\Exception\StreamMismatchException;

/**
 * Serves one recorded streamed interaction
 * (cassette-format-streaming.md, Matching and Replay Semantics).
 *
 * Send-side events are validated against the recording (order and bytes);
 * recv-side frames are delivered in seq order, never gated on send
 * progress. Timing is ignored: frames are delivered as fast as the client
 * consumes them.
 *
 * Terminal signals — one consistent falsy convention, mirroring Go's
 * io.EOF doubling as the post-completion send signal:
 * - {@see recv} returns null at end-of-stream (OK terminal);
 * - {@see send} returns false for a post-completion send (OK terminal);
 * - an error terminal throws {@see RecordedErrorException} in both places.
 */
class StreamReplay
{
    private int $sendIdx = 0;

    private int $recvIdx = 0;

    private ?StreamMismatchException $mismatch = null;

    public function __construct(
        private readonly StreamedInteraction $pair,
        private readonly string $fingerprint
    ) {}

    /** The open-time fingerprint of this interaction. */
    public function fingerprint(): string
    {
        return $this->fingerprint;
    }

    /** The recorded stream type. */
    public function type(): StreamType
    {
        return $this->pair->req->type;
    }

    /** @return array<string, mixed> the recorded open-request payload */
    public function reqPayload(): array
    {
        return $this->pair->reqPayload;
    }

    /**
     * The recorded terminal-response payload (for gRPC: the status code).
     * Available from open — adapters typically read it at terminal delivery.
     *
     * @return array<string, mixed>
     */
    public function respPayload(): array
    {
        return $this->pair->respPayload;
    }

    /**
     * Validates the i-th client message against recorded send frame i.
     * - i < S, equal bytes: accepted, returns true (the message is discarded).
     * - i < S, divergent bytes: stream mismatch — terminal for the handle.
     * - i >= S: the recording was already past its last observed send. With
     *   an OK terminal returns false (the post-completion stream-done
     *   signal) and does NOT poison the recv side; with an error terminal
     *   throws the recorded error. Bytes at i >= S are never compared.
     *
     * @throws StreamMismatchException
     * @throws RecordedErrorException
     */
    public function send(string $message): bool
    {
        if ($this->mismatch !== null) {
            throw $this->mismatch;
        }
        $i      = $this->sendIdx;
        $frames = $this->pair->req->frames;
        if ($i >= count($frames)) {
            $this->throwIfErrorTerminal();

            return false;
        }
        $recorded = $frames[$i]->bytes;
        if ($message !== $recorded) {
            throw $this->fail(new StreamMismatchException('send', $i, sprintf(
                'expected sha256 %s, got sha256 %s',
                hash('sha256', $recorded),
                hash('sha256', $message)
            )));
        }
        $this->sendIdx++;

        return true;
    }

    /**
     * Validates the client closing its send side: always accepted after all
     * recorded sends were observed (whether or not the recording has
     * half_close), a stream mismatch after fewer.
     *
     * @throws StreamMismatchException
     */
    public function halfClose(): void
    {
        if ($this->mismatch !== null) {
            throw $this->mismatch;
        }
        $s = count($this->pair->req->frames);
        if ($this->sendIdx < $s) {
            throw $this->fail(new StreamMismatchException('half_close', $this->sendIdx, sprintf(
                'half-close after %d sends, recording has %d',
                $this->sendIdx,
                $s
            )));
        }
    }

    /**
     * Delivers the j-th recorded recv frame's decoded bytes. At j = R it
     * returns the terminal — null for end-of-stream, or the recorded error
     * thrown as {@see RecordedErrorException} — and repeats it for every
     * later read. Recv never blocks on send-side progress.
     *
     * @throws StreamMismatchException
     * @throws RecordedErrorException
     */
    public function recv(): ?string
    {
        if ($this->mismatch !== null) {
            throw $this->mismatch;
        }
        $frames = $this->pair->resp->frames;
        if ($this->recvIdx >= count($frames)) {
            $this->throwIfErrorTerminal();

            return null;
        }

        return $frames[$this->recvIdx++]->bytes;
    }

    private function fail(StreamMismatchException $e): StreamMismatchException
    {
        $this->mismatch = $e;

        return $e;
    }

    /** @throws RecordedErrorException when the recording terminated in error */
    private function throwIfErrorTerminal(): void
    {
        if ($this->pair->error !== null && $this->pair->error !== '') {
            throw new RecordedErrorException($this->pair->error);
        }
    }
}
