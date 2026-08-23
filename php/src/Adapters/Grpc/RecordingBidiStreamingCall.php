<?php

declare(strict_types=1);

namespace HopTop\Xrr\Adapters\Grpc;

use Grpc\BidiStreamingCall;
use HopTop\Xrr\Session;
use HopTop\Xrr\Stream\StreamRecording;
use HopTop\Xrr\Stream\StreamType;

/**
 * Records a live bidirectional RPC: both directions stream freely.
 *
 * Bidi opens are counter-addressed, so the recording opens as soon as the
 * call is bound. Frames are sequenced in true arrival order by the core's
 * single per-interaction counter, which is what makes an interleaved
 * conversation replayable.
 *
 * {@see read} reimplements grpc-php's recv batch rather than delegating,
 * because the parent returns only the DESERIALIZED message and discards the
 * wire bytes; re-serializing a decoded message to recover them is not sound
 * in PHP (no deterministic serialization in either protobuf runtime).
 *
 * NOTE ON DUPLEX: every read and write is its own blocking batch in
 * ext-grpc, so a single PHP process drives a bidi RPC half-duplex — it
 * cannot write while blocked in a read. Recording is unaffected (frames are
 * sequenced as they actually occur), but a conversation requiring true
 * concurrent duplex cannot be driven from one process.
 */
final class RecordingBidiStreamingCall extends BidiStreamingCall
{
    use RecordsStatus;

    private ?StreamRecording $rec = null;

    public function bindXrr(Session $session, string $service, string $method): void
    {
        $this->rec = $session->openStreamRecord(GrpcStream::open(
            $session,
            StreamType::Bidi,
            $service,
            $method
        ));
    }

    public function write($data, array $options = [])
    {
        $bytes = $this->_serializeMessage($data);
        parent::write($data, $options);
        $this->rec?->recordSend($bytes);
    }

    public function read()
    {
        $batch = [OP_RECV_MESSAGE => true];
        if ($this->metadata === null) {
            $batch[OP_RECV_INITIAL_METADATA] = true;
        }
        $read_event = $this->call->startBatch($batch);
        if ($this->metadata === null) {
            $this->metadata = $read_event->metadata;
        }

        if ($read_event->message !== null) {
            // Raw wire buffer, before deserialization.
            $this->rec?->recordRecv($read_event->message);
        }

        return $this->_deserializeResponse($read_event->message);
    }

    public function writesDone()
    {
        parent::writesDone();
        $this->rec?->recordHalfClose();
    }

    public function getStatus()
    {
        $status = parent::getStatus();
        $this->finishFromStatus($this->rec, $status);

        return $status;
    }
}
