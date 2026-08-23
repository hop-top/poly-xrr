<?php

declare(strict_types=1);

namespace HopTop\Xrr\Adapters\Grpc;

use Grpc\ClientStreamingCall;
use HopTop\Xrr\Session;
use HopTop\Xrr\Stream\StreamRecording;
use HopTop\Xrr\Stream\StreamType;

/**
 * Records a live client-streaming RPC: a stream of requests out, one
 * response back.
 *
 * Client opens are counter-addressed — no message is available at open — so
 * the recording opens as soon as the call is bound, consuming the session's
 * occurrence counter in the same order replay will.
 *
 * grpc-php's {@see ClientStreamingCall::wait} performs the half-close, the
 * response read and the status read in a single batch, so half_close, the
 * single recv frame and the terminal are all recorded there. That batch is
 * reimplemented rather than delegated to, because the parent returns only
 * the DESERIALIZED response and discards the wire bytes; re-serializing a
 * decoded message to recover them is not sound in PHP (no deterministic
 * serialization in either protobuf runtime). Frames must be the bytes that
 * actually crossed the wire.
 */
final class RecordingClientStreamingCall extends ClientStreamingCall
{
    use RecordsStatus;

    private ?StreamRecording $rec = null;

    public function bindXrr(Session $session, string $service, string $method): void
    {
        $this->rec = $session->openStreamRecord(GrpcStream::open(
            $session,
            StreamType::Client,
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

    public function wait()
    {
        $event = $this->call->startBatch([
            OP_SEND_CLOSE_FROM_CLIENT => true,
            OP_RECV_INITIAL_METADATA  => true,
            OP_RECV_MESSAGE           => true,
            OP_RECV_STATUS_ON_CLIENT  => true,
        ]);
        $this->metadata = $event->metadata;

        $status                  = $event->status;
        $this->trailing_metadata = $status->metadata;

        // The batch closed the send side.
        $this->rec?->recordHalfClose();
        if ($event->message !== null) {
            // Raw wire buffer, before deserialization.
            $this->rec?->recordRecv($event->message);
        }
        $this->finishFromStatus($this->rec, $status);

        return [$this->_deserializeResponse($event->message), $status];
    }
}
