<?php

declare(strict_types=1);

namespace HopTop\Xrr\Adapters\Grpc;

use Grpc\ServerStreamingCall;
use HopTop\Xrr\Session;
use HopTop\Xrr\Stream\StreamRecording;
use HopTop\Xrr\Stream\StreamType;

/**
 * Records a live server-streaming RPC: one request message out, a stream of
 * responses back.
 *
 * The recording opens at {@see start}, where the single request message
 * first becomes available — server streams are content-addressed by that
 * message (cassette-format-streaming.md, Fingerprint Algorithms), so no
 * fingerprint exists before it. `half_close` is recorded immediately after
 * the send: generated clients half-close implicitly with the request, which
 * the spec's `server` mapping requires.
 *
 * {@see responses} reimplements grpc-php's batch loop rather than delegating
 * to the parent, because the parent yields only DESERIALIZED messages and
 * discards the wire bytes. Re-serializing a decoded message to recover them
 * is not sound in PHP: neither protobuf runtime offers deterministic
 * serialization, map entries are unordered, and the pure-PHP and C runtimes
 * can emit different bytes for the same message. Frames must be the bytes
 * that actually crossed the wire — captured, never reconstructed.
 */
final class RecordingServerStreamingCall extends ServerStreamingCall
{
    use RecordsStatus;

    private ?StreamRecording $rec = null;

    private Session $xrrSession;

    private string $xrrService;

    private string $xrrMethod;

    /**
     * Binds the recorder. Called immediately after construction by
     * {@see XrrCallInvoker}: the parent constructor signature belongs to
     * grpc-php and must stay untouched.
     */
    public function bindXrr(Session $session, string $service, string $method): void
    {
        $this->xrrSession = $session;
        $this->xrrService = $service;
        $this->xrrMethod  = $method;
    }

    public function start($data, array $metadata = [], array $options = [])
    {
        // The request bytes are produced here by the client itself, so they
        // ARE the wire bytes — no reconstruction involved.
        $bytes = $this->_serializeMessage($data);

        $this->rec = $this->xrrSession->openStreamRecord(GrpcStream::open(
            $this->xrrSession,
            StreamType::Server,
            $this->xrrService,
            $this->xrrMethod,
            $bytes
        ));

        parent::start($data, $metadata, $options);

        $this->rec->recordSend($bytes);
        // Generated clients half-close implicitly with the request; the
        // spec's server mapping requires half_close immediately after the
        // single send frame.
        $this->rec->recordHalfClose();
    }

    public function responses()
    {
        $batch = [OP_RECV_MESSAGE => true];
        if ($this->metadata === null) {
            $batch[OP_RECV_INITIAL_METADATA] = true;
        }
        $read_event = $this->call->startBatch($batch);
        if ($this->metadata === null) {
            $this->metadata = $read_event->metadata;
        }

        $response = $read_event->message;
        while ($response !== null) {
            // $response is the raw wire buffer, before deserialization.
            $this->rec?->recordRecv($response);
            yield $this->_deserializeResponse($response);
            $response = $this->call->startBatch([
                OP_RECV_MESSAGE => true,
            ])->message;
        }
    }

    public function getStatus()
    {
        $status = parent::getStatus();
        $this->finishFromStatus($this->rec, $status);

        return $status;
    }
}
