<?php

declare(strict_types=1);

namespace HopTop\Xrr\Adapters\Grpc;

use Grpc\ClientStreamingCall;
use HopTop\Xrr\Exception\RecordedErrorException;
use HopTop\Xrr\Session;
use HopTop\Xrr\Stream\StreamType;

/**
 * Replays a recorded client-streaming RPC with no network.
 *
 * Counter-addressed: the cassette is located at construction, consuming the
 * session's occurrence counter in the same order record mode did. The
 * parent constructor is deliberately not run — replay opens no channel.
 */
final class ReplayingClientStreamingCall extends ClientStreamingCall
{
    use ReplaysStream;

    /**
     * @param array{0: class-string, 1: string}|callable|null $deserialize
     */
    public function __construct(
        Session $session,
        string $service,
        string $method,
        $deserialize
    ) {
        $this->xrrDeserialize = $deserialize;
        $this->rp             = $session->openStreamReplay(GrpcStream::open(
            $session,
            StreamType::Client,
            $service,
            $method
        ));
    }

    public function start(array $metadata = [])
    {
        // Nothing to do: no channel, and metadata is not recorded.
    }

    public function write($data, array $options = [])
    {
        // Validated against the recording byte-for-byte; a divergence
        // throws StreamMismatchException.
        $this->rp->send($data->serializeToString());
    }

    public function wait()
    {
        $terminal = null;
        $response = null;

        try {
            $this->rp->halfClose();
            $frame    = $this->rp->recv();
            $response = $this->xrrDeserializeResponse($frame);
        } catch (RecordedErrorException $e) {
            $terminal = $e;
        }

        return [$response, $this->xrrStatus($terminal)];
    }

    public function getMetadata()
    {
        return [];
    }
}
