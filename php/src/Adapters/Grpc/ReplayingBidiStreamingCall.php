<?php

declare(strict_types=1);

namespace HopTop\Xrr\Adapters\Grpc;

use Grpc\BidiStreamingCall;
use HopTop\Xrr\Exception\RecordedErrorException;
use HopTop\Xrr\Session;
use HopTop\Xrr\Stream\StreamType;

/**
 * Replays a recorded bidirectional RPC with no network.
 *
 * Counter-addressed: the cassette is located at construction. Recv frames
 * are delivered in recorded order and are never gated on send progress, so
 * an interleaved conversation replays regardless of the order the client
 * happens to drive reads and writes in.
 */
final class ReplayingBidiStreamingCall extends BidiStreamingCall
{
    use ReplaysStream;

    private ?RecordedErrorException $terminal = null;

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
            StreamType::Bidi,
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
        $this->rp->send($data->serializeToString());
    }

    public function read()
    {
        try {
            $frame = $this->rp->recv();
        } catch (RecordedErrorException $e) {
            $this->terminal = $e;

            return null;
        }

        return $this->xrrDeserializeResponse($frame);
    }

    public function writesDone()
    {
        $this->rp->halfClose();
    }

    public function getStatus()
    {
        if ($this->terminal === null) {
            try {
                $this->rp->recv();
            } catch (RecordedErrorException $e) {
                $this->terminal = $e;
            }
        }

        return $this->xrrStatus($this->terminal);
    }

    public function getMetadata()
    {
        return [];
    }
}
