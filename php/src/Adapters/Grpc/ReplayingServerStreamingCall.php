<?php

declare(strict_types=1);

namespace HopTop\Xrr\Adapters\Grpc;

use Grpc\ServerStreamingCall;
use HopTop\Xrr\Exception\RecordedErrorException;
use HopTop\Xrr\Session;
use HopTop\Xrr\Stream\StreamType;

/**
 * Replays a recorded server-streaming RPC with no network.
 *
 * The parent constructor is deliberately not run: it would open a real
 * `Grpc\Call` on a channel. The cassette is located at {@see start}, where
 * the request message — this stream type's content address — arrives.
 */
final class ReplayingServerStreamingCall extends ServerStreamingCall
{
    use ReplaysStream;

    private ?RecordedErrorException $terminal = null;

    /**
     * @param array{0: class-string, 1: string}|callable|null $deserialize
     */
    public function __construct(
        private readonly Session $xrrSession,
        private readonly string $xrrService,
        private readonly string $xrrMethod,
        $deserialize
    ) {
        // No parent::__construct — replay never opens a channel or a call.
        $this->xrrDeserialize = $deserialize;
    }

    public function start($data, array $metadata = [], array $options = [])
    {
        $bytes = $data->serializeToString();

        $this->rp = $this->xrrSession->openStreamReplay(GrpcStream::open(
            $this->xrrSession,
            StreamType::Server,
            $this->xrrService,
            $this->xrrMethod,
            $bytes
        ));

        // Validate the request against the recorded send frame, then the
        // implicit half-close, exactly as the recording observed them.
        $this->rp->send($bytes);
        $this->rp->halfClose();
    }

    public function responses()
    {
        while (true) {
            try {
                $frame = $this->rp->recv();
            } catch (RecordedErrorException $e) {
                // A mid-stream error terminal ends iteration; the status is
                // surfaced by getStatus(), as with a live call.
                $this->terminal = $e;

                return;
            }
            if ($frame === null) {
                return;
            }
            yield $this->xrrDeserializeResponse($frame);
        }
    }

    public function getStatus()
    {
        if ($this->terminal === null) {
            // A caller that reads the status without draining the stream
            // must still observe an error terminal; probe for it.
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
        // The cassette format records no metadata (spec).
        return [];
    }
}
