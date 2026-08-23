<?php

declare(strict_types=1);

namespace HopTop\Xrr\Adapters\Grpc;

use Grpc\CallInvoker;
use Grpc\Channel;
use Grpc\DefaultCallInvoker;
use HopTop\Xrr\Mode;
use HopTop\Xrr\Session;

/**
 * The gRPC streaming adapter's entry point: a {@see CallInvoker} that
 * dispatches every streamed RPC on the session mode — record tees the live
 * stream into a cassette pair, replay serves the recorded conversation with
 * no network, passthrough is transparent.
 *
 * Wire it into any generated stub through grpc-php's documented
 * `grpc_call_invoker` channel option:
 *
 * ```php
 * $session = new Session(Mode::Replay, new FileCassette($dir));
 * $client  = new MyServiceClient($target, [
 *     'credentials'        => ChannelCredentials::createInsecure(),
 *     'grpc_call_invoker'  => new XrrCallInvoker($session),
 * ]);
 * ```
 *
 * The invoker is the seam grpc-php itself provides for substituting call
 * objects, so no generated code changes and no stub subclassing is needed.
 * In replay mode {@see createChannelFactory} returns a channel-shaped
 * placeholder that is never dialed: the replaying call classes hold only a
 * cassette handle, so a replay run connects to nothing.
 *
 * Unary RPCs are delegated untouched — they keep the v1 unary format and
 * never migrate to the streaming path
 * (cassette-format-streaming.md, Stream Types).
 */
final class XrrCallInvoker implements CallInvoker
{
    private readonly CallInvoker $inner;

    public function __construct(
        private readonly Session $session,
        ?CallInvoker $inner = null
    ) {
        $this->inner = $inner ?? new DefaultCallInvoker();
    }

    public function createChannelFactory($hostname, $opts)
    {
        if ($this->session->mode() === Mode::Replay) {
            // Replay must not dial. grpc-php only needs a Channel-shaped
            // object here; the replaying calls never touch it.
            return new ReplayChannel($hostname);
        }

        return $this->inner->createChannelFactory($hostname, $opts);
    }

    public function UnaryCall($channel, $method, $deserialize, $options)
    {
        // Unary RPCs keep the v1 unary shape; this adapter owns streams.
        return $this->inner->UnaryCall($channel, $method, $deserialize, $options);
    }

    public function ServerStreamingCall($channel, $method, $deserialize, $options)
    {
        if ($this->session->mode() === Mode::Passthrough) {
            return $this->inner->ServerStreamingCall($channel, $method, $deserialize, $options);
        }

        [$service, $rpc] = GrpcStream::splitFullMethod($method);

        if ($this->session->mode() === Mode::Replay) {
            return new ReplayingServerStreamingCall($this->session, $service, $rpc, $deserialize);
        }

        $call = new RecordingServerStreamingCall($channel, $method, $deserialize, $options);
        $call->bindXrr($this->session, $service, $rpc);

        return $call;
    }

    public function ClientStreamingCall($channel, $method, $deserialize, $options)
    {
        if ($this->session->mode() === Mode::Passthrough) {
            return $this->inner->ClientStreamingCall($channel, $method, $deserialize, $options);
        }

        [$service, $rpc] = GrpcStream::splitFullMethod($method);

        if ($this->session->mode() === Mode::Replay) {
            return new ReplayingClientStreamingCall($this->session, $service, $rpc, $deserialize);
        }

        $call = new RecordingClientStreamingCall($channel, $method, $deserialize, $options);
        $call->bindXrr($this->session, $service, $rpc);

        return $call;
    }

    public function BidiStreamingCall($channel, $method, $deserialize, $options)
    {
        if ($this->session->mode() === Mode::Passthrough) {
            return $this->inner->BidiStreamingCall($channel, $method, $deserialize, $options);
        }

        [$service, $rpc] = GrpcStream::splitFullMethod($method);

        if ($this->session->mode() === Mode::Replay) {
            return new ReplayingBidiStreamingCall($this->session, $service, $rpc, $deserialize);
        }

        $call = new RecordingBidiStreamingCall($channel, $method, $deserialize, $options);
        $call->bindXrr($this->session, $service, $rpc);

        return $call;
    }
}
