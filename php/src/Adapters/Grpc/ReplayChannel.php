<?php

declare(strict_types=1);

namespace HopTop\Xrr\Adapters\Grpc;

/**
 * A channel-shaped stand-in used in replay mode, so a replay run never
 * dials anything.
 *
 * {@see \Grpc\BaseStub} keeps the object its call invoker hands back and
 * calls a small surface on it (`getTarget`, `getConnectivityState`,
 * `watchConnectivityState`, `close`). Replaying call objects hold only a
 * cassette handle and never touch the channel at all, so nothing here needs
 * to reach a network — which is precisely the guarantee replay rests on: no
 * socket is opened, no target resolved, no connection attempted.
 *
 * This deliberately does NOT extend `Grpc\Channel`: that class comes from
 * the C extension and constructing it starts real channel machinery. Being
 * a plain object also means replay works with `ext-grpc` absent entirely.
 */
final class ReplayChannel
{
    /** Mirrors GRPC\CHANNEL_READY without depending on the extension. */
    private const CHANNEL_READY = 2;

    public function __construct(private readonly string $target = '') {}

    public function getTarget(): string
    {
        return $this->target;
    }

    /** Always reports ready: the cassette is always "connected". */
    public function getConnectivityState(bool $try_to_connect = false): int
    {
        return self::CHANNEL_READY;
    }

    /** Never transitions: the state is constant, so nothing to wait for. */
    public function watchConnectivityState($new_state, $deadline): bool
    {
        return false;
    }

    public function close(): void
    {
        // Nothing to release.
    }
}
