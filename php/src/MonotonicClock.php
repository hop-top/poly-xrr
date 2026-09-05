<?php

declare(strict_types=1);

namespace HopTop\Xrr;

use Psr\Clock\ClockInterface;

/**
 * The session's default clock: PSR-20 readings driven by the monotonic
 * counter, so consecutive `now()` calls never go backwards even when the
 * wall clock is stepped. The wall-clock offset is captured once, at
 * construction; every later reading is that offset plus the monotonic
 * counter (the construction Symfony's MonotonicClock uses).
 *
 * Streamed recordings stamp `at_ms` (milliseconds since stream open, which
 * the spec requires to be monotonic) and the envelope `recorded_at` from
 * one clock. Reading both from this object honours the spec by default
 * while keeping the clock injectable ({@see Session}) for cassettes whose
 * bytes must not depend on when they were recorded.
 */
final class MonotonicClock implements ClockInterface
{
    /** Wall-clock microseconds minus monotonic microseconds, fixed at construction. */
    private readonly int $offsetUs;

    public function __construct()
    {
        $this->offsetUs = (int) (microtime(true) * 1_000_000) - intdiv((int) hrtime(true), 1_000);
    }

    public function now(): \DateTimeImmutable
    {
        $us = $this->offsetUs + intdiv((int) hrtime(true), 1_000);

        return new \DateTimeImmutable(sprintf('@%d.%06d', intdiv($us, 1_000_000), $us % 1_000_000));
    }
}
