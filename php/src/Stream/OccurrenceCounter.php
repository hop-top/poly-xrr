<?php

declare(strict_types=1);

namespace HopTop\Xrr\Stream;

/**
 * Deterministic per-session occurrence counter for streamed opens whose
 * fingerprint carries `n` (counter-addressed opens): the 0-based count of
 * prior opens of the same identifying tuple. One session object is one
 * counter domain; record and replay count identically.
 */
class OccurrenceCounter
{
    /** @var array<string, int> */
    private array $counts = [];

    /**
     * Returns the 0-based occurrence ordinal for an arbitrary identifying
     * key, then increments. Session-level opens key by the adapter id plus
     * the canonical identity (sans `n`), i.e. the adapter's identifying
     * tuple.
     */
    public function nextKey(string $key): int
    {
        $n = $this->counts[$key] ?? 0;

        $this->counts[$key] = $n + 1;

        return $n;
    }

    /**
     * gRPC-shaped convenience: the ordinal for a (service, method, type)
     * tuple. Counts in its own key space, independent of {@see nextKey}.
     */
    public function next(string $service, string $method, StreamType $type): int
    {
        return $this->nextKey($service . '/' . $method . '/' . $type->value);
    }
}
