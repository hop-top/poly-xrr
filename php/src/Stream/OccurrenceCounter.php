<?php

declare(strict_types=1);

namespace HopTop\Xrr\Stream;

/**
 * Deterministic per-session occurrence counter for streamed opens whose
 * fingerprint carries `n` (client/bidi): the 0-based count of prior opens
 * of the same identifying tuple. One session object is one counter domain;
 * record and replay count identically.
 */
class OccurrenceCounter
{
    /** @var array<string, int> */
    private array $counts = [];

    /** Returns the 0-based occurrence ordinal for this open, then increments. */
    public function next(string $service, string $method, StreamType $type): int
    {
        $key = $service . '/' . $method . '/' . $type->value;
        $n   = $this->counts[$key] ?? 0;

        $this->counts[$key] = $n + 1;

        return $n;
    }
}
