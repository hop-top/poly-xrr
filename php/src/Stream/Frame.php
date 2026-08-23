<?php

declare(strict_types=1);

namespace HopTop\Xrr\Stream;

/**
 * One message frame of a streamed interaction.
 *
 * $bytes always holds the DECODED message bytes; hashing and comparison
 * operate on them, so the on-disk encoding (`message_b64` vs
 * `message_text`) is interchangeable. $text records the encoding
 * preference for re-emit — the emitter honors it only when the bytes are
 * valid UTF-8, falling back to base64 otherwise (per spec, writers MAY use
 * `message_text` only for valid UTF-8).
 */
class Frame
{
    public function __construct(
        public readonly int $seq,
        public readonly string $bytes,
        public readonly ?int $atMs = null,
        public readonly bool $text = false
    ) {}
}
