<?php

declare(strict_types=1);

namespace HopTop\Xrr\Stream;

use HopTop\Xrr\Exception\MalformedStreamException;
use HopTop\Xrr\Exception\ShapeMismatchException;

/**
 * Parses a streamed cassette pair (raw YAML envelopes) into the stream
 * model, enforcing the spec's validation rules
 * (cassette-format-streaming.md, "Validation Rules"):
 *
 *   1. stream on one file of the pair only            → reject
 *   2. req.stream.type missing / not server|client|bidi → reject
 *   3. frame lacks seq, or both/neither message encoding → reject
 *   4. frames list not strictly ascending in seq       → reject
 *   5. duplicate seq across the pair                   → reject
 *   6. end missing, or end.seq not the pair maximum    → reject
 *   7. message_b64 not valid standard base64           → reject
 *
 * base64 is validated explicitly: PHP's base64_decode($s, true) rejects
 * out-of-alphabet characters but silently tolerates whitespace, which the
 * spec forbids. Numbering is additionally required to be dense 0..N-1
 * (the writer obligation; sparse acceptance is a spec MAY we don't take).
 * Unknown extra fields are ignored (forward compat).
 */
class StreamParser
{
    private const B64_PATTERN = '{\A(?:[A-Za-z0-9+/]{4})*(?:[A-Za-z0-9+/]{2}==|[A-Za-z0-9+/]{3}=)?\z}';

    /**
     * @param array<mixed> $reqEnvelope
     * @param array<mixed> $respEnvelope
     * @throws MalformedStreamException
     * @throws ShapeMismatchException on a unary (stream-less) pair
     */
    public function parsePair(array $reqEnvelope, array $respEnvelope): StreamedInteraction
    {
        $reqHas  = array_key_exists('stream', $reqEnvelope);
        $respHas = array_key_exists('stream', $respEnvelope);

        if (!$reqHas && !$respHas) {
            throw new ShapeMismatchException('unary cassette pair loaded through the streaming path');
        }
        if ($reqHas !== $respHas) {
            throw new MalformedStreamException('stream present on one file of the pair only');
        }

        $req  = $this->parseReqStream($this->map($reqEnvelope['stream'], 'req stream'));
        $resp = $this->parseRespStream($this->map($respEnvelope['stream'], 'resp stream'));

        $this->validateSeqDomain($req, $resp);

        return new StreamedInteraction(
            adapter: $this->str($reqEnvelope['adapter'] ?? null, 'req adapter'),
            fingerprint: $this->str($reqEnvelope['fingerprint'] ?? null, 'req fingerprint'),
            req: $req,
            resp: $resp,
            reqPayload: $this->payload($reqEnvelope, 'req'),
            respPayload: $this->payload($respEnvelope, 'resp'),
            error: $this->error($respEnvelope),
            reqRecordedAt: $this->optionalStr($reqEnvelope['recorded_at'] ?? null),
            respRecordedAt: $this->optionalStr($respEnvelope['recorded_at'] ?? null)
        );
    }

    /** @param array<mixed> $stream */
    private function parseReqStream(array $stream): ReqStream
    {
        $rawType = $stream['type'] ?? null;
        $type    = is_string($rawType) ? StreamType::tryFrom($rawType) : null;
        if ($type === null) {
            throw new MalformedStreamException('req stream type missing or not one of server/client/bidi');
        }

        $halfClose = null;
        if (array_key_exists('half_close', $stream)) {
            $halfClose = $this->parseEvent($stream['half_close'], 'req half_close');
        }

        return new ReqStream($type, $this->parseFrames($stream['frames'] ?? [], 'req'), $halfClose);
    }

    /** @param array<mixed> $stream */
    private function parseRespStream(array $stream): RespStream
    {
        if (!array_key_exists('end', $stream)) {
            throw new MalformedStreamException('resp stream missing end event');
        }

        return new RespStream(
            $this->parseFrames($stream['frames'] ?? [], 'resp'),
            $this->parseEvent($stream['end'], 'resp end')
        );
    }

    /** @return list<Frame> */
    private function parseFrames(mixed $v, string $side): array
    {
        if ($v === null) {
            return []; // absent key — readers MUST treat as []
        }
        if (!is_array($v) || !array_is_list($v)) {
            throw new MalformedStreamException("$side frames must be a list");
        }

        $frames = [];
        $prev   = null;
        foreach ($v as $idx => $item) {
            if (!is_array($item)) {
                throw new MalformedStreamException("$side frame $idx must be a mapping");
            }

            $seq = $this->seq($item['seq'] ?? null, "$side frame $idx");

            $hasB64  = array_key_exists('message_b64', $item);
            $hasText = array_key_exists('message_text', $item);
            if ($hasB64 === $hasText) {
                throw new MalformedStreamException(
                    "$side frame $idx must carry exactly one of message_b64/message_text"
                );
            }

            if ($hasB64) {
                $bytes = $this->decodeB64($item['message_b64'], "$side frame $idx");
                $text  = false;
            } else {
                $raw = $item['message_text'];
                if (!is_string($raw)) {
                    throw new MalformedStreamException(
                        "$side frame $idx message_text must be a string scalar (writers must quote it)"
                    );
                }
                $bytes = $raw;
                $text  = true;
            }

            if ($prev !== null && $seq <= $prev) {
                throw new MalformedStreamException("$side frames not strictly ascending in seq");
            }
            $prev = $seq;

            $frames[] = new Frame($seq, $bytes, $this->atMs($item, "$side frame $idx"), $text);
        }

        return $frames;
    }

    private function parseEvent(mixed $v, string $ctx): StreamEvent
    {
        $event = $this->map($v, $ctx);

        return new StreamEvent($this->seq($event['seq'] ?? null, $ctx), $this->atMs($event, $ctx));
    }

    private function validateSeqDomain(ReqStream $req, RespStream $resp): void
    {
        $seqs = [];
        foreach ($req->frames as $frame) {
            $seqs[] = $frame->seq;
        }
        if ($req->halfClose !== null) {
            $seqs[] = $req->halfClose->seq;
        }
        foreach ($resp->frames as $frame) {
            $seqs[] = $frame->seq;
        }
        $seqs[] = $resp->end->seq;

        if (count(array_unique($seqs)) !== count($seqs)) {
            throw new MalformedStreamException('duplicate seq across the pair');
        }

        $max = max($seqs);
        if ($resp->end->seq !== $max) {
            throw new MalformedStreamException('end.seq is not the maximum seq of the interaction');
        }

        // Unique non-negative ints with max N-1 are exactly {0..N-1}.
        if ($max !== count($seqs) - 1) {
            throw new MalformedStreamException('seq numbering is not dense 0..N-1 across the pair');
        }
    }

    private function decodeB64(mixed $v, string $ctx): string
    {
        if (!is_string($v)) {
            throw new MalformedStreamException("$ctx message_b64 must be a string");
        }
        // Explicit strict check: base64_decode($s, true) rejects
        // out-of-alphabet characters but tolerates whitespace.
        if (preg_match(self::B64_PATTERN, $v) !== 1) {
            throw new MalformedStreamException(
                "$ctx message_b64 is not valid standard base64 (alphabet only, no whitespace, RFC 4648 padding)"
            );
        }

        $bytes = base64_decode($v, true);
        if ($bytes === false) {
            throw new MalformedStreamException("$ctx message_b64 is not valid standard base64");
        }

        return $bytes;
    }

    private function seq(mixed $v, string $ctx): int
    {
        if (!is_int($v) || $v < 0) {
            throw new MalformedStreamException("$ctx seq must be a non-negative integer");
        }

        return $v;
    }

    /** @param array<mixed> $event */
    private function atMs(array $event, string $ctx): ?int
    {
        $v = $event['at_ms'] ?? null;
        if ($v === null) {
            return null; // readers MUST tolerate absence
        }
        if (!is_int($v) || $v < 0) {
            throw new MalformedStreamException("$ctx at_ms must be a non-negative integer");
        }

        return $v;
    }

    /** @return array<mixed> */
    private function map(mixed $v, string $ctx): array
    {
        if (!is_array($v)) {
            throw new MalformedStreamException("$ctx must be a mapping");
        }

        return $v;
    }

    /**
     * @param array<mixed> $envelope
     * @return array<string, mixed>
     */
    private function payload(array $envelope, string $side): array
    {
        $payload = $envelope['payload'] ?? null;
        if (!is_array($payload)) {
            throw new MalformedStreamException("$side payload missing or not a mapping");
        }

        $out = [];
        foreach ($payload as $key => $value) {
            $out[(string) $key] = $value;
        }

        return $out;
    }

    /** @param array<mixed> $envelope */
    private function error(array $envelope): ?string
    {
        $error = $envelope['error'] ?? null;
        if ($error === null || $error === '') {
            return null;
        }
        if (!is_string($error)) {
            throw new MalformedStreamException('resp error must be a string');
        }

        return $error;
    }

    private function str(mixed $v, string $ctx): string
    {
        if (!is_string($v)) {
            throw new MalformedStreamException("$ctx must be a string");
        }

        return $v;
    }

    private function optionalStr(mixed $v): string
    {
        return is_string($v) ? $v : '';
    }
}
