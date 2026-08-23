<?php

declare(strict_types=1);

namespace HopTop\Xrr\Stream;

use Symfony\Component\Yaml\Yaml;

/**
 * Serializes a streamed interaction back to the two cassette YAML files.
 *
 * The envelope and stream sections are built by hand for byte control over
 * the spec's normative YAML rules, which a generic dumper does not honor:
 *
 * - `fingerprint` always a quoted string (an all-digit one would otherwise
 *   parse as an integer, a leading-zero one as YAML 1.1 octal);
 * - `message_text` always a quoted scalar (unquoted `on`, `12:30`, `null`
 *   would be corrupted by YAML 1.1 readers);
 * - `message_b64` standard base64, no whitespace or line wrapping;
 * - `frames: []` explicit for an empty direction.
 *
 * Quoted scalars use JSON string syntax, which is valid YAML double-quoted
 * style. The adapter-defined payload keeps using the YAML library.
 */
class StreamEmitter
{
    private const JSON_FLAGS = JSON_UNESCAPED_SLASHES | JSON_UNESCAPED_UNICODE | JSON_THROW_ON_ERROR;

    public function emitReq(StreamedInteraction $interaction): string
    {
        $out  = $this->head($interaction, $interaction->reqRecordedAt, null);
        $out .= $this->payloadBlock($interaction->reqPayload);
        $out .= "stream:\n";
        $out .= '  type: ' . $interaction->req->type->value . "\n";
        $out .= $this->framesBlock($interaction->req->frames);
        if ($interaction->req->halfClose !== null) {
            $out .= $this->eventBlock('half_close', $interaction->req->halfClose);
        }

        return $out;
    }

    public function emitResp(StreamedInteraction $interaction): string
    {
        $out  = $this->head($interaction, $interaction->respRecordedAt, $interaction->error);
        $out .= $this->payloadBlock($interaction->respPayload);
        $out .= "stream:\n";
        $out .= $this->framesBlock($interaction->resp->frames);
        $out .= $this->eventBlock('end', $interaction->resp->end);

        return $out;
    }

    private function head(StreamedInteraction $interaction, string $recordedAt, ?string $error): string
    {
        if ($recordedAt === '') {
            $recordedAt = (new \DateTimeImmutable('now', new \DateTimeZone('UTC')))->format('Y-m-d\TH:i:s\Z');
        }

        $out  = "xrr: \"1\"\n";
        $out .= 'adapter: ' . $interaction->adapter . "\n";
        $out .= 'fingerprint: ' . $this->quoted($interaction->fingerprint) . "\n";
        $out .= 'recorded_at: ' . $this->quoted($recordedAt) . "\n";
        if ($error !== null && $error !== '') {
            $out .= 'error: ' . $this->quoted($error) . "\n";
        }

        return $out;
    }

    /** @param array<string, mixed> $payload */
    private function payloadBlock(array $payload): string
    {
        if ($payload === []) {
            return "payload: {}\n";
        }

        return "payload:\n" . $this->indent(Yaml::dump($payload, 2, 2), 2);
    }

    /** @param list<Frame> $frames */
    private function framesBlock(array $frames): string
    {
        if ($frames === []) {
            return "  frames: []\n";
        }

        $out = "  frames:\n";
        foreach ($frames as $frame) {
            $out .= '    - seq: ' . $frame->seq . "\n";
            $out .= '      ' . $this->messageLine($frame) . "\n";
            if ($frame->atMs !== null) {
                $out .= '      at_ms: ' . $frame->atMs . "\n";
            }
        }

        return $out;
    }

    private function messageLine(Frame $frame): string
    {
        // message_text only when the bytes are valid UTF-8; otherwise the
        // spec requires message_b64. PHP's base64_encode never emits
        // whitespace or line breaks.
        if ($frame->text && preg_match('//u', $frame->bytes) === 1) {
            return 'message_text: ' . $this->quoted($frame->bytes);
        }

        return 'message_b64: "' . base64_encode($frame->bytes) . '"';
    }

    private function eventBlock(string $name, StreamEvent $event): string
    {
        $out = '  ' . $name . ":\n    seq: " . $event->seq . "\n";
        if ($event->atMs !== null) {
            $out .= '    at_ms: ' . $event->atMs . "\n";
        }

        return $out;
    }

    private function quoted(string $s): string
    {
        return json_encode($s, self::JSON_FLAGS);
    }

    private function indent(string $yaml, int $spaces): string
    {
        $pad   = str_repeat(' ', $spaces);
        $lines = explode("\n", rtrim($yaml, "\n"));

        return $pad . implode("\n" . $pad, $lines) . "\n";
    }
}
