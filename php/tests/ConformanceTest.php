<?php

declare(strict_types=1);

namespace HopTop\Xrr\Tests;

use HopTop\Xrr\Exception\MalformedStreamException;
use HopTop\Xrr\FileCassette;
use HopTop\Xrr\Stream\OccurrenceCounter;
use HopTop\Xrr\Stream\StreamedInteraction;
use HopTop\Xrr\Stream\StreamFingerprint;
use HopTop\Xrr\Stream\StreamType;
use PHPUnit\Framework\TestCase;
use Symfony\Component\Yaml\Yaml;

class ConformanceTest extends TestCase
{
    private function fixturesDir(): string
    {
        return dirname(__DIR__, 2) . '/spec/fixtures';
    }

    /** @return list<string> fixture dir names */
    private function fixtureDirNames(): array
    {
        $fixturesDir = $this->fixturesDir();
        $entries     = array_values(array_filter(
            scandir($fixturesDir),
            fn($e) => $e !== '.' && $e !== '..' && is_dir($fixturesDir . '/' . $e)
        ));
        $this->assertNotEmpty($entries, 'no fixture dirs found');

        return $entries;
    }

    /**
     * Manifest interactions in a spec-conforming open order.
     *
     * `interactions` is an unordered set (cassette-format-streaming.md,
     * Manifest Extension), so file order is not open order. Entries sharing a
     * counter domain — the (service, method, stream type) tuple of a
     * client/bidi open — are ordered ascending by the req payload's `n`;
     * server streams, distinct domains and non-streamed entries are
     * order-independent and keyed apart so they never interleave into a
     * domain's ascending-n run.
     *
     * @return list<array{adapter: string, fingerprint: string, streamed: bool}>
     */
    private function manifestInteractions(string $entry): array
    {
        $dir          = $this->fixturesDir() . '/' . $entry;
        $manifestPath = $dir . '/manifest.yaml';
        $this->assertFileExists($manifestPath, "manifest.yaml missing in $entry");

        $manifest     = Yaml::parseFile($manifestPath);
        $interactions = [];
        foreach ($manifest['interactions'] ?? [] as $interaction) {
            $interactions[] = [
                'adapter'     => $interaction['adapter'],
                'fingerprint' => $interaction['fingerprint'],
                'streamed'    => (bool) ($interaction['streamed'] ?? false),
            ];
        }

        $keyOf = function (array $i) use ($dir): array {
            if (!$i['streamed']) {
                return ['', '', '', 0];
            }
            $req = Yaml::parseFile(
                $dir . '/' . $i['adapter'] . '-' . $i['fingerprint'] . '.req.yaml'
            );
            $type = $req['stream']['type'];
            if ($type === 'server') {
                return ['', '', '', 0];
            }

            return [
                $req['payload']['service'] ?? '',
                $req['payload']['method'] ?? '',
                $type,
                $req['payload']['n'],
            ];
        };

        usort($interactions, fn (array $a, array $b) => $keyOf($a) <=> $keyOf($b));

        return $interactions;
    }

    public function testFixturesDirExists(): void
    {
        $this->assertDirectoryExists($this->fixturesDir());
    }

    public function testUnaryFixtures(): void
    {
        $seen = 0;
        foreach ($this->fixtureDirNames() as $entry) {
            $cassette = new FileCassette($this->fixturesDir() . '/' . $entry);
            foreach ($this->manifestInteractions($entry) as $interaction) {
                if ($interaction['streamed']) {
                    continue;
                }
                $seen++;

                $data = $cassette->load($interaction['adapter'], $interaction['fingerprint']);

                $this->assertArrayHasKey('req', $data,
                    "missing req for {$interaction['adapter']}/{$interaction['fingerprint']} in $entry");
                $this->assertArrayHasKey('resp', $data,
                    "missing resp for {$interaction['adapter']}/{$interaction['fingerprint']} in $entry");
            }
        }

        $this->assertGreaterThan(0, $seen, 'no unary fixture interactions found');
    }

    public function testStreamedFixturesRoundTrip(): void
    {
        $seen = 0;
        foreach ($this->fixtureDirNames() as $entry) {
            $cassette = new FileCassette($this->fixturesDir() . '/' . $entry);
            foreach ($this->manifestInteractions($entry) as $interaction) {
                if (!$interaction['streamed']) {
                    continue;
                }
                $seen++;

                $pair = $cassette->loadStreamed($interaction['adapter'], $interaction['fingerprint']);
                $this->assertSame($interaction['fingerprint'], $pair->fingerprint, "fingerprint field in $entry");

                // Re-emit into a fresh dir, reload, compare field-for-field.
                $tmp = sys_get_temp_dir() . '/xrr_conf_' . uniqid();
                mkdir($tmp);
                $reCassette = new FileCassette($tmp);
                $reCassette->saveStreamed($pair);
                $reloaded = $reCassette->loadStreamed($interaction['adapter'], $interaction['fingerprint']);

                $this->assertSameInteraction($pair, $reloaded, "$entry {$interaction['fingerprint']}");
            }
        }

        $this->assertGreaterThan(0, $seen, 'no streamed fixture interactions found');
    }

    public function testStreamedFingerprintsMatchFilenames(): void
    {
        foreach ($this->fixtureDirNames() as $entry) {
            $cassette = new FileCassette($this->fixturesDir() . '/' . $entry);
            $counter  = new OccurrenceCounter(); // one session per fixture dir
            foreach ($this->manifestInteractions($entry) as $interaction) {
                if (!$interaction['streamed'] || $interaction['adapter'] !== 'grpc') {
                    continue;
                }

                $pair    = $cassette->loadStreamed($interaction['adapter'], $interaction['fingerprint']);
                $service = $pair->reqPayload['service'];
                $method  = $pair->reqPayload['method'];
                $this->assertIsString($service);
                $this->assertIsString($method);

                $computed = match ($pair->req->type) {
                    StreamType::Server => StreamFingerprint::server($service, $method, $pair->req->frames[0]->bytes),
                    StreamType::Client => StreamFingerprint::client($service, $method, $counter->next($service, $method, StreamType::Client)),
                    StreamType::Bidi   => StreamFingerprint::bidi($service, $method, $counter->next($service, $method, StreamType::Bidi)),
                };

                $this->assertSame(
                    $interaction['fingerprint'],
                    $computed,
                    "recomputed fingerprint mismatch in $entry"
                );
            }
        }
    }

    public function testClientStreamRepeatScriptedTwoOpens(): void
    {
        // Spec's n=1 obligation: one session, two sequential opens of
        // (files.FileService, Upload, client) against this dir.
        $cassette = new FileCassette($this->fixturesDir() . '/grpc-client-stream-repeat');
        $counter  = new OccurrenceCounter();

        $fpFirst = StreamFingerprint::client('files.FileService', 'Upload',
            $counter->next('files.FileService', 'Upload', StreamType::Client));
        $this->assertSame('2bebfd6f', $fpFirst);
        $first = $cassette->loadStreamed('grpc', $fpFirst);
        $this->assertSame(0, $first->reqPayload['n'], 'informational n on first open');
        $this->assertSame(["alpha\n"], array_map(fn($f) => $f->bytes, $first->req->frames));

        $fpSecond = StreamFingerprint::client('files.FileService', 'Upload',
            $counter->next('files.FileService', 'Upload', StreamType::Client));
        $this->assertSame('b27b5fe1', $fpSecond);
        $second = $cassette->loadStreamed('grpc', $fpSecond);
        $this->assertSame(1, $second->reqPayload['n'], 'informational n on second open');
        $this->assertSame(["beta-1\n", "beta-2\n"], array_map(fn($f) => $f->bytes, $second->req->frames));
    }

    public function testMalformedBase64FixtureRejectedByPath(): void
    {
        // Deliberately absent from its manifest; targeted by path per its README.
        $cassette = new FileCassette($this->fixturesDir() . '/grpc-stream-malformed-b64');

        $this->expectException(MalformedStreamException::class);
        $this->expectExceptionMessage('base64');
        $cassette->loadStreamed('grpc', '8dbfb222');
    }

    public function testSseTextScalarsDecodeExactly(): void
    {
        $cassette = new FileCassette($this->fixturesDir() . '/sse-text-scalars');
        $pair     = $cassette->loadStreamed('sse', '66ecc77a');

        $this->assertSame(
            ['on', '12:30', 'null', ' leading', 'trailing ', '  padded  '],
            array_map(fn($f) => $f->bytes, $pair->resp->frames)
        );
        $this->assertNull($pair->req->halfClose);
        $this->assertSame(6, $pair->resp->end->seq);
    }

    public function testMidStreamErrorFixtureExposesRecordedError(): void
    {
        $cassette = new FileCassette($this->fixturesDir() . '/grpc-stream-error');
        $pair     = $cassette->loadStreamed('grpc', '9e8c4d4c');

        $this->assertSame('rpc error: code = Unavailable desc = connection reset', $pair->error);
        $this->assertSame(14, $pair->respPayload['status_code']);
        $this->assertSame(["log-chunk-1\n", "log-chunk-2\n"], array_map(fn($f) => $f->bytes, $pair->resp->frames));
    }

    private function assertSameInteraction(StreamedInteraction $a, StreamedInteraction $b, string $ctx): void
    {
        $this->assertSame($a->adapter, $b->adapter, "$ctx adapter");
        $this->assertSame($a->fingerprint, $b->fingerprint, "$ctx fingerprint");
        $this->assertSame($a->error, $b->error, "$ctx error");
        $this->assertSame($a->reqRecordedAt, $b->reqRecordedAt, "$ctx req recorded_at");
        $this->assertSame($a->respRecordedAt, $b->respRecordedAt, "$ctx resp recorded_at");
        $this->assertEquals($a->reqPayload, $b->reqPayload, "$ctx req payload");
        $this->assertEquals($a->respPayload, $b->respPayload, "$ctx resp payload");
        $this->assertSame($a->req->type, $b->req->type, "$ctx stream type");

        $this->assertSameEvent($a->req->halfClose, $b->req->halfClose, "$ctx half_close");
        $this->assertSameEvent($a->resp->end, $b->resp->end, "$ctx end");
        $this->assertSameFrames($a->req->frames, $b->req->frames, "$ctx req frames");
        $this->assertSameFrames($a->resp->frames, $b->resp->frames, "$ctx resp frames");
    }

    /**
     * @param list<\HopTop\Xrr\Stream\Frame> $a
     * @param list<\HopTop\Xrr\Stream\Frame> $b
     */
    private function assertSameFrames(array $a, array $b, string $ctx): void
    {
        $this->assertSameSize($a, $b, $ctx);
        foreach ($a as $idx => $frame) {
            $this->assertSame($frame->seq, $b[$idx]->seq, "$ctx [$idx] seq");
            // Decoded bytes only — the encoding choice is free on re-emit.
            $this->assertSame($frame->bytes, $b[$idx]->bytes, "$ctx [$idx] bytes");
            $this->assertSame($frame->atMs, $b[$idx]->atMs, "$ctx [$idx] at_ms");
        }
    }

    private function assertSameEvent(?\HopTop\Xrr\Stream\StreamEvent $a, ?\HopTop\Xrr\Stream\StreamEvent $b, string $ctx): void
    {
        if ($a === null) {
            $this->assertNull($b, $ctx);

            return;
        }
        $this->assertNotNull($b, $ctx);
        $this->assertSame($a->seq, $b->seq, "$ctx seq");
        $this->assertSame($a->atMs, $b->atMs, "$ctx at_ms");
    }
}
