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
    private const THIS_PORT = 'php';

    private function fixturesDir(): string
    {
        return dirname(__DIR__, 2) . '/spec/fixtures';
    }

    /**
     * Each port's own re-emission of every streamed golden pair
     * (spec/emitted/<port>/<fixture>/); see spec/emitted/README.md.
     */
    private function emittedDir(): string
    {
        return dirname(__DIR__, 2) . '/spec/emitted';
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
                try {
                    $reCassette = new FileCassette($tmp);
                    $reCassette->saveStreamed($pair);
                    $reloaded = $reCassette->loadStreamed($interaction['adapter'], $interaction['fingerprint']);
                    $this->assertSameInteraction($pair, $reloaded, "$entry {$interaction['fingerprint']}");
                } finally {
                    $this->removeTree($tmp);
                }
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

    public function testReemissionPinned(): void
    {
        // spec/emitted/php must hold exactly what saveStreamed emits today for
        // every streamed golden pair, file set and bytes alike. Every port's
        // suite loads that tree, so a stale tree would hide a PHP emit change
        // from them. XRR_UPDATE_EMITTED=1 regenerates instead of asserting
        // (`make emit-php`).
        $want = $this->reemitStreamedFixtures();
        $tree = $this->emittedDir() . '/' . self::THIS_PORT;

        if ((getenv('XRR_UPDATE_EMITTED') ?: '') !== '') {
            $this->removeTree($tree);
            foreach ($want as $rel => $text) {
                $path = "$tree/$rel";
                if (!is_dir(dirname($path))) {
                    mkdir(dirname($path), 0755, true);
                }
                file_put_contents($path, $text);
            }
            $this->addToAssertionCount(1);

            return;
        }

        $this->assertDirectoryExists($tree, "missing $tree: regenerate with `make emit-php`");
        $got = $this->readTree($tree);
        ksort($want);
        $this->assertSame(array_keys($want), array_keys($got), 'file set drifted: regenerate with `make emit-php`');
        foreach ($want as $rel => $text) {
            $this->assertSame($text, $got[$rel], "$rel drifted: regenerate with `make emit-php`");
        }
    }

    public function testCrossPortReemissionsLoadToGolden(): void
    {
        // Every port's checked-in re-emission of every streamed golden pair
        // must load through the PHP strict reader to the same model as the
        // golden pair. Self-load round-trips cannot see an emit slip the
        // emitting port's own reader tolerates; another port's reader can.
        $root = $this->emittedDir();
        $this->assertDirectoryExists($root, "missing $root: regenerate with `make emit-all`");
        $ports = array_values(array_filter(
            scandir($root),
            fn($e) => $e !== '.' && $e !== '..' && is_dir("$root/$e")
        ));
        $this->assertNotEmpty($ports, "no port trees under $root");

        foreach ($ports as $port) {
            foreach ($this->streamedFixtureEntries() as $entry => $interactions) {
                $golden  = new FileCassette($this->fixturesDir() . '/' . $entry);
                $emitted = new FileCassette("$root/$port/$entry");
                foreach ($interactions as $i) {
                    $ctx  = "$port re-emission of $entry/{$i['adapter']}-{$i['fingerprint']}";
                    $want = $golden->loadStreamed($i['adapter'], $i['fingerprint']);
                    try {
                        $got = $emitted->loadStreamed($i['adapter'], $i['fingerprint']);
                    } catch (\Throwable $e) {
                        $this->fail("$ctx: {$e->getMessage()} (regenerate with `make emit-$port`)");
                    }
                    $this->assertSameInteraction($want, $got, $ctx);
                }
            }
        }
    }

    /**
     * Fixture dirs with at least one streamed entry, sorted by name.
     *
     * @return array<string, list<array{adapter: string, fingerprint: string, streamed: bool}>>
     */
    private function streamedFixtureEntries(): array
    {
        $out = [];
        foreach ($this->fixtureDirNames() as $entry) {
            $streamed = array_values(array_filter(
                $this->manifestInteractions($entry),
                fn(array $i) => $i['streamed']
            ));
            if ($streamed !== []) {
                $out[$entry] = $streamed;
            }
        }
        ksort($out);

        return $out;
    }

    /**
     * Runs saveStreamed over every streamed golden pair.
     *
     * @return array<string, string> emitted files keyed by <fixture>/<adapter>-<fp>.<kind>.yaml
     */
    private function reemitStreamedFixtures(): array
    {
        $files = [];
        foreach ($this->streamedFixtureEntries() as $entry => $interactions) {
            $golden = new FileCassette($this->fixturesDir() . '/' . $entry);
            $tmp    = sys_get_temp_dir() . '/xrr_reemit_' . uniqid();
            mkdir($tmp);
            try {
                $cassette = new FileCassette($tmp);
                foreach ($interactions as $i) {
                    $cassette->saveStreamed($golden->loadStreamed($i['adapter'], $i['fingerprint']));
                    foreach (['req', 'resp'] as $kind) {
                        $name = "{$i['adapter']}-{$i['fingerprint']}.$kind.yaml";
                        $files["$entry/$name"] = (string) file_get_contents("$tmp/$name");
                    }
                }
            } finally {
                $this->removeTree($tmp);
            }
        }

        return $files;
    }

    /** @return array<string, string> every regular file under $root keyed by relative path */
    private function readTree(string $root): array
    {
        $files = [];
        $it    = new \RecursiveIteratorIterator(
            new \RecursiveDirectoryIterator($root, \FilesystemIterator::SKIP_DOTS)
        );
        foreach ($it as $file) {
            /** @var \SplFileInfo $file */
            $rel         = substr($file->getPathname(), strlen($root) + 1);
            $files[$rel] = (string) file_get_contents($file->getPathname());
        }
        ksort($files);

        return $files;
    }

    private function removeTree(string $dir): void
    {
        if (!is_dir($dir)) {
            return;
        }
        $it = new \RecursiveIteratorIterator(
            new \RecursiveDirectoryIterator($dir, \FilesystemIterator::SKIP_DOTS),
            \RecursiveIteratorIterator::CHILD_FIRST
        );
        foreach ($it as $file) {
            /** @var \SplFileInfo $file */
            $file->isDir() ? rmdir($file->getPathname()) : unlink($file->getPathname());
        }
        rmdir($dir);
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
