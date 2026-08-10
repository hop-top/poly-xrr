<?php

declare(strict_types=1);

namespace HopTop\Xrr;

use HopTop\Xrr\Exception\CassetteMissException;
use Symfony\Component\Yaml\Yaml;

class FileCassette
{
    /**
     * Redaction is enabled by default, configured from the XRR_REDACT_*
     * environment variables. Pass an explicit Redactor to supply a
     * policy that bypasses the environment.
     */
    public function __construct(
        private string $dir,
        private ?Redactor $redactor = null
    ) {}

    /**
     * Resolves the redactor to use for one write. When none was
     * injected, config is read from the environment on each write so a
     * test that flips XRR_REDACT_* mid-process sees the change.
     */
    private function activeRedactor(): Redactor
    {
        return $this->redactor ?? Redactor::fromEnv();
    }

    /**
     * Save request and response payloads as two YAML cassette files.
     *
     * @param array<string, mixed> $req
     * @param array<string, mixed> $resp
     */
    public function save(string $adapterID, string $fingerprint, array $req, array $resp): void
    {
        $now = (new \DateTimeImmutable('now', new \DateTimeZone('UTC')))->format('Y-m-d\TH:i:s\Z');

        $this->write($adapterID, $fingerprint, 'req', $now, $req);
        $this->write($adapterID, $fingerprint, 'resp', $now, $resp);
    }

    /** @param array<string, mixed> $payload */
    private function write(
        string $adapterID,
        string $fingerprint,
        string $kind,
        string $recordedAt,
        array $payload
    ): void {
        // Scrub credential-bearing fields before serialization — a
        // secret never reaches the YAML string, let alone the file.
        // Envelope metadata is never scrubbed: the fingerprint in
        // particular must match the filename.
        $envelope = [
            'xrr'         => '1',
            'adapter'     => $adapterID,
            'fingerprint' => $fingerprint,
            'recorded_at' => $recordedAt,
            'payload'     => $this->activeRedactor()->redactPayload($payload),
        ];

        $path = $this->path($adapterID, $fingerprint, $kind);
        file_put_contents($path, Yaml::dump($envelope, 4, 2));
    }

    /**
     * Load request and response payloads from cassette files.
     *
     * @return array{req: array<string, mixed>, resp: array<string, mixed>}
     * @throws CassetteMissException
     */
    public function load(string $adapterID, string $fingerprint): array
    {
        $req  = $this->read($adapterID, $fingerprint, 'req');
        $resp = $this->read($adapterID, $fingerprint, 'resp');

        return ['req' => $req, 'resp' => $resp];
    }

    /** @return array<string, mixed> */
    private function read(string $adapterID, string $fingerprint, string $kind): array
    {
        $path = $this->path($adapterID, $fingerprint, $kind);

        if (!file_exists($path)) {
            throw new CassetteMissException($adapterID, $fingerprint);
        }

        $envelope = Yaml::parseFile($path);

        if (!is_array($envelope) || !isset($envelope['payload']) || !is_array($envelope['payload'])) {
            throw new \RuntimeException(
                sprintf('xrr: missing or invalid payload in %s', $path)
            );
        }

        /** @var array<string, mixed> $payload */
        $payload = $envelope['payload'];

        return $payload;
    }

    private function path(string $adapterID, string $fingerprint, string $kind): string
    {
        return sprintf('%s/%s-%s.%s.yaml', $this->dir, $adapterID, $fingerprint, $kind);
    }
}
