<?php

declare(strict_types=1);

namespace HopTop\Xrr\Adapters;

use HopTop\Xrr\AdapterInterface;
use HopTop\Xrr\CanonicalJson;

/**
 * Adapter for exec (subprocess) interactions.
 *
 * Request shape:  ['argv' => string[], 'stdin' => string, 'env' => array<string, string>]
 * Response shape: ['stdout' => string, 'stderr' => string, 'exit_code' => int, 'duration_ms' => int]
 *
 * Fingerprint fields: argv + stdin
 */
class ExecAdapter implements AdapterInterface
{
    public function getId(): string
    {
        return 'exec';
    }

    public function fingerprint(mixed $req): string
    {
        /** @var array<string, mixed> $req */
        $fields = [
            'argv'  => $req['argv']  ?? [],
            'stdin' => $req['stdin'] ?? '',
        ];

        ksort($fields);
        return CanonicalJson::fingerprint($fields);
    }

    /** @return array<string, mixed> */
    public function serializeReq(mixed $req): array
    {
        /** @var array<string, mixed> $req */
        return [
            'argv'  => $req['argv']  ?? [],
            'stdin' => $req['stdin'] ?? '',
            'env'   => $req['env']   ?? [],
        ];
    }

    /** @return array<string, mixed> */
    public function serializeResp(mixed $resp): array
    {
        /** @var array<string, mixed> $resp */
        return [
            'stdout'      => $resp['stdout']      ?? '',
            'stderr'      => $resp['stderr']      ?? '',
            'exit_code'   => $resp['exit_code']   ?? 0,
            'duration_ms' => $resp['duration_ms'] ?? 0,
        ];
    }

    /** @param array<string, mixed> $data */
    public function deserializeReq(array $data): mixed
    {
        return $data;
    }

    /** @param array<string, mixed> $data */
    public function deserializeResp(array $data): mixed
    {
        return $data;
    }
}
