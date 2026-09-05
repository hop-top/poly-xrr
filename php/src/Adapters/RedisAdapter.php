<?php

declare(strict_types=1);

namespace HopTop\Xrr\Adapters;

use HopTop\Xrr\AdapterInterface;
use HopTop\Xrr\CanonicalJson;

/**
 * Adapter for Redis interactions.
 *
 * Request shape:  ['command' => string, 'args' => string[]]
 * Response shape: ['result' => mixed]
 *
 * Fingerprint fields: strtoupper(command) + implode(' ', args)
 */
class RedisAdapter implements AdapterInterface
{
    public function getId(): string
    {
        return 'redis';
    }

    public function fingerprint(mixed $req): string
    {
        /** @var array<string, mixed> $req */
        $rawCmd  = $req['command'] ?? '';
        $command = strtoupper(is_string($rawCmd) ? $rawCmd : '');
        /** @var string[] $args */
        $args    = $req['args'] ?? [];
        $parts   = array_merge([$command], $args);
        $joined  = implode(' ', $parts);

        return CanonicalJson::fingerprint($joined);
    }

    /** @return array<string, mixed> */
    public function serializeReq(mixed $req): array
    {
        /** @var array<string, mixed> $req */
        return [
            'command' => $req['command'] ?? '',
            'args'    => $req['args']    ?? [],
        ];
    }

    /** @return array<string, mixed> */
    public function serializeResp(mixed $resp): array
    {
        /** @var array<string, mixed> $resp */
        return [
            'result' => $resp['result'] ?? null,
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
