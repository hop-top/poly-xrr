<?php

declare(strict_types=1);

namespace HopTop\Xrr\Adapters;

use HopTop\Xrr\AdapterInterface;
use HopTop\Xrr\CanonicalJson;

/**
 * Adapter for SQL interactions.
 *
 * Request shape:  ['query' => string, 'args' => array<mixed>]
 * Response shape: ['rows' => array<int, array<string, mixed>>, 'affected' => int]
 *
 * Fingerprint fields: normalized query (strtolower + collapse whitespace) + args
 */
class SqlAdapter implements AdapterInterface
{
    public function getId(): string
    {
        return 'sql';
    }

    public function fingerprint(mixed $req): string
    {
        /** @var array<string, mixed> $req */
        $rawQuery = $req['query'] ?? '';
        $query    = $this->normalizeQuery(is_string($rawQuery) ? $rawQuery : '');
        $args  = $req['args'] ?? [];

        $fields = [
            'args'  => $args,
            'query' => $query,
        ];

        ksort($fields);
        return CanonicalJson::fingerprint($fields);
    }

    private function normalizeQuery(string $query): string
    {
        return trim(preg_replace('/\s+/', ' ', strtolower($query)) ?? $query);
    }

    /** @return array<string, mixed> */
    public function serializeReq(mixed $req): array
    {
        /** @var array<string, mixed> $req */
        return [
            'query' => $req['query'] ?? '',
            'args'  => $req['args']  ?? [],
        ];
    }

    /** @return array<string, mixed> */
    public function serializeResp(mixed $resp): array
    {
        /** @var array<string, mixed> $resp */
        return [
            'rows'     => $resp['rows']     ?? [],
            'affected' => $resp['affected'] ?? 0,
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
