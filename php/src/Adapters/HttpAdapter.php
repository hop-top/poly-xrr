<?php

declare(strict_types=1);

namespace HopTop\Xrr\Adapters;

use HopTop\Xrr\AdapterInterface;
use HopTop\Xrr\CanonicalJson;

/**
 * Adapter for HTTP interactions.
 *
 * Request shape:  ['method' => string, 'url' => string, 'headers' => array<string, string>, 'body' => string]
 * Response shape: ['status' => int, 'headers' => array<string, string>, 'body' => string]
 *
 * Fingerprint fields: method + path+query (no host) + sha256(body)[:8]
 */
class HttpAdapter implements AdapterInterface
{
    public function getId(): string
    {
        return 'http';
    }

    public function fingerprint(mixed $req): string
    {
        /** @var array<string, mixed> $req */
        $rawUrl    = $req['url'] ?? '';
        $url       = is_string($rawUrl) ? $rawUrl : '';
        $parsed    = parse_url($url);
        $pathQuery = is_array($parsed) ? ($parsed['path'] ?? '/') : '/';
        if (is_array($parsed) && !empty($parsed['query'])) {
            $pathQuery .= '?' . $parsed['query'];
        }

        $rawBody  = $req['body'] ?? '';
        $body     = is_string($rawBody) ? $rawBody : '';
        $bodyHash = substr(hash('sha256', $body), 0, 8);

        $fields = [
            'body_hash' => $bodyHash,
            'method'    => $req['method'] ?? 'GET',
            'path'      => $pathQuery,
        ];

        ksort($fields);
        return CanonicalJson::fingerprint($fields);
    }

    /** @return array<string, mixed> */
    public function serializeReq(mixed $req): array
    {
        /** @var array<string, mixed> $req */
        return [
            'method'  => $req['method']  ?? 'GET',
            'url'     => $req['url']     ?? '',
            'headers' => $req['headers'] ?? [],
            'body'    => $req['body']    ?? '',
        ];
    }

    /** @return array<string, mixed> */
    public function serializeResp(mixed $resp): array
    {
        /** @var array<string, mixed> $resp */
        return [
            'status'  => $resp['status']  ?? 200,
            'headers' => $resp['headers'] ?? [],
            'body'    => $resp['body']    ?? '',
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
