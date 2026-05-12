<?php

declare(strict_types=1);

namespace HopTop\Xrr\Adapters;

use HopTop\Xrr\AdapterInterface;

/**
 * Adapter for filesystem mutation interactions (write, mkdir, remove, etc.).
 *
 * Reads are intentionally not supported: tests should pre-seed disk state
 * via fixtures and use xrr only to assert on mutations.
 *
 * Request shape:
 *   [
 *     'op'        => string,           // 'write'|'mkdir'|'remove'|'rename'|...
 *     'path'      => string,
 *     'data'      => string,           // optional; UTF-8 text. Base64-encode
 *                                      // non-UTF-8 binary before passing in.
 *     'mode'      => int,              // optional
 *     'uid'       => int,              // optional
 *     'gid'       => int,              // optional
 *     'dest'      => string,           // optional (rename/symlink/hardlink target)
 *     'size'      => int,              // optional (truncate)
 *     'flags'     => int,              // optional
 *     'recursive' => bool,             // optional (mkdir/remove)
 *   ]
 *
 * Response shape:
 *   ['duration_ms' => int, 'bytes_written' => int]
 *
 * Fingerprint: sha256(canonical JSON of selected fields)[:8].
 * Keys are lexicographically sorted so canonical bytes match Go's
 * encoding/json over map[string]any. data is included as data_sha256
 * (full hex sha256 of UTF-8 bytes) when non-empty so the 8-char filename
 * suffix stays bounded for any payload size.
 */
class FsAdapter implements AdapterInterface
{
    public const OP_WRITE    = 'write';
    public const OP_MKDIR    = 'mkdir';
    public const OP_REMOVE   = 'remove';
    public const OP_RENAME   = 'rename';
    public const OP_CHMOD    = 'chmod';
    public const OP_CHOWN    = 'chown';
    public const OP_SYMLINK  = 'symlink';
    public const OP_HARDLINK = 'hardlink';
    public const OP_TRUNCATE = 'truncate';

    public function getId(): string
    {
        return 'fs';
    }

    public function fingerprint(mixed $req): string
    {
        /** @var array<string, mixed> $req */
        $fields = [
            'op'   => $req['op']   ?? '',
            'path' => $req['path'] ?? '',
        ];

        $data = $req['data'] ?? '';
        if ($data !== '' && $data !== null) {
            $fields['data_sha256'] = hash('sha256', (string) $data);
        }
        if (isset($req['mode'])) {
            $fields['mode'] = $req['mode'];
        }
        if (isset($req['uid'])) {
            $fields['uid'] = $req['uid'];
        }
        if (isset($req['gid'])) {
            $fields['gid'] = $req['gid'];
        }
        if (!empty($req['dest'])) {
            $fields['dest'] = $req['dest'];
        }
        if (isset($req['size'])) {
            $fields['size'] = $req['size'];
        }
        if (!empty($req['flags'])) {
            $fields['flags'] = $req['flags'];
        }
        if (!empty($req['recursive'])) {
            $fields['recursive'] = true;
        }

        ksort($fields);
        $canonical = json_encode($fields, JSON_UNESCAPED_SLASHES | JSON_THROW_ON_ERROR);

        return substr(hash('sha256', $canonical), 0, 8);
    }

    /** @return array<string, mixed> */
    public function serializeReq(mixed $req): array
    {
        /** @var array<string, mixed> $req */
        return $req;
    }

    /** @return array<string, mixed> */
    public function serializeResp(mixed $resp): array
    {
        /** @var array<string, mixed> $resp */
        return $resp;
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
