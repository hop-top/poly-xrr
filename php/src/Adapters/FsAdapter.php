<?php

declare(strict_types=1);

namespace HopTop\Xrr\Adapters;

use Closure;
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
 *
 * Path normalization: an optional normalizer rewrites `path` and `dest`
 * before they enter the fingerprint and before the request is serialized
 * onto the cassette, so what gets hashed and what gets stored agree
 * (spec: cassettes store post-normalizer paths). Default is identity;
 * install one via the constructor or withNormalizer(), and compose
 * several rules with chain(). `data` and every other field are never
 * touched.
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

    /** @var Closure(string): string */
    private Closure $normalizer;

    /** @param (callable(string): string)|null $normalizer Defaults to identity. */
    public function __construct(?callable $normalizer = null)
    {
        $this->normalizer = $normalizer === null
            ? static fn (string $p): string => $p
            : $normalizer(...);
    }

    /**
     * Returns a copy with the given normalizer installed; the receiver is
     * left untouched. Use chain() to compose multiple rules.
     *
     * @param callable(string): string $normalizer
     */
    public function withNormalizer(callable $normalizer): static
    {
        $copy             = clone $this;
        $copy->normalizer = $normalizer(...);

        return $copy;
    }

    /**
     * Composes normalizers left to right. An empty chain is identity.
     *
     * @param callable(string): string ...$normalizers
     * @return Closure(string): string
     */
    public static function chain(callable ...$normalizers): Closure
    {
        return static function (string $p) use ($normalizers): string {
            foreach ($normalizers as $n) {
                $p = $n($p);
            }

            return $p;
        };
    }

    /**
     * Applies the installed normalizer to $p. Wrappers may call this when
     * building a request so the path handed to the session agrees with
     * what fingerprint() hashes. Empty input short-circuits without
     * invoking the normalizer, so optional fields such as `dest` can be
     * passed through unconditionally.
     */
    public function normalize(string $p): string
    {
        if ($p === '') {
            return '';
        }

        return ($this->normalizer)($p);
    }

    public function getId(): string
    {
        return 'fs';
    }

    public function fingerprint(mixed $req): string
    {
        /** @var array<string, mixed> $req */
        $path   = $req['path'] ?? '';
        $fields = [
            'op'   => $req['op'] ?? '',
            'path' => is_string($path) ? $this->normalize($path) : $path,
        ];

        $data = $req['data'] ?? '';
        if (is_string($data) && $data !== '') {
            $fields['data_sha256'] = hash('sha256', $data);
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
        // spec: dest participates only when non-empty AFTER normalization.
        $dest = $req['dest'] ?? '';
        $dest = is_string($dest) ? $this->normalize($dest) : $dest;
        if ($dest !== '') {
            $fields['dest'] = $dest;
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
        // Persist post-normalizer paths so the cassette payload agrees with
        // the fingerprint inputs; every other field passes through verbatim.
        if (isset($req['path']) && is_string($req['path'])) {
            $req['path'] = $this->normalize($req['path']);
        }
        if (isset($req['dest']) && is_string($req['dest'])) {
            $req['dest'] = $this->normalize($req['dest']);
        }

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
