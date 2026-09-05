/**
 * fs adapter — fingerprints on op + path + presence-gated optional fields.
 *
 * Mirrors go/adapters/fs/fs.go. The fingerprint algorithm hashes a
 * canonical JSON object (sorted keys) containing `op`, `path`, and
 * each optional field only when "set" per the omit-on-zero rules
 * documented in spec/cassette-format-v1.md.
 *
 * Field naming uses snake_case in the TS interface to match the
 * YAML wire form used by every other port — js-yaml round-trips
 * the field names verbatim, so what you see in code is what you
 * see in the cassette.
 *
 * Path normalization: the adapter holds a `PathNormalizer` (default
 * identity) and applies it to `path` and `dest` in both
 * `fingerprint()` and `serializeReq()`, so what gets hashed and what
 * gets persisted agree exactly — the "cassettes store post-normalizer
 * paths" contract in spec/cassette-format-v1.md. Install one with
 * `new FsAdapter({ normalizer })` or `adapter.withNormalizer(fn)`;
 * compose rules with `chainNormalizers(...)`. Replay reads the stored
 * path verbatim: `deserializeReq()` never re-normalizes.
 */
import { createHash } from "node:crypto";
import type { Adapter } from "../xrr.js";

export type FsOp =
  | "write"
  | "mkdir"
  | "remove"
  | "rename"
  | "chmod"
  | "chown"
  | "symlink"
  | "hardlink"
  | "truncate";

export interface FsRequest {
  op: FsOp;
  path: string;
  /** UTF-8 string per spec. Binary callers MUST base64-encode beforehand. */
  data?: string;
  mode?: number;
  uid?: number;
  gid?: number;
  dest?: string;
  size?: number;
  flags?: number;
  recursive?: boolean;
}

export interface FsResponse {
  duration_ms?: number;
  bytes_written?: number;
}

/**
 * Rewrites a path before it enters the fingerprint or the cassette
 * payload. Applies to `path` and `dest` only — never to `data`.
 * Returning "" is allowed and stored literally.
 */
export type PathNormalizer = (path: string) => string;

export interface FsAdapterOptions {
  /** Applied to `path` and `dest` before hashing and serializing. Default: identity. */
  normalizer?: PathNormalizer;
}

const identity: PathNormalizer = (p) => p;

/** Composes normalizers left to right; with no arguments returns identity. */
export function chainNormalizers(...normalizers: PathNormalizer[]): PathNormalizer {
  return (p) => normalizers.reduce((acc, n) => n(acc), p);
}

function sortedKeys(obj: Record<string, unknown>): Record<string, unknown> {
  return Object.fromEntries(
    Object.keys(obj)
      .sort()
      .map((k) => [k, obj[k]])
  );
}

export class FsAdapter implements Adapter<FsRequest, FsResponse> {
  readonly id = "fs";
  private readonly normalizer: PathNormalizer;

  constructor(options: FsAdapterOptions = {}) {
    this.normalizer = options.normalizer ?? identity;
  }

  /** Returns a new adapter with `normalizer` installed; `this` is unchanged. */
  withNormalizer(normalizer: PathNormalizer): FsAdapter {
    return new FsAdapter({ normalizer });
  }

  /**
   * Applies the installed normalizer. Empty input short-circuits to ""
   * without invoking it, so callers can pass an optional `dest` through
   * unconditionally.
   */
  normalize(p: string): string {
    if (p === "") {
      return "";
    }
    return this.normalizer(p);
  }

  async fingerprint(req: FsRequest): Promise<string> {
    const fields: Record<string, unknown> = {
      op: req.op,
      path: this.normalize(req.path),
    };
    if (req.data !== undefined && req.data !== "") {
      fields.data_sha256 = createHash("sha256")
        .update(req.data, "utf8")
        .digest("hex");
    }
    if (req.mode !== undefined) {
      fields.mode = req.mode;
    }
    if (req.uid !== undefined) {
      fields.uid = req.uid;
    }
    if (req.gid !== undefined) {
      fields.gid = req.gid;
    }
    if (req.dest !== undefined && req.dest !== "") {
      fields.dest = this.normalize(req.dest);
    }
    if (req.size !== undefined) {
      fields.size = req.size;
    }
    if (req.flags !== undefined && req.flags !== 0) {
      fields.flags = req.flags;
    }
    if (req.recursive === true) {
      fields.recursive = true;
    }
    const canonical = JSON.stringify(sortedKeys(fields));
    return createHash("sha256").update(canonical).digest("hex").slice(0, 8);
  }

  /** Returns a copy of `req` with `path` and `dest` in post-normalizer form. */
  serializeReq(req: FsRequest): unknown {
    const out: FsRequest = { ...req, path: this.normalize(req.path) };
    if (req.dest !== undefined) {
      out.dest = this.normalize(req.dest);
    }
    return out;
  }

  serializeResp(resp: FsResponse): unknown {
    return resp;
  }

  deserializeReq(data: unknown): FsRequest {
    return data as FsRequest;
  }

  deserializeResp(data: unknown): FsResponse {
    return data as FsResponse;
  }
}
