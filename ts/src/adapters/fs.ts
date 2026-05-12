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
 * Path normalization: the TS adapter accepts paths verbatim. The
 * Go adapter ships a PathNormalizer hook because Go is the typical
 * record-side runtime; on replay-side (where TS is most useful),
 * paths arrive already-normalized from the cassette. Adopters that
 * need TS-side recording with normalization can pre-normalize at
 * the caller layer before constructing the request.
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

function sortedKeys(obj: Record<string, unknown>): Record<string, unknown> {
  return Object.fromEntries(
    Object.keys(obj)
      .sort()
      .map((k) => [k, obj[k]])
  );
}

export class FsAdapter implements Adapter<FsRequest, FsResponse> {
  readonly id = "fs";

  async fingerprint(req: FsRequest): Promise<string> {
    const fields: Record<string, unknown> = {
      op: req.op,
      path: req.path,
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
      fields.dest = req.dest;
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

  serializeReq(req: FsRequest): unknown {
    return req;
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
