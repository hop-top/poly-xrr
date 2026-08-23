/**
 * Streamed-interaction fingerprints — spec/cassette-format-streaming.md.
 *
 * All streaming fingerprints hash canonical JSON (lexicographically sorted
 * keys, no insignificant whitespace) that includes a `stream` discriminator,
 * keeping the streaming fingerprint space disjoint from unary inputs. The
 * algorithm itself stays v1: sha256(canonical)[:8].
 *
 * The split is structural: this core owns canonical-JSON assembly, the
 * `stream` discriminator, hashing/truncation, and (with the session) the
 * occurrence-counter lifecycle; an adapter supplies only its canonical
 * input fields (StreamOpen.identity), its payload shapes, and whether its
 * opens are counter-addressed.
 */
import { createHash } from "node:crypto";
import type { StreamType } from "./stream.js";

function sha256Hex8(input: string | Uint8Array): string {
  return createHash("sha256").update(input).digest("hex").slice(0, 8);
}

function sortedKeys(obj: Record<string, unknown>): Record<string, unknown> {
  return Object.fromEntries(
    Object.keys(obj)
      .sort()
      .map((k) => [k, obj[k]])
  );
}

/** v1 message-hash building block: sha256(message_bytes)[:8]. */
export function msgHash(message: Uint8Array): string {
  return sha256Hex8(message);
}

/**
 * StreamOpen identifies a streamed interaction at open time — everything a
 * replay needs to locate the cassette before any frames exist. The adapter
 * supplies its own canonical fingerprint inputs (identity), its req payload
 * shape (payload), and whether the open is disambiguated by the session's
 * occurrence counter (counter); the core owns canonical-JSON assembly, the
 * "stream" discriminator, hashing/truncation, and the counter lifecycle.
 */
export interface StreamOpen {
  adapterID: string;
  type: StreamType;
  /**
   * The adapter's canonical fingerprint inputs (for gRPC: service, method,
   * and msg_hash for server streams; for an SSE-style adapter: url). Keys
   * "stream" and "n" are reserved for core injection.
   */
  identity: Record<string, string | number>;
  /**
   * Marks the open as counter-addressed: the identity does not fully
   * identify the interaction, so the session's occurrence counter — keyed
   * by (adapterID, type, identity) — supplies the 0-based ordinal n,
   * injected as canonical input "n" and informational payload field "n".
   */
  counter?: boolean;
  /**
   * The adapter-defined open-request payload persisted to the req file.
   * The core injects "n" for counter-addressed opens.
   */
  payload?: Record<string, unknown>;
}

/**
 * Assembles the spec's canonical JSON for an open: the adapter identity
 * plus the injected "stream" discriminator, plus "n" when n >= 0. Keys are
 * lexicographically sorted; JSON.stringify emits no insignificant
 * whitespace — exactly the spec's canonical JSON.
 */
export function streamCanonical(open: StreamOpen, n: number): string {
  if (open.type !== "server" && open.type !== "client" && open.type !== "bidi") {
    throw new Error(
      `xrr: stream type ${JSON.stringify(open.type)} invalid (want server|client|bidi)`
    );
  }
  const inputs: Record<string, unknown> = {};
  for (const [k, v] of Object.entries(open.identity)) {
    if (k === "stream" || k === "n") {
      throw new Error(`xrr: stream identity key "${k}" is reserved for core injection`);
    }
    inputs[k] = v;
  }
  inputs.stream = open.type;
  if (n >= 0) inputs.n = n;
  return JSON.stringify(sortedKeys(inputs));
}

/**
 * Computes the streaming fingerprint for an open: sha256(canonical)[:8]
 * over the adapter's canonical inputs plus a "stream" discriminator,
 * keeping the streaming fingerprint space disjoint from the unary one.
 * Counter-addressed opens include the 0-based occurrence ordinal n as
 * canonical input "n"; n is ignored otherwise (content-addressed
 * identities, e.g. gRPC server streams, carry their content hash in
 * identity).
 */
export function streamFingerprint(open: StreamOpen, n: number): string {
  if (open.counter) {
    if (n < 0) throw new Error(`xrr: stream occurrence n must be >= 0, got ${n}`);
  } else {
    n = -1;
  }
  return sha256Hex8(streamCanonical(open, n));
}

/**
 * Server-stream fingerprint — the single request message is available at
 * open, mirroring unary: canonical
 * {"method":…,"msg_hash":…,"service":…,"stream":"server"}.
 *
 * `message` MUST already be in the form the cassette addresses. When a
 * session carries a frame scrub hook, that means the SCRUBBED bytes: the
 * spec derives `msg_hash` over scrubbed bytes in record and replay alike,
 * so passing raw bytes here on a scrubbing session computes a fingerprint
 * that no cassette holds. Pass frames loaded from a cassette (already
 * scrubbed at record time), or the output of `session.scrubStreamFrame`.
 * The gRPC adapter does the latter; prefer it over calling this directly.
 */
export function serverStreamFingerprint(
  service: string,
  method: string,
  message: Uint8Array
): string {
  return streamFingerprint(
    {
      adapterID: "grpc",
      type: "server",
      identity: { method, msg_hash: msgHash(message), service },
    },
    -1
  );
}

/**
 * Client/bidi fingerprint — no message at open; the 0-based occurrence
 * counter `n` disambiguates repeated opens of the same
 * (service, method, stream type) tuple within one session: canonical
 * {"method":…,"n":…,"service":…,"stream":…}. `n` is always included,
 * even when 0.
 */
export function counterStreamFingerprint(
  type: "client" | "bidi",
  service: string,
  method: string,
  n: number
): string {
  return streamFingerprint(
    { adapterID: "grpc", type, identity: { method, service }, counter: true },
    n
  );
}

/**
 * Per-session occurrence counter. One session object is one counter domain:
 * created with the session, keyed by the adapter's identifying tuple,
 * incremented at each stream open, counted identically in record and replay
 * modes.
 */
export class OccurrenceCounter {
  private readonly counts = new Map<string, number>();

  /** Returns the 0-based count of prior opens for the tuple, then increments. */
  next(...key: string[]): number {
    const k = key.join("\u0000");
    const n = this.counts.get(k) ?? 0;
    this.counts.set(k, n + 1);
    return n;
  }
}
