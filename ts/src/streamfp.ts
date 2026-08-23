/**
 * Streamed-interaction fingerprints — spec/cassette-format-streaming.md.
 *
 * All streaming fingerprints hash canonical JSON (lexicographically sorted
 * keys, no insignificant whitespace) that includes a `stream` discriminator,
 * keeping the streaming fingerprint space disjoint from unary inputs. The
 * algorithm itself stays v1: sha256(canonical)[:8].
 */
import { createHash } from "node:crypto";

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
 * Server-stream fingerprint — the single request message is available at
 * open, mirroring unary: canonical
 * {"method":…,"msg_hash":…,"service":…,"stream":"server"}.
 */
export function serverStreamFingerprint(
  service: string,
  method: string,
  message: Uint8Array
): string {
  const canonical = JSON.stringify(
    sortedKeys({ method, msg_hash: msgHash(message), service, stream: "server" })
  );
  return sha256Hex8(canonical);
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
  const canonical = JSON.stringify(sortedKeys({ method, n, service, stream: type }));
  return sha256Hex8(canonical);
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
