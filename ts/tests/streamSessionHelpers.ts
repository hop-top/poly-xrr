/**
 * Shared helpers for the streamed session-layer tests
 * (streamSession.test.ts, streamReplay.test.ts).
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { FileCassette } from "../src/cassette.js";
import { FileSession } from "../src/session.js";
import type { StreamType } from "../src/stream.js";
import { msgHash, type StreamOpen } from "../src/streamfp.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
export const fixturesRoot = path.resolve(__dirname, "../../spec/fixtures");

export const td = new TextDecoder();

export function utf8(s: string): Uint8Array {
  return new TextEncoder().encode(s);
}

export function tmpDir(): string {
  return fs.mkdtempSync(fs.realpathSync("/tmp") + "/xrr-stream-session-");
}

/**
 * Mirrors the gRPC adapter's open definition: canonical inputs service +
 * method (+ msg_hash for content-addressed server streams),
 * counter-addressed client/bidi, req payload {service, method}.
 */
export function grpcStreamOpen(
  type: StreamType,
  service: string,
  method: string,
  msg?: Uint8Array
): StreamOpen {
  const open: StreamOpen = {
    adapterID: "grpc",
    type,
    identity: { method, service },
    payload: { service, method },
  };
  if (type === "server") {
    open.identity.msg_hash = msgHash(msg ?? new Uint8Array(0));
  } else {
    open.counter = true;
  }
  return open;
}

export function sseStreamOpen(url: string): StreamOpen {
  return { adapterID: "sse", type: "server", identity: { url }, payload: { url } };
}

export function fixtureSession(dir: string): FileSession {
  return new FileSession("replay", new FileCassette(path.join(fixturesRoot, dir)));
}

/** Runs fn, returning what it threw; fails the test if it did not throw. */
export function caught(fn: () => unknown): unknown {
  try {
    fn();
  } catch (err) {
    return err;
  }
  throw new Error("expected the call to throw");
}
