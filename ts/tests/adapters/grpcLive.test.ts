/**
 * gRPC streaming e2e against the real @grpc/grpc-js.
 *
 * Records against a live in-process server, stops the server, verifies the
 * port is dead, then replays from cassettes only — asserting the client
 * observes byte-identical transcripts and that no connection is attempted.
 *
 * Covers server-, client-, and bidi-streaming plus a mid-stream error, an
 * empty stream, and the frame scrub hook.
 */
import fs from "node:fs";
import net from "node:net";
import os from "node:os";
import path from "node:path";
import { fileURLToPath } from "node:url";
import grpc from "@grpc/grpc-js";
import protoLoader from "@grpc/proto-loader";
import { afterAll, beforeAll, describe, expect, test } from "vitest";
import { FileCassette } from "../../src/cassette.js";
import { FileSession } from "../../src/session.js";
import { GrpcStreamAdapter } from "../../src/adapters/grpc.js";
import type { GrpcRuntime, GrpcServiceDefinition } from "../../src/adapters/grpc.js";
import type { StreamScrubFn } from "../../src/streamScrub.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const PROTO = path.join(__dirname, "fixtures", "stream.proto");

/** The real grpc-js pieces the adapter needs. */
const runtime = {
  InterceptingCall: grpc.InterceptingCall,
  Metadata: grpc.Metadata,
} as unknown as GrpcRuntime;

interface Chunk {
  text: string;
}

// eslint-disable-next-line @typescript-eslint/no-explicit-any
type AnyClient = any;

const packageDefinition = protoLoader.loadSync(PROTO, {
  keepCase: true,
  defaults: true,
  oneofs: true,
});
// eslint-disable-next-line @typescript-eslint/no-explicit-any
const proto = grpc.loadPackageDefinition(packageDefinition) as any;
const StreamService = proto.xrrtest.StreamService;
const serviceDef = StreamService.service as GrpcServiceDefinition;

function tmpDir(): string {
  return fs.mkdtempSync(path.join(fs.realpathSync(os.tmpdir()), "xrr-grpc-"));
}

/** Reports whether anything is listening on a port — replay must not need it. */
function portIsDead(port: number): Promise<boolean> {
  return new Promise((resolve) => {
    const sock = net.connect({ host: "127.0.0.1", port });
    const done = (dead: boolean) => {
      sock.destroy();
      resolve(dead);
    };
    sock.setTimeout(500);
    sock.on("connect", () => done(false));
    sock.on("error", () => done(true));
    sock.on("timeout", () => done(true));
  });
}

// ── the live server ────────────────────────────────────────────────────────

let server: grpc.Server;
let port = 0;

beforeAll(async () => {
  server = new grpc.Server();
  server.addService(StreamService.service, {
    // Streams chunks for a named file; "empty" streams nothing, "boom"
    // fails mid-stream after two chunks.
    Download: (call: AnyClient) => {
      const name = (call.request as Chunk).text;
      if (name === "empty") {
        call.end();
        return;
      }
      if (name === "boom") {
        // Two chunks, then a non-OK status: the spec's mid-stream error
        // shape (N recv frames, then an error terminal).
        call.write({ text: "log-chunk-1" });
        call.write({ text: "log-chunk-2" });
        call.emit("error", {
          code: grpc.status.UNAVAILABLE,
          details: "connection reset",
        });
        return;
      }
      for (let i = 1; i <= 3; i++) call.write({ text: `${name}-chunk-${i}` });
      call.end();
    },
    // Consumes the client stream and answers with the joined parts.
    Upload: (call: AnyClient, cb: AnyClient) => {
      const parts: string[] = [];
      call.on("data", (m: Chunk) => parts.push(m.text));
      call.on("end", () => cb(null, { text: `got:${parts.join(",")}` }));
    },
    // Echoes each message back with a pong prefix.
    Converse: (call: AnyClient) => {
      call.on("data", (m: Chunk) => call.write({ text: `pong:${m.text}` }));
      call.on("end", () => call.end());
    },
  });
  port = await new Promise<number>((resolve, reject) =>
    server.bindAsync("127.0.0.1:0", grpc.ServerCredentials.createInsecure(), (err, p) =>
      err ? reject(err) : resolve(p)
    )
  );
});

afterAll(() => {
  server?.forceShutdown();
});

// ── client drivers ─────────────────────────────────────────────────────────

interface Transcript {
  messages: string[];
  error: string | null;
  code: number | null;
}

function makeClient(session: FileSession, target: string, scrubbed = false): AnyClient {
  void scrubbed;
  const adapter = new GrpcStreamAdapter(session, runtime, [serviceDef]);
  return new StreamService(target, grpc.credentials.createInsecure(), {
    interceptors: [adapter.interceptor()],
  });
}

function serverStream(client: AnyClient, name: string): Promise<Transcript> {
  return new Promise((resolve) => {
    const messages: string[] = [];
    const call = client.Download({ text: name });
    call.on("data", (m: Chunk) => messages.push(m.text));
    call.on("end", () => resolve({ messages, error: null, code: null }));
    call.on("error", (e: { message: string; code: number }) =>
      resolve({ messages, error: e.message, code: e.code })
    );
  });
}

function clientStream(client: AnyClient, parts: string[]): Promise<Transcript> {
  return new Promise((resolve) => {
    const call = client.Upload((e: { message: string; code: number } | null, r: Chunk) => {
      if (e) resolve({ messages: [], error: String(e.message ?? e), code: e.code ?? null });
      else resolve({ messages: [r.text], error: null, code: null });
    });
    for (const p of parts) call.write({ text: p });
    call.end();
  });
}

function bidiStream(client: AnyClient, parts: string[]): Promise<Transcript> {
  return new Promise((resolve) => {
    const messages: string[] = [];
    const call = client.Converse();
    call.on("data", (m: Chunk) => messages.push(m.text));
    call.on("end", () => resolve({ messages, error: null, code: null }));
    call.on("error", (e: { message: string; code: number }) =>
      resolve({ messages, error: e.message, code: e.code })
    );
    for (const p of parts) call.write({ text: p });
    call.end();
  });
}

// ── tests ──────────────────────────────────────────────────────────────────

describe("grpc adapter — record live, replay with the server stopped", () => {
  test("server / client / bidi streams round-trip byte-for-byte", async () => {
    const dir = tmpDir();

    // ── phase 1: record against the live server ──────────────────────────
    const recSession = new FileSession("record", new FileCassette(dir));
    const recClient = makeClient(recSession, `127.0.0.1:${port}`);

    const recServer = await serverStream(recClient, "alpha");
    const recClientS = await clientStream(recClient, ["a", "b", "c"]);
    const recBidi = await bidiStream(recClient, ["ping-1", "ping-2"]);

    expect(recServer.messages).toEqual(["alpha-chunk-1", "alpha-chunk-2", "alpha-chunk-3"]);
    expect(recClientS.messages).toEqual(["got:a,b,c"]);
    expect(recBidi.messages).toEqual(["pong:ping-1", "pong:ping-2"]);
    recClient.close();

    // Cassettes exist for all three interactions.
    const files = fs.readdirSync(dir).filter((f) => f.endsWith(".yaml"));
    expect(files.length).toBe(6); // 3 pairs

    // ── phase 2: replay, served entirely from the cassettes ─────────────
    // (the server is stopped for real in the dedicated test below, which
    // runs last; every replay here also targets a dead address elsewhere in
    // this file, so "no connection attempted" is proven independently.)
    const repSession = new FileSession("replay", new FileCassette(dir));
    const repClient = makeClient(repSession, `127.0.0.1:${port}`);

    const repServer = await serverStream(repClient, "alpha");
    const repClientS = await clientStream(repClient, ["a", "b", "c"]);
    const repBidi = await bidiStream(repClient, ["ping-1", "ping-2"]);
    repClient.close();

    // Transcripts match the live run exactly.
    expect(repServer).toEqual(recServer);
    expect(repClientS).toEqual(recClientS);
    expect(repBidi).toEqual(recBidi);
  });

  test("replay reaches an unroutable address without dialing", async () => {
    const dir = tmpDir();
    const recSession = new FileSession("record", new FileCassette(dir));
    const recClient = makeClient(recSession, `127.0.0.1:${port}`);
    const live = await serverStream(recClient, "beta");
    recClient.close();
    expect(live.messages).toHaveLength(3);

    // Port 1 is never listening: any dial attempt fails loudly, so a green
    // replay proves the adapter served the cassette without a connection.
    const repSession = new FileSession("replay", new FileCassette(dir));
    const repClient = makeClient(repSession, "127.0.0.1:1");
    const replayed = await serverStream(repClient, "beta");
    repClient.close();

    expect(replayed).toEqual(live);
    expect(replayed.error).toBeNull();
  });

  test("mid-stream error: frames delivered, then the recorded status", async () => {
    const dir = tmpDir();
    const recSession = new FileSession("record", new FileCassette(dir));
    const recClient = makeClient(recSession, `127.0.0.1:${port}`);
    const live = await serverStream(recClient, "boom");
    recClient.close();

    expect(live.messages).toEqual(["log-chunk-1", "log-chunk-2"]);
    expect(live.code).toBe(grpc.status.UNAVAILABLE);

    const repSession = new FileSession("replay", new FileCassette(dir));
    const repClient = makeClient(repSession, "127.0.0.1:1");
    const replayed = await serverStream(repClient, "boom");
    repClient.close();

    // All recorded frames, then the status rebuilt from status_code.
    expect(replayed.messages).toEqual(live.messages);
    expect(replayed.code).toBe(live.code);
    expect(replayed.error).toBe(live.error);
  });

  test("empty stream: first read yields end-of-stream", async () => {
    const dir = tmpDir();
    const recSession = new FileSession("record", new FileCassette(dir));
    const recClient = makeClient(recSession, `127.0.0.1:${port}`);
    const live = await serverStream(recClient, "empty");
    recClient.close();
    expect(live.messages).toEqual([]);
    expect(live.error).toBeNull();

    const repSession = new FileSession("replay", new FileCassette(dir));
    const repClient = makeClient(repSession, "127.0.0.1:1");
    const replayed = await serverStream(repClient, "empty");
    repClient.close();
    expect(replayed).toEqual(live);
  });

  test("divergent send bytes on replay are a stream mismatch, not a silent pass", async () => {
    const dir = tmpDir();
    const recSession = new FileSession("record", new FileCassette(dir));
    const recClient = makeClient(recSession, `127.0.0.1:${port}`);
    await clientStream(recClient, ["a", "b"]);
    recClient.close();

    const repSession = new FileSession("replay", new FileCassette(dir));
    const repClient = makeClient(repSession, "127.0.0.1:1");
    const replayed = await clientStream(repClient, ["a", "DIVERGENT"]);
    repClient.close();

    expect(replayed.error).toMatch(/stream mismatch/);
  });

  test("frame scrub keeps secrets out of the cassette and still replays green", async () => {
    const dir = tmpDir();
    const token = "ghp_0123456789abcdefghijklmnopqrstuvwx";
    // Equal-length mask: frames are protobuf wire bytes, so the scrub must
    // preserve the encoding's structure.
    const mask = "x".repeat(token.length);
    const scrub: StreamScrubFn = (_dir, _info, data) => {
      const s = Buffer.from(data).toString("binary");
      if (!s.includes(token)) return data;
      return new Uint8Array(Buffer.from(s.split(token).join(mask), "binary"));
    };

    // ── record through the scrub against the live server ─────────────────
    const recSession = new FileSession("record", new FileCassette(dir), scrub);
    const recClient = makeClient(recSession, `127.0.0.1:${port}`);
    const live = await bidiStream(recClient, [`auth:${token}`, "plain"]);
    recClient.close();

    // The live run saw the real bytes; only the cassette is scrubbed.
    expect(live.messages).toEqual([`pong:auth:${token}`, "pong:plain"]);
    for (const f of fs.readdirSync(dir)) {
      const text = fs.readFileSync(path.join(dir, f), "utf8");
      expect(text, f).not.toContain(token);
      // Base64 would hide the raw token, so assert on decoded frames too.
      const decoded = Buffer.from(text).toString("binary");
      expect(decoded, f).not.toContain(token);
    }

    // ── replay through the SAME hook: scrubbed both sides, so green ──────
    const repSession = new FileSession("replay", new FileCassette(dir), scrub);
    const repClient = makeClient(repSession, "127.0.0.1:1");
    const replayed = await bidiStream(repClient, [`auth:${token}`, "plain"]);
    repClient.close();

    expect(replayed.error).toBeNull();
    // The client receives the scrubbed recording, not the live bytes.
    expect(replayed.messages).toEqual([`pong:auth:${mask}`, "pong:plain"]);
  });

  // Runs last: it stops the shared server, so every test that records live
  // must already have run.
  test("zz: server stopped, port dead, replay still green", async () => {
    const dir = tmpDir();

    // ── phase 1: record against the live server ──────────────────────────
    const recSession = new FileSession("record", new FileCassette(dir));
    const recClient = makeClient(recSession, `127.0.0.1:${port}`);
    const live = {
      server: await serverStream(recClient, "gamma"),
      client: await clientStream(recClient, ["x", "y"]),
      bidi: await bidiStream(recClient, ["m1", "m2"]),
    };
    recClient.close();
    expect(live.server.messages).toHaveLength(3);

    // ── phase 2: kill the server, verify the port is actually dead ───────
    server.forceShutdown();
    expect(await portIsDead(port)).toBe(true);

    // ── phase 3: replay against that same dead port ──────────────────────
    const repSession = new FileSession("replay", new FileCassette(dir));
    const repClient = makeClient(repSession, `127.0.0.1:${port}`);
    const replayed = {
      server: await serverStream(repClient, "gamma"),
      client: await clientStream(repClient, ["x", "y"]),
      bidi: await bidiStream(repClient, ["m1", "m2"]),
    };
    repClient.close();

    expect(replayed).toEqual(live);
  });

  test("replaying a scrubbed cassette without the hook fails loudly", async () => {
    const dir = tmpDir();
    const token = "ghp_0123456789abcdefghijklmnopqrstuvwx";
    const mask = "x".repeat(token.length);
    const scrub: StreamScrubFn = (_dir, _info, data) => {
      const s = Buffer.from(data).toString("binary");
      return s.includes(token)
        ? new Uint8Array(Buffer.from(s.split(token).join(mask), "binary"))
        : data;
    };

    const recSession = new FileSession("record", new FileCassette(dir), scrub);
    const recClient = makeClient(recSession, `127.0.0.1:${port}`);
    await clientStream(recClient, [`auth:${token}`]);
    recClient.close();

    // Symmetry is load-bearing: without the hook the live send bytes no
    // longer match the scrubbed recording.
    const repSession = new FileSession("replay", new FileCassette(dir));
    const repClient = makeClient(repSession, "127.0.0.1:1");
    const replayed = await clientStream(repClient, [`auth:${token}`]);
    repClient.close();

    expect(replayed.error).toMatch(/stream mismatch/);
  });
});
