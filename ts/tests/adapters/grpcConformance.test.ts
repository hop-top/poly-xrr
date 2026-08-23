/**
 * gRPC adapter conformance against the spec's streaming fixtures.
 *
 * The adapter-level obligations in spec/cassette-format-streaming.md
 * ("Conformance Obligations") are exercised by driving the real adapter —
 * its interceptor, its replay call, its fingerprint derivation — against
 * every gRPC fixture dir, through a stub grpc-js runtime.
 *
 * Fixture frames carry hand-authored bytes rather than protobuf wire bytes,
 * so the codec used here is byte-transparent: a message IS its wire bytes.
 * That is exactly the raw-bytes codec case a custom grpc-js codec presents,
 * and it keeps the assertions on the adapter's stream semantics (ordering,
 * validation, terminal reconstruction) instead of on protobuf.
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import yaml from "js-yaml";
import { describe, expect, test } from "vitest";
import { FileCassette } from "../../src/cassette.js";
import { FileSession } from "../../src/session.js";
import { GrpcStreamAdapter } from "../../src/adapters/grpc.js";
import type {
  GrpcInterceptorOptions,
  GrpcListener,
  GrpcMethodDefinition,
  GrpcStatus,
} from "../../src/adapters/grpc.js";
import { StreamFormatError, type StreamType } from "../../src/stream.js";
import { StubMetadata, TestCall, byteCodec, drive, stubRuntime } from "./grpcStub.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const fixturesRoot = path.resolve(__dirname, "../../../spec/fixtures");

interface Manifest {
  interactions: Array<{ adapter: string; fingerprint: string; streamed?: boolean }>;
}

interface GrpcPayload {
  service: string;
  method: string;
  n?: number;
}

function manifestOf(dir: string): Manifest {
  return yaml.load(fs.readFileSync(path.join(dir, "manifest.yaml"), "utf8")) as Manifest;
}

/**
 * Replays one fixture interaction through the adapter's interceptor and
 * returns everything the client observed. The recorded req side supplies
 * the sends a conforming client would make.
 */
async function replayFixture(
  dir: string,
  fingerprint: string,
  opts: { sends?: Uint8Array[]; halfClose?: boolean; reads?: number } = {}
): Promise<{ messages: Uint8Array[]; status: GrpcStatus | null; type: StreamType }> {
  const cassette = new FileCassette(dir);
  const pair = await cassette.loadStreamed("grpc", fingerprint);
  const payload = pair.req.payload as GrpcPayload;
  const type = pair.req.stream.type;

  const session = new FileSession("replay", cassette);
  const adapter = new GrpcStreamAdapter(session, stubRuntime);
  const md = byteCodec(`/${payload.service}/${payload.method}`, type);

  return {
    ...(await drive(adapter.interceptor(), md, {
      sends: opts.sends ?? pair.req.stream.frames.map((f) => f.message),
      halfClose: opts.halfClose ?? pair.req.stream.half_close != null,
      reads: opts.reads ?? pair.resp.stream.frames.length + 1,
    })),
    type,
  };
}

describe("grpc adapter — fixture conformance", () => {
  test("grpc-server-stream: content-addressed open locates the pair, frames in seq order", async () => {
    const dir = path.join(fixturesRoot, "grpc-server-stream");
    const { messages, status } = await replayFixture(dir, "58a4bf3f");

    expect(messages.map((m) => new TextDecoder().decode(m))).toEqual([
      "chunk-one\n",
      "chunk-two\n",
      "chunk-three\n",
    ]);
    // End-of-stream after the last frame, as an OK status.
    expect(status?.code).toBe(0);
  });

  test("grpc-server-stream: the fingerprint is derived from the open message", async () => {
    // The cassette is addressed by sha256(open message)[:8], so a divergent
    // request message must miss rather than replay the recorded stream.
    const dir = path.join(fixturesRoot, "grpc-server-stream");
    const { status } = await replayFixture(dir, "58a4bf3f", {
      sends: [new TextEncoder().encode('{"path":"/etc/shadow"}')],
      halfClose: true,
      reads: 1,
    });
    expect(status?.code).not.toBe(0);
    expect(status?.details).toMatch(/cassette miss/);
  });

  test("grpc-client-stream: counter-addressed open, sends validated, single response", async () => {
    const dir = path.join(fixturesRoot, "grpc-client-stream");
    const { messages, status } = await replayFixture(dir, "2bebfd6f");

    expect(messages).toHaveLength(1);
    expect(status?.code).toBe(0);
  });

  test("grpc-client-stream: divergent send bytes are a stream mismatch", async () => {
    const dir = path.join(fixturesRoot, "grpc-client-stream");
    const cassette = new FileCassette(dir);
    const pair = await cassette.loadStreamed("grpc", "2bebfd6f");
    const sends = pair.req.stream.frames.map((f) => f.message);
    expect(sends.length).toBeGreaterThan(0);

    const { status } = await replayFixture(dir, "2bebfd6f", {
      sends: [new TextEncoder().encode("not-what-was-recorded"), ...sends.slice(1)],
      halfClose: true,
      reads: 1,
    });
    expect(status?.code).not.toBe(0);
    expect(status?.details).toMatch(/stream mismatch/);
  });

  test("grpc-client-stream: half-close before all recorded sends is a stream mismatch", async () => {
    const dir = path.join(fixturesRoot, "grpc-client-stream");
    const { status } = await replayFixture(dir, "2bebfd6f", {
      sends: [],
      halfClose: true,
      reads: 1,
    });
    expect(status?.code).not.toBe(0);
    expect(status?.details).toMatch(/stream mismatch/);
  });

  test("grpc-client-stream-repeat: one session yields n=0 then n=1", async () => {
    // The spec's n=1 obligation: a second open of the same tuple in one
    // session must address the second cassette, driven by the session's
    // occurrence counter rather than anything on disk.
    const dir = path.join(fixturesRoot, "grpc-client-stream-repeat");
    const cassette = new FileCassette(dir);
    const session = new FileSession("replay", cassette);
    const adapter = new GrpcStreamAdapter(session, stubRuntime);
    const md = byteCodec("/files.FileService/Upload", "client");

    const first = await cassette.loadStreamed("grpc", "2bebfd6f");
    const second = await cassette.loadStreamed("grpc", "b27b5fe1");
    expect((first.req.payload as GrpcPayload).n).toBe(0);
    expect((second.req.payload as GrpcPayload).n).toBe(1);
    // The two recordings differ in send count, so replaying the second
    // cassette's sends proves the counter advanced.
    expect(second.req.stream.frames).toHaveLength(2);

    const run1 = await drive(adapter.interceptor(), md, {
      sends: first.req.stream.frames.map((f) => f.message),
      halfClose: true,
      reads: first.resp.stream.frames.length + 1,
    });
    expect(run1.status?.code).toBe(0);

    const run2 = await drive(adapter.interceptor(), md, {
      sends: second.req.stream.frames.map((f) => f.message),
      halfClose: true,
      reads: second.resp.stream.frames.length + 1,
    });
    expect(run2.status?.code).toBe(0);
    expect(run2.messages).toHaveLength(second.resp.stream.frames.length);
  });

  test("grpc-bidi-stream: interleaved seq parsed, reads never gate on sends", async () => {
    const dir = path.join(fixturesRoot, "grpc-bidi-stream");
    const cassette = new FileCassette(dir);
    const pair = await cassette.loadStreamed("grpc", "c6233d2e");
    const session = new FileSession("replay", cassette);
    const adapter = new GrpcStreamAdapter(session, stubRuntime);
    const md = byteCodec("/chat.ChatService/Converse", "bidi");

    // Read BOTH recorded recv frames before sending anything: the recording
    // interleaves send/recv (seq 0..5), so a replay that gated recv delivery
    // on send progress would deadlock here.
    const call = new TestCall(adapter.interceptor(), md);
    const observed: Uint8Array[] = [];
    let status: GrpcStatus | null = null;
    const listener: GrpcListener = {
      onReceiveMessage: (m) => observed.push(m as Uint8Array),
      onReceiveStatus: (s) => {
        status = s;
      },
    };
    call.start(new StubMetadata(), listener);
    for (let i = 0; i < pair.resp.stream.frames.length; i++) {
      call.startRead();
      await call.settle();
    }
    expect(observed.map((m) => new TextDecoder().decode(m))).toEqual(["pong-1", "pong-2"]);
    expect(status).toBeNull();

    // Sends still validate in recorded order afterwards.
    for (const f of pair.req.stream.frames) call.sendMessage(f.message);
    call.halfClose();
    call.startRead();
    await call.settle();
    expect((status as GrpcStatus | null)?.code).toBe(0);
  });

  test("grpc-stream-error: recorded frames, then the status rebuilt from status_code", async () => {
    const dir = path.join(fixturesRoot, "grpc-stream-error");
    const { messages, status } = await replayFixture(dir, "9e8c4d4c");

    expect(messages.map((m) => new TextDecoder().decode(m))).toEqual([
      "log-chunk-1\n",
      "log-chunk-2\n",
    ]);
    // status_code 14 = UNAVAILABLE, and the error string is the description.
    expect(status?.code).toBe(14);
    expect(status?.details).toBe("rpc error: code = Unavailable desc = connection reset");
  });

  test("grpc-stream-error: the terminal repeats for every later read", async () => {
    const dir = path.join(fixturesRoot, "grpc-stream-error");
    const { status } = await replayFixture(dir, "9e8c4d4c", { reads: 6 });
    expect(status?.code).toBe(14);
  });

  test("grpc-stream-empty: all three empty shapes replay end-of-stream immediately", async () => {
    const dir = path.join(fixturesRoot, "grpc-stream-empty");
    for (const entry of manifestOf(dir).interactions) {
      const { messages, status } = await replayFixture(dir, entry.fingerprint);
      expect(status?.code, entry.fingerprint).toBe(0);
      // The client-stream case records a single response frame; the
      // server/bidi cases record none.
      expect(messages.length, entry.fingerprint).toBeLessThanOrEqual(1);
    }
  });

  test("grpc-stream-malformed-b64 is rejected, not decoded leniently", async () => {
    const dir = path.join(fixturesRoot, "grpc-stream-malformed-b64");
    const cassette = new FileCassette(dir);
    const session = new FileSession("replay", cassette);
    const adapter = new GrpcStreamAdapter(session, stubRuntime);
    const md = byteCodec("/files.FileService/Download", "server");

    // The bad base64 must surface as a load failure through the adapter, not
    // as a silently truncated stream. The req side is well-formed and
    // addresses fingerprint 8dbfb222, so the open succeeds and the invalid
    // resp frames are what must be rejected.
    const { status } = await drive(adapter.interceptor(), md, {
      sends: [new TextEncoder().encode('{"path":"/opt/blob.bin"}')],
      halfClose: true,
      reads: 1,
    });
    expect(status?.code).not.toBe(0);
    expect(status?.details).toMatch(/base64/);

    // And directly, so the rejection is unambiguous.
    await expect(cassette.loadStreamed("grpc", "8dbfb222")).rejects.toThrow(StreamFormatError);
  });

  test("every gRPC fixture manifest entry is reachable through the adapter", async () => {
    // Guards against a fixture dir being added without adapter coverage.
    const dirs = fs
      .readdirSync(fixturesRoot, { withFileTypes: true })
      .filter((e) => e.isDirectory() && e.name.startsWith("grpc-"))
      .map((e) => e.name);
    expect(dirs.sort()).toEqual([
      "grpc-bidi-stream",
      "grpc-client-stream",
      "grpc-client-stream-repeat",
      "grpc-server-stream",
      "grpc-stream-empty",
      "grpc-stream-error",
      "grpc-stream-malformed-b64",
    ]);

    for (const name of dirs) {
      if (name === "grpc-stream-malformed-b64") continue; // asserted rejected above
      const dir = path.join(fixturesRoot, name);
      for (const entry of manifestOf(dir).interactions) {
        const { status } = await replayFixture(dir, entry.fingerprint);
        expect(status, `${name}/${entry.fingerprint}`).not.toBeNull();
      }
    }
  });
});

describe("grpc adapter — method mapping", () => {
  test("stream type is derived from the grpc-js direction flags", async () => {
    const { streamTypeOf } = await import("../../src/adapters/grpc.js");
    const md = (requestStream: boolean, responseStream: boolean): GrpcMethodDefinition => ({
      path: "/s.S/M",
      requestStream,
      responseStream,
      requestSerialize: () => Buffer.alloc(0),
      responseDeserialize: () => null,
    });
    expect(streamTypeOf(md(false, true))).toBe("server");
    expect(streamTypeOf(md(true, false))).toBe("client");
    expect(streamTypeOf(md(true, true))).toBe("bidi");
    // Unary RPCs keep the v1 unary format and must not reach the stream path.
    expect(() => streamTypeOf(md(false, false))).toThrow(/unary-shaped/);
  });

  test("full method paths split into proto service and method identifiers", async () => {
    const { splitFullMethod } = await import("../../src/adapters/grpc.js");
    expect(splitFullMethod("/files.FileService/Download")).toEqual({
      service: "files.FileService",
      method: "Download",
    });
    expect(() => splitFullMethod("/nomethod")).toThrow(/malformed full method/);
  });

  test("passthrough mode returns the next call untouched", async () => {
    const session = new FileSession("passthrough", new FileCassette("/nonexistent"));
    const adapter = new GrpcStreamAdapter(session, stubRuntime);
    const md = byteCodec("/s.S/M", "bidi");
    const sentinel = {} as never;
    const options = { method_definition: md } as GrpcInterceptorOptions;
    expect(adapter.interceptor()(options, () => sentinel)).toBe(sentinel);
  });
});
