import fs from "node:fs";
import path from "node:path";
import { describe, expect, test } from "vitest";
import { FileCassette } from "../src/cassette.js";
import { FileSession } from "../src/session.js";
import { type StreamType, type StreamedInteraction } from "../src/stream.js";
import { streamFingerprint } from "../src/streamfp.js";
import { ErrEndOfStream } from "../src/streamSession.js";
import { type Cassette } from "../src/xrr.js";
import {
  caught,
  fixturesRoot,
  grpcStreamOpen,
  sseStreamOpen,
  td,
  tmpDir,
  utf8,
} from "./streamSessionHelpers.js";

// ── identity seam ──────────────────────────────────────────────────────────

describe("streamFingerprint — identity seam", () => {
  test("reproduces the gRPC spec vectors", () => {
    expect(
      streamFingerprint(
        grpcStreamOpen("server", "files.FileService", "Download", utf8('{"path":"/etc/hosts"}')),
        -1
      )
    ).toBe("58a4bf3f");
    expect(
      streamFingerprint(
        grpcStreamOpen(
          "server",
          "files.FileService",
          "Download",
          utf8('{"path":"/var/log/big.log"}')
        ),
        -1
      )
    ).toBe("9e8c4d4c");
    expect(
      streamFingerprint(grpcStreamOpen("client", "files.FileService", "Upload"), 0)
    ).toBe("2bebfd6f");
    expect(
      streamFingerprint(grpcStreamOpen("client", "files.FileService", "Upload"), 1)
    ).toBe("b27b5fe1");
    expect(
      streamFingerprint(grpcStreamOpen("bidi", "chat.ChatService", "Converse"), 0)
    ).toBe("c6233d2e");
  });

  test("url-keyed identity reproduces the sse fixture fingerprint", () => {
    expect(streamFingerprint(sseStreamOpen("https://example.test/events"), -1)).toBe("66ecc77a");
  });

  test("rejects reserved identity keys", () => {
    const base = sseStreamOpen("https://example.test/events");
    expect(() =>
      streamFingerprint({ ...base, identity: { url: "u", stream: "server" } }, -1)
    ).toThrow(/reserved/);
    expect(() => streamFingerprint({ ...base, identity: { url: "u", n: 3 } }, -1)).toThrow(
      /reserved/
    );
  });

  test("rejects an invalid stream type", () => {
    const open = { ...sseStreamOpen("u"), type: "duplex" as StreamType };
    expect(() => streamFingerprint(open, -1)).toThrow(/invalid/);
  });

  test("counter-addressed opens require n >= 0", () => {
    expect(() =>
      streamFingerprint(grpcStreamOpen("client", "files.FileService", "Upload"), -1)
    ).toThrow(/n must be >= 0/);
  });
});

// ── record path ────────────────────────────────────────────────────────────

describe("openStreamRecord", () => {
  test("server round-trip: pair on disk validates, dense seq, at_ms monotonic", async () => {
    const dir = tmpDir();
    const session = new FileSession("record", new FileCassette(dir));
    const msg = utf8('{"path":"/etc/hosts"}');
    const rec = await session.openStreamRecord(
      grpcStreamOpen("server", "files.FileService", "Download", msg)
    );
    expect(rec.fingerprint).toBe("58a4bf3f");

    rec.recordSend(msg);
    rec.recordHalfClose();
    rec.recordRecv(utf8("chunk-one\n"));
    rec.recordRecv(utf8("chunk-two\n"));
    await rec.finish({ status_code: 0 });

    // Fingerprint filenames per the v1 naming convention.
    expect(fs.existsSync(path.join(dir, "grpc-58a4bf3f.req.yaml"))).toBe(true);
    expect(fs.existsSync(path.join(dir, "grpc-58a4bf3f.resp.yaml"))).toBe(true);

    // loadStreamed validates the pair (ascending frames, seq uniqueness,
    // maximal end.seq) — a load without throwing is a validity check itself.
    const pair = await new FileCassette(dir).loadStreamed("grpc", "58a4bf3f");
    expect(pair.req.stream.type).toBe("server");

    // Dense seq 0..N-1 counting all events in arrival order.
    expect(pair.req.stream.frames.map((f) => f.seq)).toEqual([0]);
    expect(pair.req.stream.half_close?.seq).toBe(1);
    expect(pair.resp.stream.frames.map((f) => f.seq)).toEqual([2, 3]);
    expect(pair.resp.stream.end.seq).toBe(4);

    expect(td.decode(pair.resp.stream.frames[0].message)).toBe("chunk-one\n");
    expect(td.decode(pair.resp.stream.frames[1].message)).toBe("chunk-two\n");

    // at_ms stamped on every event, ≥ 0 and non-decreasing in event order.
    const atMs = [
      pair.req.stream.frames[0].at_ms,
      pair.req.stream.half_close?.at_ms,
      pair.resp.stream.frames[0].at_ms,
      pair.resp.stream.frames[1].at_ms,
      pair.resp.stream.end.at_ms,
    ];
    for (const at of atMs) expect(at).toBeGreaterThanOrEqual(0);
    for (let i = 1; i < atMs.length; i++) {
      expect(atMs[i]).toBeGreaterThanOrEqual(atMs[i - 1] as number);
    }

    // Server-stream payload carries no occurrence ordinal.
    expect(pair.req.payload).toEqual({ service: "files.FileService", method: "Download" });
  });

  test("recorded bidi conversation matches the fixture shape", async () => {
    const dir = tmpDir();
    const session = new FileSession("record", new FileCassette(dir));
    const rec = await session.openStreamRecord(
      grpcStreamOpen("bidi", "chat.ChatService", "Converse")
    );
    rec.recordSend(utf8("ping-1"));
    rec.recordRecv(utf8("pong-1"));
    rec.recordSend(utf8("ping-2"));
    rec.recordRecv(utf8("pong-2"));
    rec.recordHalfClose();
    await rec.finish({ status_code: 0 });

    const shape = (pair: StreamedInteraction) => ({
      type: pair.req.stream.type,
      reqPayload: pair.req.payload,
      respPayload: pair.resp.payload,
      error: pair.resp.error ?? "",
      sends: pair.req.stream.frames.map((f) => ({ seq: f.seq, text: td.decode(f.message) })),
      recvs: pair.resp.stream.frames.map((f) => ({ seq: f.seq, text: td.decode(f.message) })),
      halfClose: pair.req.stream.half_close?.seq ?? null,
      end: pair.resp.stream.end.seq,
    });

    const recorded = await new FileCassette(dir).loadStreamed("grpc", "c6233d2e");
    const fixture = await new FileCassette(
      path.join(fixturesRoot, "grpc-bidi-stream")
    ).loadStreamed("grpc", "c6233d2e");
    expect(shape(recorded)).toEqual(shape(fixture));
  });

  test("counter-addressed opens record n=0 then n=1 in one session", async () => {
    const dir = tmpDir();
    const session = new FileSession("record", new FileCassette(dir));
    const open = () => grpcStreamOpen("client", "files.FileService", "Upload");

    const rec1 = await session.openStreamRecord(open());
    expect(rec1.fingerprint).toBe("2bebfd6f");
    rec1.recordSend(utf8("alpha\n"));
    rec1.recordHalfClose();
    rec1.recordRecv(utf8('{"received_bytes":6}'));
    await rec1.finish({ status_code: 0 });

    const rec2 = await session.openStreamRecord(open());
    expect(rec2.fingerprint).toBe("b27b5fe1");
    rec2.recordHalfClose();
    await rec2.finish({ status_code: 0 });

    // The informational occurrence ordinal is recoverable from disk.
    const cassette = new FileCassette(dir);
    const p1 = await cassette.loadStreamed("grpc", "2bebfd6f");
    expect((p1.req.payload as { n?: number }).n).toBe(0);
    const p2 = await cassette.loadStreamed("grpc", "b27b5fe1");
    expect((p2.req.payload as { n?: number }).n).toBe(1);

    // A different tuple starts its own count.
    const rec3 = await session.openStreamRecord(
      grpcStreamOpen("bidi", "chat.ChatService", "Converse")
    );
    expect(rec3.fingerprint).toBe("c6233d2e");
  });

  test("post-finish events are dropped; a second finish rejects", async () => {
    const dir = tmpDir();
    const session = new FileSession("record", new FileCassette(dir));
    const rec = await session.openStreamRecord(
      grpcStreamOpen("bidi", "chat.ChatService", "Converse")
    );
    rec.recordSend(utf8("ping-1"));
    rec.recordRecv(utf8("pong-1"));
    await rec.finish({ status_code: 0 });

    // Dropped, matching the real-world no-op.
    rec.recordSend(utf8("late"));
    rec.recordRecv(utf8("late"));
    rec.recordHalfClose();
    await expect(rec.finish({ status_code: 0 })).rejects.toThrow(/already finished/);

    const pair = await new FileCassette(dir).loadStreamed("grpc", rec.fingerprint);
    expect(pair.req.stream.frames).toHaveLength(1);
    expect(pair.resp.stream.frames).toHaveLength(1);
    expect(pair.req.stream.half_close).toBeUndefined();
    expect(pair.resp.stream.end.seq).toBe(2);
  });

  test("error terminal persists the envelope error and payload", async () => {
    const dir = tmpDir();
    const session = new FileSession("record", new FileCassette(dir));
    const msg = utf8('{"path":"/var/log/big.log"}');
    const rec = await session.openStreamRecord(
      grpcStreamOpen("server", "files.FileService", "Download", msg)
    );
    rec.recordSend(msg);
    rec.recordHalfClose();
    rec.recordRecv(utf8("log-chunk-1\n"));
    await rec.finish(
      { status_code: 14 },
      new Error("rpc error: code = Unavailable desc = connection reset")
    );

    const pair = await new FileCassette(dir).loadStreamed("grpc", "9e8c4d4c");
    expect(pair.resp.error).toBe("rpc error: code = Unavailable desc = connection reset");
    expect(pair.resp.payload).toEqual({ status_code: 14 });
  });

  test("empty recording emits frames: [] on both sides and validates", async () => {
    const dir = tmpDir();
    const session = new FileSession("record", new FileCassette(dir));
    const rec = await session.openStreamRecord(
      grpcStreamOpen("bidi", "chat.ChatService", "Ping")
    );
    rec.recordHalfClose();
    await rec.finish({ status_code: 0 });

    const pair = await new FileCassette(dir).loadStreamed("grpc", "ebbd3938");
    expect(pair.req.stream.frames).toEqual([]);
    expect(pair.resp.stream.frames).toEqual([]);
    expect(pair.req.stream.half_close?.seq).toBe(0);
    expect(pair.resp.stream.end.seq).toBe(1);
  });
});

// ── open guards ────────────────────────────────────────────────────────────

describe("stream open guards", () => {
  test("mode enforcement in both directions", async () => {
    const dir = tmpDir();
    const open = grpcStreamOpen("bidi", "s", "m");
    const replaySession = new FileSession("replay", new FileCassette(dir));
    await expect(replaySession.openStreamRecord(open)).rejects.toThrow(/requires record mode/);
    const recordSession = new FileSession("record", new FileCassette(dir));
    await expect(recordSession.openStreamReplay(open)).rejects.toThrow(/requires replay mode/);
  });

  test("adapter id is required", async () => {
    const session = new FileSession("record", new FileCassette(tmpDir()));
    const open = { ...grpcStreamOpen("bidi", "s", "m"), adapterID: "" };
    await expect(session.openStreamRecord(open)).rejects.toThrow(/adapter id/);
  });

  test("a cassette without streamed IO is rejected", async () => {
    const unary: Cassette = {
      save: async () => {},
      load: async () => ({ req: {}, resp: {} }),
    };
    const session = new FileSession("record", unary);
    await expect(
      session.openStreamRecord(grpcStreamOpen("bidi", "s", "m"))
    ).rejects.toThrow(/stream-capable/);
  });
});

// ── record → replay integration ────────────────────────────────────────────

describe("record then replay", () => {
  test("a recorded pair replays through a fresh replay session", async () => {
    const dir = tmpDir();
    const recSession = new FileSession("record", new FileCassette(dir));
    const rec = await recSession.openStreamRecord(
      grpcStreamOpen("client", "files.FileService", "Upload")
    );
    rec.recordSend(utf8("part-one\n"));
    rec.recordSend(utf8("part-two\n"));
    rec.recordHalfClose();
    rec.recordRecv(utf8('{"received_bytes":18}'));
    await rec.finish({ status_code: 0 });

    const repSession = new FileSession("replay", new FileCassette(dir));
    const rep = await repSession.openStreamReplay(
      grpcStreamOpen("client", "files.FileService", "Upload")
    );
    expect(rep.fingerprint).toBe("2bebfd6f");
    rep.send(utf8("part-one\n"));
    rep.send(utf8("part-two\n"));
    rep.halfClose();
    expect(td.decode(rep.recv())).toBe('{"received_bytes":18}');
    expect(caught(() => rep.recv())).toBe(ErrEndOfStream);
  });
});
