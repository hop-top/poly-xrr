import { describe, expect, test } from "vitest";
import { FileCassette } from "../src/cassette.js";
import { FileSession } from "../src/session.js";
import { ShapeMismatchError } from "../src/stream.js";
import { streamFingerprint } from "../src/streamfp.js";
import { ErrEndOfStream, StreamMismatchError } from "../src/streamSession.js";
import { ErrCassetteMiss } from "../src/xrr.js";
import {
  caught,
  fixtureSession,
  grpcStreamOpen,
  sseStreamOpen,
  td,
  tmpDir,
  utf8,
} from "./streamSessionHelpers.js";

// ── fixture corpus through the session API ─────────────────────────────────

describe("openStreamReplay — fixtures", () => {
  test("grpc-server-stream: frames in seq order, then end-of-stream", async () => {
    const session = fixtureSession("grpc-server-stream");
    const rep = await session.openStreamReplay(
      grpcStreamOpen("server", "files.FileService", "Download", utf8('{"path":"/etc/hosts"}'))
    );
    expect(rep.fingerprint).toBe("58a4bf3f");
    expect(rep.type).toBe("server");

    rep.send(utf8('{"path":"/etc/hosts"}'));
    rep.halfClose();
    expect(td.decode(rep.recv())).toBe("chunk-one\n");
    expect(td.decode(rep.recv())).toBe("chunk-two\n");
    expect(td.decode(rep.recv())).toBe("chunk-three\n");
    expect(caught(() => rep.recv())).toBe(ErrEndOfStream);
  });

  test("grpc-bidi-stream: reads never gate on send progress; terminal repeats", async () => {
    const session = fixtureSession("grpc-bidi-stream");
    const rep = await session.openStreamReplay(
      grpcStreamOpen("bidi", "chat.ChatService", "Converse")
    );
    expect(rep.fingerprint).toBe("c6233d2e");

    // Drain both pongs before any send.
    expect(td.decode(rep.recv())).toBe("pong-1");
    expect(td.decode(rep.recv())).toBe("pong-2");

    // Sends validated in order and bytes afterwards.
    rep.send(utf8("ping-1"));
    rep.send(utf8("ping-2"));
    rep.halfClose();

    expect(caught(() => rep.recv())).toBe(ErrEndOfStream);
    expect(caught(() => rep.recv())).toBe(ErrEndOfStream); // terminal repeats for j > R
  });

  test("grpc-client-stream: sends validated, single response, end-of-stream", async () => {
    const session = fixtureSession("grpc-client-stream");
    const rep = await session.openStreamReplay(
      grpcStreamOpen("client", "files.FileService", "Upload")
    );
    expect(rep.fingerprint).toBe("2bebfd6f");
    rep.send(utf8("part-one\n"));
    rep.send(utf8("part-two\n"));
    rep.halfClose();
    expect(td.decode(rep.recv())).toBe('{"received_bytes":18}');
    expect(caught(() => rep.recv())).toBe(ErrEndOfStream);
  });

  test("grpc-client-stream-repeat: scripted two-open session yields n=0 then n=1", async () => {
    const session = fixtureSession("grpc-client-stream-repeat");
    const open = () => grpcStreamOpen("client", "files.FileService", "Upload");

    const first = await session.openStreamReplay(open());
    expect(first.fingerprint).toBe("2bebfd6f");
    expect((first.reqPayload as { n?: number }).n).toBe(0);
    first.send(utf8("alpha\n"));
    first.halfClose();
    expect(td.decode(first.recv())).toBe('{"received_bytes":6}');
    expect(caught(() => first.recv())).toBe(ErrEndOfStream);

    const second = await session.openStreamReplay(open());
    expect(second.fingerprint).toBe("b27b5fe1");
    expect((second.reqPayload as { n?: number }).n).toBe(1);
    second.send(utf8("beta-1\n"));
    second.send(utf8("beta-2\n"));
    second.halfClose();
    expect(td.decode(second.recv())).toBe('{"received_bytes":14}');
  });

  test("grpc-stream-error: frames, then the recorded error; post-completion send returns it", async () => {
    const session = fixtureSession("grpc-stream-error");
    const rep = await session.openStreamReplay(
      grpcStreamOpen(
        "server",
        "files.FileService",
        "Download",
        utf8('{"path":"/var/log/big.log"}')
      )
    );
    expect(rep.fingerprint).toBe("9e8c4d4c");
    expect((rep.respPayload as { status_code?: number }).status_code).toBe(14);

    rep.send(utf8('{"path":"/var/log/big.log"}'));
    rep.halfClose();
    expect(td.decode(rep.recv())).toBe("log-chunk-1\n");
    expect(td.decode(rep.recv())).toBe("log-chunk-2\n");

    const wantErr = "rpc error: code = Unavailable desc = connection reset";
    const terminal = caught(() => rep.recv());
    expect(terminal).toBeInstanceOf(Error);
    expect((terminal as Error).message).toBe(wantErr);
    expect(terminal).not.toBeInstanceOf(StreamMismatchError);
    expect(caught(() => rep.recv())).toBe(terminal); // recorded error repeats for j > R

    // The recorded stream was already dead: post-completion send returns it.
    expect(caught(() => rep.send(utf8("extra")))).toBe(terminal);
  });

  test("grpc-stream-error: error-terminal send does not poison the recv side", async () => {
    const session = fixtureSession("grpc-stream-error");
    const rep = await session.openStreamReplay(
      grpcStreamOpen(
        "server",
        "files.FileService",
        "Download",
        utf8('{"path":"/var/log/big.log"}')
      )
    );
    rep.send(utf8('{"path":"/var/log/big.log"}'));
    const err = caught(() => rep.send(utf8("extra"))); // i ≥ S, error terminal
    expect((err as Error).message).toMatch(/Unavailable/);
    expect(td.decode(rep.recv())).toBe("log-chunk-1\n"); // recv unaffected
  });

  test("grpc-stream-empty: all three degenerate shapes", async () => {
    // server-stream whose server sent nothing before OK.
    const s1 = fixtureSession("grpc-stream-empty");
    const server = await s1.openStreamReplay(
      grpcStreamOpen("server", "files.FileService", "Download", utf8('{"path":"/etc/empty"}'))
    );
    expect(caught(() => server.recv())).toBe(ErrEndOfStream); // first read is end-of-stream

    // client-stream where the client half-closed immediately (S=0).
    const s2 = fixtureSession("grpc-stream-empty");
    const client = await s2.openStreamReplay(
      grpcStreamOpen("client", "telemetry.MetricsService", "Push")
    );
    client.halfClose(); // S=0: immediate half-close accepted
    expect(td.decode(client.recv())).toBe('{"count":0}');
    expect(caught(() => client.recv())).toBe(ErrEndOfStream);

    // bidi with no traffic at all.
    const s3 = fixtureSession("grpc-stream-empty");
    const bidi = await s3.openStreamReplay(grpcStreamOpen("bidi", "chat.ChatService", "Ping"));
    bidi.halfClose();
    expect(caught(() => bidi.recv())).toBe(ErrEndOfStream);
  });

  test("sse-text-scalars: url-keyed identity locates and replays the pair", async () => {
    const session = fixtureSession("sse-text-scalars");
    const rep = await session.openStreamReplay(sseStreamOpen("https://example.test/events"));
    expect(rep.fingerprint).toBe("66ecc77a");
    expect(rep.type).toBe("server");

    const texts = ["on", "12:30", "null", " leading", "trailing ", "  padded  "];
    for (const want of texts) expect(td.decode(rep.recv())).toBe(want);
    expect(caught(() => rep.recv())).toBe(ErrEndOfStream);
  });
});

// ── mismatch model ─────────────────────────────────────────────────────────

describe("openStreamReplay — mismatch model", () => {
  test("byte-divergent send at i < S is terminal and poisons everything", async () => {
    const session = fixtureSession("grpc-bidi-stream");
    const rep = await session.openStreamReplay(
      grpcStreamOpen("bidi", "chat.ChatService", "Converse")
    );

    rep.send(utf8("ping-1"));
    const err = caught(() => rep.send(utf8("ping-DIVERGED")));
    expect(err).toBeInstanceOf(StreamMismatchError);
    expect((err as StreamMismatchError).op).toBe("send");
    expect((err as StreamMismatchError).ordinal).toBe(1);
    expect((err as StreamMismatchError).message).toMatch(/sha256/);

    // Mismatch poisons every subsequent operation.
    expect(caught(() => rep.recv())).toBe(err);
    expect(caught(() => rep.halfClose())).toBe(err);
    expect(caught(() => rep.send(utf8("ping-2")))).toBe(err);
  });

  test("half-close after fewer than S sends is a mismatch", async () => {
    const session = fixtureSession("grpc-client-stream");
    const rep = await session.openStreamReplay(
      grpcStreamOpen("client", "files.FileService", "Upload")
    );
    rep.send(utf8("part-one\n"));
    const err = caught(() => rep.halfClose()); // after 1 of 2 sends
    expect(err).toBeInstanceOf(StreamMismatchError);
    expect((err as StreamMismatchError).op).toBe("half_close");
  });

  test("post-completion send with an OK terminal does not poison", async () => {
    const session = fixtureSession("grpc-client-stream");
    const rep = await session.openStreamReplay(
      grpcStreamOpen("client", "files.FileService", "Upload")
    );
    rep.send(utf8("part-one\n"));
    rep.send(utf8("part-two\n"));
    expect(caught(() => rep.send(utf8("part-three\n")))).toBe(ErrEndOfStream);
    rep.halfClose(); // half-close after all recorded sends is always accepted
    expect(td.decode(rep.recv())).toBe('{"received_bytes":18}');
    expect(caught(() => rep.recv())).toBe(ErrEndOfStream);
  });
});

// ── miss vs shape mismatch ─────────────────────────────────────────────────

describe("openStreamReplay — miss and shape mismatch", () => {
  test("miss is ErrCassetteMiss; a unary pair at the fingerprint is a shape mismatch", async () => {
    const dir = tmpDir();
    const cassette = new FileCassette(dir);
    const session = new FileSession("replay", cassette);
    const open = () => grpcStreamOpen("bidi", "s", "m");

    // No pair on disk ⇒ cassette miss. The counter is consumed, hit or miss.
    await expect(session.openStreamReplay(open())).rejects.toBe(ErrCassetteMiss);

    // A unary pair at the streamed fingerprint (n=1 now) ⇒ shape mismatch.
    const fp = streamFingerprint(open(), 1);
    await cassette.save("grpc", fp, { service: "s", method: "m" }, { status_code: 0 });
    const err = await session.openStreamReplay(open()).catch((e: unknown) => e);
    expect(err).toBeInstanceOf(ShapeMismatchError);
    expect(err).not.toBe(ErrCassetteMiss);
  });

  test("recorded stream type differing from the open is a shape mismatch", async () => {
    const dir = tmpDir();
    const cassette = new FileCassette(dir);
    const session = new FileSession("replay", cassette);
    const open = grpcStreamOpen("bidi", "chat.ChatService", "Converse");
    const fp = streamFingerprint(open, 0);
    const recordedAt = "2026-08-23T12:00:00Z";
    await cassette.saveStreamed({
      req: {
        xrr: "1",
        adapter: "grpc",
        fingerprint: fp,
        recorded_at: recordedAt,
        payload: { service: "chat.ChatService", method: "Converse", n: 0 },
        stream: { type: "client", frames: [], half_close: { seq: 0 } },
      },
      resp: {
        xrr: "1",
        adapter: "grpc",
        fingerprint: fp,
        recorded_at: recordedAt,
        payload: { status_code: 0 },
        stream: { frames: [], end: { seq: 1 } },
      },
    });
    const err = await session.openStreamReplay(open).catch((e: unknown) => e);
    expect(err).toBeInstanceOf(ShapeMismatchError);
    expect((err as Error).message).toMatch(/recorded stream type/);
  });
});
