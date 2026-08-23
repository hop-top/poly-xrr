/**
 * Frame-level scrub hook — the normative contract in
 * cassette-format-streaming.md "Frame Scrub Hook".
 *
 * Secrets are rewritten on the DECODED bytes, identically at record and
 * replay time. Symmetry is the correctness invariant: a cassette recorded
 * through a scrub only replays green when the same scrub is active on the
 * replaying session.
 */
import fs from "node:fs";
import path from "node:path";
import { describe, expect, it } from "vitest";
import { FileCassette } from "../src/cassette.js";
import { FileSession } from "../src/session.js";
import { ErrEndOfStream, StreamMismatchError } from "../src/streamSession.js";
import { msgHash, streamFingerprint, type StreamOpen } from "../src/streamfp.js";
import type { StreamDirection, StreamScrubInfo } from "../src/streamScrub.js";
import { caught, grpcStreamOpen, td, tmpDir, utf8 } from "./streamSessionHelpers.js";

const SECRET = "hunter2-FAKE-TOKEN-0123456789";
const MASK = "<scrubbed>";

/** Deterministic scrub replacing the fake token wherever it appears. */
function maskSecret(_dir: StreamDirection, _info: StreamScrubInfo, data: Uint8Array): Uint8Array {
  return utf8(td.decode(data).split(SECRET).join(MASK));
}

/**
 * Mirrors the gRPC adapter's server-stream open under the scrub contract:
 * msg_hash is derived from the SCRUBBED open-message bytes, so record and
 * replay address the cassette by scrubbed content (spec clause 3).
 */
function scrubbedServerOpen(
  s: FileSession,
  service: string,
  method: string,
  msg: Uint8Array
): StreamOpen {
  const scrubbed = s.scrubStreamFrame("send", { adapterID: "grpc", type: "server" }, msg);
  return {
    adapterID: "grpc",
    type: "server",
    identity: { service, method, msg_hash: msgHash(scrubbed) },
    payload: { service, method },
  };
}

describe("frame scrub hook", () => {
  // Clause 1 + 2: both directions scrubbed on the DECODED bytes before
  // persistence, so the secret reaches disk in no encoding.
  it("scrubs both directions before persistence", async () => {
    const dir = tmpDir();
    const s = new FileSession("record", new FileCassette(dir), maskSecret);
    const rec = await s.openStreamRecord(grpcStreamOpen("bidi", "chat.ChatService", "Converse"));
    rec.recordSend(utf8(`ping ${SECRET}`));
    rec.recordRecv(utf8(`pong ${SECRET}`));
    rec.recordHalfClose();
    await rec.finish({ status_code: 0 });

    const pair = await new FileCassette(dir).loadStreamed("grpc", rec.fingerprint);
    expect(td.decode(pair.req.stream.frames[0].message)).toBe(`ping ${MASK}`);
    expect(td.decode(pair.resp.stream.frames[0].message)).toBe(`pong ${MASK}`);

    // Base64 hides the secret from a text scan, so the decoded check above
    // is the real gate; this guards the payload side.
    for (const kind of ["req", "resp"]) {
      const raw = fs.readFileSync(path.join(dir, `grpc-${rec.fingerprint}.${kind}.yaml`), "utf8");
      expect(raw).not.toContain(SECRET);
    }
  });

  // Clause 3: content-derived identity is computed over scrubbed bytes on
  // both sides, so a scrubbed replay resolves to the scrubbed recording.
  it("derives server-stream msg_hash from scrubbed bytes", async () => {
    const dir = tmpDir();
    const msg = utf8(`{"cmd":"deploy","token":"${SECRET}"}`);

    const recS = new FileSession("record", new FileCassette(dir), maskSecret);
    const rec = await recS.openStreamRecord(scrubbedServerOpen(recS, "ops.Deploy", "Run", msg));
    rec.recordSend(msg);
    rec.recordHalfClose();
    rec.recordRecv(utf8("deployed"));
    await rec.finish({ status_code: 0 });

    // Self-consistency: recomputing the fingerprint from the persisted
    // (scrubbed) open frame reproduces the filename.
    const pair = await new FileCassette(dir).loadStreamed("grpc", rec.fingerprint);
    expect(td.decode(pair.req.stream.frames[0].message)).not.toContain(SECRET);
    const fromDisk = streamFingerprint(
      grpcStreamOpen("server", "ops.Deploy", "Run", pair.req.stream.frames[0].message),
      -1
    );
    expect(fromDisk).toBe(rec.fingerprint);

    const repS = new FileSession("replay", new FileCassette(dir), maskSecret);
    const rep = await repS.openStreamReplay(scrubbedServerOpen(repS, "ops.Deploy", "Run", msg));
    expect(rep.fingerprint).toBe(rec.fingerprint);
    rep.send(msg); // live secret-bearing open matches after symmetric scrub
    rep.halfClose();
    expect(td.decode(rep.recv())).toBe("deployed");
    expect(caught(() => rep.recv())).toBe(ErrEndOfStream);
  });

  // Clause 5: symmetry is load-bearing — the same hook replays green, and
  // replaying without it fails loudly rather than silently succeeding.
  it("replays green with the same hook and mismatches without it", async () => {
    const dir = tmpDir();
    const open = grpcStreamOpen("client", "vault.Vault", "Put");

    const recS = new FileSession("record", new FileCassette(dir), maskSecret);
    const rec = await recS.openStreamRecord(open);
    rec.recordSend(utf8(`put ${SECRET}`));
    rec.recordHalfClose();
    await rec.finish({ status_code: 0 });

    const okS = new FileSession("replay", new FileCassette(dir), maskSecret);
    const ok = await okS.openStreamReplay(open);
    ok.send(utf8(`put ${SECRET}`)); // green: symmetric scrub

    const badS = new FileSession("replay", new FileCassette(dir));
    const bad = await badS.openStreamReplay(open);
    expect(caught(() => bad.send(utf8(`put ${SECRET}`)))).toBeInstanceOf(StreamMismatchError);
  });

  // Clause 5: recorded frames were scrubbed at record time and are
  // delivered verbatim — never re-scrubbed. A deliberately non-idempotent
  // hook pins single application per frame per phase.
  it("applies the hook exactly once per frame and never re-scrubs recorded frames", async () => {
    const marker = (_d: StreamDirection, _i: StreamScrubInfo, data: Uint8Array): Uint8Array =>
      utf8(`${td.decode(data)}#`);
    const dir = tmpDir();
    const open = grpcStreamOpen("bidi", "chat.ChatService", "Converse");

    const recS = new FileSession("record", new FileCassette(dir), marker);
    const rec = await recS.openStreamRecord(open);
    rec.recordSend(utf8("ping"));
    rec.recordRecv(utf8("pong"));
    rec.recordHalfClose();
    await rec.finish({ status_code: 0 });

    const pair = await new FileCassette(dir).loadStreamed("grpc", rec.fingerprint);
    expect(td.decode(pair.req.stream.frames[0].message)).toBe("ping#");
    expect(td.decode(pair.resp.stream.frames[0].message)).toBe("pong#");

    const repS = new FileSession("replay", new FileCassette(dir), marker);
    const rep = await repS.openStreamReplay(open);
    rep.send(utf8("ping")); // live send scrubbed once, matches once-scrubbed frame
    expect(td.decode(rep.recv())).toBe("pong#"); // delivered verbatim
  });

  // Clause 2: exactly once per frame per invocation point, and nowhere
  // else. Bytes past the last recorded send are never compared, so they
  // are never scrubbed; half-close and terminal carry no payload.
  it("invokes the hook exactly at the specified points", async () => {
    const seen: string[] = [];
    const trace = (d: StreamDirection, _i: StreamScrubInfo, data: Uint8Array): Uint8Array => {
      seen.push(`${d}:${td.decode(data)}`);
      return data;
    };
    const dir = tmpDir();
    const open = grpcStreamOpen("bidi", "chat.ChatService", "Converse");

    const recS = new FileSession("record", new FileCassette(dir), trace);
    const rec = await recS.openStreamRecord(open);
    rec.recordSend(utf8("a"));
    rec.recordRecv(utf8("b"));
    rec.recordHalfClose(); // no payload — not scrubbed
    await rec.finish({ status_code: 0 }); // terminal — not scrubbed
    expect(seen).toEqual(["send:a", "recv:b"]);

    seen.length = 0;
    const repS = new FileSession("replay", new FileCassette(dir), trace);
    const rep = await repS.openStreamReplay(open);
    rep.send(utf8("a"));
    rep.recv(); // recorded frame — never re-scrubbed
    rep.halfClose();
    expect(seen).toEqual(["send:a"]);

    // Past the last recorded send: bytes are never compared, so the hook
    // is not invoked for them.
    seen.length = 0;
    caught(() => rep.send(utf8("overrun")));
    expect(seen).toEqual([]);
  });

  // Clause 6: the hook may change length; nothing assumes byte-count
  // preservation on either side.
  it("supports a length-changing hook symmetrically", async () => {
    const expand = (_d: StreamDirection, _i: StreamScrubInfo, data: Uint8Array): Uint8Array =>
      utf8(td.decode(data).split(SECRET).join("[REDACTED-MUCH-LONGER-PLACEHOLDER]"));
    const dir = tmpDir();
    const open = grpcStreamOpen("bidi", "chat.ChatService", "Converse");

    const recS = new FileSession("record", new FileCassette(dir), expand);
    const rec = await recS.openStreamRecord(open);
    rec.recordSend(utf8(`k=${SECRET}`));
    rec.recordRecv(utf8(`v=${SECRET}`));
    rec.recordHalfClose();
    await rec.finish({ status_code: 0 });

    const pair = await new FileCassette(dir).loadStreamed("grpc", rec.fingerprint);
    expect(td.decode(pair.req.stream.frames[0].message)).toBe(
      "k=[REDACTED-MUCH-LONGER-PLACEHOLDER]"
    );

    const repS = new FileSession("replay", new FileCassette(dir), expand);
    const rep = await repS.openStreamReplay(open);
    rep.send(utf8(`k=${SECRET}`)); // green despite the length change
    expect(td.decode(rep.recv())).toBe("v=[REDACTED-MUCH-LONGER-PLACEHOLDER]");
  });

  // Clause 7: no hook installed is identical to the feature not existing.
  it("records and replays verbatim with no hook", async () => {
    const dir = tmpDir();
    const open = grpcStreamOpen("bidi", "chat.ChatService", "Converse");

    const recS = new FileSession("record", new FileCassette(dir));
    const rec = await recS.openStreamRecord(open);
    rec.recordSend(utf8(`ping ${SECRET}`));
    rec.recordHalfClose();
    await rec.finish({ status_code: 0 });

    const pair = await new FileCassette(dir).loadStreamed("grpc", rec.fingerprint);
    expect(td.decode(pair.req.stream.frames[0].message)).toBe(`ping ${SECRET}`);
  });

  // Clause 8: the caller cannot mutate stored frame bytes through a buffer
  // the hook returned, nor through what replay delivered.
  it("does not alias caller buffers into stored frames", async () => {
    const passthrough = (_d: StreamDirection, _i: StreamScrubInfo, data: Uint8Array): Uint8Array =>
      data;
    const dir = tmpDir();
    const open = grpcStreamOpen("bidi", "chat.ChatService", "Converse");

    const recS = new FileSession("record", new FileCassette(dir), passthrough);
    const rec = await recS.openStreamRecord(open);
    const live = utf8("ping");
    rec.recordSend(live);
    live[0] = 0x58; // mutate after handing it over — must not reach disk
    rec.recordHalfClose();
    await rec.finish({ status_code: 0 });

    const pair = await new FileCassette(dir).loadStreamed("grpc", rec.fingerprint);
    expect(td.decode(pair.req.stream.frames[0].message)).toBe("ping");
  });
});
