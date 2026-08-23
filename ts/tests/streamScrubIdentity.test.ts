/**
 * Identity-hook conformance — spec "Scrub Hook Obligations — Identity-Hook
 * Matrix" (M1..M7).
 *
 * The scrub hook's contract is WHEN it runs and WHAT it receives, never
 * what it computes; xrr defines no scrub algorithm. Two byte-neutral hooks
 * generate the whole matrix:
 *
 *   - identity: returns its input. Installed and invoked but byte-neutral,
 *     so any divergence from a no-hook session is a mechanics defect —
 *     clause 7 fixes no-hook behaviour as the reference.
 *   - counting: identity plus a call log. Reveals invocation points,
 *     multiplicity, and — the part fixtures cannot see — non-invocation.
 *
 * Mirrors go/stream_scrub_identity_test.go.
 */
import fs from "node:fs";
import path from "node:path";
import { describe, expect, it } from "vitest";
import { FileCassette } from "../src/cassette.js";
import { FileSession } from "../src/session.js";
import type { StreamType } from "../src/stream.js";
import { ErrEndOfStream, StreamMismatchError } from "../src/streamSession.js";
import { msgHash, type StreamOpen } from "../src/streamfp.js";
import type { StreamDirection, StreamScrubFn, StreamScrubInfo } from "../src/streamScrub.js";
import { grpcStreamOpen, td, tmpDir, utf8 } from "./streamSessionHelpers.js";

const OPEN_MSG = utf8('{"room":"ops"}');

/** Clause 6's "MAY return the input unchanged": observable, byte-neutral. */
function identityScrub(_dir: StreamDirection, _info: StreamScrubInfo, data: Uint8Array): Uint8Array {
  return data;
}

type ScrubCall = [StreamDirection, string];

/**
 * Identity plus a call log. The bookkeeping is test scaffolding, not scrub
 * state — the bytes returned are the input, so clause 4's determinism holds.
 */
function countingScrub(log: ScrubCall[]): StreamScrubFn {
  return (dir, _info, data) => {
    log.push([dir, td.decode(data)]);
    return data;
  };
}

/** gRPC mapping: server streams record exactly one send frame. */
function fixedSends(type: StreamType): Uint8Array[] {
  return type === "server" ? [OPEN_MSG] : [utf8("alpha"), utf8("beta")];
}

/** gRPC mapping: client streams record at most one recv frame. */
function fixedRecvs(type: StreamType): Uint8Array[] {
  return type === "client" ? [utf8("ack")] : [utf8("one"), utf8("two")];
}

function fixedOpen(type: StreamType): StreamOpen {
  return grpcStreamOpen(type, "chat.ChatService", "Converse", OPEN_MSG);
}

/**
 * One identical scripted stream through a record session, so two sessions
 * differing only in hook installation are byte-comparable.
 */
async function recordFixed(dir: string, type: StreamType, scrub?: StreamScrubFn): Promise<string> {
  const s = new FileSession("record", new FileCassette(dir), scrub);
  const rec = await s.openStreamRecord(fixedOpen(type));
  for (const f of fixedSends(type)) rec.recordSend(f);
  rec.recordHalfClose();
  for (const f of fixedRecvs(type)) rec.recordRecv(f);
  const fp = rec.fingerprint;
  await rec.finish({ status_code: 0 });
  return fp;
}

async function replayFixed(dir: string, type: StreamType, scrub?: StreamScrubFn): Promise<void> {
  const s = new FileSession("replay", new FileCassette(dir), scrub);
  const rep = await s.openStreamReplay(fixedOpen(type));
  for (const f of fixedSends(type)) rep.send(f);
  rep.halfClose();
  for (const want of fixedRecvs(type)) expect(rep.recv()).toEqual(want);
  expect(() => rep.recv()).toThrow(ErrEndOfStream);
}

function pairBytes(dir: string, fp: string): [string, string] {
  return [
    fs.readFileSync(path.join(dir, `grpc-${fp}.req.yaml`), "utf8"),
    fs.readFileSync(path.join(dir, `grpc-${fp}.resp.yaml`), "utf8"),
  ];
}

const TYPES: StreamType[] = ["server", "client", "bidi"];

describe("identity-hook scrub conformance", () => {
  // M1: an installed identity hook is byte-indistinguishable from no hook.
  // Any divergence is a mechanics defect — an extra scrub site, a missed
  // one, or an identity input derived from the wrong bytes.
  for (const type of TYPES) {
    it(`${type}: identity hook produces the same bytes as no hook`, async () => {
      const bare = tmpDir();
      const hooked = tmpDir();
      const bareFP = await recordFixed(bare, type);
      const hookedFP = await recordFixed(hooked, type, identityScrub);
      expect(hookedFP).toBe(bareFP);
      expect(pairBytes(hooked, hookedFP)).toEqual(pairBytes(bare, bareFP));
    });
  }

  // M2: because the identity hook changes no bytes, a cassette crosses the
  // hook boundary both ways. The one legitimate exception to clause 5's
  // "same hook both sides" — it holds precisely because the two agree
  // byte-for-byte.
  for (const type of TYPES) {
    it(`${type}: replays across the hook boundary in both directions`, async () => {
      const withHook = tmpDir();
      await recordFixed(withHook, type, identityScrub);
      await replayFixed(withHook, type, undefined);

      const without = tmpDir();
      await recordFixed(without, type);
      await replayFixed(without, type, identityScrub);
    });
  }

  // M3: clause 3 routes content-derived identity through the hook. Under
  // identity it must land on the raw msg_hash in both modes — otherwise
  // the hook is applied to the wrong buffer, or applied twice.
  for (const mode of ["record", "replay"] as const) {
    it(`${mode}: identity-derived msg_hash equals the raw one`, () => {
      const s = new FileSession(mode, new FileCassette(tmpDir()), identityScrub);
      const scrubbed = s.scrubStreamFrame("send", { adapterID: "grpc", type: "server" }, OPEN_MSG);
      expect(msgHash(scrubbed)).toBe(msgHash(OPEN_MSG));
    });
  }

  // M4: exactly one call per frame per direction, in frame order, carrying
  // that frame's bytes. Half-close and the terminal carry no payload and
  // contribute no call.
  it("record: one call per frame per direction, none for half-close or terminal", async () => {
    const log: ScrubCall[] = [];
    await recordFixed(tmpDir(), "bidi", countingScrub(log));
    expect(log).toEqual([
      ["send", "alpha"],
      ["send", "beta"],
      ["recv", "one"],
      ["recv", "two"],
    ]);
  });

  // M5: replay scrubs live sends only, once each, and never touches
  // recorded frames. The trailing case caught a real cross-port divergence:
  // ts and php ran the hook BEFORE the bound check that rejects a send past
  // the end of the recording; go, py and rs ran it after. Only a counting
  // hook sees that.
  it("replay: live sends only, and nothing past the last recorded frame", async () => {
    const dir = tmpDir();
    await recordFixed(dir, "bidi", identityScrub);

    const log: ScrubCall[] = [];
    const s = new FileSession("replay", new FileCassette(dir), countingScrub(log));
    const rep = await s.openStreamReplay(fixedOpen("bidi"));
    rep.send(utf8("alpha"));
    rep.send(utf8("beta"));
    rep.halfClose();
    expect(rep.recv()).toEqual(utf8("one"));
    expect(rep.recv()).toEqual(utf8("two"));
    expect(log).toEqual([
      ["send", "alpha"],
      ["send", "beta"],
    ]);

    log.length = 0;
    expect(() => rep.send(utf8("overrun"))).toThrow();
    expect(log).toEqual([]);
  });

  // M6: clause 3's no-pre-scrub rule. The gRPC server-stream open message
  // is both an identity input and a persisted frame — two distinct
  // invocation points, one call each. An adapter that pre-scrubbed the
  // message it also hands the core would show two calls for the persist
  // point.
  it("no double scrub: each invocation point fires exactly once", async () => {
    const log: ScrubCall[] = [];
    const msg = utf8('{"cmd":"deploy"}');
    const s = new FileSession("record", new FileCassette(tmpDir()), countingScrub(log));

    // Identity point: the adapter derives msg_hash over the scrubbed bytes.
    const scrubbed = s.scrubStreamFrame("send", { adapterID: "grpc", type: "server" }, msg);
    expect(log).toHaveLength(1);

    const rec = await s.openStreamRecord({
      adapterID: "grpc",
      type: "server",
      identity: { service: "ops.Deploy", method: "Run", msg_hash: msgHash(scrubbed) },
      payload: { service: "ops.Deploy", method: "Run" },
    });
    // Persist point: the adapter passes the message RAW. The core scrubs.
    rec.recordSend(msg);
    rec.recordHalfClose();
    rec.recordRecv(utf8("deployed"));
    await rec.finish({ status_code: 0 });

    expect(log).toEqual([
      ["send", '{"cmd":"deploy"}'], // identity derivation
      ["send", '{"cmd":"deploy"}'], // persist — one call, not two
      ["recv", "deployed"],
    ]);
  });

  // M7: clause 6 permits a length change; neither the record nor the
  // replay path may assume byte-count preservation.
  it("length-changing hook round-trips", async () => {
    const grow: StreamScrubFn = (_d, _i, data) => utf8(td.decode(data) + "-PADDED-LONGER");
    const dir = tmpDir();

    const recS = new FileSession("record", new FileCassette(dir), grow);
    const rec = await recS.openStreamRecord(fixedOpen("bidi"));
    rec.recordSend(utf8("alpha"));
    rec.recordHalfClose();
    rec.recordRecv(utf8("one"));
    const fp = rec.fingerprint;
    await rec.finish({ status_code: 0 });

    const pair = await new FileCassette(dir).loadStreamed("grpc", fp);
    expect(pair.req.stream!.frames[0].message).toEqual(utf8("alpha-PADDED-LONGER"));

    const repS = new FileSession("replay", new FileCassette(dir), grow);
    const rep = await repS.openStreamReplay(fixedOpen("bidi"));
    rep.send(utf8("alpha")); // green despite the length change
    rep.halfClose();
    expect(rep.recv()).toEqual(utf8("one-PADDED-LONGER"));
  });
});
