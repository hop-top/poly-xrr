import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import yaml from "js-yaml";
import { describe, expect, test } from "vitest";
import { FileCassette } from "../src/cassette.js";
import { FileSession } from "../src/session.js";
import { StreamFormatError, type StreamedInteraction } from "../src/stream.js";
import { counterStreamFingerprint, serverStreamFingerprint } from "../src/streamfp.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const fixturesRoot = path.resolve(__dirname, "../../spec/fixtures");

interface Manifest {
  interactions: Array<{ adapter: string; fingerprint: string; streamed?: boolean }>;
}

interface GrpcStreamPayload {
  service: string;
  method: string;
  n?: number;
}

function tmpDir(): string {
  return fs.mkdtempSync(fs.realpathSync("/tmp") + "/xrr-conformance-");
}

/**
 * Round-trip a streamed pair: re-emit into a fresh dir, load back, compare
 * field-for-field. Frame messages are held as decoded bytes in the model, so
 * deep equality compares decoded bytes; YAML formatting and encoding choice
 * are free per spec.
 */
async function roundTrip(pair: StreamedInteraction): Promise<void> {
  const out = new FileCassette(tmpDir());
  await out.saveStreamed(pair);
  const reloaded = await out.loadStreamed(pair.req.adapter, pair.req.fingerprint);
  expect(reloaded).toEqual(pair);
}

/** Recompute a grpc streaming fingerprint from the loaded pair per spec. */
function recomputeGrpcFingerprint(pair: StreamedInteraction): string {
  const payload = pair.req.payload as GrpcStreamPayload;
  if (pair.req.stream.type === "server") {
    return serverStreamFingerprint(payload.service, payload.method, pair.req.stream.frames[0].message);
  }
  // client/bidi: the informational payload `n` documents the occurrence.
  return counterStreamFingerprint(pair.req.stream.type, payload.service, payload.method, payload.n ?? 0);
}

describe("conformance fixtures", () => {
  const entries = fs.readdirSync(fixturesRoot, { withFileTypes: true });
  const dirs = entries.filter((e) => e.isDirectory());

  expect(dirs.length).toBeGreaterThan(0);

  for (const entry of dirs) {
    const fixtureDir = path.join(fixturesRoot, entry.name);
    const manifestPath = path.join(fixtureDir, "manifest.yaml");

    test(entry.name, async () => {
      const raw = fs.readFileSync(manifestPath, "utf8");
      const manifest = yaml.load(raw) as Manifest;
      const cassette = new FileCassette(fixtureDir);

      for (const interaction of manifest.interactions) {
        if (interaction.streamed) {
          const pair = await cassette.loadStreamed(interaction.adapter, interaction.fingerprint);
          expect(pair.req.fingerprint).toBe(interaction.fingerprint);
          expect(pair.resp.fingerprint).toBe(interaction.fingerprint);
          await roundTrip(pair);
          if (interaction.adapter === "grpc") {
            expect(recomputeGrpcFingerprint(pair)).toBe(interaction.fingerprint);
          }
        } else {
          await expect(
            cassette.load(interaction.adapter, interaction.fingerprint)
          ).resolves.not.toThrow();
        }
      }
    });
  }
});

describe("streamed conformance details", () => {
  test("grpc-stream-malformed-b64 pair is rejected, not decoded leniently", async () => {
    // Deliberately absent from its manifest: harnesses target the pair by
    // path and assert strict loading fails (fixture README).
    const cassette = new FileCassette(path.join(fixturesRoot, "grpc-stream-malformed-b64"));
    await expect(cassette.loadStreamed("grpc", "8dbfb222")).rejects.toThrow(StreamFormatError);
    await expect(cassette.loadStreamed("grpc", "8dbfb222")).rejects.toThrow(/base64/);
  });

  test("sse-text-scalars hazard payloads decode to exact characters", async () => {
    const cassette = new FileCassette(path.join(fixturesRoot, "sse-text-scalars"));
    const pair = await cassette.loadStreamed("sse", "66ecc77a");
    const texts = pair.resp.stream.frames.map((f) => new TextDecoder().decode(f.message));
    expect(texts).toEqual(["on", "12:30", "null", " leading", "trailing ", "  padded  "]);
  });

  test("grpc-stream-error keeps the v1 error field and status payload", async () => {
    const cassette = new FileCassette(path.join(fixturesRoot, "grpc-stream-error"));
    const pair = await cassette.loadStreamed("grpc", "9e8c4d4c");
    expect(pair.resp.error).toBe("rpc error: code = Unavailable desc = connection reset");
    expect(pair.resp.payload).toEqual({ status_code: 14 });
    expect(pair.resp.stream.frames).toHaveLength(2);
    expect(pair.resp.stream.end.seq).toBe(4);
  });

  test("grpc-client-stream-repeat: scripted two-open session yields n=0 then n=1", async () => {
    // The spec's n=1 obligation: a second open of the same tuple within one
    // session. The session's occurrence counter drives the fingerprints; the
    // fixture dir supplies the static cassette material.
    const dir = path.join(fixturesRoot, "grpc-client-stream-repeat");
    const cassette = new FileCassette(dir);
    const session = new FileSession("replay", cassette);

    const open = async (service: string, method: string) => {
      const n = session.streamCounter.next(service, method, "client");
      const fp = counterStreamFingerprint("client", service, method, n);
      const pair = await cassette.loadStreamed("grpc", fp);
      return { n, fp, pair };
    };

    const first = await open("files.FileService", "Upload");
    expect(first.n).toBe(0);
    expect(first.fp).toBe("2bebfd6f");
    expect((first.pair.req.payload as GrpcStreamPayload).n).toBe(0);

    const second = await open("files.FileService", "Upload");
    expect(second.n).toBe(1);
    expect(second.fp).toBe("b27b5fe1");
    expect((second.pair.req.payload as GrpcStreamPayload).n).toBe(1);
    expect(second.pair.req.stream.frames).toHaveLength(2);
  });
});
