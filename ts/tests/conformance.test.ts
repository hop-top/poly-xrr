import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import yaml from "js-yaml";
import { describe, expect, test } from "vitest";
import { ExecAdapter, type ExecRequest } from "../src/adapters/exec.js";
import { FsAdapter, type FsRequest } from "../src/adapters/fs.js";
import { HttpAdapter, type HttpRequest } from "../src/adapters/http.js";
import { RedisAdapter, type RedisRequest } from "../src/adapters/redis.js";
import { SqlAdapter, type SqlRequest } from "../src/adapters/sql.js";
import { FileCassette } from "../src/cassette.js";
import { FileSession } from "../src/session.js";
import { StreamFormatError, type StreamFrame, type StreamedInteraction } from "../src/stream.js";
import { counterStreamFingerprint, serverStreamFingerprint } from "../src/streamfp.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const fixturesRoot = path.resolve(__dirname, "../../spec/fixtures");
// Each port's own re-emission of every streamed golden pair
// (spec/emitted/<port>/<fixture>/); see spec/emitted/README.md.
const emittedRoot = path.resolve(__dirname, "../../spec/emitted");
const thisPort = "ts";

interface ManifestEntry {
  adapter: string;
  fingerprint: string;
  streamed?: boolean;
  /**
   * Marks a unary entry whose fingerprint is a computed value: the walker
   * rebuilds the adapter's request from the req payload and recomputes it
   * with the adapter's algorithm.
   */
  verify_fingerprint?: boolean;
}

interface Manifest {
  interactions: ManifestEntry[];
}

/** One spec/fixtures dir with its streamed manifest entries. */
interface FixtureDir {
  name: string;
  entries: ManifestEntry[];
}

function readManifest(dir: string): Manifest {
  return yaml.load(fs.readFileSync(path.join(dir, "manifest.yaml"), "utf8")) as Manifest;
}

/** Fixture dirs with at least one streamed entry, sorted by name. */
function streamedFixtureDirs(): FixtureDir[] {
  return fs
    .readdirSync(fixturesRoot, { withFileTypes: true })
    .filter((e) => e.isDirectory())
    .map((e) => ({
      name: e.name,
      entries: readManifest(path.join(fixturesRoot, e.name)).interactions.filter((i) => i.streamed),
    }))
    .filter((d) => d.entries.length > 0)
    .sort((a, b) => a.name.localeCompare(b.name));
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

/**
 * Recompute a grpc streaming fingerprint from the loaded pair per spec.
 *
 * `n` comes from each pair's own payload rather than from a counter shared
 * across the dir, so the manifest loop below is order-independent: the spec's
 * ordering rule (cassette-format-streaming.md, Manifest Extension) holds
 * vacuously here, with no shared counter for a wrong order to desynchronise.
 * The "manifest order is irrelevant" test pins that so the construction cannot
 * regress into a shared-counter loop unnoticed.
 */
function recomputeGrpcFingerprint(pair: StreamedInteraction): string {
  const payload = pair.req.payload as GrpcStreamPayload;
  if (pair.req.stream.type === "server") {
    return serverStreamFingerprint(payload.service, payload.method, pair.req.stream.frames[0].message);
  }
  // client/bidi: the informational payload `n` documents the occurrence.
  return counterStreamFingerprint(pair.req.stream.type, payload.service, payload.method, payload.n ?? 0);
}

/**
 * Recompute a unary fingerprint from the loaded req payload with the
 * adapter's own algorithm. Loading alone cannot expose a canonical-JSON
 * escaping fork — the files load in every port; the derived key is what
 * differs.
 */
async function recomputeUnaryFingerprint(adapter: string, payload: unknown): Promise<string> {
  switch (adapter) {
    case "exec":
      return new ExecAdapter().fingerprint(payload as ExecRequest);
    case "http":
      return new HttpAdapter().fingerprint(payload as HttpRequest);
    case "sql":
      return new SqlAdapter().fingerprint(payload as SqlRequest);
    case "fs":
      return new FsAdapter().fingerprint(payload as FsRequest);
    case "redis":
      return new RedisAdapter().fingerprint(payload as RedisRequest);
    default:
      throw new Error(`verify_fingerprint: no unary fingerprint model for adapter ${adapter}`);
  }
}

/** Loads one unary pair (a miss throws) and verifies a pinned fingerprint. */
async function conformUnaryPair(
  cassette: FileCassette,
  interaction: ManifestEntry
): Promise<void> {
  const { req } = await cassette.load(interaction.adapter, interaction.fingerprint);
  if (interaction.verify_fingerprint) {
    expect(await recomputeUnaryFingerprint(interaction.adapter, req)).toBe(interaction.fingerprint);
  }
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
          await conformUnaryPair(cassette, interaction);
        }
      }
    });
  }
});

describe("manifest order is irrelevant", () => {
  // `interactions` is an unordered set (Manifest Extension), so reversing a
  // manifest's entries is a legal edit and must not change any result. Only
  // grpc-client-stream-repeat has entries sharing a counter domain, so it is
  // the dir a shared-counter regression would break first.
  const dirs = fs
    .readdirSync(fixturesRoot, { withFileTypes: true })
    .filter((e) => e.isDirectory());

  for (const entry of dirs) {
    test(entry.name, async () => {
      const fixtureDir = path.join(fixturesRoot, entry.name);
      const manifest = yaml.load(
        fs.readFileSync(path.join(fixtureDir, "manifest.yaml"), "utf8")
      ) as Manifest;
      const cassette = new FileCassette(fixtureDir);

      for (const interaction of [...manifest.interactions].reverse()) {
        if (interaction.streamed) {
          const pair = await cassette.loadStreamed(interaction.adapter, interaction.fingerprint);
          expect(pair.req.fingerprint).toBe(interaction.fingerprint);
          if (interaction.adapter === "grpc") {
            expect(recomputeGrpcFingerprint(pair)).toBe(interaction.fingerprint);
          }
        } else {
          await conformUnaryPair(cassette, interaction);
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

/**
 * Runs saveStreamed over every streamed golden pair; returns the emitted
 * files keyed by path relative to a port tree (<fixture>/<adapter>-<fp>.<kind>.yaml).
 */
async function reemitStreamedFixtures(): Promise<Map<string, string>> {
  const files = new Map<string, string>();
  for (const d of streamedFixtureDirs()) {
    const golden = new FileCassette(path.join(fixturesRoot, d.name));
    const out = tmpDir();
    const cassette = new FileCassette(out);
    for (const e of d.entries) {
      await cassette.saveStreamed(await golden.loadStreamed(e.adapter, e.fingerprint));
      for (const kind of ["req", "resp"]) {
        const name = `${e.adapter}-${e.fingerprint}.${kind}.yaml`;
        files.set(path.join(d.name, name), fs.readFileSync(path.join(out, name), "utf8"));
      }
    }
  }
  return files;
}

/** Every regular file under root keyed by relative path. */
function readTree(root: string): Map<string, string> {
  const files = new Map<string, string>();
  for (const rel of fs.readdirSync(root, { recursive: true }) as string[]) {
    const full = path.join(root, rel);
    if (fs.statSync(full).isFile()) files.set(rel, fs.readFileSync(full, "utf8"));
  }
  return files;
}

/**
 * Model view for cross-port equality: the frame encoding flag is dropped
 * because the message-encoding choice is free on re-emit; messages compare
 * as decoded bytes. Absent at_ms stays absent (toEqual ignores undefined).
 */
function comparable(pair: StreamedInteraction): unknown {
  const frames = (list: StreamFrame[]) =>
    list.map((f) => ({ seq: f.seq, message: f.message, at_ms: f.at_ms }));
  return {
    req: { ...pair.req, stream: { ...pair.req.stream, frames: frames(pair.req.stream.frames) } },
    resp: { ...pair.resp, stream: { ...pair.resp.stream, frames: frames(pair.resp.stream.frames) } },
  };
}

describe("re-emission pinned", () => {
  // spec/emitted/ts must hold exactly what saveStreamed emits today for every
  // streamed golden pair, file set and bytes alike. Every port's suite loads
  // that tree, so a stale tree would hide a TS emit change from them.
  // XRR_UPDATE_EMITTED=1 regenerates instead of asserting (`make emit-ts`).
  test("spec/emitted/ts matches a fresh saveStreamed of every streamed pair", async () => {
    const want = await reemitStreamedFixtures();
    const tree = path.join(emittedRoot, thisPort);

    if (process.env.XRR_UPDATE_EMITTED) {
      fs.rmSync(tree, { recursive: true, force: true });
      for (const [rel, text] of want) {
        fs.mkdirSync(path.dirname(path.join(tree, rel)), { recursive: true });
        fs.writeFileSync(path.join(tree, rel), text, "utf8");
      }
      return;
    }

    expect(fs.existsSync(tree), `missing ${tree}: regenerate with make emit-ts`).toBe(true);
    const got = readTree(tree);
    expect([...got.keys()].sort(), "file set drifted: regenerate with make emit-ts").toEqual(
      [...want.keys()].sort()
    );
    for (const [rel, text] of want) {
      expect(got.get(rel), `${rel} drifted: regenerate with make emit-ts`).toBe(text);
    }
  });
});

describe("cross-port re-emissions", () => {
  // Every port's checked-in re-emission of every streamed golden pair must
  // load through the TS strict reader to the same model as the golden pair.
  // Self-load round-trips cannot see an emit slip the emitting port's own
  // reader tolerates; another port's reader can.
  const ports = fs.existsSync(emittedRoot)
    ? fs
        .readdirSync(emittedRoot, { withFileTypes: true })
        .filter((e) => e.isDirectory())
        .map((e) => e.name)
        .sort()
    : [];

  test("at least one port tree is checked in", () => {
    expect(ports, `no port trees under ${emittedRoot}: regenerate with make emit-all`).not.toEqual([]);
  });

  for (const port of ports) {
    test(`${port} re-emission loads to the golden model`, async () => {
      for (const d of streamedFixtureDirs()) {
        const golden = new FileCassette(path.join(fixturesRoot, d.name));
        const emitted = new FileCassette(path.join(emittedRoot, port, d.name));
        for (const e of d.entries) {
          const ctx = `${port} re-emission of ${d.name}/${e.adapter}-${e.fingerprint}`;
          const want = await golden.loadStreamed(e.adapter, e.fingerprint);
          const got = await emitted.loadStreamed(e.adapter, e.fingerprint).catch((err: unknown) => {
            throw new Error(`${ctx}: ${String(err)} (regenerate with make emit-${port})`);
          });
          expect(comparable(got), ctx).toEqual(comparable(want));
        }
      }
    });
  }
});
