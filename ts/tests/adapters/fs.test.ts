import yaml from "js-yaml";
import { describe, expect, test } from "vitest";
import {
  FsAdapter,
  chainNormalizers,
  type FsRequest,
  type PathNormalizer,
} from "../../src/adapters/fs.js";

describe("FsAdapter", () => {
  test("adapter id is 'fs'", () => {
    const a = new FsAdapter();
    expect(a.id).toBe("fs");
  });

  test("fingerprint is deterministic", async () => {
    const a = new FsAdapter();
    const req: FsRequest = {
      op: "write",
      path: "/etc/hosts",
      data: "127.0.0.1 localhost\n",
    };
    const fp1 = await a.fingerprint(req);
    const fp2 = await a.fingerprint(req);
    expect(fp1).toHaveLength(8);
    expect(fp1).toBe(fp2);
  });

  test("fingerprint discriminates op", async () => {
    const a = new FsAdapter();
    const fpW = await a.fingerprint({ op: "write", path: "/x" });
    const fpR = await a.fingerprint({ op: "remove", path: "/x" });
    expect(fpW).not.toBe(fpR);
  });

  test("fingerprint discriminates path", async () => {
    const a = new FsAdapter();
    const fpA = await a.fingerprint({ op: "write", path: "/a", data: "x" });
    const fpB = await a.fingerprint({ op: "write", path: "/b", data: "x" });
    expect(fpA).not.toBe(fpB);
  });

  test("fingerprint discriminates data", async () => {
    const a = new FsAdapter();
    const fpA = await a.fingerprint({ op: "write", path: "/x", data: "foo" });
    const fpB = await a.fingerprint({ op: "write", path: "/x", data: "bar" });
    expect(fpA).not.toBe(fpB);
  });

  test("fingerprint discriminates mode", async () => {
    const a = new FsAdapter();
    const fpA = await a.fingerprint({
      op: "write",
      path: "/x",
      data: "y",
      mode: 0o644,
    });
    const fpB = await a.fingerprint({
      op: "write",
      path: "/x",
      data: "y",
      mode: 0o755,
    });
    expect(fpA).not.toBe(fpB);
  });

  test("fingerprint omits mode when undefined", async () => {
    const a = new FsAdapter();
    const bare = await a.fingerprint({ op: "write", path: "/x", data: "y" });
    const withUndef = await a.fingerprint({
      op: "write",
      path: "/x",
      data: "y",
      mode: undefined,
    });
    expect(bare).toBe(withUndef);
  });

  test("mode: 0 produces different fingerprint than mode undefined", async () => {
    // Matches Go's *uint32 semantics: pointer-to-zero vs nil.
    const a = new FsAdapter();
    const undef = await a.fingerprint({ op: "write", path: "/x", data: "y" });
    const zero = await a.fingerprint({
      op: "write",
      path: "/x",
      data: "y",
      mode: 0,
    });
    expect(undef).not.toBe(zero);
  });

  test("flags omitted when zero (matches Go's omitempty)", async () => {
    const a = new FsAdapter();
    const bare = await a.fingerprint({ op: "write", path: "/x", data: "y" });
    const withZeroFlags = await a.fingerprint({
      op: "write",
      path: "/x",
      data: "y",
      flags: 0,
    });
    expect(bare).toBe(withZeroFlags);
    const withFlags = await a.fingerprint({
      op: "write",
      path: "/x",
      data: "y",
      flags: 1,
    });
    expect(withFlags).not.toBe(bare);
  });

  test("recursive omitted when false", async () => {
    const a = new FsAdapter();
    const bare = await a.fingerprint({ op: "remove", path: "/x" });
    const withFalse = await a.fingerprint({
      op: "remove",
      path: "/x",
      recursive: false,
    });
    const withTrue = await a.fingerprint({
      op: "remove",
      path: "/x",
      recursive: true,
    });
    expect(bare).toBe(withFalse);
    expect(bare).not.toBe(withTrue);
  });

  test("dest omitted when empty", async () => {
    const a = new FsAdapter();
    const bare = await a.fingerprint({ op: "rename", path: "/a" });
    const withEmpty = await a.fingerprint({
      op: "rename",
      path: "/a",
      dest: "",
    });
    const withDest = await a.fingerprint({
      op: "rename",
      path: "/a",
      dest: "/b",
    });
    expect(bare).toBe(withEmpty);
    expect(bare).not.toBe(withDest);
  });

  test("data omitted when empty string", async () => {
    const a = new FsAdapter();
    const bare = await a.fingerprint({ op: "write", path: "/x" });
    const empty = await a.fingerprint({ op: "write", path: "/x", data: "" });
    expect(bare).toBe(empty);
  });

  // CRITICAL: conformance fixture — locks the cross-runtime contract.
  test("conformance: fs-write fixture fingerprint equals 667a7680", async () => {
    const a = new FsAdapter();
    const fp = await a.fingerprint({
      op: "write",
      path: "$TMP/greeting.txt",
      data: "hello, world\n",
      mode: 420,
    });
    expect(fp).toBe("667a7680");
  });

  test("serialize/deserialize round-trip", () => {
    const a = new FsAdapter();
    const req: FsRequest = {
      op: "write",
      path: "/etc/hosts",
      data: "127.0.0.1 localhost\n",
      mode: 0o644,
    };
    const ser = a.serializeReq(req);
    const got = a.deserializeReq(ser);
    expect(got).toEqual(req);
  });

  test("response serialize/deserialize round-trip", () => {
    const a = new FsAdapter();
    const resp = { duration_ms: 42, bytes_written: 1024 };
    const ser = a.serializeResp(resp);
    const got = a.deserializeResp(ser);
    expect(got).toEqual(resp);
  });

  test("base64 payload round-trips byte-exact through YAML", async () => {
    // Spec "Data Field Encoding": binary callers base64-encode before the
    // adapter sees `data`; the adapter and the YAML layer treat the string
    // as opaque; the caller decodes on the way back.
    const a = new FsAdapter();
    const raw = Buffer.from([0x00, 0xff, 0xc3, 0x28, 0x80, 0x01, 0x02, 0x03]);
    const encoded = raw.toString("base64");
    const req: FsRequest = { op: "write", path: "/bin/x", data: encoded };

    const text = yaml.dump(a.serializeReq(req));
    expect(text).toContain(encoded);
    expect(text).not.toContain("!!binary");

    const got = a.deserializeReq(yaml.load(text));
    expect(got.data).toBe(encoded);
    expect(Buffer.from(got.data ?? "", "base64")).toEqual(raw);
    // Opaque to the fingerprint too: text or base64, only the bytes matter.
    expect(await a.fingerprint(got)).toBe(await a.fingerprint(req));
  });
});

describe("FsAdapter path normalizer", () => {
  const tmpRoot = "/var/folders/abc/T/run-123";
  const stripTmp: PathNormalizer = (p) => p.replace(tmpRoot, "$TMP");

  test("default normalizer is identity", () => {
    const a = new FsAdapter();
    expect(a.normalize("/var/folders/abc/T/run-123/x")).toBe(
      "/var/folders/abc/T/run-123/x"
    );
  });

  test("normalize() rewrites paths and is idempotent for prefix rules", () => {
    const a = new FsAdapter().withNormalizer(stripTmp);
    expect(a.normalize(`${tmpRoot}/x`)).toBe("$TMP/x");
    expect(a.normalize("$TMP/x")).toBe("$TMP/x");
  });

  test("normalizer is applied to path before fingerprinting", async () => {
    const plain = new FsAdapter();
    const norm = new FsAdapter().withNormalizer(stripTmp);
    const raw: FsRequest = {
      op: "write",
      path: `${tmpRoot}/config.yaml`,
      data: "k: v",
    };
    const pre: FsRequest = { op: "write", path: "$TMP/config.yaml", data: "k: v" };

    const fpRawNorm = await norm.fingerprint(raw);
    const fpPreNorm = await norm.fingerprint(pre);
    expect(fpRawNorm).toBe(fpPreNorm);

    const fpRawPlain = await plain.fingerprint(raw);
    expect(fpRawPlain).not.toBe(fpRawNorm);
  });

  test("two tmp roots collapse to one fingerprint when the root is stripped", async () => {
    const rootA = "/var/folders/aa/T/run-1";
    const rootB = "/private/var/folders/bb/T/run-2";
    const a = new FsAdapter().withNormalizer((p) => p.replace(rootA, "$TMP"));
    const b = new FsAdapter().withNormalizer((p) => p.replace(rootB, "$TMP"));
    const fpA = await a.fingerprint({ op: "write", path: `${rootA}/out.txt`, data: "x" });
    const fpB = await b.fingerprint({ op: "write", path: `${rootB}/out.txt`, data: "x" });
    expect(fpA).toBe(fpB);
  });

  test.each<FsRequest["op"]>(["rename", "symlink", "hardlink"])(
    "normalizer is applied to dest for %s",
    async (op) => {
      const plain = new FsAdapter();
      const norm = new FsAdapter().withNormalizer(stripTmp);
      const raw: FsRequest = { op, path: `${tmpRoot}/a`, dest: `${tmpRoot}/b` };
      const pre: FsRequest = { op, path: "$TMP/a", dest: "$TMP/b" };
      expect(await norm.fingerprint(raw)).toBe(await norm.fingerprint(pre));
      expect(await plain.fingerprint(raw)).not.toBe(await norm.fingerprint(raw));
    }
  );

  test("empty path and dest short-circuit without invoking the normalizer", async () => {
    let calls = 0;
    const a = new FsAdapter().withNormalizer(() => {
      calls++;
      return "NEVER";
    });
    expect(a.normalize("")).toBe("");
    await a.fingerprint({ op: "chmod", path: "", mode: 0o644 });
    const ser = a.serializeReq({ op: "rename", path: "", dest: "" }) as FsRequest;
    expect(ser.path).toBe("");
    expect(ser.dest).toBe("");
    expect(calls).toBe(0);
  });

  test("dest is gated on the normalized value, not the raw one", async () => {
    // spec: `dest` participates only when non-empty AFTER normalization.
    const drop: PathNormalizer = (p) => (p === "/x/drop" ? "" : p);
    const a = new FsAdapter().withNormalizer(drop);
    const noDest = await a.fingerprint({ op: "rename", path: "/a" });
    const dropped = await a.fingerprint({ op: "rename", path: "/a", dest: "/x/drop" });
    const kept = await a.fingerprint({ op: "rename", path: "/a", dest: "/x/keep" });
    expect(dropped).toBe(noDest);
    expect(kept).not.toBe(noDest);
  });

  test("empty dest stays omitted regardless of the normalizer", async () => {
    const a = new FsAdapter().withNormalizer((p) => (p === "" ? "/ghost" : p));
    const noDest = await a.fingerprint({ op: "rename", path: "/a" });
    const empty = await a.fingerprint({ op: "rename", path: "/a", dest: "" });
    expect(empty).toBe(noDest);
  });

  test("chainNormalizers composes left to right", () => {
    const tmpToPlaceholder: PathNormalizer = (p) => p.replace("/tmp", "$TMP");
    const placeholderToShort: PathNormalizer = (p) => p.replace("$TMP", "$T");
    expect(chainNormalizers(tmpToPlaceholder, placeholderToShort)("/tmp/x")).toBe("$T/x");
    expect(chainNormalizers(placeholderToShort, tmpToPlaceholder)("/tmp/x")).toBe("$TMP/x");
    expect(chainNormalizers()("/tmp/x")).toBe("/tmp/x");
  });

  test("chained normalizer drives the fingerprint", async () => {
    const tmpNorm: PathNormalizer = (p) => p.replace("/tmp", "$TMP");
    const homeNorm: PathNormalizer = (p) => p.replace("/home/u", "$HOME");
    const a = new FsAdapter().withNormalizer(chainNormalizers(tmpNorm, homeNorm));
    const fp1 = await a.fingerprint({ op: "rename", path: "/tmp/foo", dest: "/home/u/bar" });
    const fp2 = await a.fingerprint({ op: "rename", path: "$TMP/foo", dest: "$HOME/bar" });
    expect(fp1).toBe(fp2);
  });

  test("serializeReq stores post-normalizer path and dest", () => {
    const a = new FsAdapter().withNormalizer(stripTmp);
    const req: FsRequest = {
      op: "rename",
      path: `${tmpRoot}/old`,
      dest: `${tmpRoot}/new`,
      mode: 0o644,
    };
    const out = a.serializeReq(req) as FsRequest;
    expect(out).toEqual({
      op: "rename",
      path: "$TMP/old",
      dest: "$TMP/new",
      mode: 0o644,
    });
    expect(JSON.stringify(out)).not.toContain(tmpRoot);
    // Caller's request object is left untouched.
    expect(req.path).toBe(`${tmpRoot}/old`);
    expect(req.dest).toBe(`${tmpRoot}/new`);
  });

  test("serializeReq does not introduce a dest key when absent", () => {
    const a = new FsAdapter().withNormalizer(stripTmp);
    const out = a.serializeReq({ op: "write", path: `${tmpRoot}/f`, data: "x" });
    expect(Object.keys(out as object)).toEqual(["op", "path", "data"]);
  });

  test("replay-side plain adapter re-derives the recorded fingerprint from the stored request", async () => {
    // Cross-runtime contract: the cassette carries the post-normalizer
    // path, so a replay side with NO normalizer (any other port) must
    // hash the stored request to the same fingerprint the recorder used.
    const recorder = new FsAdapter().withNormalizer(stripTmp);
    const replayer = new FsAdapter();
    const raw: FsRequest = { op: "rename", path: `${tmpRoot}/a`, dest: `${tmpRoot}/b` };
    const stored = replayer.deserializeReq(recorder.serializeReq(raw));
    expect(await replayer.fingerprint(stored)).toBe(await recorder.fingerprint(raw));
  });

  test("withNormalizer returns a new adapter and leaves the original untouched", async () => {
    const base = new FsAdapter();
    const norm = base.withNormalizer(stripTmp);
    expect(norm).not.toBe(base);
    expect(norm.id).toBe("fs");
    const raw: FsRequest = { op: "write", path: `${tmpRoot}/f`, data: "x" };
    expect(await base.fingerprint(raw)).not.toBe(await norm.fingerprint(raw));
    expect(base.normalize(`${tmpRoot}/f`)).toBe(`${tmpRoot}/f`);
  });

  test("constructor option is equivalent to withNormalizer", async () => {
    const viaCtor = new FsAdapter({ normalizer: stripTmp });
    const viaWith = new FsAdapter().withNormalizer(stripTmp);
    const raw: FsRequest = { op: "write", path: `${tmpRoot}/f`, data: "x" };
    expect(await viaCtor.fingerprint(raw)).toBe(await viaWith.fingerprint(raw));
  });

  test("conformance: normalized tmp path reproduces the fs-write pin 667a7680", async () => {
    const a = new FsAdapter().withNormalizer(stripTmp);
    const fp = await a.fingerprint({
      op: "write",
      path: `${tmpRoot}/greeting.txt`,
      data: "hello, world\n",
      mode: 420,
    });
    expect(fp).toBe("667a7680");
  });
});
