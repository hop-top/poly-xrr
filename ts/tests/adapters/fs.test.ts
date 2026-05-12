import { describe, expect, test } from "vitest";
import { FsAdapter, type FsRequest } from "../../src/adapters/fs.js";

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
});
