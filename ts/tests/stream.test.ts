import fs from "node:fs";
import { describe, expect, test } from "vitest";
import { FileCassette } from "../src/cassette.js";
import { FileSession } from "../src/session.js";
import {
  ShapeMismatchError,
  StreamFormatError,
  type ReqStream,
  type RespStream,
  type StreamedEnvelope,
  emitStreamedEnvelope,
  extractStreamNode,
  parseReqStream,
  parseRespStream,
  strictBase64Decode,
  validateStreamPair,
} from "../src/stream.js";
import {
  OccurrenceCounter,
  counterStreamFingerprint,
  msgHash,
  serverStreamFingerprint,
} from "../src/streamfp.js";

function tmpDir(): string {
  return fs.mkdtempSync(fs.realpathSync("/tmp") + "/xrr-stream-");
}

function utf8(s: string): Uint8Array {
  return new TextEncoder().encode(s);
}

// ── fingerprint spec vectors (cassette-format-streaming.md) ────────────────

describe("stream fingerprints — spec vectors", () => {
  test("msg_hash of {\"path\":\"/etc/hosts\"} is f1e315a5", () => {
    expect(msgHash(utf8('{"path":"/etc/hosts"}'))).toBe("f1e315a5");
  });

  test("msg_hash of {\"path\":\"/var/log/big.log\"} is 164658bd", () => {
    expect(msgHash(utf8('{"path":"/var/log/big.log"}'))).toBe("164658bd");
  });

  test("server fingerprint for Download(/etc/hosts) is 58a4bf3f", () => {
    expect(
      serverStreamFingerprint("files.FileService", "Download", utf8('{"path":"/etc/hosts"}'))
    ).toBe("58a4bf3f");
  });

  test("server fingerprint for Download(/var/log/big.log) is 9e8c4d4c", () => {
    expect(
      serverStreamFingerprint("files.FileService", "Download", utf8('{"path":"/var/log/big.log"}'))
    ).toBe("9e8c4d4c");
  });

  test("client fingerprint for Upload n=0 is 2bebfd6f", () => {
    expect(counterStreamFingerprint("client", "files.FileService", "Upload", 0)).toBe("2bebfd6f");
  });

  test("bidi fingerprint for Converse n=0 is c6233d2e", () => {
    expect(counterStreamFingerprint("bidi", "chat.ChatService", "Converse", 0)).toBe("c6233d2e");
  });
});

// ── occurrence counter ─────────────────────────────────────────────────────

describe("OccurrenceCounter", () => {
  test("counts 0-based per key", () => {
    const c = new OccurrenceCounter();
    expect(c.next("files.FileService", "Upload", "client")).toBe(0);
    expect(c.next("files.FileService", "Upload", "client")).toBe(1);
    expect(c.next("files.FileService", "Upload", "client")).toBe(2);
  });

  test("keys are independent", () => {
    const c = new OccurrenceCounter();
    expect(c.next("a.Svc", "M", "client")).toBe(0);
    expect(c.next("a.Svc", "M", "bidi")).toBe(0);
    expect(c.next("b.Svc", "M", "client")).toBe(0);
    expect(c.next("a.Svc", "M", "client")).toBe(1);
  });

  test("one session object is one counter domain", () => {
    const a = new FileSession("replay", new FileCassette(tmpDir()));
    const b = new FileSession("replay", new FileCassette(tmpDir()));
    expect(a.streamCounter.next("a.Svc", "M", "client")).toBe(0);
    expect(a.streamCounter.next("a.Svc", "M", "client")).toBe(1);
    expect(b.streamCounter.next("a.Svc", "M", "client")).toBe(0);
  });
});

// ── strict base64 ──────────────────────────────────────────────────────────

describe("strictBase64Decode", () => {
  test("decodes standard padded base64", () => {
    expect(strictBase64Decode("cGluZy0x")).toEqual(utf8("ping-1"));
    expect(strictBase64Decode("Y2h1bmstb25lCg==")).toEqual(utf8("chunk-one\n"));
    expect(strictBase64Decode("YmV0YS0xCg==")).toEqual(utf8("beta-1\n"));
  });

  test("empty string is the empty message", () => {
    expect(strictBase64Decode("")).toEqual(new Uint8Array(0));
  });

  test("rejects embedded whitespace", () => {
    expect(() => strictBase64Decode("YmxvYi1jaHVu ayAy")).toThrow(StreamFormatError);
    expect(() => strictBase64Decode("YWJj\nZGVm")).toThrow(StreamFormatError);
  });

  test("rejects out-of-alphabet characters", () => {
    expect(() => strictBase64Decode("YmxvYi1jaHVuayEh!")).toThrow(StreamFormatError);
    expect(() => strictBase64Decode("YWJj%A==")).toThrow(StreamFormatError);
    expect(() => strictBase64Decode("YWJj-_==")).toThrow(StreamFormatError);
  });

  test("rejects non-multiple-of-4 and bad padding", () => {
    expect(() => strictBase64Decode("A")).toThrow(StreamFormatError);
    expect(() => strictBase64Decode("AB=C")).toThrow(StreamFormatError);
    expect(() => strictBase64Decode("====")).toThrow(StreamFormatError);
  });
});

// ── parsing ────────────────────────────────────────────────────────────────

function loadReq(text: string): ReqStream {
  return parseReqStream(extractStreamNode(text));
}

function loadResp(text: string): RespStream {
  return parseRespStream(extractStreamNode(text));
}

describe("parseReqStream", () => {
  test("parses type, frames, half_close", () => {
    const s = loadReq(
      [
        "stream:",
        "  type: bidi",
        "  frames:",
        "    - seq: 0",
        '      message_b64: "cGluZy0x"',
        "      at_ms: 0",
        "    - seq: 2",
        '      message_b64: "cGluZy0y"',
        "      at_ms: 40",
        "  half_close:",
        "    seq: 4",
        "    at_ms: 45",
      ].join("\n")
    );
    expect(s.type).toBe("bidi");
    expect(s.frames).toHaveLength(2);
    expect(s.frames[0].seq).toBe(0);
    expect(s.frames[0].message).toEqual(utf8("ping-1"));
    expect(s.frames[0].encoding).toBe("b64");
    expect(s.frames[1].seq).toBe(2);
    expect(s.frames[1].at_ms).toBe(40);
    expect(s.half_close).toEqual({ seq: 4, at_ms: 45 });
  });

  test("treats absent frames as []", () => {
    const s = loadReq("stream:\n  type: server");
    expect(s.frames).toEqual([]);
  });

  test("parses explicit empty frames", () => {
    const s = loadReq("stream:\n  type: client\n  frames: []\n  half_close:\n    seq: 0");
    expect(s.frames).toEqual([]);
    expect(s.half_close).toEqual({ seq: 0 });
  });

  test("tolerates absent at_ms", () => {
    const s = loadReq(
      'stream:\n  type: server\n  frames:\n    - seq: 0\n      message_b64: "cGluZy0x"'
    );
    expect(s.frames[0].at_ms).toBeUndefined();
  });

  test("rejects missing type", () => {
    expect(() => loadReq("stream:\n  frames: []")).toThrow(StreamFormatError);
  });

  test("rejects bad type", () => {
    expect(() => loadReq("stream:\n  type: duplex\n  frames: []")).toThrow(StreamFormatError);
  });

  test("rejects frame without seq", () => {
    expect(() =>
      loadReq('stream:\n  type: server\n  frames:\n    - message_b64: "cGluZy0x"')
    ).toThrow(StreamFormatError);
  });

  test("rejects frame with both encodings", () => {
    expect(() =>
      loadReq(
        'stream:\n  type: server\n  frames:\n    - seq: 0\n      message_b64: "cGluZy0x"\n      message_text: "ping-1"'
      )
    ).toThrow(StreamFormatError);
  });

  test("rejects frame with neither encoding", () => {
    expect(() => loadReq("stream:\n  type: server\n  frames:\n    - seq: 0")).toThrow(
      StreamFormatError
    );
  });

  test("rejects non-ascending frames", () => {
    expect(() =>
      loadReq(
        'stream:\n  type: bidi\n  frames:\n    - seq: 2\n      message_b64: "cGluZy0x"\n    - seq: 1\n      message_b64: "cGluZy0y"'
      )
    ).toThrow(StreamFormatError);
  });

  test("rejects invalid base64 in a frame", () => {
    expect(() =>
      loadReq('stream:\n  type: server\n  frames:\n    - seq: 0\n      message_b64: "not b64!"')
    ).toThrow(StreamFormatError);
  });

  test("rejects negative seq", () => {
    expect(() =>
      loadReq('stream:\n  type: server\n  frames:\n    - seq: -1\n      message_b64: "cGluZy0x"')
    ).toThrow(StreamFormatError);
  });

  test("ignores unknown fields (forward compat)", () => {
    const s = loadReq(
      'stream:\n  type: server\n  future: true\n  frames:\n    - seq: 0\n      message_b64: "cGluZy0x"\n      event: message'
    );
    expect(s.frames).toHaveLength(1);
  });
});

describe("parseRespStream", () => {
  test("parses frames and end", () => {
    const s = loadResp(
      'stream:\n  frames:\n    - seq: 1\n      message_b64: "cG9uZy0x"\n      at_ms: 3\n  end:\n    seq: 2\n    at_ms: 5'
    );
    expect(s.frames).toHaveLength(1);
    expect(s.end).toEqual({ seq: 2, at_ms: 5 });
  });

  test("rejects missing end", () => {
    expect(() => loadResp("stream:\n  frames: []")).toThrow(StreamFormatError);
  });
});

describe("message_text scalar hazards", () => {
  test("decodes quoted hazard scalars as exact strings", () => {
    const s = loadResp(
      [
        "stream:",
        "  frames:",
        '    - seq: 0',
        '      message_text: "on"',
        '    - seq: 1',
        '      message_text: "12:30"',
        '    - seq: 2',
        '      message_text: "null"',
        '    - seq: 3',
        '      message_text: " leading"',
        '    - seq: 4',
        '      message_text: "trailing "',
        "  end:",
        "    seq: 5",
      ].join("\n")
    );
    const texts = s.frames.map((f) => new TextDecoder().decode(f.message));
    expect(texts).toEqual(["on", "12:30", "null", " leading", "trailing "]);
    expect(s.frames.every((f) => f.encoding === "text")).toBe(true);
  });

  test("reads even unquoted hazard scalars as strings (resolution-blind)", () => {
    // A conformant writer always quotes; a resolution-blind reader must not
    // corrupt even when a non-conformant one did not.
    const s = loadResp(
      [
        "stream:",
        "  frames:",
        "    - seq: 0",
        "      message_text: on",
        "    - seq: 1",
        "      message_text: 12:30",
        "    - seq: 2",
        "      message_text: null",
        "  end:",
        "    seq: 3",
      ].join("\n")
    );
    const texts = s.frames.map((f) => new TextDecoder().decode(f.message));
    expect(texts).toEqual(["on", "12:30", "null"]);
  });
});

// ── pair validation ────────────────────────────────────────────────────────

function pair(reqYaml: string, respYaml: string): void {
  validateStreamPair(loadReq(reqYaml), loadResp(respYaml));
}

describe("validateStreamPair", () => {
  const reqOK = 'stream:\n  type: bidi\n  frames:\n    - seq: 0\n      message_b64: "cGluZy0x"\n  half_close:\n    seq: 2';

  test("accepts a valid pair", () => {
    expect(() =>
      pair(reqOK, 'stream:\n  frames:\n    - seq: 1\n      message_b64: "cG9uZy0x"\n  end:\n    seq: 3')
    ).not.toThrow();
  });

  test("accepts sparse seq numbering", () => {
    expect(() =>
      pair(
        'stream:\n  type: bidi\n  frames:\n    - seq: 0\n      message_b64: "cGluZy0x"',
        "stream:\n  frames: []\n  end:\n    seq: 9"
      )
    ).not.toThrow();
  });

  test("rejects duplicate seq across the pair", () => {
    expect(() =>
      pair(reqOK, 'stream:\n  frames:\n    - seq: 0\n      message_b64: "cG9uZy0x"\n  end:\n    seq: 3')
    ).toThrow(StreamFormatError);
  });

  test("rejects duplicate seq between half_close and end", () => {
    expect(() => pair(reqOK, "stream:\n  frames: []\n  end:\n    seq: 2")).toThrow(
      StreamFormatError
    );
  });

  test("rejects end.seq that is not the pair maximum", () => {
    expect(() =>
      pair(reqOK, 'stream:\n  frames:\n    - seq: 4\n      message_b64: "cG9uZy0x"\n  end:\n    seq: 3')
    ).toThrow(StreamFormatError);
  });
});

// ── emission ───────────────────────────────────────────────────────────────

function reqEnvelope(stream: ReqStream): StreamedEnvelope<ReqStream> {
  return {
    xrr: "1",
    adapter: "grpc",
    fingerprint: "c6233d2e",
    recorded_at: "2026-08-23T12:00:00Z",
    payload: { service: "chat.ChatService", method: "Converse", n: 0 },
    stream,
  };
}

describe("emitStreamedEnvelope", () => {
  test("round-trips a req envelope", () => {
    const stream: ReqStream = {
      type: "bidi",
      frames: [
        { seq: 0, message: utf8("ping-1"), encoding: "b64", at_ms: 0 },
        { seq: 2, message: utf8("ping-2"), encoding: "b64", at_ms: 40 },
      ],
      half_close: { seq: 4, at_ms: 45 },
    };
    const text = emitStreamedEnvelope(reqEnvelope(stream), "req");
    const back = loadReq(text);
    expect(back).toEqual(stream);
  });

  test("quotes fingerprint", () => {
    const text = emitStreamedEnvelope(reqEnvelope({ type: "bidi", frames: [] }), "req");
    expect(text).toContain('fingerprint: "c6233d2e"');
  });

  test("emits frames: [] for empty streams", () => {
    const text = emitStreamedEnvelope(reqEnvelope({ type: "bidi", frames: [] }), "req");
    expect(text).toContain("frames: []");
  });

  test("always quotes message_text, including hazard scalars", () => {
    const stream: RespStream = {
      frames: [
        { seq: 0, message: utf8("on"), encoding: "text", at_ms: 1 },
        { seq: 1, message: utf8("12:30"), encoding: "text", at_ms: 2 },
        { seq: 2, message: utf8("plain"), encoding: "text", at_ms: 3 },
      ],
      end: { seq: 3, at_ms: 4 },
    };
    const env: StreamedEnvelope<RespStream> = {
      xrr: "1",
      adapter: "sse",
      fingerprint: "66ecc77a",
      recorded_at: "2026-08-23T12:00:00Z",
      payload: {},
      stream,
    };
    const text = emitStreamedEnvelope(env, "resp");
    expect(text).toContain('message_text: "on"');
    expect(text).toContain('message_text: "12:30"');
    expect(text).toContain('message_text: "plain"');
    expect(text).not.toMatch(/message_text: [^"']/);
    const back = loadResp(text);
    expect(back).toEqual(stream);
  });

  test("emits base64 without whitespace", () => {
    const big = new Uint8Array(300).fill(7);
    const env = reqEnvelope({
      type: "server",
      frames: [{ seq: 0, message: big, encoding: "b64", at_ms: 0 }],
      half_close: { seq: 1, at_ms: 0 },
    });
    const text = emitStreamedEnvelope(env, "req");
    const line = text.split("\n").find((l) => l.includes("message_b64"));
    expect(line).toBeDefined();
    expect(strictBase64Decode(JSON.parse(line!.trim().slice("message_b64: ".length)))).toEqual(big);
  });

  test("falls back to b64 when text encoding cannot round-trip", () => {
    const invalid = new Uint8Array([0xff, 0xfe]);
    const env = reqEnvelope({
      type: "server",
      frames: [{ seq: 0, message: invalid, encoding: "text" }],
      half_close: { seq: 1 },
    });
    const text = emitStreamedEnvelope(env, "req");
    expect(text).toContain("message_b64:");
    const back = loadReq(text);
    expect(back.frames[0].message).toEqual(invalid);
  });

  test("emits resp error field when non-empty", () => {
    const env: StreamedEnvelope<RespStream> = {
      xrr: "1",
      adapter: "grpc",
      fingerprint: "9e8c4d4c",
      recorded_at: "2026-08-23T12:00:00Z",
      error: "rpc error: code = Unavailable desc = connection reset",
      payload: { status_code: 14 },
      stream: { frames: [], end: { seq: 0 } },
    };
    const text = emitStreamedEnvelope(env, "resp");
    expect(text).toContain('error: "rpc error: code = Unavailable desc = connection reset"');
  });
});

// ── cassette integration ───────────────────────────────────────────────────

describe("FileCassette streamed IO", () => {
  function interaction() {
    return {
      req: reqEnvelope({
        type: "bidi" as const,
        frames: [{ seq: 0, message: utf8("ping-1"), encoding: "b64" as const, at_ms: 0 }],
        half_close: { seq: 2, at_ms: 5 },
      }),
      resp: {
        xrr: "1",
        adapter: "grpc",
        fingerprint: "c6233d2e",
        recorded_at: "2026-08-23T12:00:00Z",
        payload: { status_code: 0 },
        stream: {
          frames: [{ seq: 1, message: utf8("pong-1"), encoding: "b64" as const, at_ms: 3 }],
          end: { seq: 3, at_ms: 6 },
        },
      },
    };
  }

  test("saveStreamed / loadStreamed round-trip", async () => {
    const c = new FileCassette(tmpDir());
    const want = interaction();
    await c.saveStreamed(want);
    const got = await c.loadStreamed("grpc", "c6233d2e");
    expect(got).toEqual(want);
  });

  test("loadStreamed on a unary pair is a shape mismatch", async () => {
    const c = new FileCassette(tmpDir());
    await c.save("exec", "a3f9c1b2", { argv: ["ls"] }, { stdout: "", exit_code: 0 });
    await expect(c.loadStreamed("exec", "a3f9c1b2")).rejects.toThrow(ShapeMismatchError);
  });

  test("unary load on a streamed pair is a shape mismatch", async () => {
    const c = new FileCassette(tmpDir());
    await c.saveStreamed(interaction());
    await expect(c.load("grpc", "c6233d2e")).rejects.toThrow(ShapeMismatchError);
  });

  test("one-sided stream is rejected on both paths", async () => {
    const dir = tmpDir();
    const c = new FileCassette(dir);
    await c.saveStreamed(interaction());
    // strip stream from the resp file only
    const respPath = `${dir}/grpc-c6233d2e.resp.yaml`;
    const stripped = fs
      .readFileSync(respPath, "utf8")
      .split("\nstream:")[0]
      .concat("\n");
    fs.writeFileSync(respPath, stripped);
    await expect(c.loadStreamed("grpc", "c6233d2e")).rejects.toThrow(StreamFormatError);
    await expect(c.load("grpc", "c6233d2e")).rejects.toThrow(StreamFormatError);
  });

  test("loadStreamed missing pair still misses like v1", async () => {
    const c = new FileCassette(tmpDir());
    await expect(c.loadStreamed("grpc", "deadbeef")).rejects.toThrow("xrr: cassette miss");
  });
});
