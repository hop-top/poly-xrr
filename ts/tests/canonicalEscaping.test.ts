/**
 * Cross-port canonical-JSON escaping vectors (spec: Fingerprint Algorithm).
 *
 * The hazard input covers every string-escaping class that has forked
 * fingerprints across ports: HTML-sensitive & < >, a slash, non-ASCII,
 * U+2028/U+2029, the backspace and form-feed short forms, a control byte
 * (U+001F) and DEL. JSON.stringify is RFC 8785 §3.2.2.2 string
 * serialization by definition, so TS is the reference the other ports pin
 * against.
 */
import { describe, expect, it } from "vitest";
import { ExecAdapter } from "../src/adapters/exec.js";
import { FsAdapter } from "../src/adapters/fs.js";
import { streamCanonical, streamFingerprint } from "../src/streamfp.js";

const HAZARD =
  "a&b<c>/é" + String.fromCharCode(0x2028) + String.fromCharCode(0x2029) + "\b\f\x1f\x7f";

// {"k":"a&b<c>/é<U+2028><U+2029>\b\f<U+001F escaped><DEL>","stream":"server"}
const STREAM_CANONICAL_HEX =
  "7b226b223a226126623c633e2fc3a9e280a8e280a95c625c665c75303031667f22" +
  "2c2273747265616d223a22736572766572227d";

describe("canonical JSON escaping (RFC 8785)", () => {
  it("stream canonical bytes match the spec hazard vector", () => {
    const open = { adapterID: "x", type: "server" as const, identity: { k: HAZARD } };
    expect(Buffer.from(streamCanonical(open, -1), "utf8").toString("hex")).toBe(
      STREAM_CANONICAL_HEX
    );
    expect(streamFingerprint(open, -1)).toBe("bcc2c6c3");
  });

  it("fs fingerprint matches the spec hazard vector", async () => {
    expect(await new FsAdapter().fingerprint({ op: "write", path: HAZARD })).toBe("6f2fb087");
  });

  it("exec fingerprint matches the spec hazard vector", async () => {
    expect(await new ExecAdapter().fingerprint({ argv: ["echo", HAZARD] })).toBe("97618387");
  });
});
