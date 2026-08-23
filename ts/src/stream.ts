/**
 * Streamed-interaction format layer — spec/cassette-format-streaming.md.
 *
 * Parses, validates, and emits the optional v1 `stream` envelope field
 * (frames with seq/at_ms/message_b64|message_text, half_close, end). This
 * is format-layer only: the TS port ships no gRPC adapter, but MUST still
 * round-trip streamed cassettes per the spec's conformance obligations.
 *
 * Resolution-blind reading: the `stream` subtree is re-parsed with the
 * FAILSAFE schema (every scalar a string), so `message_text` payloads like
 * `on`, `12:30`, or `null` always yield exactly those characters no matter
 * how a YAML reader would resolve them. Integers (`seq`, `at_ms`) are then
 * parsed strictly from the scalar text.
 */
import yaml from "js-yaml";

export type StreamType = "server" | "client" | "bidi";
export type FrameEncoding = "b64" | "text";

export interface StreamFrame {
  seq: number;
  /** Decoded message bytes — the comparison and hashing basis. */
  message: Uint8Array;
  /** Wire encoding this frame was read with / prefers on emit. */
  encoding: FrameEncoding;
  at_ms?: number;
}

export interface StreamEventPos {
  seq: number;
  at_ms?: number;
}

/** Client→server half (`.req.yaml`). */
export interface ReqStream {
  type: StreamType;
  frames: StreamFrame[];
  half_close?: StreamEventPos;
}

/** Server→client half (`.resp.yaml`). */
export interface RespStream {
  frames: StreamFrame[];
  end: StreamEventPos;
}

/** Full envelope of one side of a streamed pair. */
export interface StreamedEnvelope<S> {
  xrr: string;
  adapter: string;
  fingerprint: string;
  recorded_at: string;
  payload: unknown;
  /** resp only; non-empty ⇔ the stream terminated with an error. */
  error?: string;
  stream: S;
}

export interface StreamedInteraction {
  req: StreamedEnvelope<ReqStream>;
  resp: StreamedEnvelope<RespStream>;
}

/** A streamed pair violating the spec's validation rules. */
export class StreamFormatError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "StreamFormatError";
  }
}

/** Streamed cassette on the unary path, or unary cassette on the streaming path. */
export class ShapeMismatchError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "ShapeMismatchError";
  }
}

const encoder = new TextEncoder();
const decoder = new TextDecoder();

// ── strict base64 ──────────────────────────────────────────────────────────

// RFC 4648 standard alphabet with padding; no whitespace, no line breaks.
const STRICT_B64 = /^(?:[A-Za-z0-9+/]{4})*(?:[A-Za-z0-9+/]{2}==|[A-Za-z0-9+/]{3}=)?$/;

/**
 * Decodes standard base64, rejecting anything Buffer.from would silently
 * discard (whitespace, out-of-alphabet chars, bad padding).
 */
export function strictBase64Decode(s: string): Uint8Array {
  if (!STRICT_B64.test(s)) {
    throw new StreamFormatError(`xrr: stream: invalid base64: ${JSON.stringify(s)}`);
  }
  return new Uint8Array(Buffer.from(s, "base64"));
}

// ── parsing ────────────────────────────────────────────────────────────────

/**
 * Extracts the raw `stream` subtree from a cassette file's YAML text using
 * the FAILSAFE schema, keeping every scalar a string (resolution-blind).
 */
export function extractStreamNode(text: string): unknown {
  const doc = yaml.load(text, { schema: yaml.FAILSAFE_SCHEMA });
  return isMap(doc) ? doc.stream : undefined;
}

function isMap(v: unknown): v is Record<string, unknown> {
  return typeof v === "object" && v !== null && !Array.isArray(v);
}

function parseNonNegInt(v: unknown, ctx: string): number {
  if (typeof v === "number" && Number.isInteger(v) && v >= 0) return v;
  if (typeof v === "string" && /^(0|[1-9][0-9]*)$/.test(v)) return Number(v);
  throw new StreamFormatError(`xrr: stream: ${ctx} must be a non-negative integer`);
}

function parseFrame(raw: unknown, ctx: string): StreamFrame {
  if (!isMap(raw)) throw new StreamFormatError(`xrr: stream: ${ctx} must be a mapping`);
  if (raw.seq == null) throw new StreamFormatError(`xrr: stream: ${ctx} lacks seq`);
  const seq = parseNonNegInt(raw.seq, `${ctx}.seq`);

  const hasB64 = raw.message_b64 != null;
  const hasText = raw.message_text != null;
  if (hasB64 === hasText) {
    throw new StreamFormatError(
      `xrr: stream: ${ctx} must carry exactly one of message_b64 / message_text`
    );
  }
  let message: Uint8Array;
  let encoding: FrameEncoding;
  if (hasB64) {
    if (typeof raw.message_b64 !== "string") {
      throw new StreamFormatError(`xrr: stream: ${ctx}.message_b64 must be a string`);
    }
    message = strictBase64Decode(raw.message_b64);
    encoding = "b64";
  } else {
    if (typeof raw.message_text !== "string") {
      throw new StreamFormatError(`xrr: stream: ${ctx}.message_text must be a string scalar`);
    }
    message = encoder.encode(raw.message_text);
    encoding = "text";
  }

  const frame: StreamFrame = { seq, message, encoding };
  if (raw.at_ms != null) frame.at_ms = parseNonNegInt(raw.at_ms, `${ctx}.at_ms`);
  return frame;
}

function parseFrames(raw: unknown, ctx: string): StreamFrame[] {
  if (raw == null) return []; // absent key ⇒ []
  if (!Array.isArray(raw)) throw new StreamFormatError(`xrr: stream: ${ctx} must be a list`);
  const frames = raw.map((f, i) => parseFrame(f, `${ctx}[${i}]`));
  for (let i = 1; i < frames.length; i++) {
    if (frames[i].seq <= frames[i - 1].seq) {
      throw new StreamFormatError(`xrr: stream: ${ctx} not strictly ascending in seq`);
    }
  }
  return frames;
}

function parseEventPos(raw: unknown, ctx: string): StreamEventPos {
  if (!isMap(raw)) throw new StreamFormatError(`xrr: stream: ${ctx} must be a mapping`);
  if (raw.seq == null) throw new StreamFormatError(`xrr: stream: ${ctx} lacks seq`);
  const ev: StreamEventPos = { seq: parseNonNegInt(raw.seq, `${ctx}.seq`) };
  if (raw.at_ms != null) ev.at_ms = parseNonNegInt(raw.at_ms, `${ctx}.at_ms`);
  return ev;
}

/** Parses the req-side `stream` object (type, frames, half_close). */
export function parseReqStream(raw: unknown): ReqStream {
  if (!isMap(raw)) throw new StreamFormatError("xrr: stream: req stream must be a mapping");
  const type = raw.type;
  if (type !== "server" && type !== "client" && type !== "bidi") {
    throw new StreamFormatError('xrr: stream: type must be one of "server" / "client" / "bidi"');
  }
  const s: ReqStream = { type, frames: parseFrames(raw.frames, "req.frames") };
  if (raw.half_close != null) s.half_close = parseEventPos(raw.half_close, "req.half_close");
  return s;
}

/** Parses the resp-side `stream` object (frames, end). */
export function parseRespStream(raw: unknown): RespStream {
  if (!isMap(raw)) throw new StreamFormatError("xrr: stream: resp stream must be a mapping");
  if (raw.end == null) throw new StreamFormatError("xrr: stream: resp lacks end");
  return {
    frames: parseFrames(raw.frames, "resp.frames"),
    end: parseEventPos(raw.end, "resp.end"),
  };
}

// ── pair validation ────────────────────────────────────────────────────────

/**
 * Cross-file validation rules: no duplicate seq across the pair (frames,
 * half_close, end) and end.seq is the pair maximum. Per-file rules
 * (ascending frames, frame shape, base64) are enforced at parse time.
 */
export function validateStreamPair(req: ReqStream, resp: RespStream): void {
  const seqs: number[] = [
    ...req.frames.map((f) => f.seq),
    ...(req.half_close ? [req.half_close.seq] : []),
    ...resp.frames.map((f) => f.seq),
    resp.end.seq,
  ];
  if (new Set(seqs).size !== seqs.length) {
    throw new StreamFormatError("xrr: stream: duplicate seq across the pair");
  }
  if (resp.end.seq !== Math.max(...seqs)) {
    throw new StreamFormatError("xrr: stream: end.seq is not the maximum seq of the pair");
  }
}

// ── emission ───────────────────────────────────────────────────────────────

function bytesEqual(a: Uint8Array, b: Uint8Array): boolean {
  if (a.length !== b.length) return false;
  for (let i = 0; i < a.length; i++) if (a[i] !== b[i]) return false;
  return true;
}

// JSON string literals are valid YAML 1.2 double-quoted scalars, so
// JSON.stringify gives guaranteed-quoted emission for the fields the spec
// requires quoted (fingerprint, message_text) regardless of content.
function emitFrame(f: StreamFrame, lines: string[]): void {
  lines.push(`    - seq: ${f.seq}`);
  let asText: string | undefined;
  if (f.encoding === "text") {
    const text = decoder.decode(f.message);
    // message_text only when the bytes round-trip losslessly through UTF-8.
    if (bytesEqual(encoder.encode(text), f.message)) asText = text;
  }
  if (asText !== undefined) {
    lines.push(`      message_text: ${JSON.stringify(asText)}`);
  } else {
    lines.push(`      message_b64: ${JSON.stringify(Buffer.from(f.message).toString("base64"))}`);
  }
  if (f.at_ms !== undefined) lines.push(`      at_ms: ${f.at_ms}`);
}

function emitFrames(frames: StreamFrame[], lines: string[]): void {
  if (frames.length === 0) {
    lines.push("  frames: []");
    return;
  }
  lines.push("  frames:");
  for (const f of frames) emitFrame(f, lines);
}

function emitEventPos(name: string, ev: StreamEventPos, lines: string[]): void {
  lines.push(`  ${name}:`);
  lines.push(`    seq: ${ev.seq}`);
  if (ev.at_ms !== undefined) lines.push(`    at_ms: ${ev.at_ms}`);
}

function emitPayload(payload: unknown, lines: string[]): void {
  const dumped = yaml.dump(payload ?? {}, { lineWidth: -1 });
  if (dumped === "{}\n") {
    lines.push("payload: {}");
    return;
  }
  lines.push("payload:");
  for (const line of dumped.replace(/\n$/, "").split("\n")) {
    lines.push(line === "" ? "" : `  ${line}`);
  }
}

/**
 * Serializes one side of a streamed pair to YAML text obeying the spec's
 * normative writer rules: quoted fingerprint, quoted message_text,
 * whitespace-free base64, frames listed in ascending seq, `frames: []`
 * explicit for empty streams.
 */
export function emitStreamedEnvelope(
  env: StreamedEnvelope<ReqStream | RespStream>,
  side: "req" | "resp"
): string {
  const lines: string[] = [];
  lines.push(`xrr: ${JSON.stringify(env.xrr)}`);
  lines.push(`adapter: ${env.adapter}`); // adapter ids ([a-z][a-z0-9-]*) are plain-safe
  lines.push(`fingerprint: ${JSON.stringify(env.fingerprint)}`);
  lines.push(`recorded_at: ${JSON.stringify(env.recorded_at)}`);
  if (side === "resp" && env.error != null && env.error !== "") {
    lines.push(`error: ${JSON.stringify(env.error)}`);
  }
  emitPayload(env.payload, lines);
  lines.push("stream:");
  if (side === "req") {
    const s = env.stream as ReqStream;
    lines.push(`  type: ${s.type}`);
    emitFrames(s.frames, lines);
    if (s.half_close) emitEventPos("half_close", s.half_close, lines);
  } else {
    const s = env.stream as RespStream;
    emitFrames(s.frames, lines);
    emitEventPos("end", s.end, lines);
  }
  return lines.join("\n") + "\n";
}
