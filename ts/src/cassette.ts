/**
 * FileCassette — reads/writes YAML envelope files.
 *
 * Unary pairs go through save/load (payloads only, v1 behavior). Streamed
 * pairs go through saveStreamed/loadStreamed; routing a streamed cassette
 * down the unary path (or vice versa) is a ShapeMismatchError, distinct
 * from a cassette miss, per spec/cassette-format-streaming.md.
 */
import fs from "node:fs/promises";
import path from "node:path";
import yaml from "js-yaml";
import { Redactor, redactConfigFromEnv } from "./redact.js";
import { ErrCassetteMiss, type Cassette } from "./xrr.js";
import {
  ShapeMismatchError,
  StreamFormatError,
  type StreamedInteraction,
  emitStreamedEnvelope,
  extractStreamNode,
  parseReqStream,
  parseRespStream,
  validateStreamPair,
} from "./stream.js";

interface Envelope {
  xrr: string;
  adapter: string;
  fingerprint: string;
  recorded_at: string;
  payload: unknown;
  error?: string;
  stream?: unknown;
}

export class FileCassette implements Cassette {
  /**
   * Redaction is enabled by default, configured from the XRR_REDACT_*
   * environment variables. Pass an explicit Redactor to supply a policy
   * that bypasses the environment.
   */
  constructor(
    private readonly dir: string,
    private readonly redactor?: Redactor
  ) {}

  // Resolves the redactor to use for one write. When none was injected,
  // config is read from the environment on each write so a test that
  // flips XRR_REDACT_* mid-process sees the change.
  private activeRedactor(): Redactor {
    return this.redactor ?? new Redactor(redactConfigFromEnv());
  }

  async save(
    adapterID: string,
    fingerprint: string,
    req: unknown,
    resp: unknown
  ): Promise<void> {
    const now = new Date().toISOString().replace(/\.\d{3}Z$/, "Z");
    await this.write(adapterID, fingerprint, "req", now, req);
    await this.write(adapterID, fingerprint, "resp", now, resp);
  }

  /** Writes both files of a streamed pair per the streaming writer rules. */
  async saveStreamed(interaction: StreamedInteraction): Promise<void> {
    const { req, resp } = interaction;
    await fs.writeFile(
      this.filePath(req.adapter, req.fingerprint, "req"),
      emitStreamedEnvelope(req, "req"),
      "utf8"
    );
    await fs.writeFile(
      this.filePath(req.adapter, req.fingerprint, "resp"),
      emitStreamedEnvelope(resp, "resp"),
      "utf8"
    );
  }

  private async write(
    adapterID: string,
    fingerprint: string,
    kind: "req" | "resp",
    recordedAt: string,
    payload: unknown
  ): Promise<void> {
    // Scrub credential-bearing fields before serialization — a secret
    // never reaches the YAML string, let alone the file. Envelope
    // metadata is built after redaction and is never scrubbed: the
    // fingerprint in particular must match the filename.
    const env: Envelope = {
      xrr: "1",
      adapter: adapterID,
      fingerprint,
      recorded_at: recordedAt,
      payload: this.activeRedactor().redactPayload(payload),
    };
    const data = yaml.dump(env, { lineWidth: -1 });
    await fs.writeFile(this.filePath(adapterID, fingerprint, kind), data, "utf8");
  }

  async load(
    adapterID: string,
    fingerprint: string
  ): Promise<{ req: unknown; resp: unknown }> {
    const { req, resp } = await this.loadPair(adapterID, fingerprint);
    if (req.stream != null) {
      throw new ShapeMismatchError("xrr: shape mismatch: streamed cassette on unary path");
    }
    return { req: req.payload, resp: resp.payload };
  }

  /** Loads and validates a streamed pair into the stream model. */
  async loadStreamed(adapterID: string, fingerprint: string): Promise<StreamedInteraction> {
    const { req, resp, reqText, respText } = await this.loadPair(adapterID, fingerprint);
    if (req.stream == null) {
      throw new ShapeMismatchError("xrr: shape mismatch: unary cassette on streaming path");
    }
    const reqStream = parseReqStream(extractStreamNode(reqText));
    const respStream = parseRespStream(extractStreamNode(respText));
    validateStreamPair(reqStream, respStream);
    return {
      req: {
        xrr: req.xrr,
        adapter: req.adapter,
        fingerprint: req.fingerprint,
        recorded_at: req.recorded_at,
        payload: req.payload,
        stream: reqStream,
      },
      resp: {
        xrr: resp.xrr,
        adapter: resp.adapter,
        fingerprint: resp.fingerprint,
        recorded_at: resp.recorded_at,
        payload: resp.payload,
        ...(resp.error != null && resp.error !== "" ? { error: resp.error } : {}),
        stream: respStream,
      },
    };
  }

  private async loadPair(
    adapterID: string,
    fingerprint: string
  ): Promise<{ req: Envelope; resp: Envelope; reqText: string; respText: string }> {
    const reqText = await this.readText(adapterID, fingerprint, "req");
    const respText = await this.readText(adapterID, fingerprint, "resp");
    const req = parseEnvelope(reqText, "req");
    const resp = parseEnvelope(respText, "resp");
    if ((req.stream != null) !== (resp.stream != null)) {
      throw new StreamFormatError(
        "xrr: stream: present on one file of the pair but not the other"
      );
    }
    return { req, resp, reqText, respText };
  }

  private async readText(
    adapterID: string,
    fingerprint: string,
    kind: "req" | "resp"
  ): Promise<string> {
    try {
      return await fs.readFile(this.filePath(adapterID, fingerprint, kind), "utf8");
    } catch (err: unknown) {
      if ((err as NodeJS.ErrnoException).code === "ENOENT") {
        throw ErrCassetteMiss;
      }
      throw err;
    }
  }

  private filePath(adapterID: string, fingerprint: string, kind: "req" | "resp"): string {
    return path.join(this.dir, `${adapterID}-${fingerprint}.${kind}.yaml`);
  }
}

function parseEnvelope(data: string, kind: "req" | "resp"): Envelope {
  const env = yaml.load(data) as Envelope | null | undefined;
  if (!env || typeof env !== "object" || !("payload" in env)) {
    throw new Error(`xrr: missing payload in ${kind}`);
  }
  return env;
}
