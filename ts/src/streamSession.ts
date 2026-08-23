/**
 * Session-level streamed record/replay handles — the interactive layer on
 * top of the stream format model. Adapters build their stream wrappers on
 * these; see spec/cassette-format-streaming.md for the normative semantics.
 *
 * StreamRecording accumulates the event log of one live stream (seq from
 * one per-interaction counter in arrival order, at_ms from a monotonic
 * clock) and persists the validated pair only at finish. StreamReplay
 * serves one recorded interaction: sends are validated in order and bytes,
 * recv frames are delivered in seq order and never gate on send progress.
 */
import { createHash } from "node:crypto";
import type {
  StreamEventPos,
  StreamFrame,
  StreamType,
  StreamedInteraction,
} from "./stream.js";

/**
 * End-of-stream signal (the io.EOF analogue): thrown by recv past the last
 * recorded frame of an OK terminal, and by send past the last recorded send
 * frame of an OK terminal (the post-completion stream-done signal). Compare
 * by identity, like ErrCassetteMiss.
 */
export const ErrEndOfStream = new Error("xrr: end of stream");

/**
 * A replay-time divergence between the client's send-side behaviour and the
 * recording (byte-divergent send at i < S, or short half-close). Mismatch
 * is terminal: every subsequent operation on the stream throws the same
 * error instance. Distinct from a cassette miss and from a recorded
 * (replayed) error.
 */
export class StreamMismatchError extends Error {
  constructor(
    /** "send" or "half_close" — the offending client operation. */
    readonly op: "send" | "half_close",
    /** 0-based ordinal of the offending client operation. */
    readonly ordinal: number,
    detail: string
  ) {
    super(`xrr: stream mismatch: ${op} ${ordinal}: ${detail}`);
    this.name = "StreamMismatchError";
  }
}

/** Cassette IO surface the streamed session layer requires. */
export interface StreamCassette {
  saveStreamed(interaction: StreamedInteraction): Promise<void>;
  loadStreamed(adapterID: string, fingerprint: string): Promise<StreamedInteraction>;
}

function sha256Hex(bytes: Uint8Array): string {
  return createHash("sha256").update(bytes).digest("hex");
}

function bytesEqual(a: Uint8Array, b: Uint8Array): boolean {
  if (a.length !== b.length) return false;
  for (let i = 0; i < a.length; i++) if (a[i] !== b[i]) return false;
  return true;
}

// ── record path ────────────────────────────────────────────────────────────

/**
 * Accumulates the event log of one live stream and writes the cassette pair
 * at terminal. The adapter mirrors the live stream into it: recordSend /
 * recordRecv per message, recordHalfClose when the client closes its send
 * side, then finish exactly once when the terminal is observed — only
 * finish persists the pair, so a stream that never reaches terminal
 * produces no cassette.
 */
export class StreamRecording {
  private seq = 0;
  private readonly sends: StreamFrame[] = [];
  private readonly recvs: StreamFrame[] = [];
  private halfClose?: StreamEventPos;
  private finished = false;
  private readonly opened = performance.now();

  constructor(
    private readonly cassette: StreamCassette,
    private readonly adapterID: string,
    /** The open-time fingerprint of this interaction. */
    readonly fingerprint: string,
    private readonly type: StreamType,
    private readonly reqPayload: Record<string, unknown>
  ) {}

  private elapsedMs(): number {
    return Math.max(0, Math.floor(performance.now() - this.opened));
  }

  private nextFrame(message: Uint8Array): StreamFrame {
    return { seq: this.seq++, message: message.slice(), encoding: "b64", at_ms: this.elapsedMs() };
  }

  /** Logs one client→server message. Dropped after finish. */
  recordSend(message: Uint8Array): void {
    if (this.finished) return;
    this.sends.push(this.nextFrame(message));
  }

  /** Logs one server→client message. Dropped after finish. */
  recordRecv(message: Uint8Array): void {
    if (this.finished) return;
    this.recvs.push(this.nextFrame(message));
  }

  /**
   * Logs the client closing its send side. It occurs at most once; repeats
   * and post-terminal calls are dropped, matching their real-world no-op.
   */
  recordHalfClose(): void {
    if (this.finished || this.halfClose) return;
    this.halfClose = { seq: this.seq++, at_ms: this.elapsedMs() };
  }

  /**
   * Records the terminal event and persists the pair. terminalError is
   * absent for an OK terminal; when present its message is persisted as the
   * resp envelope error field so replay re-emits it. No events are recorded
   * after finish, and calling it twice is an error.
   */
  async finish(
    respPayload: Record<string, unknown> | null,
    terminalError?: Error | null
  ): Promise<void> {
    if (this.finished) throw new Error("xrr: stream already finished");
    this.finished = true;

    const end: StreamEventPos = { seq: this.seq++, at_ms: this.elapsedMs() };
    const recordedAt = new Date().toISOString().replace(/\.\d{3}Z$/, "Z");
    const errStr = terminalError?.message ?? "";
    await this.cassette.saveStreamed({
      req: {
        xrr: "1",
        adapter: this.adapterID,
        fingerprint: this.fingerprint,
        recorded_at: recordedAt,
        payload: this.reqPayload,
        stream: {
          type: this.type,
          frames: this.sends,
          ...(this.halfClose ? { half_close: this.halfClose } : {}),
        },
      },
      resp: {
        xrr: "1",
        adapter: this.adapterID,
        fingerprint: this.fingerprint,
        recorded_at: recordedAt,
        ...(errStr !== "" ? { error: errStr } : {}),
        payload: respPayload ?? {},
        stream: { frames: this.recvs, end },
      },
    });
  }
}

// ── replay path ────────────────────────────────────────────────────────────

/**
 * Serves one recorded streamed interaction. Send-side events are validated
 * against the recording (order and bytes); recv-side frames are delivered
 * in seq order, never gated on send progress. Timing is ignored: frames are
 * delivered as fast as the client consumes them (at_ms stays available on
 * the loaded pair for a future opt-in replay-timing mode).
 */
export class StreamReplay {
  private sendIdx = 0;
  private recvIdx = 0;
  private mismatch: StreamMismatchError | null = null;
  private readonly recordedError: Error | null;

  constructor(
    /** The open-time fingerprint of this interaction. */
    readonly fingerprint: string,
    private readonly pair: StreamedInteraction
  ) {
    const err = pair.resp.error;
    this.recordedError = err != null && err !== "" ? new Error(err) : null;
  }

  /** The recorded stream type. */
  get type(): StreamType {
    return this.pair.req.stream.type;
  }

  /** The recorded open-request payload. */
  get reqPayload(): unknown {
    return this.pair.req.payload;
  }

  /**
   * The recorded terminal-response payload (for gRPC: the status code).
   * Available from open — adapters typically read it only at terminal
   * delivery.
   */
  get respPayload(): unknown {
    return this.pair.resp.payload;
  }

  /**
   * The terminal result: the recorded error when the resp envelope error is
   * non-empty, ErrEndOfStream otherwise. One instance per handle, so it
   * repeats identically for every read past the terminal.
   */
  private terminal(): Error {
    return this.recordedError ?? ErrEndOfStream;
  }

  private fail(m: StreamMismatchError): StreamMismatchError {
    this.mismatch = m;
    return m;
  }

  /**
   * Validates the i-th client message against recorded send frame i.
   *   - i < S, equal bytes: accepted (the message is discarded).
   *   - i < S, divergent bytes: stream mismatch — terminal for the handle.
   *   - i ≥ S: the recording was already past its last observed send. With
   *     an OK terminal send throws ErrEndOfStream (the post-completion
   *     stream-done signal) and does NOT poison the recv side; with an
   *     error terminal it throws the recorded error. Bytes at i ≥ S are
   *     never compared.
   */
  send(message: Uint8Array): void {
    if (this.mismatch) throw this.mismatch;
    const frames = this.pair.req.stream.frames;
    const i = this.sendIdx;
    if (i >= frames.length) throw this.terminal();
    const recorded = frames[i].message;
    if (!bytesEqual(message, recorded)) {
      throw this.fail(
        new StreamMismatchError(
          "send",
          i,
          `expected sha256 ${sha256Hex(recorded)}, got sha256 ${sha256Hex(message)}`
        )
      );
    }
    this.sendIdx++;
  }

  /**
   * Validates the client closing its send side: always accepted after all
   * recorded sends were observed (whether or not the recording has
   * half_close), a stream mismatch after fewer.
   */
  halfClose(): void {
    if (this.mismatch) throw this.mismatch;
    const s = this.pair.req.stream.frames.length;
    if (this.sendIdx < s) {
      throw this.fail(
        new StreamMismatchError(
          "half_close",
          this.sendIdx,
          `half-close after ${this.sendIdx} sends, recording has ${s}`
        )
      );
    }
  }

  /**
   * Delivers the j-th recorded recv frame's bytes. At j = R it throws the
   * terminal — the recorded error or ErrEndOfStream — and repeats it for
   * every later read. recv never blocks on send-side progress.
   */
  recv(): Uint8Array {
    if (this.mismatch) throw this.mismatch;
    const frames = this.pair.resp.stream.frames;
    if (this.recvIdx >= frames.length) throw this.terminal();
    return frames[this.recvIdx++].message.slice();
  }
}
