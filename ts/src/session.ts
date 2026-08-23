/**
 * FileSession — record/replay/passthrough session.
 */
import { ShapeMismatchError } from "./stream.js";
import {
  OccurrenceCounter,
  type StreamOpen,
  streamCanonical,
  streamFingerprint,
} from "./streamfp.js";
import {
  type StreamDirection,
  type StreamScrubFn,
  type StreamScrubInfo,
  scrubFrame,
} from "./streamScrub.js";
import {
  type StreamCassette,
  StreamRecording,
  StreamReplay,
} from "./streamSession.js";
import { type Adapter, type Cassette, ErrCassetteMiss, type Mode, type Session } from "./xrr.js";

export class FileSession implements Session {
  /**
   * Per-session occurrence counter for streamed opens — one session object
   * is one counter domain (spec/cassette-format-streaming.md). Streaming
   * adapters key it by their identifying tuple at each stream open.
   */
  readonly streamCounter = new OccurrenceCounter();

  /**
   * Creates a session. `streamScrub` installs the frame-level scrub hook
   * (see streamScrub.ts): every streamed frame passes through it at record
   * time and every live send passes through it at replay time. Install the
   * SAME hook when recording and when replaying — scrubbing is symmetric by
   * design, and a session replaying a scrubbed cassette without the hook
   * fails with a stream mismatch.
   */
  constructor(
    private readonly mode: Mode,
    private readonly cassette: Cassette,
    private readonly streamScrub?: StreamScrubFn
  ) {}

  /**
   * The session mode. Streaming adapters dispatch on it to pick the record,
   * replay, or passthrough path at stream open.
   */
  get sessionMode(): Mode {
    return this.mode;
  }

  /**
   * Applies the session's frame scrub hook to data, returning data
   * unchanged when no hook is installed.
   *
   * Adapters whose open identity derives from message bytes (the gRPC
   * server-stream msg_hash) MUST compute the derived identity over this
   * method's output, in record and replay mode alike, so both modes address
   * the cassette by the scrubbed content. Frames handed to the core
   * (recordSend / recordRecv, replay send) are scrubbed by the core
   * itself — adapters pass them raw and never double-scrub.
   */
  scrubStreamFrame(dir: StreamDirection, info: StreamScrubInfo, data: Uint8Array): Uint8Array {
    return scrubFrame(this.streamScrub, dir, info, data);
  }

  async record<Req, Resp>(
    adapter: Adapter<Req, Resp>,
    req: Req,
    do_: () => Promise<Resp>
  ): Promise<Resp> {
    switch (this.mode) {
      case "record":
        return this.doRecord(adapter, req, do_);
      case "replay":
        return this.doReplay(adapter, req);
      case "passthrough":
        return do_();
      default: {
        const exhaustive: never = this.mode;
        throw new Error(`xrr: unknown mode "${exhaustive}"`);
      }
    }
  }

  private async doRecord<Req, Resp>(
    adapter: Adapter<Req, Resp>,
    req: Req,
    do_: () => Promise<Resp>
  ): Promise<Resp> {
    const resp = await do_();
    const fp = await adapter.fingerprint(req);
    await this.cassette.save(
      adapter.id,
      fp,
      adapter.serializeReq(req),
      adapter.serializeResp(resp)
    );
    return resp;
  }

  private async doReplay<Req, Resp>(
    adapter: Adapter<Req, Resp>,
    req: Req
  ): Promise<Resp> {
    const fp = await adapter.fingerprint(req);
    let loaded: { req: unknown; resp: unknown };
    try {
      loaded = await this.cassette.load(adapter.id, fp);
    } catch (err) {
      if (err === ErrCassetteMiss) throw ErrCassetteMiss;
      throw err;
    }
    return adapter.deserializeResp(loaded.resp);
  }

  // ── streamed interactions ────────────────────────────────────────────────

  /**
   * Opens a streamed interaction for recording. The adapter observes the
   * live stream and mirrors it into the returned recording: recordSend /
   * recordRecv per message, recordHalfClose when the client closes its send
   * side, then finish exactly once when the terminal is observed — only
   * finish persists the pair.
   */
  async openStreamRecord(open: StreamOpen): Promise<StreamRecording> {
    const cassette = this.checkStreamOpen(open, "record", "openStreamRecord");
    const { fp, n } = this.streamOpenFingerprint(open);
    const payload: Record<string, unknown> = { ...open.payload };
    // Informational occurrence ordinal: recoverable from disk, never read
    // back to drive matching.
    if (n >= 0) payload.n = n;
    return new StreamRecording(
      cassette,
      open.adapterID,
      fp,
      open.type,
      payload,
      this.streamScrub
    );
  }

  /**
   * Locates the cassette pair for a streamed open and returns a replay
   * handle. Throws ErrCassetteMiss when no pair exists and
   * ShapeMismatchError when the pair is unary or its recorded stream type
   * differs. The occurrence counter is consumed exactly as in record mode,
   * hit or miss.
   */
  async openStreamReplay(open: StreamOpen): Promise<StreamReplay> {
    const cassette = this.checkStreamOpen(open, "replay", "openStreamReplay");
    const { fp } = this.streamOpenFingerprint(open);
    const pair = await cassette.loadStreamed(open.adapterID, fp);
    if (pair.req.stream.type !== open.type) {
      throw new ShapeMismatchError(
        `xrr: shape mismatch: recorded stream type "${pair.req.stream.type}", requested "${open.type}"`
      );
    }
    return new StreamReplay(fp, pair, this.streamScrub, open.adapterID);
  }

  private checkStreamOpen(open: StreamOpen, want: Mode, verb: string): StreamCassette {
    if (this.mode !== want) {
      throw new Error(`xrr: ${verb} requires ${want} mode (session is "${this.mode}")`);
    }
    if (!open.adapterID) {
      throw new Error(`xrr: ${verb} requires an adapter id`);
    }
    const c = this.cassette as Cassette & Partial<StreamCassette>;
    if (typeof c.saveStreamed !== "function" || typeof c.loadStreamed !== "function") {
      throw new Error(`xrr: ${verb} requires a stream-capable cassette`);
    }
    return c as Cassette & StreamCassette;
  }

  /**
   * Computes the open-time fingerprint, consuming the occurrence counter
   * for counter-addressed opens. The counter is keyed by the adapter id
   * plus the canonical identity (sans "n"), i.e. the adapter's identifying
   * tuple. n is -1 for content-addressed opens.
   */
  private streamOpenFingerprint(open: StreamOpen): { fp: string; n: number } {
    let n = -1;
    if (open.counter) {
      n = this.streamCounter.next(open.adapterID, streamCanonical(open, -1));
    }
    return { fp: streamFingerprint(open, n), n };
  }
}
