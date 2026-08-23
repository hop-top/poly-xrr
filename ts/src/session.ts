/**
 * FileSession — record/replay/passthrough session.
 */
import { OccurrenceCounter } from "./streamfp.js";
import { type Adapter, type Cassette, ErrCassetteMiss, type Mode, type Session } from "./xrr.js";

export class FileSession implements Session {
  /**
   * Per-session occurrence counter for streamed opens — one session object
   * is one counter domain (spec/cassette-format-streaming.md). Streaming
   * adapters key it by their identifying tuple at each stream open.
   */
  readonly streamCounter = new OccurrenceCounter();

  constructor(
    private readonly mode: Mode,
    private readonly cassette: Cassette
  ) {}

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
}
