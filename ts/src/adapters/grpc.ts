/**
 * Streaming gRPC adapter — records and replays server-, client-, and
 * bidi-streamed RPCs through a @grpc/grpc-js client interceptor, on top of
 * the core stream session API. See spec/cassette-format-streaming.md (gRPC
 * Adapter Mapping) for the normative semantics.
 *
 * This is a recording seam, not a gRPC implementation. grpc-js composes
 * client interceptors into a chain whose bottom link owns the channel; an
 * interceptor returns an `InterceptingCall` wrapping the next link. The
 * adapter dispatches on session mode at that seam:
 *
 *   - passthrough: return the next call untouched.
 *   - record: wrap the next call, teeing every observed event (send frame,
 *     recv frame, half-close, terminal) into a StreamRecording.
 *   - replay: return an InterceptingCall over a synthetic bottom call that
 *     never touches the channel — frames and the terminal are served from
 *     the cassette, so no connection is attempted.
 *
 * Messages cross the interceptor boundary as *deserialized* objects:
 * grpc-js serializes below the interceptor chain. Frames must hold
 * protobuf wire bytes (spec: message_b64), so the adapter converts at the
 * seam using the method's own codec — `requestSerialize` for sends (present
 * on the interceptor's method_definition) and the service definition's
 * `responseSerialize` / `responseDeserialize` for recvs. The runtime
 * method_definition carries only the client half of the codec, which is why
 * the adapter is constructed with the service definitions it records.
 */
import type {
  ReqStream,
  StreamType,
  StreamedInteraction,
} from "../stream.js";
import { msgHash } from "../streamfp.js";
import type { StreamOpen } from "../streamfp.js";
import type { FileSession } from "../session.js";
import {
  ErrEndOfStream,
  StreamMismatchError,
  type StreamRecording,
  type StreamReplay,
} from "../streamSession.js";

const ADAPTER_ID = "grpc";

/** gRPC status codes used by the adapter (mirrors grpc.status). */
const STATUS_OK = 0;
const STATUS_UNKNOWN = 2;
const STATUS_FAILED_PRECONDITION = 9;

// ── minimal structural typing of the grpc-js surface ───────────────────────
//
// The adapter is typed against the shapes it actually uses rather than
// importing @grpc/grpc-js: the library stays a peer concern of the adopter
// (and an optional peer dependency), and these shapes are the stable,
// documented parts of its interceptor contract.

/** grpc-js `StatusObject`. */
export interface GrpcStatus {
  code: number;
  details: string;
  metadata?: unknown;
}

/** grpc-js `InterceptingListener` (partial, as the bottom call sees it). */
export interface GrpcListener {
  onReceiveMetadata?: (metadata: unknown) => void;
  onReceiveMessage?: (message: unknown) => void;
  onReceiveStatus?: (status: GrpcStatus) => void;
}

/** grpc-js `InterceptingCallInterface` — the bottom-call contract. */
export interface GrpcCall {
  start(metadata: unknown, listener?: GrpcListener): void;
  sendMessage(message: unknown): void;
  sendMessageWithContext(context: unknown, message: unknown): void;
  startRead(): void;
  halfClose(): void;
  cancelWithStatus(code: number, details: string): void;
  getPeer(): string;
  getAuthContext(): unknown;
}

/** The client half of a grpc-js `MethodDefinition`, as interceptors see it. */
export interface GrpcMethodDefinition {
  path: string;
  requestStream: boolean;
  responseStream: boolean;
  requestSerialize(value: unknown): Buffer;
  responseDeserialize(bytes: Buffer): unknown;
  /** Present on full service definitions; absent on the interceptor's copy. */
  responseSerialize?(value: unknown): Buffer;
}

/** grpc-js `InterceptorOptions`. */
export interface GrpcInterceptorOptions {
  method_definition: GrpcMethodDefinition;
  [key: string]: unknown;
}

export type GrpcNextCall = (options: GrpcInterceptorOptions) => GrpcCall;

/** grpc-js `Interceptor`. */
export type GrpcInterceptor = (
  options: GrpcInterceptorOptions,
  nextCall: GrpcNextCall
) => GrpcCall;

/**
 * The pieces of `@grpc/grpc-js` the adapter needs handed in: the
 * `InterceptingCall` constructor (the library composes interceptor chains
 * out of it) and the `Metadata` constructor (replay synthesizes empty
 * metadata — the format records none).
 */
export interface GrpcRuntime {
  InterceptingCall: new (nextCall: GrpcCall, requester?: unknown) => GrpcCall;
  Metadata: new () => unknown;
}

/** A grpc-js `ServiceDefinition`: method name → full method definition. */
export type GrpcServiceDefinition = Record<string, GrpcMethodDefinition>;

/** A streamed cassette addressed by a request the recording never saw. */
export class GrpcAdapterError extends Error {
  constructor(message: string) {
    super(`grpc: ${message}`);
    this.name = "GrpcAdapterError";
  }
}

// ── method identity ────────────────────────────────────────────────────────

/** Splits "/pkg.Service/Method" into its service and method identifiers. */
export function splitFullMethod(full: string): { service: string; method: string } {
  const s = full.startsWith("/") ? full.slice(1) : full;
  const i = s.lastIndexOf("/");
  if (i <= 0 || i === s.length - 1) {
    throw new GrpcAdapterError(`malformed full method ${JSON.stringify(full)} (want /service/method)`);
  }
  return { service: s.slice(0, i), method: s.slice(i + 1) };
}

/**
 * Maps grpc-js request/response stream flags to the spec's stream types.
 * Unary-shaped methods (neither flag) are rejected: unary RPCs keep the v1
 * unary format and never migrate to the stream path.
 */
export function streamTypeOf(md: GrpcMethodDefinition): StreamType {
  if (md.requestStream && md.responseStream) return "bidi";
  if (md.responseStream) return "server";
  if (md.requestStream) return "client";
  throw new GrpcAdapterError(
    `${md.path} is unary-shaped (no stream direction); unary RPCs use the unary adapter path`
  );
}

function bytesOf(b: Buffer | Uint8Array): Uint8Array {
  return new Uint8Array(b.buffer, b.byteOffset, b.byteLength);
}

// ── adapter ────────────────────────────────────────────────────────────────

/**
 * Builds the recording interceptor for a session.
 *
 * `services` supplies the full method definitions (including
 * `responseSerialize`) keyed by their `path`, because the interceptor's own
 * `method_definition` carries only the client half of the codec while recv
 * frames must be persisted as wire bytes. Pass the `service` property of
 * every generated client the session records (or any package definition
 * entry); the constructor indexes them by path.
 */
export class GrpcStreamAdapter {
  readonly id = ADAPTER_ID;

  private readonly byPath = new Map<string, GrpcMethodDefinition>();

  constructor(
    private readonly session: FileSession,
    private readonly runtime: GrpcRuntime,
    services: GrpcServiceDefinition[] = []
  ) {
    for (const svc of services) this.registerService(svc);
  }

  /** Indexes one service definition's methods by their full path. */
  registerService(service: GrpcServiceDefinition): this {
    for (const md of Object.values(service)) {
      if (md && typeof md.path === "string") this.byPath.set(md.path, md);
    }
    return this;
  }

  /**
   * The grpc-js client interceptor. Pass it as `{ interceptors: [...] }` in
   * client options (or per call). Streamed RPCs are recorded, replayed, or
   * passed through per session mode; unary RPCs are rejected loudly rather
   * than silently bypassed, so a misrouted call cannot look recorded.
   */
  interceptor(): GrpcInterceptor {
    return (options, nextCall) => {
      const md = options.method_definition;
      const type = streamTypeOf(md);
      switch (this.session.sessionMode) {
        case "passthrough":
          return nextCall(options);
        case "record":
          return new this.runtime.InterceptingCall(
            new RecordCall(this.session, this.runtime, this.codec(md), type, nextCall(options))
          );
        case "replay":
          return new this.runtime.InterceptingCall(
            new ReplayCall(this.session, this.runtime, this.codec(md), type)
          );
        default:
          throw new GrpcAdapterError(`unknown session mode ${JSON.stringify(this.session.sessionMode)}`);
      }
    };
  }

  /**
   * Resolves the codec for a method: the interceptor's own definition for
   * the request half, the registered service definition for the response
   * half. A recv-carrying method with no registered service definition
   * fails loudly — silently skipping recv frames would write a cassette
   * that replays as a truncated stream.
   */
  private codec(md: GrpcMethodDefinition): MethodCodec {
    const { service, method } = splitFullMethod(md.path);
    const full = this.byPath.get(md.path);
    const responseSerialize = md.responseSerialize ?? full?.responseSerialize;
    if (!responseSerialize) {
      throw new GrpcAdapterError(
        `no responseSerialize for ${md.path}: register the service definition with ` +
          `new GrpcStreamAdapter(session, runtime, [Client.service]) — the interceptor's ` +
          `method_definition carries only the client half of the codec`
      );
    }
    return {
      service,
      method,
      path: md.path,
      requestSerialize: (v) => bytesOf(md.requestSerialize(v)),
      responseSerialize: (v) => bytesOf(responseSerialize(v)),
      responseDeserialize: (b) => md.responseDeserialize(Buffer.from(b)),
    };
  }
}

interface MethodCodec {
  service: string;
  method: string;
  path: string;
  requestSerialize(value: unknown): Uint8Array;
  responseSerialize(value: unknown): Uint8Array;
  responseDeserialize(bytes: Uint8Array): unknown;
}

/**
 * Builds the core open value for a gRPC streamed RPC per the spec's gRPC
 * mapping: canonical inputs service + method, req payload
 * {service, method}. Server streams are content-addressed by
 * msg_hash = sha256(message_bytes)[:8] over the single request message;
 * client/bidi opens are counter-addressed.
 *
 * The msg_hash is the one identity input derived from message bytes, and it
 * is computed here, before the core's frame seam — so it is derived from
 * the session-scrubbed bytes. Record and replay both pass through this
 * path, which keeps a scrubbed recording and a scrubbed replay of the same
 * live traffic addressing the same cassette. The raw message itself is
 * handed to the core untouched: the core scrubs frames exactly once.
 */
function streamOpen(
  session: FileSession,
  type: StreamType,
  codec: MethodCodec,
  openMsg?: Uint8Array
): StreamOpen {
  const identity: Record<string, string | number> = {
    service: codec.service,
    method: codec.method,
  };
  const open: StreamOpen = {
    adapterID: ADAPTER_ID,
    type,
    identity,
    payload: { service: codec.service, method: codec.method },
  };
  if (type === "server") {
    const scrubbed = session.scrubStreamFrame(
      "send",
      { adapterID: ADAPTER_ID, type },
      openMsg ?? new Uint8Array(0)
    );
    identity.msg_hash = msgHash(scrubbed);
  } else {
    open.counter = true;
  }
  return open;
}

// ── record ─────────────────────────────────────────────────────────────────

/**
 * Wraps the live bottom call and tees every observed event into a
 * StreamRecording. Metadata, peer, and auth context pass straight through
 * to the live call — the format records no metadata.
 */
class RecordCall implements GrpcCall {
  private rec: StreamRecording | null = null;
  private recPromise: Promise<StreamRecording> | null = null;
  private finished = false;
  /** Serializes the async record chain so frames keep their arrival order. */
  private tail: Promise<unknown> = Promise.resolve();
  private pendingError: unknown = null;

  constructor(
    private readonly session: FileSession,
    private readonly runtime: GrpcRuntime,
    private readonly codec: MethodCodec,
    private readonly type: StreamType,
    private readonly live: GrpcCall
  ) {
    // Client/bidi opens are fingerprinted by the occurrence counter, so the
    // recording opens now, mirroring replay's counter consumption. Server
    // streams are content-addressed by the open message, which only
    // surfaces at the first sendMessage — their open is deferred there.
    if (type !== "server") this.ensureOpen(new Uint8Array(0));
  }

  private ensureOpen(openMsg: Uint8Array): Promise<StreamRecording> {
    this.recPromise ??= this.session
      .openStreamRecord(streamOpen(this.session, this.type, this.codec, openMsg))
      .then((rec) => {
        this.rec = rec;
        return rec;
      });
    return this.recPromise;
  }

  /**
   * Queues one recording step behind the previous ones. Recording is async
   * (the cassette write is), while the grpc-js call surface is
   * synchronous — the tail chain preserves the observed event order and
   * carries any failure to the terminal.
   */
  private queue(step: (rec: StreamRecording) => void | Promise<void>): void {
    this.tail = this.tail
      .then(() => this.recPromise)
      .then((rec) => (rec ? step(rec) : undefined))
      .catch((err: unknown) => {
        this.pendingError ??= err;
      });
  }

  start(metadata: unknown, listener?: GrpcListener): void {
    this.live.start(metadata, {
      onReceiveMetadata: (md) => listener?.onReceiveMetadata?.(md),
      onReceiveMessage: (message) => {
        // The live message is recorded as wire bytes and forwarded
        // untouched: the caller must observe exactly what the server sent.
        const bytes = this.codec.responseSerialize(message);
        this.queue((rec) => rec.recordRecv(bytes));
        listener?.onReceiveMessage?.(message);
      },
      onReceiveStatus: (status) => {
        this.finish(status);
        // The cassette write is async while grpc-js's status delivery is
        // not. Deliver the status only once the pair is on disk, so a
        // record run cannot finish before its cassette exists.
        void this.tail.then(() => {
          listener?.onReceiveStatus?.(status);
        });
      },
    });
  }

  /** Records the terminal event and persists the pair, exactly once. */
  private finish(status: GrpcStatus): void {
    if (this.finished) return;
    this.finished = true;
    let code = status.code;
    const failed = code !== STATUS_OK;
    // Spec invariant: the envelope error is non-empty iff status_code != 0.
    const terminalError = failed
      ? new Error(renderStatus(status))
      : null;
    if (terminalError && code === STATUS_OK) code = STATUS_UNKNOWN;
    this.queue((rec) => rec.finish({ status_code: code }, terminalError));
  }

  sendMessage(message: unknown): void {
    this.sendMessageWithContext({}, message);
  }

  sendMessageWithContext(context: unknown, message: unknown): void {
    const bytes = this.codec.requestSerialize(message);
    // A deferred server-stream open needs the request bytes; opening here
    // keeps the fingerprint derived from the message actually sent.
    void this.ensureOpen(bytes);
    this.queue((rec) => rec.recordSend(bytes));
    this.live.sendMessageWithContext(context, message);
  }

  halfClose(): void {
    this.queue((rec) => rec.recordHalfClose());
    this.live.halfClose();
  }

  startRead(): void {
    this.live.startRead();
  }

  cancelWithStatus(code: number, details: string): void {
    this.live.cancelWithStatus(code, details);
  }

  getPeer(): string {
    return this.live.getPeer();
  }

  getAuthContext(): unknown {
    return this.live.getAuthContext();
  }
}

/**
 * Renders a status the way the grpc-js client does, so a replayed error
 * carries the same text a live one did.
 */
function renderStatus(status: GrpcStatus): string {
  const details = status.details === "" ? `status code ${status.code}` : status.details;
  return `${status.code} ${details}`;
}

// ── replay ─────────────────────────────────────────────────────────────────

/**
 * Serves one recorded interaction as a grpc-js bottom call. It never
 * touches a channel: sends are validated against the recording, recv frames
 * and the terminal come from the cassette. Header/trailer metadata is
 * empty — the format records none.
 */
class ReplayCall implements GrpcCall {
  private rp: StreamReplay | null = null;
  private rpPromise: Promise<StreamReplay> | null = null;
  private listener: GrpcListener | null = null;
  private done = false;
  private started = false;
  /** Serializes the async replay chain so events keep their order. */
  private tail: Promise<unknown> = Promise.resolve();

  constructor(
    private readonly session: FileSession,
    private readonly runtime: GrpcRuntime,
    private readonly codec: MethodCodec,
    private readonly type: StreamType
  ) {
    // Client/bidi cassettes are located by the occurrence counter, known
    // now. Server streams are located by the request message, so their open
    // is deferred to the first sendMessage.
    if (type !== "server") this.ensureOpen(new Uint8Array(0));
  }

  private ensureOpen(openMsg: Uint8Array): Promise<StreamReplay> {
    this.rpPromise ??= this.session
      .openStreamReplay(streamOpen(this.session, this.type, this.codec, openMsg))
      .then((rp) => {
        this.rp = rp;
        return rp;
      });
    return this.rpPromise;
  }

  /**
   * Queues one replay step behind the previous ones, routing any failure to
   * the terminal. Cassette loads are async while the grpc-js call surface
   * is synchronous, so ordering lives in this chain.
   */
  private queue(step: (rp: StreamReplay) => void): void {
    this.tail = this.tail
      .then(() => this.rpPromise)
      .then((rp) => {
        if (rp) step(rp);
      })
      .catch((err: unknown) => this.abort(err));
  }

  start(metadata: unknown, listener?: GrpcListener): void {
    this.started = true;
    this.listener = listener ?? null;
    listener?.onReceiveMetadata?.(new this.runtime.Metadata());
    if (!this.codec.path || !this.type) return;
    // grpc-js drives reads itself for unary-response methods (its bottom
    // call calls startRead() from start()); mirror that so a
    // client-streaming call completes without the caller reading.
    if (this.type === "client") this.startRead();
  }

  sendMessage(message: unknown): void {
    this.sendMessageWithContext({}, message);
  }

  sendMessageWithContext(_context: unknown, message: unknown): void {
    const bytes = this.codec.requestSerialize(message);
    void this.ensureOpen(bytes).catch(() => undefined);
    this.queue((rp) => {
      try {
        rp.send(bytes);
      } catch (err) {
        // Post-terminal sends on an OK recording are the canonical
        // "produce until the stream reports done" pattern: the recorder
        // dropped them, so they must not poison the recv side.
        if (err === ErrEndOfStream) return;
        throw err;
      }
    });
  }

  halfClose(): void {
    this.queue((rp) => rp.halfClose());
  }

  startRead(): void {
    if (this.done) return;
    this.queue((rp) => {
      if (this.done) return;
      let bytes: Uint8Array;
      try {
        bytes = rp.recv();
      } catch (err) {
        if (err === ErrEndOfStream) {
          this.terminate({ code: STATUS_OK, details: "OK" });
          return;
        }
        throw err;
      }
      this.listener?.onReceiveMessage?.(this.codec.responseDeserialize(bytes));
    });
  }

  /**
   * Delivers the terminal exactly once. Every later read is a no-op: the
   * grpc-js client tears the call down on the first status.
   */
  private terminate(status: GrpcStatus): void {
    if (this.done) return;
    this.done = true;
    this.listener?.onReceiveStatus?.({ ...status, metadata: new this.runtime.Metadata() });
  }

  /**
   * Fails the call with a status reconstructed for the offending condition.
   * Recorded error terminals rebuild their gRPC status from the resp
   * payload's status_code, so generated-client code behaves identically to
   * a live stream; mismatches and misses surface as their own statuses
   * rather than masquerading as recorded failures.
   */
  private abort(err: unknown): void {
    if (this.done) return;
    const message = err instanceof Error ? err.message : String(err);
    if (err instanceof StreamMismatchError) {
      this.terminate({ code: STATUS_FAILED_PRECONDITION, details: message });
      return;
    }
    if (this.rp && this.isRecordedError(err)) {
      this.terminate({ code: recordedStatusCode(this.rp), details: recordedDetails(this.rp, message) });
      return;
    }
    this.terminate({ code: STATUS_UNKNOWN, details: message });
  }

  /** A recorded error terminal surfaces as the replay's own terminal error. */
  private isRecordedError(err: unknown): boolean {
    return err instanceof Error && err !== ErrEndOfStream;
  }

  cancelWithStatus(code: number, details: string): void {
    this.terminate({ code, details });
  }

  getPeer(): string {
    return "xrr:replay";
  }

  getAuthContext(): unknown {
    return null;
  }
}

/**
 * Extracts the recorded status_code from the resp payload. Absent or
 * malformed ⇒ UNKNOWN: the spec requires the field, and replay must fail
 * loudly rather than succeed.
 */
function recordedStatusCode(rp: StreamReplay): number {
  const payload = rp.respPayload;
  if (payload && typeof payload === "object" && "status_code" in payload) {
    const v = (payload as { status_code: unknown }).status_code;
    if (typeof v === "number" && Number.isInteger(v)) {
      // An error terminal can never be OK; guard hand-authored cassettes
      // violating the spec invariant.
      return v === STATUS_OK ? STATUS_UNKNOWN : v;
    }
  }
  return STATUS_UNKNOWN;
}

/**
 * Recovers the status description from the recorded error string. When it
 * is the standard client rendering ("<code> <details>"), the description is
 * extracted so the reconstructed error renders like the live one instead of
 * nesting.
 */
function recordedDetails(rp: StreamReplay, message: string): string {
  const code = recordedStatusCode(rp);
  const prefix = `${code} `;
  return message.startsWith(prefix) ? message.slice(prefix.length) : message;
}

export type { ReqStream, StreamedInteraction };
