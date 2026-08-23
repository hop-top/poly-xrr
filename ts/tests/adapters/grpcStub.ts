/**
 * A stub of the pieces of @grpc/grpc-js the adapter talks to.
 *
 * The adapter is typed against grpc-js's interceptor contract rather than
 * against the library, so the fixture-conformance tests can drive it
 * without a channel, a server, or protobuf. The real library is exercised
 * separately in grpcLive.test.ts — this stub exists so the conformance
 * assertions stay on the adapter's stream semantics.
 *
 * StubInterceptingCall mirrors the real `InterceptingCall` closely enough
 * for the adapter's purposes: it forwards the call surface to the
 * intercepting call the adapter returns, which for replay is the adapter's
 * own synthetic bottom call.
 */
import type {
  GrpcCall,
  GrpcInterceptor,
  GrpcInterceptorOptions,
  GrpcListener,
  GrpcMethodDefinition,
  GrpcRuntime,
  GrpcStatus,
} from "../../src/adapters/grpc.js";
import { drain, drainCall } from "../../src/adapters/grpc.js";
import type { StreamType } from "../../src/stream.js";

/** grpc-js `Metadata` stand-in: the format records none, so it stays empty. */
export class StubMetadata {}

/**
 * A byte-transparent codec: a message IS its wire bytes. This is the
 * raw-bytes codec case (a custom grpc-js codec), and it lets fixtures whose
 * frames hold hand-authored bytes drive the adapter directly.
 */
export function byteCodec(path: string, type: StreamType): GrpcMethodDefinition {
  const toBuffer = (v: unknown): Buffer => Buffer.from(v as Uint8Array);
  return {
    path,
    requestStream: type === "client" || type === "bidi",
    responseStream: type === "server" || type === "bidi",
    requestSerialize: toBuffer,
    responseSerialize: toBuffer,
    responseDeserialize: (b: Buffer) => new Uint8Array(b),
  };
}

/**
 * Forwards the grpc-js call surface to the call it wraps, standing in for
 * the library's `InterceptingCall`. The adapter constructs it around its own
 * record/replay call, exactly as grpc-js does.
 */
export class StubInterceptingCall implements GrpcCall {
  constructor(private readonly inner: GrpcCall) {}

  /** Forwards the adapter's drain hook through the wrapper. */
  [drainCall] = (): Promise<void> => drain(this.inner);

  start(metadata: unknown, listener?: GrpcListener): void {
    this.inner.start(metadata, listener);
  }
  sendMessage(message: unknown): void {
    this.inner.sendMessage(message);
  }
  sendMessageWithContext(context: unknown, message: unknown): void {
    this.inner.sendMessageWithContext(context, message);
  }
  startRead(): void {
    this.inner.startRead();
  }
  halfClose(): void {
    this.inner.halfClose();
  }
  cancelWithStatus(code: number, details: string): void {
    this.inner.cancelWithStatus(code, details);
  }
  getPeer(): string {
    return this.inner.getPeer();
  }
  getAuthContext(): unknown {
    return this.inner.getAuthContext();
  }
}

/** The grpc-js runtime surface the adapter needs, backed by the stubs. */
export const stubRuntime: GrpcRuntime = {
  InterceptingCall: StubInterceptingCall,
  Metadata: StubMetadata,
};

/**
 * A call under test: the adapter's interceptor applied to one method,
 * plus `settle` to drain the adapter's internal async chains (which the
 * real client does implicitly by awaiting IO).
 */
export class TestCall implements GrpcCall {
  private readonly inner: GrpcCall;

  constructor(interceptor: GrpcInterceptor, md: GrpcMethodDefinition, bottom?: GrpcCall) {
    const options = { method_definition: md } as GrpcInterceptorOptions;
    this.inner = interceptor(options, () => {
      if (!bottom) throw new Error("stub: no bottom call supplied");
      return bottom;
    });
  }

  /**
   * Waits for the adapter's internal async chain to settle. The real client
   * gets this for free by awaiting network IO; driving a call directly needs
   * the adapter's own drain hook, so waiting is deterministic rather than a
   * guessed number of event-loop turns.
   */
  async settle(): Promise<void> {
    await drain(this.inner);
    await new Promise((r) => setImmediate(r));
    await drain(this.inner);
  }

  start(metadata: unknown, listener?: GrpcListener): void {
    this.inner.start(metadata, listener);
  }
  sendMessage(message: unknown): void {
    this.inner.sendMessage(message);
  }
  sendMessageWithContext(context: unknown, message: unknown): void {
    this.inner.sendMessageWithContext(context, message);
  }
  startRead(): void {
    this.inner.startRead();
  }
  halfClose(): void {
    this.inner.halfClose();
  }
  cancelWithStatus(code: number, details: string): void {
    this.inner.cancelWithStatus(code, details);
  }
  getPeer(): string {
    return this.inner.getPeer();
  }
  getAuthContext(): unknown {
    return this.inner.getAuthContext();
  }
}

export interface DriveOptions {
  sends: Uint8Array[];
  halfClose: boolean;
  reads: number;
}

/**
 * Drives one replayed call the way a generated client would: start, send
 * the recorded messages, half-close, then read until the terminal.
 */
export async function drive(
  interceptor: GrpcInterceptor,
  md: GrpcMethodDefinition,
  opts: DriveOptions
): Promise<{ messages: Uint8Array[]; status: GrpcStatus | null }> {
  const call = new TestCall(interceptor, md);
  const messages: Uint8Array[] = [];
  let status: GrpcStatus | null = null;

  call.start(new StubMetadata(), {
    onReceiveMessage: (m) => {
      if (m != null) messages.push(m as Uint8Array);
    },
    onReceiveStatus: (s) => {
      status ??= s;
    },
  });

  for (const s of opts.sends) call.sendMessage(s);
  if (opts.halfClose) call.halfClose();
  await call.settle();

  // Unary-response methods are driven by the adapter itself at half-close
  // (mirroring grpc-js's own bottom call), so an extra startRead here would
  // race that read rather than sequence after it.
  if (md.responseStream) {
    for (let i = 0; i < opts.reads && status === null; i++) {
      call.startRead();
      await call.settle();
    }
  } else {
    // Give the adapter-driven read time to land, then drive any further
    // reads a caller would make.
    for (let i = 0; i < opts.reads && status === null; i++) {
      if (i > 0) call.startRead();
      await call.settle();
    }
  }
  await call.settle();
  return { messages, status };
}
