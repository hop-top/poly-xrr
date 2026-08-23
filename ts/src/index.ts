/**
 * Barrel export for @hop-top/xrr.
 */
export { ErrCassetteMiss } from "./xrr.js";
export type { Adapter, Cassette, Mode, Session } from "./xrr.js";
export { FileCassette } from "./cassette.js";
export { FileSession } from "./session.js";
export {
  ShapeMismatchError,
  StreamFormatError,
  emitStreamedEnvelope,
  extractStreamNode,
  parseReqStream,
  parseRespStream,
  strictBase64Decode,
  validateStreamPair,
} from "./stream.js";
export type {
  FrameEncoding,
  ReqStream,
  RespStream,
  StreamEventPos,
  StreamFrame,
  StreamType,
  StreamedEnvelope,
  StreamedInteraction,
} from "./stream.js";
export {
  OccurrenceCounter,
  counterStreamFingerprint,
  msgHash,
  serverStreamFingerprint,
  streamFingerprint,
} from "./streamfp.js";
export type { StreamOpen } from "./streamfp.js";
export {
  ErrEndOfStream,
  StreamMismatchError,
  StreamRecording,
  StreamReplay,
} from "./streamSession.js";
export type { StreamCassette } from "./streamSession.js";
export { ExecAdapter } from "./adapters/exec.js";
export type { ExecRequest, ExecResponse } from "./adapters/exec.js";
export { HttpAdapter } from "./adapters/http.js";
export type { HttpRequest, HttpResponse } from "./adapters/http.js";
export { RedisAdapter } from "./adapters/redis.js";
export type { RedisRequest, RedisResponse } from "./adapters/redis.js";
export { SqlAdapter } from "./adapters/sql.js";
export type { SqlRequest, SqlResponse } from "./adapters/sql.js";
export { FsAdapter } from "./adapters/fs.js";
export type { FsOp, FsRequest, FsResponse } from "./adapters/fs.js";
