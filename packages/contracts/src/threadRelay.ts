import * as Schema from "effect/Schema";

import { EnvironmentId, ThreadId } from "./baseSchemas.ts";
import { ProviderInteractionMode, RuntimeMode } from "./providerPolicy.ts";

/** Thread tools an agent can aim at another environment through a connected app. */
export const ThreadRelayOperation = Schema.Literals(["list", "read", "search", "wait", "send"]);
export type ThreadRelayOperation = typeof ThreadRelayOperation.Type;

/** The calling thread and the limits the target environment must enforce on its behalf. */
export const ThreadRelayCaller = Schema.Struct({
  environmentId: EnvironmentId,
  threadId: ThreadId,
  title: Schema.String,
  runtimeMode: RuntimeMode,
  interactionMode: ProviderInteractionMode,
  readOnly: Schema.Boolean,
});
export type ThreadRelayCaller = typeof ThreadRelayCaller.Type;

/** An app that can reach other environments with its own sessions. */
export const ThreadRelayHost = Schema.Struct({
  clientId: Schema.String,
  reachableEnvironmentIds: Schema.Array(EnvironmentId),
});
export type ThreadRelayHost = typeof ThreadRelayHost.Type;

export const ThreadRelayUpdateHostInput = Schema.Struct({
  connectionId: Schema.String,
  reachableEnvironmentIds: Schema.Array(EnvironmentId),
});
export type ThreadRelayUpdateHostInput = typeof ThreadRelayUpdateHostInput.Type;

export const ThreadRelayRequest = Schema.Struct({
  requestId: Schema.String,
  targetEnvironmentId: EnvironmentId,
  operation: ThreadRelayOperation,
  input: Schema.Unknown,
  caller: ThreadRelayCaller,
  timeoutMs: Schema.Number,
});
export type ThreadRelayRequest = typeof ThreadRelayRequest.Type;

export const ThreadRelayStreamEvent = Schema.Union([
  Schema.Struct({ type: Schema.Literal("connected"), connectionId: Schema.String }),
  Schema.Struct({
    type: Schema.Literal("request"),
    connectionId: Schema.String,
    request: ThreadRelayRequest,
  }),
]);
export type ThreadRelayStreamEvent = typeof ThreadRelayStreamEvent.Type;

export const ThreadRelayResponse = Schema.Struct({
  clientId: Schema.String,
  connectionId: Schema.String,
  requestId: Schema.String,
  ok: Schema.Boolean,
  result: Schema.optional(Schema.Unknown),
  error: Schema.optional(Schema.Struct({ _tag: Schema.String, message: Schema.String })),
});
export type ThreadRelayResponse = typeof ThreadRelayResponse.Type;

/** What the app sends to the target environment. */
export const ThreadRelayInvokeInput = Schema.Struct({
  operation: ThreadRelayOperation,
  input: Schema.Unknown,
  caller: ThreadRelayCaller,
});
export type ThreadRelayInvokeInput = typeof ThreadRelayInvokeInput.Type;

/** The OrchestratorMcp result of the operation, as returned by the target environment. */
export const ThreadRelayInvokeResult = Schema.Unknown;
export type ThreadRelayInvokeResult = typeof ThreadRelayInvokeResult.Type;

export class ThreadRelayEnvironmentUnreachableError extends Schema.TaggedError<ThreadRelayEnvironmentUnreachableError>()(
  "ThreadRelayEnvironmentUnreachableError",
  {
    environmentId: EnvironmentId,
  },
) {
  override get message(): string {
    return `Environment ${this.environmentId} is not reachable. Open T3 Code on a device connected to it, with thread relay enabled.`;
  }
}

export const ThreadRelayErrorReason = Schema.Literals([
  "timeout",
  "host_disconnected",
  /** The target environment never accepted the app's relay connection. */
  "unreachable",
  "forbidden",
  "not_found",
  "invalid_input",
  "failed",
]);
export type ThreadRelayErrorReason = typeof ThreadRelayErrorReason.Type;

export class ThreadRelayError extends Schema.TaggedError<ThreadRelayError>()("ThreadRelayError", {
  reason: ThreadRelayErrorReason,
  message: Schema.String,
}) {}
