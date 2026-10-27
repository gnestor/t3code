import {
  AuthOrchestrationOperateScope,
  type EnvironmentId,
  OrchestratorMcpFailure,
  OrchestratorMcpThreadListInput,
  OrchestratorMcpThreadReadInput,
  OrchestratorMcpThreadSearchInput,
  OrchestratorMcpThreadSendInput,
  OrchestratorMcpThreadWaitInput,
  ThreadRelayError,
  type ThreadRelayErrorReason,
  type ThreadRelayInvokeInput,
} from "@t3tools/contracts";
import { formatThreadLink, stampThreadLinkEnvironment } from "@t3tools/shared/threadLinks";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Ref from "effect/Ref";
import * as Schema from "effect/Schema";

import type { AuthenticatedSession } from "../auth/EnvironmentAuth.ts";
import * as ServerEnvironment from "../environment/ServerEnvironment.ts";
import {
  DispatchModeLimit,
  type DispatchModeRefusal,
} from "../orchestration-v2/DispatchModeLimit.ts";
import * as ThreadSearch from "../orchestration-v2/ThreadSearch.ts";
import { type McpInvocationScope, threadlessCallerLimits } from "./McpInvocationContext.ts";
import * as OrchestratorMcpService from "./OrchestratorMcpService.ts";

export interface ThreadRelayExecutionShape {
  /**
   * Runs a thread operation relayed from another environment. The caller has no thread here, so
   * its limits are the narrowest of the relayed thread's modes and what the session may do.
   */
  readonly invoke: (
    session: AuthenticatedSession,
    input: ThreadRelayInvokeInput,
  ) => Effect.Effect<unknown, ThreadRelayError>;
}

export class ThreadRelayExecution extends Context.Service<
  ThreadRelayExecution,
  ThreadRelayExecutionShape
>()("t3/mcp/ThreadRelayExecution") {}

const relayError = (reason: ThreadRelayErrorReason, message: string) =>
  new ThreadRelayError({ reason, message });

const reasonForFailure = (code: OrchestratorMcpFailure["code"]): ThreadRelayErrorReason => {
  switch (code) {
    case "thread_not_found":
    case "run_not_found":
    case "task_not_found":
      return "not_found";
    case "capability_denied":
    case "parent_not_active":
    case "runtime_mode_escalation_denied":
    case "interaction_mode_escalation_denied":
      return "forbidden";
    case "invalid_request":
    case "target_required":
      return "invalid_input";
    default:
      return "failed";
  }
};

const fromFailure = (error: OrchestratorMcpFailure) =>
  relayError(reasonForFailure(error.code), error.message);

const refusalError = (refusal: DispatchModeRefusal) =>
  relayError(
    "forbidden",
    `Thread ${refusal.threadId} now runs in ${refusal.runtimeMode}/${refusal.interactionMode} mode, above this caller's. Its user changed it while this call ran.`,
  );

const isThreadRelayError = Schema.is(ThreadRelayError);

const asRelayError = (error: OrchestratorMcpFailure | ThreadRelayError) =>
  isThreadRelayError(error) ? error : fromFailure(error);

const decodeInput =
  <S extends Schema.Decoder<unknown>>(schema: S) =>
  (input: unknown) =>
    Schema.decodeUnknownEffect(schema)(input).pipe(
      Effect.mapError((error) => relayError("invalid_input", error.message)),
    );

/** Links in a relayed result point at threads here, so they must name this environment. */
const stampLinks = (value: unknown, environmentId: EnvironmentId): unknown => {
  if (Array.isArray(value)) return value.map((item) => stampLinks(item, environmentId));
  if (typeof value !== "object" || value === null) return value;
  return Object.fromEntries(
    Object.entries(value).map(([key, entry]) => [
      key,
      key === "link" && typeof entry === "string"
        ? stampThreadLinkEnvironment(entry, environmentId)
        : stampLinks(entry, environmentId),
    ]),
  );
};

const make = Effect.gen(function* () {
  const orchestrator = yield* OrchestratorMcpService.OrchestratorMcpService;
  const threadSearch = yield* ThreadSearch.ThreadSearch;
  const environment = yield* ServerEnvironment.ServerEnvironment;

  // The orchestrator rechecks the target's modes under its lock, since its user can raise them after the preflight.
  const sendWithLimits = Effect.fn("ThreadRelayExecution.send")(function* (
    scope: McpInvocationScope,
    input: OrchestratorMcpThreadSendInput,
  ) {
    const refused = yield* Ref.make<DispatchModeRefusal | undefined>(undefined);
    return yield* orchestrator.sendToThread(scope, input).pipe(
      Effect.provideService(DispatchModeLimit, { ...threadlessCallerLimits(scope), refused }),
      Effect.catch((error) =>
        Ref.get(refused).pipe(
          Effect.flatMap((refusal) =>
            Effect.fail(refusal === undefined ? fromFailure(error) : refusalError(refusal)),
          ),
        ),
      ),
    );
  });

  const execute: ThreadRelayExecutionShape["invoke"] = Effect.fn("ThreadRelayExecution.execute")(
    function* (session, { operation, input, caller }) {
      // A read-only session or caller can look but not act.
      const readOnly = caller.readOnly || !session.scopes.includes(AuthOrchestrationOperateScope);
      if (operation === "send" && readOnly) {
        return yield* relayError(
          "forbidden",
          "This relayed caller is read-only, so it cannot send messages to threads.",
        );
      }
      const scope: McpInvocationScope = {
        environmentId: yield* environment.getEnvironmentId,
        requestNamespace: `relay:${session.sessionId}:${caller.environmentId}:${caller.threadId}`,
        thread: undefined,
        client: {
          sessionId: session.sessionId,
          label: `${caller.title} (${caller.environmentId})`,
          access: readOnly ? "read-only" : "full-access",
        },
        remote: {
          environmentId: caller.environmentId,
          threadId: caller.threadId,
          title: caller.title,
          runtimeMode: caller.runtimeMode,
          interactionMode: caller.interactionMode,
        },
        capabilities: new Set(["orchestration"]),
        issuedAt: 0,
      };

      switch (operation) {
        case "list":
          return yield* decodeInput(OrchestratorMcpThreadListInput)(input).pipe(
            Effect.flatMap((decoded) => orchestrator.listThreads(scope, decoded)),
            Effect.mapError(asRelayError),
          );
        case "read":
          return yield* decodeInput(OrchestratorMcpThreadReadInput)(input).pipe(
            Effect.flatMap((decoded) => orchestrator.readThread(scope, decoded)),
            Effect.mapError(asRelayError),
          );
        case "wait":
          return yield* decodeInput(OrchestratorMcpThreadWaitInput)(input).pipe(
            Effect.flatMap((decoded) => orchestrator.waitForThread(scope, decoded)),
            Effect.mapError(asRelayError),
          );
        case "search":
          return yield* decodeInput(OrchestratorMcpThreadSearchInput)(input).pipe(
            Effect.flatMap(({ environmentId: _environmentId, ...query }) =>
              threadSearch
                .searchThreads(query)
                .pipe(
                  Effect.mapError(() => relayError("failed", "The thread search could not run.")),
                ),
            ),
            Effect.map(({ matches }) => ({
              matches: matches.map((match) => ({
                ...match,
                link: formatThreadLink(match.threadId, match.title),
              })),
            })),
          );
        case "send":
          return yield* decodeInput(OrchestratorMcpThreadSendInput)(input).pipe(
            Effect.flatMap((decoded) => sendWithLimits(scope, decoded)),
          );
      }
    },
  );

  const invoke: ThreadRelayExecutionShape["invoke"] = Effect.fn("ThreadRelayExecution.invoke")(
    function* (session, request) {
      const result = yield* execute(session, request);
      return stampLinks(result, yield* environment.getEnvironmentId);
    },
  );

  return ThreadRelayExecution.of({ invoke });
});

export const layer = Layer.effect(ThreadRelayExecution, make);
