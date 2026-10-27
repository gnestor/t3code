import {
  type AuthSessionId,
  type EnvironmentId,
  OrchestratorMcpFailure,
  ThreadRelayEnvironmentUnreachableError,
  ThreadRelayError,
  ThreadRelayErrorReason,
  type ThreadRelayCaller,
  type ThreadRelayHost,
  type ThreadRelayOperation,
  type ThreadRelayResponse,
  type ThreadRelayStreamEvent,
  type ThreadRelayUpdateHostInput,
} from "@t3tools/contracts";
import * as Context from "effect/Context";
import type * as Cause from "effect/Cause";
import * as Crypto from "effect/Crypto";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Queue from "effect/Queue";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";
import * as SynchronizedRef from "effect/SynchronizedRef";

import * as McpInvocationContext from "./McpInvocationContext.ts";
import { readCaller } from "./threadAccess.ts";

/** The host's own timer starts on delivery; leave room for its timeout response. */
const HOST_RESPONSE_GRACE_MS = 1_000;
const REQUEST_TIMEOUT_MS = 30_000;
const DEFAULT_WAIT_TIMEOUT_MS = 10 * 60 * 1_000;
const MAX_WAIT_TIMEOUT_MS = 60 * 60 * 1_000;

export interface ThreadRelayInvokeOptions {
  readonly targetEnvironmentId: EnvironmentId;
  readonly operation: ThreadRelayOperation;
  readonly input: unknown;
  readonly caller: ThreadRelayCaller;
  /** How long the host may take; the broker adds a grace period before giving up. */
  readonly timeoutMs?: number;
}

export class ThreadRelayBroker extends Context.Service<
  ThreadRelayBroker,
  {
    /** Hosts belong to the session that registered them; only that session can update or answer. */
    readonly connect: (
      sessionId: AuthSessionId,
      host: ThreadRelayHost,
    ) => Effect.Effect<Stream.Stream<ThreadRelayStreamEvent>>;
    readonly updateHost: (
      sessionId: AuthSessionId,
      input: ThreadRelayUpdateHostInput,
    ) => Effect.Effect<void>;
    readonly respond: (
      sessionId: AuthSessionId,
      response: ThreadRelayResponse,
    ) => Effect.Effect<void>;
    readonly invoke: <A = unknown>(
      request: ThreadRelayInvokeOptions,
    ) => Effect.Effect<A, ThreadRelayEnvironmentUnreachableError | ThreadRelayError>;
  }
>()("t3/mcp/ThreadRelayBroker") {}

interface HostConnection {
  readonly hostKey: string;
  readonly sessionId: AuthSessionId;
  readonly clientId: string;
  readonly connectionId: string;
  readonly reachableEnvironmentIds: ReadonlySet<EnvironmentId>;
  /** Higher is more recently connected or updated. */
  readonly order: number;
  readonly queue: Queue.Queue<ThreadRelayStreamEvent, Cause.Done>;
}

interface PendingRequest {
  readonly queue: HostConnection["queue"];
  readonly sessionId: AuthSessionId;
  readonly clientId: string;
  readonly connectionId: string;
  readonly deferred: Deferred.Deferred<unknown, ThreadRelayError>;
}

type RelayRoute =
  | { readonly _tag: "unreachable" }
  | { readonly _tag: "closed" }
  | { readonly _tag: "sent"; readonly requestId: string };

interface BrokerState {
  readonly hosts: ReadonlyMap<string, HostConnection>;
  readonly pending: ReadonlyMap<string, PendingRequest>;
  readonly orderSequence: number;
  readonly requestSequence: number;
}

const hostDisconnected = () =>
  new ThreadRelayError({
    reason: "host_disconnected",
    message: "The T3 Code app relaying this request disconnected before it answered.",
  });

const isErrorReason = Schema.is(ThreadRelayErrorReason);

/** Hosts report `_tag` as the error reason; anything else is a plain failure. */
const classifyResponseError = (error: NonNullable<ThreadRelayResponse["error"]>) =>
  new ThreadRelayError({
    reason: isErrorReason(error._tag) ? error._tag : "failed",
    message: error.message,
  });

/**
 * `wait` carries its own timeout, which the target enforces and answers with `timedOut`. The
 * host's timer starts earlier and covers the same wait, so it gets a request's worth of slack to
 * let that answer through instead of failing first.
 */
const relayTimeoutMs = (operation: ThreadRelayOperation, input: unknown): number => {
  if (operation !== "wait") return REQUEST_TIMEOUT_MS;
  const requested =
    typeof input === "object" &&
    input !== null &&
    "timeoutMs" in input &&
    typeof input.timeoutMs === "number"
      ? input.timeoutMs
      : undefined;
  const waitMs = Math.min(MAX_WAIT_TIMEOUT_MS, Math.max(1, requested ?? DEFAULT_WAIT_TIMEOUT_MS));
  return waitMs + REQUEST_TIMEOUT_MS;
};

/** Two sessions may reuse a clientId, so a session can never displace another session's host. */
const hostKeyOf = (sessionId: AuthSessionId, clientId: string) => `${sessionId}\u0000${clientId}`;

const withoutConnection = (
  current: BrokerState,
  hostKey: string,
  queue: HostConnection["queue"],
): { readonly state: BrokerState; readonly disconnected: ReadonlyArray<PendingRequest> } => {
  const hosts = new Map(current.hosts);
  const pending = new Map(current.pending);
  const disconnected: PendingRequest[] = [];
  if (current.hosts.get(hostKey)?.queue === queue) hosts.delete(hostKey);
  for (const [requestId, entry] of pending) {
    if (entry.queue !== queue) continue;
    pending.delete(requestId);
    disconnected.push(entry);
  }
  return { state: { ...current, hosts, pending }, disconnected };
};

export const make = Effect.gen(function* ThreadRelayBrokerMake() {
  const crypto = yield* Crypto.Crypto;
  const state = yield* SynchronizedRef.make<BrokerState>({
    hosts: new Map(),
    pending: new Map(),
    orderSequence: 0,
    requestSequence: 0,
  });

  const closeConnection = Effect.fn("ThreadRelayBroker.closeConnection")(function* (
    queue: HostConnection["queue"],
    disconnected: ReadonlyArray<PendingRequest>,
  ) {
    yield* Queue.shutdown(queue);
    yield* Effect.forEach(
      disconnected,
      ({ deferred }) => Deferred.fail(deferred, hostDisconnected()),
      {
        discard: true,
      },
    );
  });

  const disconnect = Effect.fn("ThreadRelayBroker.disconnect")(function* (
    hostKey: string,
    queue: HostConnection["queue"],
  ) {
    yield* SynchronizedRef.modifyEffect(state, (current) => {
      if (current.hosts.get(hostKey)?.queue !== queue) {
        return Effect.succeed([undefined, current] as const);
      }
      const removed = withoutConnection(current, hostKey, queue);
      return closeConnection(queue, removed.disconnected).pipe(
        Effect.as([undefined, removed.state] as const),
      );
    });
  });

  const acquireConnection = Effect.fn("ThreadRelayBroker.acquireConnection")(function* (
    sessionId: AuthSessionId,
    host: ThreadRelayHost,
  ) {
    const hostKey = hostKeyOf(sessionId, host.clientId);
    const queue = yield* Queue.unbounded<ThreadRelayStreamEvent, Cause.Done>();
    const connectionId = yield* crypto.randomUUIDv4().pipe(Effect.orDie);
    yield* Queue.offer(queue, { type: "connected", connectionId });
    const registration = yield* SynchronizedRef.modify(state, (current) => {
      const previous = current.hosts.get(hostKey);
      const removed = previous
        ? withoutConnection(current, hostKey, previous.queue)
        : { state: current, disconnected: [] };
      const hosts = new Map(removed.state.hosts);
      const orderSequence = removed.state.orderSequence + 1;
      const connection: HostConnection = {
        hostKey,
        sessionId,
        clientId: host.clientId,
        connectionId,
        reachableEnvironmentIds: new Set(host.reachableEnvironmentIds),
        order: orderSequence,
        queue,
      };
      hosts.set(hostKey, connection);
      return [
        { previous, disconnected: removed.disconnected, connection },
        { ...removed.state, hosts, orderSequence },
      ] as const;
    });
    // A reconnecting app replaces its older stream, which must not displace the new one.
    if (registration.previous) {
      yield* closeConnection(registration.previous.queue, registration.disconnected);
    }
    return registration.connection;
  });

  const connect: ThreadRelayBroker["Service"]["connect"] = Effect.fn("ThreadRelayBroker.connect")(
    (sessionId, host) =>
      Effect.succeed(
        Stream.unwrap(
          Effect.acquireRelease(acquireConnection(sessionId, host), (connection) =>
            disconnect(connection.hostKey, connection.queue),
          ).pipe(Effect.map((connection) => Stream.fromQueue(connection.queue))),
        ),
      ),
  );

  const updateHost: ThreadRelayBroker["Service"]["updateHost"] = Effect.fn(
    "ThreadRelayBroker.updateHost",
  )(function* (sessionId, input) {
    // A stale connectionId means the stream already ended; the app reconnects on its own.
    yield* SynchronizedRef.update(state, (current) => {
      const host = Array.from(current.hosts.values()).find(
        (candidate) =>
          candidate.connectionId === input.connectionId && candidate.sessionId === sessionId,
      );
      if (!host) return current;
      const hosts = new Map(current.hosts);
      const orderSequence = current.orderSequence + 1;
      hosts.set(host.hostKey, {
        ...host,
        reachableEnvironmentIds: new Set(input.reachableEnvironmentIds),
        order: orderSequence,
      });
      return { ...current, hosts, orderSequence };
    });
  });

  const respond: ThreadRelayBroker["Service"]["respond"] = Effect.fn("ThreadRelayBroker.respond")(
    function* (sessionId, response) {
      const entry = yield* SynchronizedRef.modify(state, (current) => {
        const found = current.pending.get(response.requestId);
        if (
          !found ||
          found.sessionId !== sessionId ||
          found.clientId !== response.clientId ||
          found.connectionId !== response.connectionId
        ) {
          return [undefined, current] as const;
        }
        const pending = new Map(current.pending);
        pending.delete(response.requestId);
        return [found, { ...current, pending }] as const;
      });
      if (!entry) return;
      if (response.ok) {
        yield* Deferred.succeed(entry.deferred, response.result);
      } else {
        yield* Deferred.fail(
          entry.deferred,
          response.error
            ? classifyResponseError(response.error)
            : new ThreadRelayError({
                reason: "failed",
                message: "The relaying app reported a failure without details.",
              }),
        );
      }
    },
  );

  const invoke = Effect.fn("ThreadRelayBroker.invoke")(function* <A = unknown>(
    options: ThreadRelayInvokeOptions,
  ): Effect.fn.Return<A, ThreadRelayEnvironmentUnreachableError | ThreadRelayError> {
    const deferred = yield* Deferred.make<unknown, ThreadRelayError>();
    const timeoutMs = options.timeoutMs ?? relayTimeoutMs(options.operation, options.input);
    const route = yield* SynchronizedRef.modifyEffect<BrokerState, RelayRoute, never, never>(
      state,
      (current) => {
        const host = Array.from(current.hosts.values())
          .filter((candidate) => candidate.reachableEnvironmentIds.has(options.targetEnvironmentId))
          .reduce<HostConnection | undefined>(
            (best, candidate) =>
              best === undefined || candidate.order > best.order ? candidate : best,
            undefined,
          );
        if (!host) return Effect.succeed([{ _tag: "unreachable" }, current] as const);
        const requestId = `thread-relay-${current.requestSequence}`;
        const pending = new Map(current.pending);
        pending.set(requestId, {
          queue: host.queue,
          sessionId: host.sessionId,
          clientId: host.clientId,
          connectionId: host.connectionId,
          deferred,
        });
        const next = { ...current, pending, requestSequence: current.requestSequence + 1 };
        // Offering under the lock keeps the host from being closed between choosing and sending.
        return Queue.offer(host.queue, {
          type: "request",
          connectionId: host.connectionId,
          request: {
            requestId,
            targetEnvironmentId: options.targetEnvironmentId,
            operation: options.operation,
            input: options.input,
            caller: options.caller,
            timeoutMs,
          },
        }).pipe(
          Effect.map((offered) =>
            offered
              ? ([{ _tag: "sent", requestId }, next] as const)
              : ([{ _tag: "closed" }, current] as const),
          ),
        );
      },
    );
    if (route._tag === "unreachable") {
      return yield* new ThreadRelayEnvironmentUnreachableError({
        environmentId: options.targetEnvironmentId,
      });
    }
    if (route._tag === "closed") return yield* hostDisconnected();
    const { requestId } = route;
    const removePending = SynchronizedRef.update(state, (current) => {
      if (!current.pending.has(requestId)) return current;
      const pending = new Map(current.pending);
      pending.delete(requestId);
      return { ...current, pending };
    });
    const result = yield* Deferred.await(deferred).pipe(
      Effect.timeoutOption(timeoutMs + HOST_RESPONSE_GRACE_MS),
      Effect.ensuring(removePending),
    );
    return yield* Option.match(result, {
      onNone: () =>
        Effect.fail(
          new ThreadRelayError({
            reason: "timeout",
            message: `Environment ${options.targetEnvironmentId} did not answer the ${options.operation} request within ${timeoutMs}ms.`,
          }),
        ),
      onSome: (value) => Effect.succeed(value as A),
    });
  });

  return ThreadRelayBroker.of({ connect, updateHost, respond, invoke });
}).pipe(Effect.withSpan("ThreadRelayBroker.make"));

export const layer = Layer.effect(ThreadRelayBroker, make);

const failureCode = (reason: ThreadRelayErrorReason): OrchestratorMcpFailure["code"] => {
  switch (reason) {
    case "not_found":
      return "thread_not_found";
    case "forbidden":
      return "capability_denied";
    case "invalid_input":
      return "invalid_request";
    default:
      return "orchestration_error";
  }
};

/**
 * Runs `local` unless the tool input names another environment, in which case the call goes
 * through a connected app as the calling thread. The target enforces the caller's limits.
 */
export const routeThreadTool = Effect.fn("ThreadRelayBroker.routeThreadTool")(function* <A, E, R>(
  operation: ThreadRelayOperation,
  input: { readonly environmentId?: EnvironmentId | undefined },
  local: Effect.Effect<A, E, R>,
) {
  const scope = yield* McpInvocationContext.McpInvocationContext;
  const targetEnvironmentId = input.environmentId;
  if (targetEnvironmentId === undefined || targetEnvironmentId === scope.environmentId) {
    return yield* local;
  }
  const { caller: shell, limits } = yield* readCaller();
  if (shell === undefined) {
    return yield* new OrchestratorMcpFailure({
      code: "thread_credential_required",
      message:
        "Reaching a thread on another environment acts as the calling T3 thread, so it needs an agent running inside T3 Code.",
    });
  }
  const broker = yield* ThreadRelayBroker;
  return yield* broker
    .invoke<A>({
      targetEnvironmentId,
      operation,
      input,
      caller: {
        environmentId: scope.environmentId,
        threadId: shell.id,
        title: shell.title,
        runtimeMode: limits.runtimeMode,
        interactionMode: limits.interactionMode,
        readOnly: scope.client?.access === "read-only",
      },
    })
    .pipe(
      Effect.mapError(
        (error) =>
          new OrchestratorMcpFailure({
            code:
              error._tag === "ThreadRelayError" ? failureCode(error.reason) : "orchestration_error",
            message: error.message,
          }),
      ),
    );
});
