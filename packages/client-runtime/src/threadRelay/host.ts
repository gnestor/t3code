import {
  type EnvironmentAuthorizationError,
  type EnvironmentId,
  type ThreadRelayEnvironmentUnreachableError,
  type ThreadRelayError,
  type ThreadRelayErrorReason,
  type ThreadRelayHost,
  type ThreadRelayInvokeInput,
  type ThreadRelayRequest,
  type ThreadRelayResponse,
  type ThreadRelayStreamEvent,
  type ThreadRelayUpdateHostInput,
  WS_METHODS,
} from "@t3tools/contracts";
import * as Cause from "effect/Cause";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Ref from "effect/Ref";
import * as Scope from "effect/Scope";
import * as Semaphore from "effect/Semaphore";
import * as Stream from "effect/Stream";
import * as SubscriptionRef from "effect/SubscriptionRef";
import type * as RpcClientError from "effect/rpc/RpcClientError";

import * as EnvironmentRegistry from "../connection/registry.ts";
import {
  type EnvironmentRpcUnavailableError,
  requestGuarded,
  runStreamGuarded,
} from "../rpc/client.ts";

const RECONNECT_DELAY = "5 seconds";

export type ThreadRelayGatewayError =
  | ThreadRelayError
  | ThreadRelayEnvironmentUnreachableError
  | EnvironmentAuthorizationError
  | EnvironmentRegistry.EnvironmentNotRegisteredError
  | EnvironmentRpcUnavailableError
  | RpcClientError.RpcClientError;

/** What the host needs from the connected environments. Tests substitute this. */
export class ThreadRelayGateway extends Context.Service<
  ThreadRelayGateway,
  {
    /** Sorted ids of the enabled environments that are currently connected. */
    readonly connectedEnvironmentIds: Stream.Stream<ReadonlyArray<EnvironmentId>>;
    readonly connect: (
      environmentId: EnvironmentId,
      host: ThreadRelayHost,
    ) => Stream.Stream<ThreadRelayStreamEvent, ThreadRelayGatewayError>;
    readonly updateHost: (
      environmentId: EnvironmentId,
      input: ThreadRelayUpdateHostInput,
    ) => Effect.Effect<void, ThreadRelayGatewayError>;
    readonly respond: (
      environmentId: EnvironmentId,
      response: ThreadRelayResponse,
    ) => Effect.Effect<void, ThreadRelayGatewayError>;
    readonly invoke: (
      environmentId: EnvironmentId,
      input: ThreadRelayInvokeInput,
    ) => Effect.Effect<unknown, ThreadRelayGatewayError>;
  }
>()("@t3tools/client-runtime/threadRelay/host/ThreadRelayGateway") {}

const hasStringProperty = <K extends string>(
  value: unknown,
  key: K,
): value is { readonly [P in K]: string } =>
  typeof value === "object" &&
  value !== null &&
  key in value &&
  typeof (value as Record<string, unknown>)[key] === "string";

const THREAD_RELAY_ERROR_REASONS: ReadonlySet<string> = new Set<ThreadRelayErrorReason>([
  "timeout",
  "host_disconnected",
  "forbidden",
  "not_found",
  "invalid_input",
  "failed",
]);

/**
 * The response `_tag` is always a ThreadRelayErrorReason so the requesting environment can
 * rebuild a ThreadRelayError from it without knowing this app's error types.
 */
function threadRelayResponseError(error: unknown): {
  readonly _tag: ThreadRelayErrorReason;
  readonly message: string;
} {
  const message = hasStringProperty(error, "message")
    ? error.message
    : "The thread relay request failed.";
  if (
    hasStringProperty(error, "_tag") &&
    error._tag === "ThreadRelayError" &&
    hasStringProperty(error, "reason") &&
    THREAD_RELAY_ERROR_REASONS.has(error.reason)
  ) {
    return { _tag: error.reason as ThreadRelayErrorReason, message };
  }
  if (hasStringProperty(error, "_tag") && error._tag === "EnvironmentAuthorizationError") {
    return { _tag: "forbidden", message };
  }
  return { _tag: "failed", message };
}

const sameIds = (a: ReadonlyArray<EnvironmentId>, b: ReadonlyArray<EnvironmentId>) =>
  a.length === b.length && a.every((id, index) => id === b[index]);

const failureResponse = (
  base: Pick<ThreadRelayResponse, "clientId" | "connectionId" | "requestId">,
  error: { readonly _tag: string; readonly message: string },
): ThreadRelayResponse => ({ ...base, ok: false, error });

/** The environment sending a request is untrusted: it may only aim at what this app reported. */
const rejectionFor = (input: {
  readonly hostEnvironmentId: EnvironmentId;
  readonly connectionId: string;
  readonly activeConnectionId: Option.Option<string>;
  readonly reachable: ReadonlyArray<EnvironmentId>;
  readonly request: ThreadRelayRequest;
}): string | undefined => {
  if (
    Option.isNone(input.activeConnectionId) ||
    input.activeConnectionId.value !== input.connectionId
  ) {
    return "The request does not belong to this app's relay connection.";
  }
  const target = input.request.targetEnvironmentId;
  if (target === input.hostEnvironmentId || !input.reachable.includes(target)) {
    return `Environment ${target} is not reachable through this app from ${input.hostEnvironmentId}.`;
  }
  return undefined;
};

const handleRequest = Effect.fn("ThreadRelayHost.handleRequest")(function* (input: {
  readonly clientId: string;
  readonly hostEnvironmentId: EnvironmentId;
  readonly connectionId: string;
  readonly activeConnectionId: Option.Option<string>;
  readonly reachable: ReadonlyArray<EnvironmentId>;
  readonly request: ThreadRelayRequest;
}) {
  const gateway = yield* ThreadRelayGateway;
  const { request } = input;
  const base = {
    clientId: input.clientId,
    connectionId: input.connectionId,
    requestId: request.requestId,
  };
  const rejection = rejectionFor(input);
  const response: ThreadRelayResponse =
    rejection !== undefined
      ? failureResponse(base, { _tag: "forbidden", message: rejection })
      : yield* gateway
          .invoke(request.targetEnvironmentId, {
            operation: request.operation,
            input: request.input,
            // The caller is always a thread of the environment this request arrived from.
            caller: { ...request.caller, environmentId: input.hostEnvironmentId },
          })
          .pipe(
            Effect.timeoutOption(request.timeoutMs),
            Effect.map(
              Option.match({
                onNone: () =>
                  failureResponse(base, {
                    _tag: "timeout",
                    message: `Environment ${request.targetEnvironmentId} did not answer in time.`,
                  }),
                onSome: (result): ThreadRelayResponse => ({ ...base, ok: true, result }),
              }),
            ),
            Effect.catch((error) =>
              Effect.succeed(failureResponse(base, threadRelayResponseError(error))),
            ),
            Effect.catchDefect((defect) =>
              Effect.succeed(failureResponse(base, threadRelayResponseError(defect))),
            ),
          );
  yield* gateway.respond(input.hostEnvironmentId, response).pipe(
    Effect.catch((error) =>
      Effect.logWarning("Could not return a thread relay response.", {
        environmentId: input.hostEnvironmentId,
        requestId: request.requestId,
        error,
      }),
    ),
  );
});

const connectOnce = Effect.fn("ThreadRelayHost.connectOnce")(function* (input: {
  readonly clientId: string;
  readonly environmentId: EnvironmentId;
  readonly reachable: SubscriptionRef.SubscriptionRef<ReadonlyArray<EnvironmentId>>;
}) {
  const gateway = yield* ThreadRelayGateway;
  const initial = yield* SubscriptionRef.get(input.reachable);
  const connection = yield* Ref.make(
    Option.none<{ readonly id: string; readonly reported: ReadonlyArray<EnvironmentId> }>(),
  );
  const reportLock = yield* Semaphore.make(1);

  const report = (ids: ReadonlyArray<EnvironmentId>) =>
    Semaphore.withPermits(
      reportLock,
      1,
      Effect.gen(function* () {
        const current = yield* Ref.get(connection);
        if (Option.isNone(current) || sameIds(current.value.reported, ids)) return;
        yield* gateway.updateHost(input.environmentId, {
          connectionId: current.value.id,
          reachableEnvironmentIds: ids,
        });
        yield* Ref.set(connection, Option.some({ id: current.value.id, reported: ids }));
      }),
    );

  yield* SubscriptionRef.changes(input.reachable).pipe(
    Stream.runForEach(report),
    Effect.forkScoped,
  );

  yield* gateway
    .connect(input.environmentId, {
      clientId: input.clientId,
      reachableEnvironmentIds: initial,
    })
    .pipe(
      Stream.runForEach((event) => {
        if (event.type === "connected") {
          return Ref.set(
            connection,
            Option.some({ id: event.connectionId, reported: initial }),
          ).pipe(
            // Environments may have changed between reading `initial` and being connected.
            Effect.andThen(SubscriptionRef.get(input.reachable)),
            Effect.flatMap(report),
          );
        }
        return Effect.gen(function* () {
          const active = yield* Ref.get(connection);
          const reachable = yield* SubscriptionRef.get(input.reachable);
          yield* handleRequest({
            clientId: input.clientId,
            hostEnvironmentId: input.environmentId,
            connectionId: event.connectionId,
            activeConnectionId: Option.map(active, (current) => current.id),
            reachable,
            request: event.request,
          }).pipe(Effect.forkScoped);
        });
      }),
      Effect.ensuring(Ref.set(connection, Option.none())),
    );
});

const runEnvironmentConnection = (input: {
  readonly clientId: string;
  readonly environmentId: EnvironmentId;
  readonly reachable: SubscriptionRef.SubscriptionRef<ReadonlyArray<EnvironmentId>>;
}) =>
  Effect.scoped(connectOnce(input)).pipe(
    Effect.catch((error) =>
      Effect.logWarning("Thread relay connection ended.", {
        environmentId: input.environmentId,
        error,
      }),
    ),
    Effect.catchDefect((defect) =>
      Effect.logWarning("Thread relay connection failed.", {
        environmentId: input.environmentId,
        defect: Cause.pretty(Cause.die(defect)),
      }),
    ),
    Effect.andThen(Effect.sleep(RECONNECT_DELAY)),
    Effect.forever,
  );

/**
 * A host only matters when the app reaches at least two environments, so a connection is
 * opened for each environment only then, and reports the others as reachable through it.
 */
const manageConnections = (clientId: string) =>
  Effect.scoped(
    Effect.gen(function* () {
      const gateway = yield* ThreadRelayGateway;
      const scope = yield* Scope.Scope;
      const workers = new Map<
        EnvironmentId,
        {
          readonly reachable: SubscriptionRef.SubscriptionRef<ReadonlyArray<EnvironmentId>>;
          readonly fiber: Fiber.Fiber<never>;
        }
      >();

      yield* gateway.connectedEnvironmentIds.pipe(
        Stream.runForEach((ids) =>
          Effect.gen(function* () {
            const active = ids.length >= 2 ? ids : [];
            for (const [environmentId, worker] of workers) {
              if (!active.includes(environmentId)) {
                workers.delete(environmentId);
                yield* Fiber.interrupt(worker.fiber);
              }
            }
            for (const environmentId of active) {
              const others = active.filter((id) => id !== environmentId);
              const existing = workers.get(environmentId);
              if (existing !== undefined) {
                const current = yield* SubscriptionRef.get(existing.reachable);
                if (!sameIds(current, others)) {
                  yield* SubscriptionRef.set(existing.reachable, others);
                }
                continue;
              }
              const reachable = yield* SubscriptionRef.make<ReadonlyArray<EnvironmentId>>(others);
              const fiber = yield* runEnvironmentConnection({
                clientId,
                environmentId,
                reachable,
              }).pipe(Effect.forkIn(scope));
              workers.set(environmentId, { reachable, fiber });
            }
          }),
        ),
      );
    }),
  );

export interface ThreadRelayHostOptions {
  readonly clientId: string;
  /** Emits the current value of the "reach each other through this app" setting. */
  readonly enabled: Stream.Stream<boolean>;
}

/** Runs until interrupted. Nothing is opened while the setting is off. */
export const run = (options: ThreadRelayHostOptions) =>
  options.enabled.pipe(
    Stream.changes,
    Stream.switchMap((enabled) =>
      enabled ? Stream.fromEffect(manageConnections(options.clientId)) : Stream.empty,
    ),
    Stream.runDrain,
  );

type RegistryConnections = Pick<
  EnvironmentRegistry.EnvironmentRegistry["Service"],
  "entries" | "stateChanges"
>;

/**
 * Emits only once every enabled environment has reported a state, so adding or removing one
 * never shows a partial or empty set that would tear down the other environments' connections.
 */
export const connectedEnvironmentIdsOf = (
  registry: RegistryConnections,
): Stream.Stream<ReadonlyArray<EnvironmentId>> =>
  SubscriptionRef.changes(registry.entries).pipe(
    Stream.map((entries) =>
      [...entries.entries()]
        .filter(([, entry]) => entry.enabled)
        .map(([environmentId]) => environmentId)
        .sort(),
    ),
    Stream.changesWith(sameIds),
    Stream.switchMap((ids) =>
      ids.length === 0
        ? Stream.succeed<ReadonlyArray<EnvironmentId>>([])
        : Stream.zipLatestAll(
            ...ids.map((environmentId) =>
              registry.stateChanges(environmentId).pipe(
                Stream.map((state) => state.phase === "connected"),
                Stream.catch(() => Stream.make(false)),
              ),
            ),
          ).pipe(
            Stream.map((flags) => ids.filter((_, index) => flags[index] === true)),
            Stream.changesWith(sameIds),
          ),
    ),
  );

const layerGateway = Layer.effect(
  ThreadRelayGateway,
  Effect.gen(function* () {
    const registry = yield* EnvironmentRegistry.EnvironmentRegistry;
    const connectedEnvironmentIds = connectedEnvironmentIdsOf(registry);

    return ThreadRelayGateway.of({
      connectedEnvironmentIds,
      connect: (environmentId, host) =>
        registry.runStream(environmentId, runStreamGuarded(WS_METHODS.threadRelayConnect, host)),
      updateHost: (environmentId, input) =>
        registry.run(environmentId, requestGuarded(WS_METHODS.threadRelayUpdateHost, input)),
      respond: (environmentId, response) =>
        registry.run(environmentId, requestGuarded(WS_METHODS.threadRelayRespond, response)),
      invoke: (environmentId, input) =>
        registry.run(environmentId, requestGuarded(WS_METHODS.threadRelayInvoke, input)),
    });
  }),
);

/** Mount once per app; needs the connection layer's EnvironmentRegistry. */
export const layer = (options: ThreadRelayHostOptions) =>
  Layer.effectDiscard(run(options).pipe(Effect.forkScoped)).pipe(Layer.provide(layerGateway));
