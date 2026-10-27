import * as NodeServices from "@effect/platform-node/NodeServices";
import { expect, it } from "@effect/vitest";
import {
  AuthSessionId,
  EnvironmentId,
  OrchestratorMcpFailure,
  ProviderInstanceId,
  ThreadId,
  ThreadRelayEnvironmentUnreachableError,
  ThreadRelayError,
  type ThreadRelayCaller,
  type ThreadRelayHost,
  type ThreadRelayRequest,
} from "@t3tools/contracts";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as Queue from "effect/Queue";
import * as Stream from "effect/Stream";
import { TestClock } from "effect/testing";

import * as McpInvocationContext from "./McpInvocationContext.ts";
import { liveThreadsLayer } from "./McpToolAccess.testkit.ts";
import * as ThreadRelayBroker from "./ThreadRelayBroker.ts";

const makeBroker = ThreadRelayBroker.make.pipe(Effect.provide(NodeServices.layer));

const localEnvironment = EnvironmentId.make("environment-local");
const remoteEnvironment = EnvironmentId.make("environment-remote");
const otherEnvironment = EnvironmentId.make("environment-other");

const session = AuthSessionId.make("session-1");
const otherSession = AuthSessionId.make("session-2");

const caller: ThreadRelayCaller = {
  environmentId: localEnvironment,
  threadId: ThreadId.make("thread-1"),
  title: "Caller",
  runtimeMode: "full-access",
  interactionMode: "default",
  readOnly: false,
};

const host = (clientId: string, reachable: ReadonlyArray<EnvironmentId>): ThreadRelayHost => ({
  clientId,
  reachableEnvironmentIds: reachable,
});

type RoutedRequest = ThreadRelayRequest & { readonly connectionId: string };

const connectHost = Effect.fn("test.connectHost")(function* (
  broker: ThreadRelayBroker.ThreadRelayBroker["Service"],
  registration: ThreadRelayHost,
  sessionId: AuthSessionId = session,
) {
  const events = yield* broker.connect(sessionId, registration);
  const requests = yield* Queue.unbounded<RoutedRequest>();
  const connected = yield* Deferred.make<string>();
  const fiber = yield* Stream.runForEach(events, (event) =>
    event.type === "connected"
      ? Deferred.succeed(connected, event.connectionId)
      : Queue.offer(requests, { ...event.request, connectionId: event.connectionId }),
  ).pipe(Effect.forkScoped);
  const connectionId = yield* Deferred.await(connected);
  const reply = (request: RoutedRequest, result: unknown) =>
    broker.respond(sessionId, {
      clientId: registration.clientId,
      connectionId: request.connectionId,
      requestId: request.requestId,
      ok: true,
      result,
    });
  return { connectionId, requests, fiber, reply };
});

const invokeRead = (
  broker: ThreadRelayBroker.ThreadRelayBroker["Service"],
  targetEnvironmentId: EnvironmentId,
) =>
  broker.invoke<void>({
    targetEnvironmentId,
    operation: "read",
    input: { threadId: "thread-remote" },
    caller,
  });

it.effect("routes a request to a host that reaches the target environment", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const broker = yield* makeBroker;
      const toRemote = yield* connectHost(broker, host("app-remote", [remoteEnvironment]));
      const toOther = yield* connectHost(broker, host("app-other", [otherEnvironment]));

      const pending = yield* invokeRead(broker, remoteEnvironment).pipe(Effect.forkScoped);
      const request = yield* Queue.take(toRemote.requests);
      yield* toRemote.reply(request, { ok: "remote" });

      expect(yield* Fiber.join(pending)).toEqual({ ok: "remote" });
      expect(request).toMatchObject({
        targetEnvironmentId: remoteEnvironment,
        operation: "read",
        input: { threadId: "thread-remote" },
        caller,
        timeoutMs: 30_000,
      });
      expect(Queue.sizeUnsafe(toOther.requests)).toBe(0);
    }),
  ).pipe(Effect.provide(NodeServices.layer)),
);

it.effect("fails with the unreachable error when no host reaches the environment", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const broker = yield* makeBroker;
      const none = yield* invokeRead(broker, remoteEnvironment).pipe(Effect.flip);
      expect(none).toBeInstanceOf(ThreadRelayEnvironmentUnreachableError);

      yield* connectHost(broker, host("app-other", [otherEnvironment]));
      const wrongHost = yield* invokeRead(broker, remoteEnvironment).pipe(Effect.flip);
      expect(wrongHost).toBeInstanceOf(ThreadRelayEnvironmentUnreachableError);
      expect(wrongHost.message).toContain(remoteEnvironment);
    }),
  ),
);

it.effect("fails pending requests when their host disconnects", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const broker = yield* makeBroker;
      const app = yield* connectHost(broker, host("app", [remoteEnvironment]));

      const pending = yield* invokeRead(broker, remoteEnvironment).pipe(
        Effect.flip,
        Effect.forkScoped,
      );
      yield* Queue.take(app.requests);
      yield* Fiber.interrupt(app.fiber);

      const error = yield* Fiber.join(pending);
      expect(error).toBeInstanceOf(ThreadRelayError);
      expect(error).toMatchObject({ reason: "host_disconnected" });
      const after = yield* invokeRead(broker, remoteEnvironment).pipe(Effect.flip);
      expect(after).toBeInstanceOf(ThreadRelayEnvironmentUnreachableError);
    }),
  ),
);

it.effect("prefers the most recently connected or updated host", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const broker = yield* makeBroker;
      const first = yield* connectHost(broker, host("app-first", [remoteEnvironment]));
      const second = yield* connectHost(broker, host("app-second", [remoteEnvironment]));

      const viaSecond = yield* invokeRead(broker, remoteEnvironment).pipe(Effect.forkScoped);
      yield* second.reply(yield* Queue.take(second.requests), "second");
      expect(yield* Fiber.join(viaSecond)).toBe("second");

      yield* broker.updateHost(session, {
        connectionId: first.connectionId,
        reachableEnvironmentIds: [remoteEnvironment, otherEnvironment],
      });
      const viaFirst = yield* invokeRead(broker, remoteEnvironment).pipe(Effect.forkScoped);
      yield* first.reply(yield* Queue.take(first.requests), "first");
      expect(yield* Fiber.join(viaFirst)).toBe("first");

      yield* broker.updateHost(session, {
        connectionId: first.connectionId,
        reachableEnvironmentIds: [otherEnvironment],
      });
      const viaSecondAgain = yield* invokeRead(broker, remoteEnvironment).pipe(Effect.forkScoped);
      yield* second.reply(yield* Queue.take(second.requests), "second again");
      expect(yield* Fiber.join(viaSecondAgain)).toBe("second again");
    }),
  ),
);

it.effect("times out an unanswered request after its timeout plus grace", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const broker = yield* makeBroker;
      const app = yield* connectHost(broker, host("app", [remoteEnvironment]));

      const pending = yield* invokeRead(broker, remoteEnvironment).pipe(
        Effect.flip,
        Effect.forkScoped,
      );
      yield* Queue.take(app.requests);
      yield* TestClock.adjust(30_000);
      expect(pending.pollUnsafe()).toBeUndefined();
      yield* TestClock.adjust(1_000);

      expect(yield* Fiber.join(pending)).toMatchObject({ reason: "timeout" });
    }),
  ),
);

it.effect("gives the host more time than the caller's wait so the target can answer timedOut", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const broker = yield* makeBroker;
      const app = yield* connectHost(broker, host("app", [remoteEnvironment]));

      const pending = yield* broker
        .invoke<{ readonly timedOut: boolean }>({
          targetEnvironmentId: remoteEnvironment,
          operation: "wait",
          input: { threadId: "thread-remote", timeoutMs: 120_000 },
          caller,
        })
        .pipe(Effect.forkScoped);
      const request = yield* Queue.take(app.requests);
      expect(request.timeoutMs).toBeGreaterThan(120_000);

      // The target's own wait expires first and its answer arrives after the wait itself.
      yield* TestClock.adjust(121_500);
      expect(pending.pollUnsafe()).toBeUndefined();
      yield* app.reply(request, { timedOut: true });

      expect(yield* Fiber.join(pending)).toEqual({ timedOut: true });
    }),
  ),
);

it.effect("fails a wait that no host answers once its timeout plus slack and grace pass", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const broker = yield* makeBroker;
      const app = yield* connectHost(broker, host("app", [remoteEnvironment]));

      const pending = yield* broker
        .invoke<void>({
          targetEnvironmentId: remoteEnvironment,
          operation: "wait",
          input: { threadId: "thread-remote", timeoutMs: 120_000 },
          caller,
        })
        .pipe(Effect.flip, Effect.forkScoped);
      const request = yield* Queue.take(app.requests);

      yield* TestClock.adjust(request.timeoutMs);
      expect(pending.pollUnsafe()).toBeUndefined();
      yield* TestClock.adjust(1_000);
      expect(yield* Fiber.join(pending)).toMatchObject({ reason: "timeout" });
    }),
  ),
);

it.effect("maps failed responses to typed relay errors", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const broker = yield* makeBroker;
      const app = yield* connectHost(broker, host("app", [remoteEnvironment]));

      const respondWith = (tag: string) =>
        Effect.gen(function* () {
          const pending = yield* invokeRead(broker, remoteEnvironment).pipe(
            Effect.flip,
            Effect.forkScoped,
          );
          const request = yield* Queue.take(app.requests);
          yield* broker.respond(session, {
            clientId: "app",
            connectionId: request.connectionId,
            requestId: request.requestId,
            ok: false,
            error: { _tag: tag, message: "nope" },
          });
          return yield* Fiber.join(pending);
        });

      expect(yield* respondWith("not_found")).toMatchObject({
        reason: "not_found",
        message: "nope",
      });
      expect(yield* respondWith("SomethingElse")).toMatchObject({ reason: "failed" });
    }),
  ),
);

it.effect("ignores responses from a connection that did not receive the request", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const broker = yield* makeBroker;
      const app = yield* connectHost(broker, host("app", [remoteEnvironment]));

      const pending = yield* invokeRead(broker, remoteEnvironment).pipe(Effect.forkScoped);
      const request = yield* Queue.take(app.requests);
      yield* broker.respond(session, {
        clientId: "intruder",
        connectionId: request.connectionId,
        requestId: request.requestId,
        ok: true,
        result: "forged",
      });
      yield* app.reply(request, "genuine");

      expect(yield* Fiber.join(pending)).toBe("genuine");
    }),
  ),
);

it.effect("ignores responses from another session that knows the ids", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const broker = yield* makeBroker;
      const app = yield* connectHost(broker, host("app", [remoteEnvironment]));

      const pending = yield* invokeRead(broker, remoteEnvironment).pipe(Effect.forkScoped);
      const request = yield* Queue.take(app.requests);
      yield* broker.respond(otherSession, {
        clientId: "app",
        connectionId: request.connectionId,
        requestId: request.requestId,
        ok: true,
        result: "forged",
      });
      yield* app.reply(request, "genuine");

      expect(yield* Fiber.join(pending)).toBe("genuine");
    }),
  ),
);

it.effect("does not let another session replace or retarget a host with the same clientId", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const broker = yield* makeBroker;
      const app = yield* connectHost(broker, host("app", [remoteEnvironment]));
      const impostor = yield* connectHost(broker, host("app", [otherEnvironment]), otherSession);

      yield* broker.updateHost(otherSession, {
        connectionId: app.connectionId,
        reachableEnvironmentIds: [otherEnvironment],
      });

      const viaApp = yield* invokeRead(broker, remoteEnvironment).pipe(Effect.forkScoped);
      yield* app.reply(yield* Queue.take(app.requests), "genuine");
      expect(yield* Fiber.join(viaApp)).toBe("genuine");

      const viaImpostor = yield* invokeRead(broker, otherEnvironment).pipe(Effect.forkScoped);
      yield* impostor.reply(yield* Queue.take(impostor.requests), "own session");
      expect(yield* Fiber.join(viaImpostor)).toBe("own session");
    }),
  ),
);

const scopeFor = (client: McpInvocationContext.McpInvocationScope["client"] = undefined) =>
  ({
    environmentId: localEnvironment,
    capabilities: new Set(["orchestration"] as const),
    issuedAt: 1,
    requestNamespace: "provider-session-1",
    thread: {
      threadId: caller.threadId,
      providerSessionId: "provider-session-1",
      providerInstanceId: ProviderInstanceId.make("codex"),
    },
    client,
  }) satisfies McpInvocationContext.McpInvocationScope;

const routeWith = <A>(
  scope: McpInvocationContext.McpInvocationScope,
  input: { readonly environmentId?: EnvironmentId | undefined },
  local: Effect.Effect<A>,
  invoke: ThreadRelayBroker.ThreadRelayBroker["Service"]["invoke"],
) =>
  ThreadRelayBroker.routeThreadTool("read", input, local).pipe(
    Effect.provideService(McpInvocationContext.McpInvocationContext, scope),
    Effect.provide(
      Layer.mergeAll(
        liveThreadsLayer,
        Layer.succeed(
          ThreadRelayBroker.ThreadRelayBroker,
          ThreadRelayBroker.ThreadRelayBroker.of({
            connect: () => Effect.succeed(Stream.empty),
            updateHost: () => Effect.void,
            respond: () => Effect.void,
            invoke,
          }),
        ),
      ),
    ),
  );

it.effect("tools without an environmentId, or with this one, keep the local path", () =>
  Effect.gen(function* () {
    const relayed: unknown[] = [];
    const invoke: ThreadRelayBroker.ThreadRelayBroker["Service"]["invoke"] = (options) => {
      relayed.push(options);
      return Effect.succeed(undefined as never);
    };

    expect(yield* routeWith(scopeFor(), {}, Effect.succeed("local"), invoke)).toBe("local");
    expect(
      yield* routeWith(
        scopeFor(),
        { environmentId: localEnvironment },
        Effect.succeed("local"),
        invoke,
      ),
    ).toBe("local");
    expect(relayed).toEqual([]);
  }),
);

it.effect("tools naming another environment relay as the calling thread", () =>
  Effect.gen(function* () {
    const relayed: ThreadRelayBroker.ThreadRelayInvokeOptions[] = [];
    const invoke: ThreadRelayBroker.ThreadRelayBroker["Service"]["invoke"] = (options) => {
      relayed.push(options);
      return Effect.succeed("remote" as never);
    };
    const input = { environmentId: remoteEnvironment, threadId: "thread-remote" };

    const result = yield* routeWith(
      scopeFor({ sessionId: "session", label: "client", access: "read-only" }),
      input,
      Effect.die("local path must not run"),
      invoke,
    );

    expect(result).toBe("remote");
    expect(relayed).toHaveLength(1);
    expect(relayed[0]).toMatchObject({
      targetEnvironmentId: remoteEnvironment,
      operation: "read",
      input,
      caller: {
        environmentId: localEnvironment,
        threadId: caller.threadId,
        title: caller.threadId,
        runtimeMode: "full-access",
        interactionMode: "default",
        readOnly: true,
      },
    });
  }),
);

it.effect("surfaces an unreachable environment as an MCP failure naming it", () =>
  Effect.gen(function* () {
    const error = yield* routeWith(
      scopeFor(),
      { environmentId: remoteEnvironment },
      Effect.die("local path must not run"),
      (options) =>
        Effect.fail(
          new ThreadRelayEnvironmentUnreachableError({
            environmentId: options.targetEnvironmentId,
          }),
        ),
    ).pipe(Effect.flip);

    expect(error).toBeInstanceOf(OrchestratorMcpFailure);
    expect(error).toMatchObject({ code: "orchestration_error" });
    expect(error.message).toContain(remoteEnvironment);
    expect(error.message).toContain("Open T3 Code");
  }),
);

it.effect("maps relay error reasons to MCP failure codes", () =>
  Effect.gen(function* () {
    const failWith = (reason: ThreadRelayError["reason"]) =>
      routeWith(
        scopeFor(),
        { environmentId: remoteEnvironment },
        Effect.die("local path must not run"),
        () => Effect.fail(new ThreadRelayError({ reason, message: `remote said ${reason}` })),
      ).pipe(Effect.flip);

    expect(yield* failWith("not_found")).toMatchObject({
      code: "thread_not_found",
      message: "remote said not_found",
    });
    expect(yield* failWith("forbidden")).toMatchObject({ code: "capability_denied" });
    expect(yield* failWith("invalid_input")).toMatchObject({ code: "invalid_request" });
    expect(yield* failWith("timeout")).toMatchObject({ code: "orchestration_error" });
  }),
);
