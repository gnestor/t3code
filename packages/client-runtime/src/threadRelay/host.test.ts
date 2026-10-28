import {
  AuthOrchestrationOperateScope,
  EnvironmentAuthorizationError,
  EnvironmentId,
  ThreadId,
  ThreadRelayError,
  type ThreadRelayCaller,
  type ThreadRelayHost,
  type ThreadRelayInvokeInput,
  type ThreadRelayRequest,
  type ThreadRelayResponse,
  type ThreadRelayStreamEvent,
  type ThreadRelayUpdateHostInput,
} from "@t3tools/contracts";
import { describe, expect, it } from "@effect/vitest";
import { vi } from "vite-plus/test";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Queue from "effect/Queue";
import * as Stream from "effect/Stream";
import * as SubscriptionRef from "effect/SubscriptionRef";
import * as TestClock from "effect/testing/TestClock";

import { AVAILABLE_CONNECTION_STATE, type SupervisorConnectionState } from "../connection/model.ts";
import { EnvironmentRpcUnavailableError } from "../rpc/client.ts";
import * as ThreadRelayHostModule from "./host.ts";

const envA = EnvironmentId.make("environment-a");
const envB = EnvironmentId.make("environment-b");
const envC = EnvironmentId.make("environment-c");

const caller: ThreadRelayCaller = {
  environmentId: envA,
  threadId: ThreadId.make("thread-caller"),
  title: "Caller",
  runtimeMode: "approval-required",
  interactionMode: "default",
  readOnly: false,
};

type GatewayCall =
  | {
      readonly type: "connect";
      readonly environmentId: EnvironmentId;
      readonly host: ThreadRelayHost;
    }
  | {
      readonly type: "updateHost";
      readonly environmentId: EnvironmentId;
      readonly input: ThreadRelayUpdateHostInput;
    }
  | {
      readonly type: "respond";
      readonly environmentId: EnvironmentId;
      readonly response: ThreadRelayResponse;
    };

const makeFixture = Effect.fn("makeFixture")(function* (options: {
  readonly initialConnected: ReadonlyArray<EnvironmentId>;
  readonly invoke?: (
    environmentId: EnvironmentId,
    input: ThreadRelayInvokeInput,
  ) => Effect.Effect<unknown, ThreadRelayHostModule.ThreadRelayGatewayError>;
}) {
  const connected = yield* SubscriptionRef.make(options.initialConnected);
  const calls = yield* Queue.unbounded<GatewayCall>();
  const acceptedChanges = yield* Queue.unbounded<readonly [EnvironmentId, boolean]>();
  const streams = new Map<EnvironmentId, Queue.Queue<ThreadRelayStreamEvent>>();
  for (const environmentId of [envA, envB, envC]) {
    streams.set(environmentId, yield* Queue.unbounded<ThreadRelayStreamEvent>());
  }
  const gateway = ThreadRelayHostModule.ThreadRelayGateway.of({
    connectedEnvironmentIds: SubscriptionRef.changes(connected),
    connect: (environmentId, host) =>
      Stream.fromEffect(Queue.offer(calls, { type: "connect", environmentId, host })).pipe(
        Stream.drain,
        Stream.concat(Stream.fromQueue(streams.get(environmentId)!)),
      ),
    updateHost: (environmentId, input) =>
      Queue.offer(calls, { type: "updateHost", environmentId, input }).pipe(Effect.asVoid),
    respond: (environmentId, response) =>
      Queue.offer(calls, { type: "respond", environmentId, response }).pipe(Effect.asVoid),
    invoke: options.invoke ?? (() => Effect.succeed(null)),
  });
  return {
    connected,
    calls,
    acceptedChanges,
    gateway: Layer.succeed(ThreadRelayHostModule.ThreadRelayGateway, gateway),
    send: (environmentId: EnvironmentId, event: ThreadRelayStreamEvent) =>
      Queue.offer(streams.get(environmentId)!, event),
    takeCalls: (count: number) => Queue.takeN(calls, count),
    /** The broker always announces the connection before it routes requests to it. */
    announce: (environmentId: EnvironmentId, connectionId: string) =>
      Queue.offer(streams.get(environmentId)!, { type: "connected", connectionId }).pipe(
        Effect.andThen(Queue.take(acceptedChanges)),
      ),
  };
});

const makeRequest = (overrides: Partial<ThreadRelayRequest> = {}): ThreadRelayRequest => ({
  requestId: "request-1",
  targetEnvironmentId: envB,
  operation: "read",
  input: { threadId: "thread-target" },
  caller,
  timeoutMs: 60_000,
  ...overrides,
});

const runHost = (
  fixture: {
    readonly gateway: Layer.Layer<ThreadRelayHostModule.ThreadRelayGateway>;
    readonly acceptedChanges: Queue.Queue<readonly [EnvironmentId, boolean]>;
  },
  enabled: Stream.Stream<boolean>,
) =>
  ThreadRelayHostModule.run({
    clientId: "client-1",
    enabled,
    onConnectedChange: (environmentId, connected) =>
      Queue.offerUnsafe(fixture.acceptedChanges, [environmentId, connected]),
  }).pipe(Effect.provide(fixture.gateway), Effect.forkScoped);

const byEnvironment = <T extends { readonly environmentId: EnvironmentId }>(
  calls: ReadonlyArray<T>,
) => [...calls].toSorted((left, right) => left.environmentId.localeCompare(right.environmentId));

describe("thread relay host", () => {
  it.effect("reports the other connected environments and re-reports when they change", () =>
    Effect.gen(function* () {
      const fixture = yield* makeFixture({ initialConnected: [envA, envB, envC] });
      yield* runHost(fixture, Stream.make(true));

      const opened = byEnvironment(
        (yield* fixture.takeCalls(3)).flatMap((call) => (call.type === "connect" ? [call] : [])),
      );
      expect(opened.map((call) => [call.environmentId, call.host])).toEqual([
        [envA, { clientId: "client-1", reachableEnvironmentIds: [envB, envC] }],
        [envB, { clientId: "client-1", reachableEnvironmentIds: [envA, envC] }],
        [envC, { clientId: "client-1", reachableEnvironmentIds: [envA, envB] }],
      ]);

      yield* fixture.send(envA, { type: "connected", connectionId: "connection-a" });
      yield* fixture.send(envB, { type: "connected", connectionId: "connection-b" });
      yield* SubscriptionRef.set(fixture.connected, [envA, envB]);

      const updates = byEnvironment(
        (yield* fixture.takeCalls(2)).flatMap((call) => (call.type === "updateHost" ? [call] : [])),
      );
      expect(updates.map((call) => [call.environmentId, call.input])).toEqual([
        [envA, { connectionId: "connection-a", reachableEnvironmentIds: [envB] }],
        [envB, { connectionId: "connection-b", reachableEnvironmentIds: [envA] }],
      ]);
    }).pipe(Effect.scoped),
  );

  it.effect("does not open a connection until two environments are connected", () =>
    Effect.gen(function* () {
      const fixture = yield* makeFixture({ initialConnected: [envA] });
      yield* runHost(fixture, Stream.make(true));
      yield* SubscriptionRef.set(fixture.connected, [envA, envB]);

      const opened = byEnvironment(
        (yield* fixture.takeCalls(2)).flatMap((call) => (call.type === "connect" ? [call] : [])),
      );
      expect(opened.map((call) => [call.environmentId, call.host.reachableEnvironmentIds])).toEqual(
        [
          [envA, [envB]],
          [envB, [envA]],
        ],
      );
    }).pipe(Effect.scoped),
  );

  it.effect("does nothing while the setting is off", () =>
    Effect.gen(function* () {
      const fixture = yield* makeFixture({ initialConnected: [envA, envB] });
      const enabled = yield* SubscriptionRef.make(false);
      yield* runHost(fixture, SubscriptionRef.changes(enabled));
      yield* SubscriptionRef.set(fixture.connected, [envA, envB, envC]);
      yield* SubscriptionRef.set(enabled, true);

      // Anything opened while the setting was off would arrive before these.
      const opened = byEnvironment(
        (yield* fixture.takeCalls(3)).flatMap((call) => (call.type === "connect" ? [call] : [])),
      );
      expect(opened.map((call) => [call.environmentId, call.host.reachableEnvironmentIds])).toEqual(
        [
          [envA, [envB, envC]],
          [envB, [envA, envC]],
          [envC, [envA, envB]],
        ],
      );
    }).pipe(Effect.scoped),
  );

  it.effect("tells the app which environments accepted the relay and when they leave it", () =>
    Effect.gen(function* () {
      const fixture = yield* makeFixture({ initialConnected: [envA, envB] });
      yield* runHost(fixture, Stream.make(true));
      yield* fixture.takeCalls(2);

      expect(yield* fixture.announce(envA, "connection-a")).toEqual([envA, true]);

      yield* SubscriptionRef.set(fixture.connected, [envA]);
      expect(
        byEnvironment(
          (yield* Queue.takeN(fixture.acceptedChanges, 2)).map(([environmentId, connected]) => ({
            environmentId,
            connected,
          })),
        ),
      ).toEqual([
        { environmentId: envA, connected: false },
        { environmentId: envB, connected: false },
      ]);
    }).pipe(Effect.scoped),
  );

  it.effect("invokes the target environment and responds with the result", () =>
    Effect.gen(function* () {
      const invoked = yield* Deferred.make<readonly [EnvironmentId, ThreadRelayInvokeInput]>();
      const fixture = yield* makeFixture({
        initialConnected: [envA, envB],
        invoke: (environmentId, input) =>
          Deferred.succeed(invoked, [environmentId, input] as const).pipe(
            Effect.as({ title: "Remote thread" }),
          ),
      });
      yield* runHost(fixture, Stream.make(true));
      yield* fixture.takeCalls(2);

      const request = makeRequest();
      yield* fixture.announce(envA, "connection-a");
      yield* fixture.announce(envB, "connection-b");
      yield* fixture.send(envA, { type: "request", connectionId: "connection-a", request });

      const [target, input] = yield* Deferred.await(invoked);
      expect(target).toBe(envB);
      expect(input).toEqual({ operation: "read", input: request.input, caller });
      const [respond] = yield* fixture.takeCalls(1);
      expect(respond).toEqual({
        type: "respond",
        environmentId: envA,
        response: {
          clientId: "client-1",
          connectionId: "connection-a",
          requestId: "request-1",
          ok: true,
          result: { title: "Remote thread" },
        },
      });
    }).pipe(Effect.scoped),
  );

  it.effect("refuses a target that never accepted the relay, like a server without it", () =>
    Effect.gen(function* () {
      const invoke = vi.fn(() => Effect.succeed(null));
      const fixture = yield* makeFixture({ initialConnected: [envA, envB], invoke });
      yield* runHost(fixture, Stream.make(true));
      yield* fixture.takeCalls(2);
      yield* fixture.announce(envA, "connection-a");
      yield* fixture.send(envA, {
        type: "request",
        connectionId: "connection-a",
        request: makeRequest(),
      });

      const [call] = yield* fixture.takeCalls(1);
      expect(call?.type === "respond" ? call.response.error?._tag : undefined).toBe("unreachable");
      expect(invoke).not.toHaveBeenCalled();
    }).pipe(Effect.scoped),
  );

  describe("untrusted requests", () => {
    const rejectedWith = Effect.fn("rejectedWith")(function* (input: {
      readonly request: Partial<ThreadRelayRequest>;
      readonly connectionId?: string;
    }) {
      const invoke = vi.fn(() => Effect.succeed(null));
      const fixture = yield* makeFixture({ initialConnected: [envA, envB, envC], invoke });
      yield* runHost(fixture, Stream.make(true));
      yield* fixture.takeCalls(3);
      yield* fixture.announce(envA, "connection-a");
      yield* fixture.announce(envB, "connection-b");
      yield* fixture.send(envA, {
        type: "request",
        connectionId: input.connectionId ?? "connection-a",
        request: makeRequest(input.request),
      });
      const [call] = yield* fixture.takeCalls(1);
      expect(invoke).not.toHaveBeenCalled();
      return call?.type === "respond" ? call.response : undefined;
    });

    it.effect("refuses to aim at the environment the request arrived from", () =>
      Effect.gen(function* () {
        const response = yield* rejectedWith({ request: { targetEnvironmentId: envA } });
        expect(response).toMatchObject({ ok: false, error: { _tag: "forbidden" } });
      }).pipe(Effect.scoped),
    );

    it.effect("refuses an environment this app did not report as reachable", () =>
      Effect.gen(function* () {
        const response = yield* rejectedWith({
          request: { targetEnvironmentId: EnvironmentId.make("environment-unknown") },
        });
        expect(response).toMatchObject({ ok: false, error: { _tag: "forbidden" } });
      }).pipe(Effect.scoped),
    );

    it.effect("refuses a request that names a different connection", () =>
      Effect.gen(function* () {
        const response = yield* rejectedWith({ request: {}, connectionId: "connection-other" });
        expect(response).toMatchObject({ ok: false, error: { _tag: "forbidden" } });
      }).pipe(Effect.scoped),
    );

    it.effect("stamps the caller with the environment the request arrived from", () =>
      Effect.gen(function* () {
        const invoked = yield* Deferred.make<ThreadRelayInvokeInput>();
        const fixture = yield* makeFixture({
          initialConnected: [envA, envB, envC],
          invoke: (_environmentId, input) => Deferred.succeed(invoked, input).pipe(Effect.as(null)),
        });
        yield* runHost(fixture, Stream.make(true));
        yield* fixture.takeCalls(3);
        yield* fixture.announce(envA, "connection-a");
        yield* fixture.announce(envB, "connection-b");
        yield* fixture.send(envA, {
          type: "request",
          connectionId: "connection-a",
          request: makeRequest({ caller: { ...caller, environmentId: envC } }),
        });

        expect((yield* Deferred.await(invoked)).caller.environmentId).toBe(envA);
      }).pipe(Effect.scoped),
    );
  });

  describe("error mapping", () => {
    const failWith = Effect.fn("failWith")(function* (
      error: ThreadRelayHostModule.ThreadRelayGatewayError,
    ) {
      const fixture = yield* makeFixture({
        initialConnected: [envA, envB],
        invoke: () => Effect.fail(error),
      });
      yield* runHost(fixture, Stream.make(true));
      yield* fixture.takeCalls(2);
      yield* fixture.announce(envA, "connection-a");
      yield* fixture.announce(envB, "connection-b");
      yield* fixture.send(envA, {
        type: "request",
        connectionId: "connection-a",
        request: makeRequest(),
      });
      const [call] = yield* fixture.takeCalls(1);
      return call?.type === "respond" ? call.response : undefined;
    });

    it.effect("keeps the reason of a relay error", () =>
      Effect.gen(function* () {
        const response = yield* failWith(
          new ThreadRelayError({ reason: "not_found", message: "No such thread." }),
        );
        expect(response).toMatchObject({
          ok: false,
          requestId: "request-1",
          error: { _tag: "not_found", message: "No such thread." },
        });
      }).pipe(Effect.scoped),
    );

    it.effect("reports authorization failures as forbidden", () =>
      Effect.gen(function* () {
        const response = yield* failWith(
          new EnvironmentAuthorizationError({
            message: "This connection requires orchestration:operate.",
            requiredScope: AuthOrchestrationOperateScope,
          }),
        );
        expect(response?.error).toEqual({
          _tag: "forbidden",
          message: "This connection requires orchestration:operate.",
        });
      }).pipe(Effect.scoped),
    );

    it.effect("reports anything else as failed", () =>
      Effect.gen(function* () {
        const response = yield* failWith(
          new EnvironmentRpcUnavailableError({
            environmentId: envB,
            message: "socket closed",
          }),
        );
        expect(response?.error).toEqual({ _tag: "failed", message: "socket closed" });
      }).pipe(Effect.scoped),
    );

    it.effect("reports a defect as failed", () =>
      Effect.gen(function* () {
        const fixture = yield* makeFixture({
          initialConnected: [envA, envB],
          invoke: () => Effect.die(new Error("boom")),
        });
        yield* runHost(fixture, Stream.make(true));
        yield* fixture.takeCalls(2);
        yield* fixture.announce(envA, "connection-a");
        yield* fixture.announce(envB, "connection-b");
        yield* fixture.send(envA, {
          type: "request",
          connectionId: "connection-a",
          request: makeRequest(),
        });
        const [call] = yield* fixture.takeCalls(1);
        expect(call?.type === "respond" ? call.response.error : undefined).toEqual({
          _tag: "failed",
          message: "boom",
        });
      }).pipe(Effect.scoped),
    );

    it.effect("answers with a timeout when the target does not respond in time", () =>
      Effect.gen(function* () {
        const started = yield* Deferred.make<void>();
        const fixture = yield* makeFixture({
          initialConnected: [envA, envB],
          invoke: () => Deferred.succeed(started, undefined).pipe(Effect.andThen(Effect.never)),
        });
        yield* runHost(fixture, Stream.make(true));
        yield* fixture.takeCalls(2);
        yield* fixture.announce(envA, "connection-a");
        yield* fixture.announce(envB, "connection-b");
        yield* fixture.send(envA, {
          type: "request",
          connectionId: "connection-a",
          request: makeRequest({ timeoutMs: 5_000 }),
        });
        yield* Deferred.await(started);
        yield* TestClock.adjust("5 seconds");

        const [call] = yield* fixture.takeCalls(1);
        expect(call?.type === "respond" ? call.response.error?._tag : undefined).toBe("timeout");
      }).pipe(Effect.scoped),
    );
  });
});

describe("connected environment ids", () => {
  const stateOf = (connected: boolean): SupervisorConnectionState => ({
    ...AVAILABLE_CONNECTION_STATE,
    phase: connected ? "connected" : "available",
  });

  it.effect("never emits a partial set while an environment is added", () =>
    Effect.gen(function* () {
      const entries = yield* SubscriptionRef.make(
        new Map([
          [envA, { enabled: true }],
          [envB, { enabled: true }],
        ]),
      );
      const states = new Map<
        EnvironmentId,
        SubscriptionRef.SubscriptionRef<SupervisorConnectionState>
      >([
        [envA, yield* SubscriptionRef.make(stateOf(true))],
        [envB, yield* SubscriptionRef.make(stateOf(true))],
        [envC, yield* SubscriptionRef.make(stateOf(false))],
      ]);
      const emitted = yield* Queue.unbounded<ReadonlyArray<EnvironmentId>>();
      yield* ThreadRelayHostModule.connectedEnvironmentIdsOf({
        entries: entries as never,
        stateChanges: (environmentId) => SubscriptionRef.changes(states.get(environmentId)!),
      }).pipe(
        Stream.runForEach((ids) => Queue.offer(emitted, ids)),
        Effect.forkScoped,
      );

      expect(yield* Queue.take(emitted)).toEqual([envA, envB]);

      yield* SubscriptionRef.update(entries, (current) =>
        new Map(current).set(envC, { enabled: true }),
      );
      expect(yield* Queue.take(emitted)).toEqual([envA, envB]);

      yield* SubscriptionRef.set(states.get(envC)!, stateOf(true));
      expect(yield* Queue.take(emitted)).toEqual([envA, envB, envC]);
    }).pipe(Effect.scoped),
  );
});
