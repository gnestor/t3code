import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, describe, it } from "@effect/vitest";
import {
  AuthOrchestrationOperateScope,
  AuthOrchestrationReadScope,
  AuthSessionId,
  EnvironmentId,
  ProjectId,
  ThreadId,
  type OrchestrationMessageContext,
  type ProviderInteractionMode,
  type RuntimeMode,
  type ThreadRelayInvokeInput,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Ref from "effect/Ref";

import type { AuthenticatedSession } from "../auth/EnvironmentAuth.ts";
import * as ServerEnvironment from "../environment/ServerEnvironment.ts";
import { DispatchModeLimit } from "../orchestration-v2/DispatchModeLimit.ts";
import * as ProviderAdapterRegistry from "../orchestration-v2/ProviderAdapterRegistry.ts";
import * as ThreadLaunchService from "../orchestration-v2/ThreadLaunchService.ts";
import * as ThreadManagementService from "../orchestration-v2/ThreadManagementService.ts";
import * as ThreadSearch from "../orchestration-v2/ThreadSearch.ts";
import * as ProjectService from "../project/ProjectService.ts";
import * as ProviderRegistry from "../provider/ProviderRegistry.ts";
import * as ScheduledTaskService from "../scheduledTasks/ScheduledTaskService.ts";
import * as SecretRequests from "../secrets/SecretRequests.ts";
import * as ServerSettings from "../serverSettings.ts";
import * as GitVcsDriver from "../vcs/GitVcsDriver.ts";
import { idleThreadProjection, liveThreadShell } from "./McpToolAccess.testkit.ts";
import * as OrchestratorMcpService from "./OrchestratorMcpService.ts";
import * as ThreadRelayExecution from "./ThreadRelayExecution.ts";

const localEnvironmentId = EnvironmentId.make("environment:target");
const callerEnvironmentId = EnvironmentId.make("environment:caller");
const targetThreadId = ThreadId.make("thread:target");
const projectId = ProjectId.make("project:mcp-test");

const operatorSession: AuthenticatedSession = {
  sessionId: AuthSessionId.make("session:operator"),
  subject: "operator",
  method: "bearer-access-token",
  scopes: [AuthOrchestrationReadScope, AuthOrchestrationOperateScope],
};
const readerSession: AuthenticatedSession = {
  ...operatorSession,
  scopes: [AuthOrchestrationReadScope],
};

const relayed = (
  operation: ThreadRelayInvokeInput["operation"],
  input: unknown,
  caller: Partial<{
    runtimeMode: RuntimeMode;
    interactionMode: ProviderInteractionMode;
    readOnly: boolean;
  }> = {},
): ThreadRelayInvokeInput => ({
  operation,
  input,
  caller: {
    environmentId: callerEnvironmentId,
    threadId: ThreadId.make("thread:caller"),
    title: "Caller",
    runtimeMode: "full-access",
    interactionMode: "default",
    readOnly: false,
    ...caller,
  },
});

interface Harness {
  readonly sends: Ref.Ref<number>;
  readonly sentTexts: Ref.Ref<ReadonlyArray<string>>;
  readonly sentContexts: Ref.Ref<ReadonlyArray<OrchestrationMessageContext | undefined>>;
  readonly limits: Ref.Ref<ReadonlyArray<unknown>>;
  readonly searches: Ref.Ref<ReadonlyArray<unknown>>;
}

const withExecution = <A, E>(
  shells: ReadonlyArray<ReturnType<typeof liveThreadShell>>,
  use: (
    execution: ThreadRelayExecution.ThreadRelayExecutionShape,
    harness: Harness,
  ) => Effect.Effect<A, E>,
) =>
  Effect.gen(function* () {
    const harness: Harness = {
      sends: yield* Ref.make(0),
      sentTexts: yield* Ref.make<ReadonlyArray<string>>([]),
      sentContexts: yield* Ref.make<ReadonlyArray<OrchestrationMessageContext | undefined>>([]),
      limits: yield* Ref.make<ReadonlyArray<unknown>>([]),
      searches: yield* Ref.make<ReadonlyArray<unknown>>([]),
    };
    const dependencies = Layer.mergeAll(
      NodeServices.layer,
      Layer.mock(ThreadManagementService.ThreadManagementService)({
        getThreadShell: (threadId) =>
          Effect.succeed(shells.find((shell) => shell.id === threadId) ?? null),
        getProjectThreadRecords: (target) =>
          Effect.succeed(
            idleThreadProjection(shells.find((shell) => shell.id === target.threadId)!),
          ) as never,
        listProjectThreads: () => Effect.succeed(shells),
        sendToThread: (input) =>
          Effect.gen(function* () {
            yield* Ref.update(harness.sends, (count) => count + 1);
            yield* Ref.update(harness.sentTexts, (texts) => [...texts, input.text]);
            yield* Ref.update(harness.sentContexts, (contexts) => [...contexts, input.context]);
            yield* Ref.update(harness.limits, (limits) => [...limits, undefined]);
            const limit = yield* DispatchModeLimit;
            yield* Ref.update(harness.limits, (limits) => [
              ...limits.slice(0, -1),
              limit === undefined
                ? undefined
                : { runtimeMode: limit.runtimeMode, interactionMode: limit.interactionMode },
            ]);
            return {
              run: { id: "run:sent", status: "running" },
              delivery: "started",
            } as never;
          }),
      }),
      ServerSettings.layerTest(),
      Layer.mock(ProviderRegistry.ProviderRegistry)({ getProviders: Effect.succeed([]) }),
      Layer.mock(ProviderAdapterRegistry.ProviderAdapterRegistryV2)({
        list: () => Effect.succeed([]),
      }),
      Layer.mock(ProjectService.ProjectService)({}),
      Layer.mock(SecretRequests.SecretRequests)({}),
      Layer.mock(ScheduledTaskService.ScheduledTaskService)({}),
      Layer.mock(ThreadLaunchService.ThreadLaunchService)({}),
      Layer.mock(GitVcsDriver.GitVcsDriver)({}),
    );
    const layer = ThreadRelayExecution.layer.pipe(
      Layer.provide(OrchestratorMcpService.layer),
      Layer.provide(dependencies),
      Layer.provide(
        Layer.succeed(
          ServerEnvironment.ServerEnvironment,
          ServerEnvironment.ServerEnvironment.of({
            getEnvironmentId: Effect.succeed(localEnvironmentId),
            getDescriptor: Effect.die("unused"),
          }),
        ),
      ),
      Layer.provide(
        Layer.mock(ThreadSearch.ThreadSearch)({
          searchThreads: (query) =>
            Ref.update(harness.searches, (searches) => [...searches, query]).pipe(
              Effect.as({
                matches: [
                  {
                    threadId: targetThreadId,
                    projectId,
                    title: "Target",
                    source: "title" as const,
                  },
                ],
              }),
            ),
        }),
      ),
      Layer.provide(NodeServices.layer),
    );
    return yield* Effect.gen(function* () {
      const execution = yield* ThreadRelayExecution.ThreadRelayExecution;
      return yield* use(execution, harness);
    }).pipe(Effect.provide(layer));
  });

describe("ThreadRelayExecution", () => {
  it.effect("refuses a send to a thread running broader modes than the relayed caller", () =>
    withExecution(
      [liveThreadShell(targetThreadId, { runtimeMode: "full-access" })],
      (execution, harness) =>
        Effect.gen(function* () {
          const error = yield* execution
            .invoke(
              operatorSession,
              relayed(
                "send",
                { threadId: targetThreadId, message: "hi" },
                { runtimeMode: "approval-required" },
              ),
            )
            .pipe(Effect.asVoid, Effect.flip);
          assert.equal(error._tag, "ThreadRelayError");
          assert.equal(error.reason, "forbidden");
          assert.equal(yield* Ref.get(harness.sends), 0);
        }),
    ),
  );

  it.effect("refuses a send to a default-mode thread from a plan-mode caller", () =>
    withExecution(
      [liveThreadShell(targetThreadId, { interactionMode: "default" })],
      (execution, harness) =>
        Effect.gen(function* () {
          const error = yield* execution
            .invoke(
              operatorSession,
              relayed(
                "send",
                { threadId: targetThreadId, message: "hi" },
                { interactionMode: "plan" },
              ),
            )
            .pipe(Effect.asVoid, Effect.flip);
          assert.equal(error.reason, "forbidden");
          assert.equal(yield* Ref.get(harness.sends), 0);
        }),
    ),
  );

  it.effect("delivers a send within the caller's limits and rechecks them under the lock", () =>
    withExecution(
      [
        liveThreadShell(targetThreadId, {
          runtimeMode: "auto-accept-edits",
          interactionMode: "plan",
        }),
      ],
      (execution, harness) =>
        Effect.gen(function* () {
          const result = yield* execution.invoke(
            operatorSession,
            relayed(
              "send",
              { threadId: targetThreadId, message: "hi" },
              { runtimeMode: "auto", interactionMode: "plan" },
            ),
          );
          assert.equal((result as { delivery: string }).delivery, "started");
          assert.equal(yield* Ref.get(harness.sends), 1);
          assert.deepEqual(yield* Ref.get(harness.limits), [
            { runtimeMode: "auto", interactionMode: "plan" },
          ]);
        }),
    ),
  );

  it.effect("records which remote thread a relayed message came from", () =>
    withExecution([liveThreadShell(targetThreadId)], (execution, harness) =>
      Effect.gen(function* () {
        yield* execution.invoke(
          operatorSession,
          relayed("send", { threadId: targetThreadId, message: "please review" }),
        );
        assert.deepEqual(yield* Ref.get(harness.sentTexts), [
          `[Relayed from thread "Caller" (thread:caller) on environment ${callerEnvironmentId}]\n\nplease review`,
        ]);
        // Clients show the relayed words under the origin, not the agent's provenance line.
        assert.deepEqual(yield* Ref.get(harness.sentContexts), [
          {
            version: 1,
            records: [],
            relay: {
              environmentId: callerEnvironmentId,
              threadId: ThreadId.make("thread:caller"),
              title: "Caller",
              text: "please review",
            },
          },
        ]);
      }),
    ),
  );

  it.effect("cannot send for a read-only caller or a read-only session", () =>
    withExecution([liveThreadShell(targetThreadId)], (execution, harness) =>
      Effect.gen(function* () {
        const send = { threadId: targetThreadId, message: "hi" };
        const readOnlyCaller = yield* execution
          .invoke(operatorSession, relayed("send", send, { readOnly: true }))
          .pipe(Effect.asVoid, Effect.flip);
        const readOnlySession = yield* execution
          .invoke(readerSession, relayed("send", send))
          .pipe(Effect.asVoid, Effect.flip);
        assert.equal(readOnlyCaller.reason, "forbidden");
        assert.equal(readOnlySession.reason, "forbidden");
        assert.equal(yield* Ref.get(harness.sends), 0);
      }),
    ),
  );

  it.effect("lets a read-only session list and search", () =>
    withExecution([liveThreadShell(targetThreadId)], (execution, harness) =>
      Effect.gen(function* () {
        const list = yield* execution.invoke(readerSession, relayed("list", { projectId }));
        assert.equal((list as { total: number }).total, 1);
        assert.equal((list as { currentThreadId: unknown }).currentThreadId, null);
        const listed = (list as { threads: ReadonlyArray<{ link?: string }> }).threads[0]?.link;
        assert.include(listed, "?environmentId=environment%3Atarget)");

        const search = yield* execution.invoke(
          readerSession,
          relayed("search", { query: "target", environmentId: localEnvironmentId }),
        );
        const matches = (search as { matches: ReadonlyArray<{ link: string }> }).matches;
        assert.equal(matches.length, 1);
        assert.include(matches[0]?.link, "?environmentId=environment%3Atarget)");
        assert.deepEqual(yield* Ref.get(harness.searches), [{ query: "target" }]);
      }),
    ),
  );

  it.effect("tells a relayed list without a project how to find one on this environment", () =>
    withExecution([liveThreadShell(targetThreadId)], (execution) =>
      Effect.gen(function* () {
        const error = yield* execution
          .invoke(readerSession, relayed("list", {}))
          .pipe(Effect.asVoid, Effect.flip);
        assert.equal(error.reason, "invalid_input");
        assert.include(error.message, "t3_thread_search");
        assert.notInclude(error.message, "not running inside a T3 thread");
      }),
    ),
  );

  it.effect("answers an unknown thread with a typed not-found error", () =>
    withExecution([], (execution) =>
      Effect.gen(function* () {
        const missing = ThreadId.make("thread:missing");
        for (const call of [
          relayed("read", { threadId: missing }),
          relayed("wait", { threadId: missing }),
          relayed("send", { threadId: missing, message: "hi" }),
        ]) {
          const error = yield* execution
            .invoke(operatorSession, call)
            .pipe(Effect.asVoid, Effect.flip);
          assert.equal(error._tag, "ThreadRelayError");
          assert.equal(error.reason, "not_found");
        }
      }),
    ),
  );

  it.effect("rejects input that does not decode", () =>
    withExecution([], (execution) =>
      Effect.gen(function* () {
        const error = yield* execution
          .invoke(operatorSession, relayed("read", { threadId: 7 }))
          .pipe(Effect.asVoid, Effect.flip);
        assert.equal(error.reason, "invalid_input");
      }),
    ),
  );
});
