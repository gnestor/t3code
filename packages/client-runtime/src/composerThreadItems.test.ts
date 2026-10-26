import { EnvironmentId, ThreadId } from "@t3tools/contracts";
import { expect, it } from "vite-plus/test";

import { matchComposerThreadItems, type ComposerThreadCandidate } from "./composerThreadItems.ts";

const environmentId = EnvironmentId.make("env-composer");

function shell(
  id: string,
  title: string,
  overrides: Partial<ComposerThreadCandidate> = {},
): ComposerThreadCandidate {
  return {
    environmentId,
    id: ThreadId.make(id),
    title,
    branch: null,
    createdAt: "2026-10-01T00:00:00.000Z",
    updatedAt: "2026-10-01T00:00:00.000Z",
    latestUserMessageAt: null,
    opens: null,
    archivedAt: null,
    lineage: { rootThreadId: ThreadId.make(id), parentThreadId: null, relationshipToParent: null },
    ...overrides,
  };
}

it("ranks thread mentions like the palette and leaves out subagent threads", () => {
  const items = matchComposerThreadItems({
    shells: [
      shell("newer-mid-word", "Remodel the settings page", {
        updatedAt: "2026-10-20T00:00:00.000Z",
      }),
      shell("word", "Fix model picker"),
      shell("subagent", "Model review", {
        lineage: {
          rootThreadId: ThreadId.make("word"),
          parentThreadId: ThreadId.make("word"),
          relationshipToParent: "subagent",
        },
      }),
      shell("other-environment", "Model docs", { environmentId: EnvironmentId.make("env-other") }),
    ],
    environmentId,
    excludeThreadId: null,
    query: "model",
    now: Date.parse("2026-10-23T00:00:00.000Z"),
  });
  expect(items.map((item) => item.thread.threadId)).toEqual(["word", "newer-mid-word"]);
});

it("offers threads from other connected environments only when given their labels", () => {
  const otherEnvironmentId = EnvironmentId.make("env-other");
  const input = {
    shells: [
      shell("local", "Model picker"),
      shell("remote", "Model docs", { environmentId: otherEnvironmentId }),
      shell("unlabeled", "Model review", {
        environmentId: EnvironmentId.make("env-disconnected"),
      }),
    ],
    environmentId,
    excludeThreadId: null,
    query: "model",
    now: Date.parse("2026-10-23T00:00:00.000Z"),
  };
  expect(matchComposerThreadItems(input).map((item) => item.thread.threadId)).toEqual(["local"]);
  const items = matchComposerThreadItems({
    ...input,
    otherEnvironmentLabels: new Map([[otherEnvironmentId, "MacBook Pro"]]),
  });
  expect(items.map((item) => [item.thread.threadId, item.description])).toEqual([
    ["local", "Thread"],
    ["remote", "Thread · MacBook Pro"],
  ]);
});

it("excludes the active thread only in its own environment", () => {
  const otherEnvironmentId = EnvironmentId.make("env-other");
  const items = matchComposerThreadItems({
    shells: [
      shell("same-id", "Model picker"),
      shell("same-id", "Model picker", { environmentId: otherEnvironmentId }),
    ],
    environmentId,
    excludeThreadId: ThreadId.make("same-id"),
    query: "model",
    now: Date.parse("2026-10-23T00:00:00.000Z"),
    otherEnvironmentLabels: new Map([[otherEnvironmentId, "MacBook Pro"]]),
  });
  expect(items.map((item) => item.thread.environmentId)).toEqual([otherEnvironmentId]);
});
