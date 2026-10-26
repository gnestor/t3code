import type { EnvironmentId, ScopedThreadRef, ThreadId } from "@t3tools/contracts";
import { foldSearchQuery, rankSearchItems, scoreThreadSearch } from "@t3tools/shared/paletteSearch";

import type { EnvironmentThreadShell } from "./state/models.ts";
import { threadSearchSubject, type ThreadSearchShell } from "./state/threadSearch.ts";

const COMPOSER_THREAD_RESULT_LIMIT = 5;

export type ComposerThreadCandidate = ThreadSearchShell &
  Pick<EnvironmentThreadShell, "archivedAt" | "lineage">;

export interface ComposerThreadItem {
  readonly id: string;
  readonly type: "thread";
  readonly thread: ScopedThreadRef;
  readonly label: string;
  readonly description: string;
}

/**
 * Threads to reference from the composer, matched and ranked like the command
 * palette, without subagent threads. The agent can only read threads on its
 * own server, so a thread from another environment is only offered when the
 * caller can attach it as a transcript, as dropping it from the sidebar does.
 */
export function matchComposerThreadItems(input: {
  shells: ReadonlyArray<ComposerThreadCandidate>;
  environmentId: EnvironmentId;
  excludeThreadId: ThreadId | null;
  query: string;
  /** Recency is measured against this time. */
  now: number;
  /** Other connected environments to include, by label. Omit to stay environment-local. */
  otherEnvironmentLabels?: ReadonlyMap<EnvironmentId, string>;
}): ComposerThreadItem[] {
  const query = foldSearchQuery(input.query);
  if (query.length === 0) return [];
  const labelOf = (shell: ComposerThreadCandidate) =>
    shell.environmentId === input.environmentId
      ? null
      : input.otherEnvironmentLabels?.get(shell.environmentId);
  const candidates = input.shells.filter(
    (shell) =>
      labelOf(shell) !== undefined &&
      !(shell.environmentId === input.environmentId && shell.id === input.excludeThreadId) &&
      shell.archivedAt === null &&
      shell.lineage.relationshipToParent !== "subagent",
  );
  return rankSearchItems(candidates, (shell, options) =>
    scoreThreadSearch(query, threadSearchSubject(shell, null, labelOf(shell)), input.now, {
      ...options,
      picks: false,
    }),
  )
    .slice(0, COMPOSER_THREAD_RESULT_LIMIT)
    .map(({ item: shell }) => {
      const environmentLabel = labelOf(shell);
      return {
        id: `thread:${shell.environmentId}:${shell.id}`,
        type: "thread",
        thread: { environmentId: shell.environmentId, threadId: shell.id },
        label: shell.title,
        description: environmentLabel ? `Thread · ${environmentLabel}` : "Thread",
      };
    });
}
