import { EnvironmentId, ThreadId, type ScopedThreadRef } from "@t3tools/contracts";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";

export const THREAD_LINK_PROTOCOL = "t3-thread";
const THREAD_LINK_HREF_PREFIX = `${THREAD_LINK_PROTOCOL}://v1/`;
// Code comes first in the alternation so a link written inside a code span or fence is skipped.
const THREAD_LINK_OUTSIDE_CODE =
  /(?<fence>(`{3,}|~{3,})[\s\S]*?(?:\2|$))|(?<span>(`+)[^\n]*?\4)|\[[^\]\n]*\]\((?<href>t3-thread:\/\/v1\/[^\s)]+)\)/g;

const decodeThreadId = Schema.decodeUnknownOption(ThreadId);
const decodeEnvironmentId = Schema.decodeUnknownOption(EnvironmentId);
const ENVIRONMENT_PARAM = "environmentId";

export interface ThreadLink {
  readonly threadId: ThreadId;
  /** Set for a thread on a specific environment; null means the environment of the message holding the link. */
  readonly environmentId: EnvironmentId | null;
}

/**
 * `t3-thread://v1/<threadId>` with an optional `?environmentId=<id>`. The thread id is as
 * written: ids can hold percent escapes of their own, so it is not decoded.
 */
export function parseThreadLink(href: string): ThreadLink | null {
  if (!href.startsWith(THREAD_LINK_HREF_PREFIX)) return null;
  const rest = href.slice(THREAD_LINK_HREF_PREFIX.length);
  const queryStart = rest.indexOf("?");
  const threadId = Option.getOrNull(
    decodeThreadId(queryStart === -1 ? rest : rest.slice(0, queryStart)),
  );
  if (threadId === null) return null;
  const written =
    queryStart === -1
      ? null
      : new URLSearchParams(rest.slice(queryStart + 1)).get(ENVIRONMENT_PARAM);
  return {
    threadId,
    environmentId: written === null ? null : Option.getOrNull(decodeEnvironmentId(written)),
  };
}

export function parseThreadLinkHref(href: string): ThreadId | null {
  return parseThreadLink(href)?.threadId ?? null;
}

export function percentDecodedThreadLinkId(threadId: ThreadId): ThreadId | null {
  try {
    const decoded = decodeURIComponent(threadId);
    return decoded === threadId ? null : Option.getOrNull(decodeThreadId(decoded));
  } catch {
    return null;
  }
}

export function resolveThreadLinkRef(
  source: ScopedThreadRef,
  refs: ReadonlyArray<ScopedThreadRef>,
): ScopedThreadRef | null {
  const decodedId = percentDecodedThreadLinkId(source.threadId);
  const ids = decodedId === null ? [source.threadId] : [source.threadId, decodedId];
  for (const threadId of ids) {
    const local = refs.find(
      (ref) => ref.environmentId === source.environmentId && ref.threadId === threadId,
    );
    if (local) return local;
  }
  for (const threadId of ids) {
    let match: ScopedThreadRef | null = null;
    for (const ref of refs) {
      if (ref.threadId !== threadId) continue;
      if (match !== null) return null;
      match = ref;
    }
    if (match !== null) return match;
  }
  return null;
}

export function formatThreadLink(
  threadId: string,
  label: string,
  environmentId?: string | null,
): string {
  const cleaned = label
    .replace(/[[\]\\\r\n]/g, " ")
    .replace(/\s+/g, " ")
    .trim();
  const query = environmentId ? `?${ENVIRONMENT_PARAM}=${encodeURIComponent(environmentId)}` : "";
  return `[${cleaned || threadId}](${THREAD_LINK_HREF_PREFIX}${threadId}${query})`;
}

/** Names `environmentId` in every thread link that does not already carry one. */
export function stampThreadLinkEnvironment(markdown: string, environmentId: string): string {
  const query = `?${ENVIRONMENT_PARAM}=${encodeURIComponent(environmentId)}`;
  return markdown.replace(
    /\]\((t3-thread:\/\/v1\/[^\s)?]+)\)/g,
    (_, href: string) => `](${href}${query})`,
  );
}

export function hasThreadLinks(markdown: string): boolean {
  return markdown.includes(`](${THREAD_LINK_HREF_PREFIX}`);
}

export function relabelThreadLinks(
  markdown: string,
  title: (threadId: ThreadId) => string | undefined,
): string {
  if (!hasThreadLinks(markdown)) return markdown;
  return markdown.replace(THREAD_LINK_OUTSIDE_CODE, (source, ...args) => {
    const href = (args.at(-1) as { href?: string }).href;
    if (href === undefined) return source;
    const link = parseThreadLink(href);
    if (link === null) return source;
    const written = link.threadId;
    const decoded = percentDecodedThreadLinkId(written);
    const threadId =
      title(written) === undefined && decoded !== null && title(decoded) !== undefined
        ? decoded
        : written;
    const label = title(threadId)?.trim();
    return label ? formatThreadLink(threadId, label, link.environmentId) : source;
  });
}
