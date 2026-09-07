import type { ThreadId } from "@t3tools/contracts";

/**
 * Threads whose live provider session holds cross-provider agent tools that
 * cannot be re-injected on resume (Codex grants them at `thread/start` only).
 * The idle reaper consults this so it never trades a tool-bearing session
 * for one without tools. Process-local by design: a restart resumes without
 * tools and says so in the thread.
 */
const grantedThreads = new Set<ThreadId>();

export function markCrossProviderToolsGranted(threadId: ThreadId): void {
  grantedThreads.add(threadId);
}

export function clearCrossProviderToolsGranted(threadId: ThreadId): void {
  grantedThreads.delete(threadId);
}

export function hasCrossProviderToolsGranted(threadId: ThreadId): boolean {
  return grantedThreads.has(threadId);
}
