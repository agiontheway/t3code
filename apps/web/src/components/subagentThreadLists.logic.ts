// fork: subagent threads stay out of every thread list, as in 0.0.45.
/**
 * Drops subagent threads from a list-shaped surface (the command palette and
 * the Settings archived list). The sidebar and mobile home already hide them
 * upstream. Callers filter their own list: the shared shell and archived
 * atoms stay complete because point reads, deep links, and the parent's
 * Lineage graph need the children. Returns the input array when nothing is
 * dropped, so memoized consumers keep their identity.
 */
export function withoutSubagentThreads<
  T extends { readonly lineage: { readonly relationshipToParent: string | null } },
>(threads: ReadonlyArray<T>): ReadonlyArray<T> {
  return threads.some(isSubagentThread)
    ? threads.filter((thread) => !isSubagentThread(thread))
    : threads;
}

function isSubagentThread(thread: {
  readonly lineage: { readonly relationshipToParent: string | null };
}): boolean {
  return thread.lineage.relationshipToParent === "subagent";
}
