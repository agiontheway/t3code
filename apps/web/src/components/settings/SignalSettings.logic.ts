import type { EnvironmentId } from "@t3tools/contracts";

interface SignalThreadOption {
  readonly id: string;
  readonly environmentId: EnvironmentId;
  readonly title: string;
  readonly archivedAt: string | null;
  readonly settledAt: string | null;
  readonly updatedAt: string;
}

export function signalThreadOptions<T extends SignalThreadOption>(
  threads: readonly T[],
  environmentId: EnvironmentId,
  query: string,
): T[] {
  const search = query.trim().toLocaleLowerCase();
  return threads
    .filter((thread) => thread.environmentId === environmentId && thread.archivedAt === null)
    .filter(
      (thread) => !search || `${thread.title} ${thread.id}`.toLocaleLowerCase().includes(search),
    )
    .sort(
      (a, b) =>
        Number(a.settledAt !== null) - Number(b.settledAt !== null) ||
        b.updatedAt.localeCompare(a.updatedAt) ||
        a.title.localeCompare(b.title) ||
        a.id.localeCompare(b.id),
    );
}
