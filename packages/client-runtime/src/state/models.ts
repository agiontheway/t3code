import type {
  EnvironmentId,
  OrchestrationMessage,
  OrchestrationProjectShell,
  OrchestrationThread,
  OrchestrationThreadShell,
} from "@t3tools/contracts";

export interface EnvironmentProject extends OrchestrationProjectShell {
  readonly environmentId: EnvironmentId;
}

export interface EnvironmentThreadShell extends OrchestrationThreadShell {
  readonly environmentId: EnvironmentId;
}

export type EnvironmentMessage = OrchestrationMessage;

export interface EnvironmentThread extends OrchestrationThread {
  readonly environmentId: EnvironmentId;
}

/**
 * A thread another thread spawned through the cross-provider agent tools.
 * Such a thread belongs to its parent's Direct Spawns roster, not to any
 * thread list: every list-shaped surface (sidebar, palette, mobile home,
 * archived views, latest-thread pickers) excludes it, while point reads by
 * id keep working so the thread stays openable from the parent row or a
 * deep link.
 */
export function isSpawnedChildThread(thread: Pick<OrchestrationThreadShell, "spawn">): boolean {
  return thread.spawn !== undefined;
}

export function scopeProject(
  environmentId: EnvironmentId,
  project: OrchestrationProjectShell,
): EnvironmentProject {
  return { ...project, environmentId };
}

export function scopeThreadShell(
  environmentId: EnvironmentId,
  thread: OrchestrationThreadShell,
): EnvironmentThreadShell {
  return { ...thread, environmentId };
}

export function scopeThread(
  environmentId: EnvironmentId,
  thread: OrchestrationThread,
): EnvironmentThread {
  return { ...thread, environmentId };
}
