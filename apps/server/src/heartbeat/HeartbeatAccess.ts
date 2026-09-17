import { type OrchestrationThreadShell, type ServerSettings } from "@t3tools/contracts";
import { resolveProjectSettings } from "@t3tools/shared/projectSettings";

export const heartbeatAccessAllowed = (
  settings: ServerSettings,
  thread: Pick<OrchestrationThreadShell, "projectId" | "spawn">,
): boolean => {
  const resolved = resolveProjectSettings(settings, thread.projectId).settings;
  return (
    resolved.enableHeartbeatAccess &&
    (thread.spawn === undefined || thread.spawn.allowOrchestration === true)
  );
};
