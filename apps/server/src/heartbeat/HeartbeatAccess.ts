import {
  isProviderNativeSubagentThread,
  type OrchestrationV2ThreadShell,
  type ServerSettings,
} from "@t3tools/contracts";
import { resolveProjectSettings } from "@t3tools/shared/projectSettings";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";

import { DelegationGrants } from "../orchestration-v2/fork/DelegationGrants.ts";

/**
 * Heartbeat is for main threads and children allowed to orchestrate. A thread
 * qualifies when its project-resolved setting is on and it is a root (no
 * subagent lineage) or a delegated child whose grant allows orchestration.
 * Provider-native subagent threads never qualify.
 */
export const heartbeatAccessAllowed = Effect.fn("heartbeatAccessAllowed")(function* (
  settings: ServerSettings,
  thread: Pick<OrchestrationV2ThreadShell, "id" | "projectId" | "lineage" | "creationSource">,
) {
  if (!resolveProjectSettings(settings, thread.projectId).settings.enableHeartbeatAccess) {
    return false;
  }
  if (isProviderNativeSubagentThread(thread)) return false;
  if (thread.lineage.relationshipToParent !== "subagent") return true;
  const grant = yield* (yield* DelegationGrants).read(thread.id);
  return Option.isSome(grant) && grant.value.allowOrchestration;
});
