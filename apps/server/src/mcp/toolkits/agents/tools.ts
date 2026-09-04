import {
  ProviderInstanceId,
  ProviderOptionSelections,
  TrimmedNonEmptyString,
} from "@t3tools/contracts";
import * as Crypto from "effect/Crypto";
import * as Schema from "effect/Schema";
import { Tool, Toolkit } from "effect/unstable/ai";

import * as McpInvocationContext from "../../McpInvocationContext.ts";
import * as ProjectionSnapshotQuery from "../../../orchestration/Services/ProjectionSnapshotQuery.ts";
import * as OrchestrationEngine from "../../../orchestration/Services/OrchestrationEngine.ts";
import * as ProviderService from "../../../provider/Services/ProviderService.ts";
import { ServerSettingsService } from "../../../serverSettings.ts";

export class CrossProviderAgentError extends Schema.TaggedErrorClass<CrossProviderAgentError>()(
  "CrossProviderAgentError",
  {
    operation: Schema.Literals(["spawn", "status", "follow_up", "interrupt"]),
    detail: TrimmedNonEmptyString,
  },
) {
  override get message(): string {
    return this.detail;
  }
}

const AgentId = TrimmedNonEmptyString.annotate({
  description: "The stable T3 child-agent id returned by agent_spawn.",
});

const AgentStatus = Schema.Literals([
  "pending",
  "running",
  "waiting",
  "idle",
  "completed",
  "failed",
  "interrupted",
]);

const AgentHandle = Schema.Struct({
  agentId: AgentId,
  childThreadId: TrimmedNonEmptyString,
  providerInstanceId: ProviderInstanceId,
  model: TrimmedNonEmptyString,
});

const AgentState = Schema.Struct({
  ...AgentHandle.fields,
  status: AgentStatus,
  output: Schema.optional(Schema.String),
  error: Schema.optional(Schema.String),
});

const dependencies = [
  McpInvocationContext.McpInvocationContext,
  ProjectionSnapshotQuery.ProjectionSnapshotQuery,
  OrchestrationEngine.OrchestrationEngineService,
  ProviderService.ProviderService,
  ServerSettingsService,
  Crypto.Crypto,
];

export const AgentSpawnTool = Tool.make("agent_spawn", {
  description:
    "Spawn a child agent as a real T3 Code thread on an explicitly selected provider instance. Use this instead of a provider-native spawn when the child must run through another configured provider, appear in the parent Agents panel, and preserve its provider session for follow-ups.",
  parameters: Schema.Struct({
    providerInstanceId: ProviderInstanceId.annotate({
      description:
        "Exact T3 provider instance id or a case-insensitive alias configured under Integrations > Agents. Exact ids take precedence. The resolved route never falls back to the parent provider.",
    }),
    model: TrimmedNonEmptyString,
    options: Schema.optionalKey(Schema.NullOr(ProviderOptionSelections)),
    prompt: TrimmedNonEmptyString,
    title: Schema.optionalKey(Schema.NullOr(TrimmedNonEmptyString)),
    role: Schema.optionalKey(Schema.NullOr(TrimmedNonEmptyString)),
  }),
  success: AgentHandle,
  failure: CrossProviderAgentError,
  dependencies,
})
  .annotate(Tool.Title, "Spawn cross-provider agent")
  .annotate(Tool.Destructive, true)
  .annotate(Tool.OpenWorld, false);

export const AgentStatusTool = Tool.make("agent_status", {
  description:
    "Read a child agent's current T3 lifecycle state and latest final output. This also synchronizes that state into the parent thread's Agents panel.",
  parameters: Schema.Struct({ agentId: AgentId }),
  success: AgentState,
  failure: CrossProviderAgentError,
  dependencies,
})
  .annotate(Tool.Title, "Get cross-provider agent status")
  .annotate(Tool.Readonly, false)
  .annotate(Tool.Destructive, false)
  .annotate(Tool.OpenWorld, false);

export const AgentFollowUpTool = Tool.make("agent_follow_up", {
  description:
    "Send another turn to the same child T3 thread and provider session. Reusing the child preserves the native provider resume identity and makes prompt-prefix cache reuse possible.",
  parameters: Schema.Struct({
    agentId: AgentId,
    prompt: TrimmedNonEmptyString,
  }),
  success: AgentHandle,
  failure: CrossProviderAgentError,
  dependencies,
})
  .annotate(Tool.Title, "Follow up with cross-provider agent")
  .annotate(Tool.Destructive, true)
  .annotate(Tool.OpenWorld, false);

export const AgentInterruptTool = Tool.make("agent_interrupt", {
  description: "Interrupt the active turn of a child agent owned by this parent thread.",
  parameters: Schema.Struct({ agentId: AgentId }),
  success: AgentState,
  failure: CrossProviderAgentError,
  dependencies,
})
  .annotate(Tool.Title, "Interrupt cross-provider agent")
  .annotate(Tool.Destructive, true)
  .annotate(Tool.OpenWorld, false);

export const AgentToolkit = Toolkit.make(
  AgentSpawnTool,
  AgentStatusTool,
  AgentFollowUpTool,
  AgentInterruptTool,
);
