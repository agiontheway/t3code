import { ProviderInstanceId, TrimmedNonEmptyString } from "@t3tools/contracts";
import * as Crypto from "effect/Crypto";
import * as Schema from "effect/Schema";
import { Tool, Toolkit } from "effect/unstable/ai";

import * as McpInvocationContext from "../../McpInvocationContext.ts";
import * as OrchestrationEngine from "../../../orchestration/Services/OrchestrationEngine.ts";
import * as ProjectionSnapshotQuery from "../../../orchestration/Services/ProjectionSnapshotQuery.ts";
import * as ProviderRegistry from "../../../provider/Services/ProviderRegistry.ts";
import { ServerSettingsService } from "../../../serverSettings.ts";

export class CrossProviderAgentError extends Schema.TaggedErrorClass<CrossProviderAgentError>()(
  "CrossProviderAgentError",
  {
    operation: Schema.Literals(["catalog", "spawn", "wait", "follow_up", "interrupt"]),
    detail: TrimmedNonEmptyString,
  },
) {
  override get message(): string {
    return this.detail;
  }
}

const AgentId = TrimmedNonEmptyString.annotate({
  description: "Stable T3 child-agent id returned by agent_spawn.",
});

const AgentHandle = Schema.Struct({
  agentId: AgentId,
  childThreadId: TrimmedNonEmptyString,
  providerInstanceId: ProviderInstanceId,
  model: TrimmedNonEmptyString,
});

const AgentResult = Schema.Struct({
  ...AgentHandle.fields,
  status: Schema.Literals(["idle", "failed", "interrupted"]),
  output: Schema.optional(Schema.String),
  error: Schema.optional(Schema.String),
});

const CatalogModel = Schema.Struct({ id: TrimmedNonEmptyString, name: TrimmedNonEmptyString });
const CatalogProvider = Schema.Struct({
  instanceId: ProviderInstanceId,
  displayName: TrimmedNonEmptyString,
  driver: TrimmedNonEmptyString,
  aliases: Schema.Array(TrimmedNonEmptyString),
  models: Schema.Array(CatalogModel),
});

const dependencies = [
  McpInvocationContext.McpInvocationContext,
  ProjectionSnapshotQuery.ProjectionSnapshotQuery,
  OrchestrationEngine.OrchestrationEngineService,
  ProviderRegistry.ProviderRegistry,
  ServerSettingsService,
  Crypto.Crypto,
];

export const AgentCatalogTool = Tool.make("agent_catalog", {
  description:
    "List enabled T3 provider subscriptions, their exact providerInstanceId values, aliases, and supported models. Call this before a cross-provider spawn when the route is not already known.",
  parameters: Schema.Struct({}),
  success: Schema.Struct({ providers: Schema.Array(CatalogProvider) }),
  failure: CrossProviderAgentError,
  dependencies,
})
  .annotate(Tool.Title, "List cross-provider agent routes")
  .annotate(Tool.Readonly, true)
  .annotate(Tool.Destructive, false)
  .annotate(Tool.OpenWorld, false);

export const AgentSpawnTool = Tool.make("agent_spawn", {
  description:
    "Start an ordinary T3 child thread on another configured provider subscription. Use this instead of a provider-native spawnAgent whenever the child providerInstanceId differs from this session. The child appears in the standard Agents panel and its lifecycle updates automatically; call agent_wait once when you need its answer.",
  parameters: Schema.Struct({
    providerInstanceId: ProviderInstanceId.annotate({
      description: "Exact id or case-insensitive alias from agent_catalog. Routing fails closed.",
    }),
    model: TrimmedNonEmptyString,
    prompt: TrimmedNonEmptyString,
    title: Schema.optionalKey(TrimmedNonEmptyString),
    role: Schema.optionalKey(TrimmedNonEmptyString),
  }),
  success: AgentHandle,
  failure: CrossProviderAgentError,
  dependencies,
})
  .annotate(Tool.Title, "Spawn cross-provider agent")
  .annotate(Tool.Destructive, true)
  .annotate(Tool.OpenWorld, false);

export const AgentWaitTool = Tool.make("agent_wait", {
  description:
    "Wait server-side for one cross-provider child turn to settle, then return its latest answer. This is one blocking call, not a polling status API.",
  parameters: Schema.Struct({ agentId: AgentId }),
  success: AgentResult,
  failure: CrossProviderAgentError,
  dependencies,
})
  .annotate(Tool.Title, "Wait for cross-provider agent")
  .annotate(Tool.Readonly, true)
  .annotate(Tool.Destructive, false)
  .annotate(Tool.OpenWorld, false);

export const AgentFollowUpTool = Tool.make("agent_follow_up", {
  description:
    "Send another turn to the same child T3 thread and native provider session, preserving its provider resume identity. Call agent_wait once for the answer.",
  parameters: Schema.Struct({ agentId: AgentId, prompt: TrimmedNonEmptyString }),
  success: AgentHandle,
  failure: CrossProviderAgentError,
  dependencies,
})
  .annotate(Tool.Title, "Follow up with cross-provider agent")
  .annotate(Tool.Destructive, true)
  .annotate(Tool.OpenWorld, false);

export const AgentInterruptTool = Tool.make("agent_interrupt", {
  description: "Interrupt the active turn of a cross-provider child owned by this thread.",
  parameters: Schema.Struct({ agentId: AgentId }),
  success: AgentHandle,
  failure: CrossProviderAgentError,
  dependencies,
})
  .annotate(Tool.Title, "Interrupt cross-provider agent")
  .annotate(Tool.Destructive, true)
  .annotate(Tool.OpenWorld, false);

export const AgentToolkit = Toolkit.make(
  AgentCatalogTool,
  AgentSpawnTool,
  AgentWaitTool,
  AgentFollowUpTool,
  AgentInterruptTool,
);
