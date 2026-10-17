import {
  CROSS_PROVIDER_AGENT_TOOL_DESCRIPTIONS,
  CrossProviderAgentCatalogOutput,
  CrossProviderAgentErrorOutput,
  CrossProviderAgentFollowUpInput,
  CrossProviderAgentFollowUpOutput,
  CrossProviderAgentInterruptInput,
  CrossProviderAgentInterruptOutput,
  CrossProviderAgentResultInput,
  CrossProviderAgentResultOutput,
  CrossProviderAgentSpawnInput,
  CrossProviderAgentSpawnOutput,
  CrossProviderAgentWaitInput,
  CrossProviderAgentWaitOutput,
  OrchestratorMcpFailure,
} from "@t3tools/contracts";
import { Tool, Toolkit } from "effect/ai";
import * as Schema from "effect/Schema";

import * as ThreadManagementService from "../../../orchestration-v2/ThreadManagementService.ts";
import * as McpInvocationContext from "../../McpInvocationContext.ts";
import * as CrossProviderAgentMcpService from "./service.ts";

const dependencies = [
  McpInvocationContext.McpInvocationContext,
  ThreadManagementService.ThreadManagementService,
  CrossProviderAgentMcpService.CrossProviderAgentMcpService,
];

/**
 * fork: the 0.0.45 `{ error: { code } }` result, plus the access gate's refusal. Both return as
 * an `isError` result whose text is the encoded payload.
 */
const CrossProviderAgentFailure = Schema.Union([
  CrossProviderAgentErrorOutput,
  OrchestratorMcpFailure,
]);

const AgentCatalogTool = Tool.make("agent_catalog", {
  description: CROSS_PROVIDER_AGENT_TOOL_DESCRIPTIONS.agent_catalog,
  // No parameters: an empty struct serializes as `anyOf [object, array]`, which some providers
  // reject along with every other tool on the server.
  success: CrossProviderAgentCatalogOutput,
  failure: CrossProviderAgentFailure,
  failureMode: "return",
  dependencies,
})
  .annotate(Tool.Title, "List cross-provider agent routes")
  .annotate(Tool.Readonly, true)
  .annotate(Tool.Destructive, false)
  .annotate(Tool.Idempotent, true);

const AgentSpawnTool = Tool.make("agent_spawn", {
  description: CROSS_PROVIDER_AGENT_TOOL_DESCRIPTIONS.agent_spawn,
  parameters: CrossProviderAgentSpawnInput,
  success: CrossProviderAgentSpawnOutput,
  failure: CrossProviderAgentFailure,
  failureMode: "return",
  dependencies,
})
  .annotate(Tool.Title, "Spawn a cross-provider agent")
  .annotate(Tool.Destructive, true)
  .annotate(Tool.OpenWorld, true);

const AgentWaitTool = Tool.make("agent_wait", {
  description: CROSS_PROVIDER_AGENT_TOOL_DESCRIPTIONS.agent_wait,
  parameters: CrossProviderAgentWaitInput,
  success: CrossProviderAgentWaitOutput,
  failure: CrossProviderAgentFailure,
  failureMode: "return",
  dependencies,
})
  .annotate(Tool.Title, "Wait for cross-provider agents")
  .annotate(Tool.Readonly, true)
  .annotate(Tool.Destructive, false)
  .annotate(Tool.Idempotent, true);

const AgentResultTool = Tool.make("agent_result", {
  description: CROSS_PROVIDER_AGENT_TOOL_DESCRIPTIONS.agent_result,
  parameters: CrossProviderAgentResultInput,
  success: CrossProviderAgentResultOutput,
  failure: CrossProviderAgentFailure,
  failureMode: "return",
  dependencies,
})
  .annotate(Tool.Title, "Read a cross-provider agent result")
  .annotate(Tool.Readonly, true)
  .annotate(Tool.Destructive, false)
  .annotate(Tool.Idempotent, true);

const AgentFollowUpTool = Tool.make("agent_follow_up", {
  description: CROSS_PROVIDER_AGENT_TOOL_DESCRIPTIONS.agent_follow_up,
  parameters: CrossProviderAgentFollowUpInput,
  success: CrossProviderAgentFollowUpOutput,
  failure: CrossProviderAgentFailure,
  failureMode: "return",
  dependencies,
})
  .annotate(Tool.Title, "Follow up a cross-provider agent")
  .annotate(Tool.Destructive, true)
  .annotate(Tool.OpenWorld, true);

const AgentInterruptTool = Tool.make("agent_interrupt", {
  description: CROSS_PROVIDER_AGENT_TOOL_DESCRIPTIONS.agent_interrupt,
  parameters: CrossProviderAgentInterruptInput,
  success: CrossProviderAgentInterruptOutput,
  failure: CrossProviderAgentFailure,
  failureMode: "return",
  dependencies,
})
  .annotate(Tool.Title, "Interrupt a cross-provider agent")
  .annotate(Tool.Destructive, true);

/** fork: the six `agent_*` cross-provider agent tools. */
export const CrossProviderAgentToolkit = Toolkit.make(
  AgentCatalogTool,
  AgentSpawnTool,
  AgentWaitTool,
  AgentResultTool,
  AgentFollowUpTool,
  AgentInterruptTool,
);
