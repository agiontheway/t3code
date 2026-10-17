import {
  CROSS_PROVIDER_AGENT_SUPPORTED_DRIVERS,
  CROSS_PROVIDER_AGENT_TOOL_NAMES,
  isProviderNativeSubagentThread,
  type OrchestrationV2AppThread,
  type ProviderDriverKind,
} from "@t3tools/contracts";

/** The fork's MCP capability bits, minted per thread credential. */
export const FORK_MCP_CAPABILITIES = ["crossProviderAgents", "delegation", "heartbeat"] as const;
export type ForkMcpCapability = (typeof FORK_MCP_CAPABILITIES)[number];

export const HEARTBEAT_TOOL_NAMES = [
  "t3_heartbeat_create",
  "t3_heartbeat_list",
  "t3_heartbeat_delete",
  "t3_heartbeat_wakeup",
] as const;

/**
 * Bare `t3-code` tool names each bit unlocks. The MCP server serves one tool list to every
 * credential, so a session without the bit has these names hidden by its provider client
 * (Claude `disallowedTools`, Codex `disabled_tools`) and the handler refuses the call.
 */
const GATED_T3_MCP_TOOLS: Record<ForkMcpCapability, ReadonlyArray<string>> = {
  crossProviderAgents: CROSS_PROVIDER_AGENT_TOOL_NAMES,
  delegation: ["delegate_task"],
  heartbeat: HEARTBEAT_TOOL_NAMES,
};

/**
 * The fork bits for one thread's credential.
 *
 * A thread is a root unless it is a subagent child. A child may delegate only when its grant
 * allows orchestration; a child without a grant row (native `delegate_task` before the grant
 * existed, provider-native subagents) may not. Cross-provider agents also need the setting and a
 * Claude or Codex caller; heartbeat needs its project-resolved setting and is never offered to a
 * provider-native subagent thread.
 */
export function forkMcpCapabilities(input: {
  readonly thread: Pick<OrchestrationV2AppThread, "lineage" | "creationSource">;
  readonly grantAllowsOrchestration: boolean;
  readonly driver: ProviderDriverKind | undefined;
  readonly crossProviderAgentAccess: boolean;
  readonly heartbeatAccess: boolean;
}): ReadonlyArray<ForkMcpCapability> {
  const isRoot = input.thread.lineage.relationshipToParent !== "subagent";
  const delegation = isRoot || input.grantAllowsOrchestration;
  const crossProviderAgents =
    delegation &&
    input.crossProviderAgentAccess &&
    input.driver !== undefined &&
    CROSS_PROVIDER_AGENT_SUPPORTED_DRIVERS.includes(input.driver);
  const heartbeat =
    delegation && input.heartbeatAccess && !isProviderNativeSubagentThread(input.thread);
  const granted: Record<ForkMcpCapability, boolean> = {
    crossProviderAgents,
    delegation,
    heartbeat,
  };
  return FORK_MCP_CAPABILITIES.filter((capability) => granted[capability]);
}

/**
 * Bare `t3-code` tool names to hide for a credential. A config without a capability set predates
 * capability minting and hides nothing; every credential the session manager mints carries one.
 */
export function forkDisabledT3McpToolNames(
  capabilities: ReadonlySet<string> | undefined,
): ReadonlyArray<string> {
  if (capabilities === undefined) return [];
  return FORK_MCP_CAPABILITIES.filter((capability) => !capabilities.has(capability)).flatMap(
    (capability) => GATED_T3_MCP_TOOLS[capability],
  );
}
