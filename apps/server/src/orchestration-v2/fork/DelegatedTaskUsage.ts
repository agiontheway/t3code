import type { OrchestrationV2ProviderTurn, OrchestrationV2Subagent } from "@t3tools/contracts";

export type DelegatedTaskUsage = NonNullable<OrchestrationV2Subagent["usage"]>;

/**
 * Child telemetry mirrored onto an app-owned delegated task row: tokens summed
 * over every child turn that reported complete or partial usage, plus the
 * child's tool_call node count. `totalTokens` stays absent until some turn
 * reports usage, so the client can show an unknown total instead of zero.
 */
export function delegatedTaskUsage(input: {
  readonly providerTurns: ReadonlyArray<Pick<OrchestrationV2ProviderTurn, "turnTokenUsage">>;
  readonly toolCallCount: number;
}): DelegatedTaskUsage {
  let totalTokens: number | undefined;
  for (const { turnTokenUsage: usage } of input.providerTurns) {
    if (usage === undefined || usage.usageStatus === "unavailable") continue;
    totalTokens =
      (totalTokens ?? 0) +
      (usage.inputTokens ?? 0) +
      (usage.cachedInputTokens ?? 0) +
      (usage.cacheCreationTokens ?? 0) +
      (usage.outputTokens ?? 0);
  }
  return {
    ...(totalTokens === undefined ? {} : { totalTokens }),
    toolUses: input.toolCallCount,
  };
}

export function sameDelegatedTaskUsage(
  left: OrchestrationV2Subagent["usage"],
  right: OrchestrationV2Subagent["usage"],
): boolean {
  return left?.totalTokens === right?.totalTokens && left?.toolUses === right?.toolUses;
}
