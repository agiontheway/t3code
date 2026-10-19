import {
  OrchestrationV2ProviderTurnTokenUsage,
  TurnTokenUsage,
  type OrchestrationV2ProviderTurn,
  type OrchestrationV2ThreadShell,
} from "@t3tools/contracts";
import * as Schema from "effect/Schema";
import { delegatedTaskUsage } from "./DelegatedTaskUsage.ts";

// Read just the usage JSON, never whole provider-turn projections for a shell.
export const decodeShellTurnUsage = Schema.decodeEffect(
  Schema.fromJsonString(Schema.Array(TurnTokenUsage)),
);
export const decodeShellContextUsage = Schema.decodeEffect(
  Schema.fromJsonString(Schema.Array(OrchestrationV2ProviderTurnTokenUsage)),
);

/** Match the client selector: compare instants, with ties going to the later entry. */
export function latestShellContextUsage(
  reports: ReadonlyArray<OrchestrationV2ProviderTurnTokenUsage>,
): OrchestrationV2ProviderTurnTokenUsage | null {
  let latest: OrchestrationV2ProviderTurnTokenUsage | null = null;
  for (const report of reports) {
    if (latest === null || Date.parse(report.updatedAt) >= Date.parse(latest.updatedAt)) {
      latest = report;
    }
  }
  return latest;
}

export function threadShellUsage(input: {
  readonly providerTurns: ReadonlyArray<Pick<OrchestrationV2ProviderTurn, "turnTokenUsage">>;
  readonly contextUsage: {
    readonly usedTokens: number;
    readonly maxTokens?: number | null | undefined;
  } | null;
  readonly toolCallCount: number;
}): OrchestrationV2ThreadShell["usage"] {
  const usage = delegatedTaskUsage(input);
  if (input.contextUsage === null && usage.totalTokens === undefined && usage.toolUses === 0) {
    return undefined;
  }
  return {
    ...usage,
    ...(input.contextUsage === null
      ? {}
      : {
          contextUsedTokens: input.contextUsage.usedTokens,
          ...(input.contextUsage.maxTokens != null && input.contextUsage.maxTokens > 0
            ? { contextMaxTokens: input.contextUsage.maxTokens }
            : {}),
        }),
  };
}
