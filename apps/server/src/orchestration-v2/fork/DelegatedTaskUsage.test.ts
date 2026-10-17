import type { TurnTokenUsage } from "@t3tools/contracts";
import { describe, expect, it } from "vite-plus/test";

import { delegatedTaskUsage, sameDelegatedTaskUsage } from "./DelegatedTaskUsage.ts";

const turn = (turnTokenUsage?: TurnTokenUsage) => ({ turnTokenUsage });

describe("delegatedTaskUsage", () => {
  it("sums input, cached, cache-creation and output tokens across complete and partial turns", () => {
    expect(
      delegatedTaskUsage({
        providerTurns: [
          turn({
            usageScope: "main_agent",
            usageStatus: "complete",
            hasSubagents: false,
            inputTokens: 1000,
            cachedInputTokens: 200,
            cacheCreationTokens: 30,
            outputTokens: 4,
            reasoningTokens: 999,
          }),
          turn({
            usageScope: "main_agent",
            usageStatus: "partial",
            hasSubagents: false,
            outputTokens: 50,
          }),
          turn({ usageScope: "main_agent", usageStatus: "unavailable", hasSubagents: false }),
          turn(),
        ],
        toolCallCount: 7,
      }),
    ).toEqual({ totalTokens: 1284, toolUses: 7 });
  });

  it("leaves the total unknown when no turn reported usage", () => {
    expect(
      delegatedTaskUsage({
        providerTurns: [
          turn(),
          turn({ usageScope: "main_agent", usageStatus: "unavailable", hasSubagents: false }),
        ],
        toolCallCount: 0,
      }),
    ).toEqual({ toolUses: 0 });
    expect(delegatedTaskUsage({ providerTurns: [], toolCallCount: 3 })).toEqual({ toolUses: 3 });
  });

  it("compares rows by their numbers", () => {
    expect(sameDelegatedTaskUsage(undefined, undefined)).toBe(true);
    expect(sameDelegatedTaskUsage(undefined, { toolUses: 0 })).toBe(false);
    expect(sameDelegatedTaskUsage({ toolUses: 2 }, { totalTokens: 0, toolUses: 2 })).toBe(false);
    expect(
      sameDelegatedTaskUsage({ totalTokens: 10, toolUses: 2 }, { totalTokens: 10, toolUses: 2 }),
    ).toBe(true);
  });
});
