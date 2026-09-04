import { ProviderDriverKind, ProviderInstanceId, type ServerProvider } from "@t3tools/contracts";
import { describe, expect, it } from "vite-plus/test";

import {
  deriveDefaultCrossProviderAgentAliases,
  mergeMissingCrossProviderAgentAliases,
} from "./crossProviderAgentAliases";

const provider = (input: {
  readonly instanceId: string;
  readonly driver: string;
  readonly enabled?: boolean;
}): ServerProvider =>
  ({
    instanceId: ProviderInstanceId.make(input.instanceId),
    driver: ProviderDriverKind.make(input.driver),
    enabled: input.enabled ?? true,
    availability: "available",
  }) as ServerProvider;

describe("cross-provider agent aliases", () => {
  it("prefers enabled native defaults over custom instances using the same driver", () => {
    expect(
      deriveDefaultCrossProviderAgentAliases([
        provider({ instanceId: "codex_custom", driver: "codex" }),
        provider({ instanceId: "codex", driver: "codex" }),
        provider({ instanceId: "claudeAgent", driver: "claudeAgent" }),
      ]),
    ).toEqual({
      OpenAI: ProviderInstanceId.make("codex"),
      ChatGPT: ProviderInstanceId.make("codex"),
      Claude: ProviderInstanceId.make("claudeAgent"),
      Anthropic: ProviderInstanceId.make("claudeAgent"),
      "Claude Code": ProviderInstanceId.make("claudeAgent"),
    });
  });

  it("uses a sole enabled custom instance and leaves ambiguous drivers unmapped", () => {
    expect(
      deriveDefaultCrossProviderAgentAliases([
        provider({ instanceId: "claude_work", driver: "claudeAgent" }),
        provider({ instanceId: "codex_one", driver: "codex" }),
        provider({ instanceId: "codex_two", driver: "codex" }),
        provider({ instanceId: "codex", driver: "codex", enabled: false }),
      ]),
    ).toEqual({
      Claude: ProviderInstanceId.make("claude_work"),
      Anthropic: ProviderInstanceId.make("claude_work"),
      "Claude Code": ProviderInstanceId.make("claude_work"),
    });
  });

  it("adds missing defaults without replacing manual targets", () => {
    expect(
      mergeMissingCrossProviderAgentAliases(
        { Claude: ProviderInstanceId.make("claude_personal") },
        {
          Claude: ProviderInstanceId.make("claudeAgent"),
          OpenAI: ProviderInstanceId.make("codex"),
        },
      ),
    ).toEqual({
      Claude: ProviderInstanceId.make("claude_personal"),
      OpenAI: ProviderInstanceId.make("codex"),
    });
  });
});
