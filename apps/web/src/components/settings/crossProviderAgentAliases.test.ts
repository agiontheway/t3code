import { ProviderDriverKind, ProviderInstanceId, type ServerProvider } from "@t3tools/contracts";
import { describe, expect, it } from "vite-plus/test";

import {
  deriveDefaultCrossProviderAgentAliases,
  mergeMissingCrossProviderAgentAliases,
} from "./crossProviderAgentAliases";

const provider = (
  driver: string,
  instanceId: string,
  overrides: Partial<ServerProvider> = {},
): ServerProvider => ({
  instanceId: ProviderInstanceId.make(instanceId),
  driver: ProviderDriverKind.make(driver),
  enabled: true,
  installed: true,
  version: "1.0.0",
  status: "ready",
  auth: { status: "authenticated" },
  checkedAt: "2026-09-04T00:00:00.000Z",
  models: [],
  slashCommands: [],
  skills: [],
  ...overrides,
});

describe("cross-provider agent aliases", () => {
  it("derives friendly aliases for enabled default provider instances", () => {
    expect(
      deriveDefaultCrossProviderAgentAliases([
        provider("codex", "codex"),
        provider("claudeAgent", "claudeAgent"),
        provider("cursor", "cursor"),
      ]),
    ).toEqual({
      OpenAI: "codex",
      ChatGPT: "codex",
      Claude: "claudeAgent",
      Anthropic: "claudeAgent",
      "Claude Code": "claudeAgent",
      "Cursor AI": "cursor",
    });
  });

  it("does not guess when several custom instances of one driver are ambiguous", () => {
    expect(
      deriveDefaultCrossProviderAgentAliases([
        provider("codex", "codex-work"),
        provider("codex", "codex-personal"),
      ]),
    ).toEqual({});
  });

  it("preserves edits while adding newly available defaults", () => {
    expect(
      mergeMissingCrossProviderAgentAliases(
        { OpenAI: ProviderInstanceId.make("codex-work") },
        {
          OpenAI: ProviderInstanceId.make("codex"),
          Claude: ProviderInstanceId.make("claudeAgent"),
        },
      ),
    ).toEqual({ OpenAI: "codex-work", Claude: "claudeAgent" });
  });
});
