import { ProviderDriverKind, ProviderInstanceId, type ServerProvider } from "@t3tools/contracts";
import { describe, expect, it } from "vite-plus/test";

import {
  deriveDefaultCrossProviderAgentRoutes,
  resolveCrossProviderAgentRoutes,
} from "./crossProviderAgentRoutes.ts";

function provider(
  instanceId: string,
  driver: string,
  overrides: Partial<ServerProvider> = {},
): ServerProvider {
  return {
    instanceId: ProviderInstanceId.make(instanceId),
    driver: ProviderDriverKind.make(driver),
    enabled: true,
    installed: true,
    version: null,
    status: "ready",
    auth: { status: "authenticated" },
    checkedAt: "2026-07-27T00:00:00.000Z",
    models: [],
    slashCommands: [],
    skills: [],
    ...overrides,
  };
}

const codex = provider("codex", "codex");
const claude = provider("claudeAgent", "claudeAgent");
// API-key-backed instances report `unknown` auth and are legitimate targets.
const codexApiKey = provider("codex_openrouter", "codex", { auth: { status: "unknown" } });
const claudeSignedOut = provider("claude_work", "claudeAgent", {
  auth: { status: "unauthenticated" },
});
const cursor = provider("cursor", "cursor");
const codexDisabled = provider("codex_off", "codex", { enabled: false });
const codexMissing = provider("codex_missing", "codex", { installed: false });
const codexUnavailable = provider("codex_shadow", "codex", { availability: "unavailable" });

const allProviders = [
  codex,
  claude,
  codexApiKey,
  claudeSignedOut,
  cursor,
  codexDisabled,
  codexMissing,
  codexUnavailable,
];

describe("deriveDefaultCrossProviderAgentRoutes", () => {
  it("routes every enabled, installed, not signed-out Claude/Codex instance with all models", () => {
    expect(deriveDefaultCrossProviderAgentRoutes(allProviders)).toEqual({
      [codex.instanceId]: { enabled: true, models: [] },
      [claude.instanceId]: { enabled: true, models: [] },
      [codexApiKey.instanceId]: { enabled: true, models: [] },
    });
  });
});

describe("resolveCrossProviderAgentRoutes", () => {
  it("falls back to the generated defaults when no routes are configured", () => {
    expect(
      resolveCrossProviderAgentRoutes({}, allProviders).map((route) => route.providerInstanceId),
    ).toEqual([codex.instanceId, claude.instanceId, codexApiKey.instanceId]);
  });

  it("keeps only enabled explicit routes and never widens to unlisted instances", () => {
    const resolved = resolveCrossProviderAgentRoutes(
      {
        [codex.instanceId]: { enabled: true, models: ["gpt-5.6-sol"] },
        [claude.instanceId]: { enabled: false, models: [] },
      },
      allProviders,
    );
    expect(resolved).toEqual([
      { providerInstanceId: codex.instanceId, provider: codex, models: ["gpt-5.6-sol"] },
    ]);
  });

  it("drops an explicit route whose instance is no longer a candidate", () => {
    expect(
      resolveCrossProviderAgentRoutes(
        {
          [claudeSignedOut.instanceId]: { enabled: true, models: [] },
          [cursor.instanceId]: { enabled: true, models: [] },
          [codexDisabled.instanceId]: { enabled: true, models: [] },
          [codexUnavailable.instanceId]: { enabled: true, models: [] },
        },
        allProviders,
      ),
    ).toEqual([]);
  });
});
