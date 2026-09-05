import {
  defaultInstanceIdForDriver,
  type ProviderInstanceId,
  type ServerProvider,
  type ServerSettings,
} from "@t3tools/contracts";

const aliasesByDriver: Readonly<Record<string, ReadonlyArray<string>>> = {
  codex: ["OpenAI", "ChatGPT"],
  claudeAgent: ["Claude", "Anthropic", "Claude Code"],
  cursor: ["Cursor AI"],
  grok: ["xAI"],
  antigravity: ["Google", "Gemini"],
};

export function deriveDefaultCrossProviderAgentAliases(
  providers: ReadonlyArray<ServerProvider>,
): ServerSettings["crossProviderAgentAliases"] {
  const byDriver = new Map<string, ServerProvider[]>();
  for (const provider of providers) {
    if (!provider.enabled || !provider.installed || provider.availability === "unavailable")
      continue;
    const candidates = byDriver.get(provider.driver) ?? [];
    candidates.push(provider);
    byDriver.set(provider.driver, candidates);
  }
  const aliases: Record<string, ProviderInstanceId> = {};
  for (const [driver, candidates] of byDriver) {
    const names = aliasesByDriver[driver];
    if (!names || candidates.length === 0) continue;
    const defaultId = defaultInstanceIdForDriver(candidates[0]!.driver);
    const resolved =
      candidates.find((candidate) => candidate.instanceId === defaultId) ??
      (candidates.length === 1 ? candidates[0] : undefined);
    if (!resolved) continue;
    for (const alias of names) aliases[alias] = resolved.instanceId;
  }
  return aliases as ServerSettings["crossProviderAgentAliases"];
}

export function mergeMissingCrossProviderAgentAliases(
  current: ServerSettings["crossProviderAgentAliases"],
  defaults: ServerSettings["crossProviderAgentAliases"],
): ServerSettings["crossProviderAgentAliases"] {
  return { ...defaults, ...current } as ServerSettings["crossProviderAgentAliases"];
}
