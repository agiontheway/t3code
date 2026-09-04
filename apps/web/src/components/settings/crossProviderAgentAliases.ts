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

/**
 * Build model-facing aliases from the providers currently enabled on this
 * environment. Prefer the driver's default instance; with no enabled default,
 * use a sole enabled custom instance and leave ambiguous drivers unmapped.
 */
export function deriveDefaultCrossProviderAgentAliases(
  providers: ReadonlyArray<ServerProvider>,
): ServerSettings["crossProviderAgentAliases"] {
  const byDriver = new Map<string, ServerProvider[]>();
  for (const provider of providers) {
    if (!provider.enabled || provider.availability === "unavailable") continue;
    const candidates = byDriver.get(provider.driver) ?? [];
    candidates.push(provider);
    byDriver.set(provider.driver, candidates);
  }

  const aliases: Record<string, ProviderInstanceId> = {};
  for (const [driver, candidates] of byDriver) {
    const names = aliasesByDriver[driver];
    if (!names) continue;
    const defaultId = defaultInstanceIdForDriver(candidates[0]!.driver);
    const target = candidates.find((candidate) => candidate.instanceId === defaultId);
    const resolved = target ?? (candidates.length === 1 ? candidates[0] : undefined);
    if (!resolved) continue;
    for (const alias of names) aliases[alias] = resolved.instanceId;
  }
  return aliases as ServerSettings["crossProviderAgentAliases"];
}

/** Preserve every user-edited target and append only aliases not yet stored. */
export function mergeMissingCrossProviderAgentAliases(
  current: ServerSettings["crossProviderAgentAliases"],
  defaults: ServerSettings["crossProviderAgentAliases"],
): ServerSettings["crossProviderAgentAliases"] {
  return { ...defaults, ...current } as ServerSettings["crossProviderAgentAliases"];
}
