import {
  CROSS_PROVIDER_AGENT_SUPPORTED_DRIVERS,
  isProviderAvailable,
  type CrossProviderAgentRoutes,
  type ProviderInstanceId,
  type ServerProvider,
} from "@t3tools/contracts";

/**
 * A provider instance the cross-provider tools may target, resolved against
 * live provider snapshots. `eligible` is the server-authoritative answer;
 * clients only render it.
 */
export interface CrossProviderAgentResolvedRoute {
  readonly providerInstanceId: ProviderInstanceId;
  readonly provider: ServerProvider;
  /** Model slugs the route permits; empty means every slug in the catalog. */
  readonly models: ReadonlyArray<string>;
}

/** Instances a route can be generated for: supported driver, enabled, installed, authenticated. */
export function isCrossProviderAgentCandidate(provider: ServerProvider): boolean {
  return (
    CROSS_PROVIDER_AGENT_SUPPORTED_DRIVERS.includes(provider.driver) &&
    isProviderAvailable(provider) &&
    provider.enabled &&
    provider.installed &&
    provider.auth.status === "authenticated"
  );
}

/**
 * Generated defaults: every candidate instance, all of its catalog models.
 * This is what an empty `crossProviderAgentRoutes` means and what
 * "Restore defaults" writes back.
 */
export function deriveDefaultCrossProviderAgentRoutes(
  providers: ReadonlyArray<ServerProvider>,
): CrossProviderAgentRoutes {
  return Object.fromEntries(
    providers
      .filter(isCrossProviderAgentCandidate)
      .map((provider) => [provider.instanceId, { enabled: true, models: [] as string[] }]),
  );
}

/**
 * Effective routes: explicit settings when present (only candidates that the
 * user left enabled), otherwise the generated defaults. An explicit route for
 * an instance that is no longer a candidate is dropped, never widened.
 */
export function resolveCrossProviderAgentRoutes(
  routes: CrossProviderAgentRoutes,
  providers: ReadonlyArray<ServerProvider>,
): ReadonlyArray<CrossProviderAgentResolvedRoute> {
  const explicit = Object.keys(routes).length > 0;
  const resolved: CrossProviderAgentResolvedRoute[] = [];
  for (const provider of providers) {
    if (!isCrossProviderAgentCandidate(provider)) continue;
    const route = routes[provider.instanceId];
    if (explicit && (route === undefined || !route.enabled)) continue;
    resolved.push({
      providerInstanceId: provider.instanceId,
      provider,
      models: route?.models ?? [],
    });
  }
  return resolved;
}
