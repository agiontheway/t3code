import type {
  CrossProviderAgentRoutes,
  ProviderDriverKind,
  ProviderInstanceId,
  ServerProvider,
} from "@t3tools/contracts";
import {
  deriveDefaultCrossProviderAgentRoutes,
  isCrossProviderAgentCandidate,
} from "@t3tools/shared/crossProviderAgentRoutes";

/**
 * One row of the route editor. `selectedModels` empty means every model in
 * the catalog, which is also what the generated defaults say.
 */
export interface CrossProviderRouteView {
  readonly providerInstanceId: ProviderInstanceId;
  readonly displayName: string;
  readonly driver: ProviderDriverKind;
  readonly enabled: boolean;
  readonly catalog: ReadonlyArray<{ readonly slug: string; readonly name: string }>;
  readonly selectedModels: ReadonlyArray<string>;
}

/** Stored routes are edits; an empty map means the generated defaults apply. */
export function hasCrossProviderRouteEdits(routes: CrossProviderAgentRoutes): boolean {
  return Object.keys(routes).length > 0;
}

/**
 * The map an edit starts from. Editing the generated defaults must first
 * materialise them, otherwise the first switch would leave every other
 * candidate route out of the stored map and silently disable it.
 */
export function materializeCrossProviderRoutes(
  routes: CrossProviderAgentRoutes,
  providers: ReadonlyArray<ServerProvider>,
): CrossProviderAgentRoutes {
  return hasCrossProviderRouteEdits(routes)
    ? routes
    : deriveDefaultCrossProviderAgentRoutes(providers);
}

export function viewCrossProviderRoutes(
  routes: CrossProviderAgentRoutes,
  providers: ReadonlyArray<ServerProvider>,
): ReadonlyArray<CrossProviderRouteView> {
  const effective = materializeCrossProviderRoutes(routes, providers);
  return providers.filter(isCrossProviderAgentCandidate).map((provider) => {
    const route = effective[provider.instanceId];
    return {
      providerInstanceId: provider.instanceId,
      displayName: provider.displayName ?? provider.instanceId,
      driver: provider.driver,
      enabled: route?.enabled ?? false,
      catalog: provider.models.map((model) => ({ slug: model.slug, name: model.name })),
      selectedModels: route?.models ?? [],
    };
  });
}

export function setCrossProviderRouteEnabled(
  routes: CrossProviderAgentRoutes,
  providers: ReadonlyArray<ServerProvider>,
  providerInstanceId: ProviderInstanceId,
  enabled: boolean,
): CrossProviderAgentRoutes {
  const base = materializeCrossProviderRoutes(routes, providers);
  return {
    ...base,
    [providerInstanceId]: { enabled, models: base[providerInstanceId]?.models ?? [] },
  };
}

/**
 * Toggle one model on a route. The stored list stays explicit while it is a
 * strict subset of the catalog and collapses back to "all" (empty) when the
 * user re-checks the last one. Unchecking the only selected model is a no-op:
 * an empty list would mean "all", the opposite of what was asked.
 */
export function setCrossProviderRouteModel(
  routes: CrossProviderAgentRoutes,
  providers: ReadonlyArray<ServerProvider>,
  providerInstanceId: ProviderInstanceId,
  slug: string,
  checked: boolean,
): CrossProviderAgentRoutes {
  const base = materializeCrossProviderRoutes(routes, providers);
  const catalog =
    providers
      .find((provider) => provider.instanceId === providerInstanceId)
      ?.models.map((model) => model.slug) ?? [];
  const route = base[providerInstanceId] ?? { enabled: true, models: [] };
  const current = route.models.length === 0 ? catalog : route.models;
  const next = checked
    ? catalog.filter((candidate) => candidate === slug || current.includes(candidate))
    : current.filter((candidate) => candidate !== slug);
  if (next.length === 0) return routes;
  const models = next.length === catalog.length ? [] : next;
  return { ...base, [providerInstanceId]: { enabled: route.enabled, models } };
}
