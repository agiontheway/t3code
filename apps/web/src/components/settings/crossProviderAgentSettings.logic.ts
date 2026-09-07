import type {
  CrossProviderAgentRoutes,
  ProviderDriverKind,
  ProviderInstanceId,
  ServerProvider,
} from "@t3tools/contracts";

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
 * The map an edit starts from. `defaults` is the server's generated map
 * (`ServerConfig.crossProviderAgentRouteDefaults`); the client never derives
 * candidacy from its own provider snapshot, which can be partial or mid-probe.
 * Editing the defaults materialises them first so the first switch does not
 * silently drop every other candidate route from the stored map.
 */
export function materializeCrossProviderRoutes(
  routes: CrossProviderAgentRoutes,
  defaults: CrossProviderAgentRoutes,
): CrossProviderAgentRoutes {
  return hasCrossProviderRouteEdits(routes) ? routes : defaults;
}

/**
 * Rows are the server's candidates (the keys of `defaults`), drawn with the
 * provider snapshot's names and catalogs. A candidate missing from an edited
 * map renders disabled and can be switched on.
 */
export function viewCrossProviderRoutes(
  routes: CrossProviderAgentRoutes,
  defaults: CrossProviderAgentRoutes,
  providers: ReadonlyArray<ServerProvider>,
): ReadonlyArray<CrossProviderRouteView> {
  const effective = materializeCrossProviderRoutes(routes, defaults);
  return providers
    .filter((provider) => provider.instanceId in defaults)
    .map((provider) => {
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
  defaults: CrossProviderAgentRoutes,
  providerInstanceId: ProviderInstanceId,
  enabled: boolean,
): CrossProviderAgentRoutes {
  const base = materializeCrossProviderRoutes(routes, defaults);
  return {
    ...base,
    [providerInstanceId]: { enabled, models: base[providerInstanceId]?.models ?? [] },
  };
}

/**
 * Toggle one model on a route. The stored list stays explicit while it is a
 * strict subset of the catalog and collapses back to "all" (empty) when the
 * user re-checks the last one. Unchecking the only selected model disables
 * the route instead: an empty list would mean "all", the opposite of what
 * was asked, and the route switch is the way back.
 */
export function setCrossProviderRouteModel(
  routes: CrossProviderAgentRoutes,
  defaults: CrossProviderAgentRoutes,
  providers: ReadonlyArray<ServerProvider>,
  providerInstanceId: ProviderInstanceId,
  slug: string,
  checked: boolean,
): CrossProviderAgentRoutes {
  const base = materializeCrossProviderRoutes(routes, defaults);
  const catalog =
    providers
      .find((provider) => provider.instanceId === providerInstanceId)
      ?.models.map((model) => model.slug) ?? [];
  const route = base[providerInstanceId] ?? { enabled: true, models: [] };
  const current = route.models.length === 0 ? catalog : route.models;
  const next = checked
    ? catalog.filter((candidate) => candidate === slug || current.includes(candidate))
    : current.filter((candidate) => candidate !== slug);
  if (next.length === 0) {
    return { ...base, [providerInstanceId]: { enabled: false, models: [] } };
  }
  const models = next.length === catalog.length ? [] : next;
  return { ...base, [providerInstanceId]: { enabled: route.enabled, models } };
}
