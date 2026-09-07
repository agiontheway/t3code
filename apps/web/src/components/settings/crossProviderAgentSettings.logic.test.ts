import {
  ProviderDriverKind,
  ProviderInstanceId,
  type CrossProviderAgentRoutes,
  type ServerProvider,
} from "@t3tools/contracts";
import { describe, expect, it } from "vite-plus/test";

import {
  hasCrossProviderRouteEdits,
  setCrossProviderRouteEnabled,
  setCrossProviderRouteModel,
  viewCrossProviderRoutes,
} from "./crossProviderAgentSettings.logic";

const CLAUDE = ProviderInstanceId.make("claudeAgent");
const CODEX = ProviderInstanceId.make("codex");
const CURSOR = ProviderInstanceId.make("cursor");
const PROBING = ProviderInstanceId.make("codex-probing");

const provider = (
  instanceId: ProviderInstanceId,
  driver: string,
  models: ReadonlyArray<string>,
  overrides?: Partial<ServerProvider>,
): ServerProvider => ({
  instanceId,
  driver: ProviderDriverKind.make(driver),
  enabled: true,
  installed: true,
  version: "1.0.0",
  status: "ready",
  auth: { status: "authenticated" },
  checkedAt: "2026-09-07T00:00:00.000Z",
  models: models.map((slug) => ({ slug, name: slug, isCustom: false, capabilities: null })),
  slashCommands: [],
  skills: [],
  ...overrides,
});

// The client snapshot: one instance is mid-probe (auth unknown) yet the
// SERVER already lists it as a candidate. Candidacy comes from the server.
const providers = [
  provider(CLAUDE, "claudeAgent", ["haiku", "fable"]),
  provider(CODEX, "codex", ["sol", "luna"]),
  provider(CURSOR, "cursor", ["composer"]),
  provider(PROBING, "codex", ["sol"], { auth: { status: "unknown" } }),
];
const serverDefaults: CrossProviderAgentRoutes = {
  [CLAUDE]: { enabled: true, models: [] },
  [CODEX]: { enabled: true, models: [] },
  [PROBING]: { enabled: true, models: [] },
};

describe("crossProviderAgentSettings.logic", () => {
  it("renders the server's candidates, not a client-side derivation", () => {
    const view = viewCrossProviderRoutes({}, serverDefaults, providers);
    expect(
      view.map((route) => [route.providerInstanceId, route.enabled, route.selectedModels]),
    ).toEqual([
      [CLAUDE, true, []],
      [CODEX, true, []],
      [PROBING, true, []],
    ]);
    expect(hasCrossProviderRouteEdits({})).toBe(false);
  });

  it("materialises the server defaults on the first edit so other routes survive", () => {
    const routes = setCrossProviderRouteEnabled({}, serverDefaults, CODEX, false);
    expect(routes).toEqual({
      [CLAUDE]: { enabled: true, models: [] },
      [CODEX]: { enabled: false, models: [] },
      [PROBING]: { enabled: true, models: [] },
    });
    expect(hasCrossProviderRouteEdits(routes)).toBe(true);
    // A candidate the server adds later renders disabled and can be enabled.
    const later = { ...serverDefaults, [CURSOR]: { enabled: true, models: [] } };
    const view = viewCrossProviderRoutes(routes, later, providers);
    expect(view.find((route) => route.providerInstanceId === CURSOR)?.enabled).toBe(false);
  });

  it("keeps model lists explicit while a subset and disables the route on the last uncheck", () => {
    const one = setCrossProviderRouteModel({}, serverDefaults, providers, CODEX, "luna", false);
    expect(one[CODEX]).toEqual({ enabled: true, models: ["sol"] });
    // Unchecking the only remaining model cannot mean "all": it turns the route off.
    const off = setCrossProviderRouteModel(one, serverDefaults, providers, CODEX, "sol", false);
    expect(off[CODEX]).toEqual({ enabled: false, models: [] });
    const all = setCrossProviderRouteModel(one, serverDefaults, providers, CODEX, "luna", true);
    expect(all[CODEX]).toEqual({ enabled: true, models: [] });
    // Catalog order is preserved regardless of the order models were re-added.
    const none = setCrossProviderRouteModel({}, serverDefaults, providers, CLAUDE, "haiku", false);
    expect(
      setCrossProviderRouteModel(none, serverDefaults, providers, CLAUDE, "haiku", true)[CLAUDE],
    ).toEqual({ enabled: true, models: [] });
  });
});
