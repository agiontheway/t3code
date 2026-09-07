import { ProviderDriverKind, ProviderInstanceId, type ServerProvider } from "@t3tools/contracts";
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

const providers = [
  provider(CLAUDE, "claudeAgent", ["haiku", "fable"]),
  provider(CODEX, "codex", ["sol", "luna"]),
  provider(CURSOR, "cursor", ["composer"]),
  provider(ProviderInstanceId.make("codex-work"), "codex", ["sol"], {
    auth: { status: "unauthenticated" },
  }),
];

describe("crossProviderAgentSettings.logic", () => {
  it("renders generated defaults for candidates only when nothing is stored", () => {
    const view = viewCrossProviderRoutes({}, providers);
    expect(
      view.map((route) => [route.providerInstanceId, route.enabled, route.selectedModels]),
    ).toEqual([
      [CLAUDE, true, []],
      [CODEX, true, []],
    ]);
    expect(hasCrossProviderRouteEdits({})).toBe(false);
  });

  it("materialises the defaults on the first edit so other routes survive", () => {
    const routes = setCrossProviderRouteEnabled({}, providers, CODEX, false);
    expect(routes).toEqual({
      [CLAUDE]: { enabled: true, models: [] },
      [CODEX]: { enabled: false, models: [] },
    });
    expect(hasCrossProviderRouteEdits(routes)).toBe(true);
    expect(viewCrossProviderRoutes(routes, providers).map((route) => route.enabled)).toEqual([
      true,
      false,
    ]);
  });

  it("keeps model lists explicit while a subset and collapses back to all", () => {
    const one = setCrossProviderRouteModel({}, providers, CODEX, "luna", false);
    expect(one[CODEX]).toEqual({ enabled: true, models: ["sol"] });
    // The only remaining model cannot be unchecked: empty would mean "all".
    expect(setCrossProviderRouteModel(one, providers, CODEX, "sol", false)).toBe(one);
    const all = setCrossProviderRouteModel(one, providers, CODEX, "luna", true);
    expect(all[CODEX]).toEqual({ enabled: true, models: [] });
    // Catalog order is preserved regardless of the order models were re-added.
    const none = setCrossProviderRouteModel(one, providers, CLAUDE, "haiku", false);
    expect(setCrossProviderRouteModel(none, providers, CLAUDE, "haiku", true)[CLAUDE]).toEqual({
      enabled: true,
      models: [],
    });
  });
});
