import { ProviderInstanceId, type ServerConfig } from "@t3tools/contracts";
import { describe, expect, it } from "@effect/vitest";
import * as Option from "effect/Option";

import { applyServerConfigProjection } from "./serverConfigProjection.ts";

const CODEX = ProviderInstanceId.make("codex");
const CLAUDE = ProviderInstanceId.make("claudeAgent");

const CONFIG = {
  availableEditors: [],
  issues: [],
  keybindings: {},
  keybindingsConfigPath: null,
  observability: null,
  providers: [],
  settings: {},
  environment: { capabilities: {} },
  crossProviderAgentRouteDefaults: { [CODEX]: { enabled: true, models: [] } },
} as unknown as ServerConfig;

// fork
describe("cross-provider route defaults projection", () => {
  const snapshot = applyServerConfigProjection(Option.none(), {
    version: 1,
    type: "snapshot",
    config: CONFIG,
  });

  it("replaces the route defaults with the ones a provider change carries", () => {
    const defaults = { [CLAUDE]: { enabled: true, models: [] } };
    const projected = applyServerConfigProjection(snapshot, {
      version: 1,
      type: "providerStatuses",
      payload: { providers: [], crossProviderAgentRouteDefaults: defaults },
    });
    expect(Option.getOrThrow(projected).config.crossProviderAgentRouteDefaults).toEqual(defaults);
  });

  it("keeps the snapshot's route defaults when an older server omits them", () => {
    const projected = applyServerConfigProjection(snapshot, {
      version: 1,
      type: "providerStatuses",
      payload: { providers: [] },
    });
    expect(Option.getOrThrow(projected).config.crossProviderAgentRouteDefaults).toEqual(
      CONFIG.crossProviderAgentRouteDefaults,
    );
  });
});
