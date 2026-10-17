import { describe, expect, it } from "vite-plus/test";
import * as Schema from "effect/Schema";

import { ServerSettings, ServerSettingsPatch } from "./settings.ts";

const decodeServerSettings = Schema.decodeUnknownSync(ServerSettings);
const decodeServerSettingsPatch = Schema.decodeUnknownSync(ServerSettingsPatch);

describe("fork agent settings", () => {
  it("decodes settings saved before the fork settings existed with their defaults", () => {
    const settings = decodeServerSettings({});
    expect(settings.enableCrossProviderAgentAccess).toBe(false);
    expect(settings.crossProviderAgentMaxDepth).toBe(2);
    expect(settings.crossProviderAgentOutputCapChars).toBe(5000);
    expect(settings.crossProviderAgentRoutes).toEqual({});
    expect(settings.enableHeartbeatAccess).toBe(true);
  });

  it("defaults a route's models to every model", () => {
    expect(
      decodeServerSettings({ crossProviderAgentRoutes: { codex: { enabled: true } } })
        .crossProviderAgentRoutes,
    ).toEqual({ codex: { enabled: true, models: [] } });
  });

  it.each([1, 5])("accepts max depth %s", (depth) => {
    expect(decodeServerSettingsPatch({ crossProviderAgentMaxDepth: depth })).toEqual({
      crossProviderAgentMaxDepth: depth,
    });
  });

  it.each([0, 6, 1.5])("rejects max depth %s", (depth) => {
    expect(() => decodeServerSettings({ crossProviderAgentMaxDepth: depth })).toThrow();
    expect(() => decodeServerSettingsPatch({ crossProviderAgentMaxDepth: depth })).toThrow();
  });

  it.each([499, 200_001, 1000.5])("rejects output cap %s", (cap) => {
    expect(() => decodeServerSettings({ crossProviderAgentOutputCapChars: cap })).toThrow();
    expect(() => decodeServerSettingsPatch({ crossProviderAgentOutputCapChars: cap })).toThrow();
  });

  it("accepts heartbeat access as a project override", () => {
    expect(
      decodeServerSettings({
        projectSettingsOverrides: { project: { enableHeartbeatAccess: false } },
      }).projectSettingsOverrides,
    ).toEqual({ project: { enableHeartbeatAccess: false } });
  });
});
