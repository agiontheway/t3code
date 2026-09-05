import { describe, expect, it } from "vite-plus/test";

import { buildCodexDeveloperInstructions } from "./CodexDeveloperInstructions.ts";

describe("CodexDeveloperInstructions", () => {
  it("describes cross-provider routing only when those MCP tools are attached", () => {
    const enabled = buildCodexDeveloperInstructions(
      "default",
      { model: "glm-5.3-flash", reasoningEffort: "low" },
      true,
      true,
    );
    const disabled = buildCodexDeveloperInstructions(
      "default",
      { model: "glm-5.3-flash", reasoningEffort: "low" },
      true,
      false,
    );
    expect(enabled).toContain("agent_catalog");
    expect(enabled).toContain("agent_spawn");
    expect(enabled).toContain("agent_wait");
    expect(enabled).toContain("takes precedence over matching skills");
    expect(enabled).toContain("Do not read or invoke those alternative dispatch skills");
    expect(enabled).toContain("Call `agent_wait` at most once");
    expect(enabled).toContain("Native `spawnAgent` remains correct only");
    expect(disabled).not.toContain("agent_catalog");
    expect(disabled).not.toContain("agent_spawn");
  });
});
