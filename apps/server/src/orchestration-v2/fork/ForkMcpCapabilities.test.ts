import { describe, expect, it } from "@effect/vitest";
import { ProviderDriverKind, ThreadId } from "@t3tools/contracts";

import { forkDisabledT3McpToolNames, forkMcpCapabilities } from "./ForkMcpCapabilities.ts";

const claude = ProviderDriverKind.make("claudeAgent");
const codex = ProviderDriverKind.make("codex");
const cursor = ProviderDriverKind.make("cursor");

const root = {
  lineage: {
    parentThreadId: null,
    relationshipToParent: null,
    rootThreadId: ThreadId.make("root"),
  },
  creationSource: "web",
} as const;
const forked = {
  lineage: {
    parentThreadId: ThreadId.make("root"),
    relationshipToParent: "fork",
    rootThreadId: ThreadId.make("root"),
  },
  creationSource: "web",
} as const;
const delegatedChild = {
  lineage: {
    parentThreadId: ThreadId.make("root"),
    relationshipToParent: "subagent",
    rootThreadId: ThreadId.make("root"),
  },
  creationSource: "mcp",
} as const;
const providerNativeChild = { ...delegatedChild, creationSource: "provider" } as const;

const everythingOn = { crossProviderAgentAccess: true, heartbeatAccess: true };

describe("forkMcpCapabilities", () => {
  it("grants a Claude or Codex root everything the settings allow", () => {
    for (const driver of [claude, codex]) {
      expect(
        forkMcpCapabilities({
          thread: root,
          grantAllowsOrchestration: false,
          driver,
          ...everythingOn,
        }),
      ).toEqual(["crossProviderAgents", "delegation", "heartbeat"]);
    }
    expect(
      forkMcpCapabilities({
        thread: forked,
        grantAllowsOrchestration: false,
        driver: codex,
        ...everythingOn,
      }),
    ).toEqual(["crossProviderAgents", "delegation", "heartbeat"]);
  });

  it("withholds the agent tools when cross-provider access is off, leaving native delegation", () => {
    expect(
      forkMcpCapabilities({
        thread: root,
        grantAllowsOrchestration: false,
        driver: claude,
        crossProviderAgentAccess: false,
        heartbeatAccess: true,
      }),
    ).toEqual(["delegation", "heartbeat"]);
  });

  it("never grants agent tools to an unsupported caller", () => {
    for (const driver of [cursor, undefined]) {
      expect(
        forkMcpCapabilities({
          thread: root,
          grantAllowsOrchestration: false,
          driver,
          ...everythingOn,
        }),
      ).toEqual(["delegation", "heartbeat"]);
    }
  });

  it("gives a child without an orchestration grant no spawn, agent or heartbeat tools", () => {
    expect(
      forkMcpCapabilities({
        thread: delegatedChild,
        grantAllowsOrchestration: false,
        driver: codex,
        ...everythingOn,
      }),
    ).toEqual([]);
  });

  it("gives a granted child the same tools as a root", () => {
    expect(
      forkMcpCapabilities({
        thread: delegatedChild,
        grantAllowsOrchestration: true,
        driver: claude,
        ...everythingOn,
      }),
    ).toEqual(["crossProviderAgents", "delegation", "heartbeat"]);
  });

  it("never offers heartbeat to a provider-native subagent or with the setting off", () => {
    expect(
      forkMcpCapabilities({
        thread: providerNativeChild,
        grantAllowsOrchestration: true,
        driver: codex,
        ...everythingOn,
      }),
    ).not.toContain("heartbeat");
    expect(
      forkMcpCapabilities({
        thread: root,
        grantAllowsOrchestration: false,
        driver: codex,
        crossProviderAgentAccess: true,
        heartbeatAccess: false,
      }),
    ).toEqual(["crossProviderAgents", "delegation"]);
  });
});

describe("forkDisabledT3McpToolNames", () => {
  it("hides the tools of every missing bit", () => {
    expect(forkDisabledT3McpToolNames(new Set(["orchestration"]))).toEqual([
      "agent_catalog",
      "agent_spawn",
      "agent_wait",
      "agent_result",
      "agent_follow_up",
      "agent_interrupt",
      "delegate_task",
      "t3_heartbeat_create",
      "t3_heartbeat_list",
      "t3_heartbeat_delete",
      "t3_heartbeat_wakeup",
    ]);
    expect(forkDisabledT3McpToolNames(new Set(["delegation", "heartbeat"]))).toEqual([
      "agent_catalog",
      "agent_spawn",
      "agent_wait",
      "agent_result",
      "agent_follow_up",
      "agent_interrupt",
    ]);
  });

  it("hides nothing for a fully granted credential or one without a capability set", () => {
    expect(
      forkDisabledT3McpToolNames(new Set(["crossProviderAgents", "delegation", "heartbeat"])),
    ).toEqual([]);
    expect(forkDisabledT3McpToolNames(undefined)).toEqual([]);
  });
});
