import { describe, expect, it } from "@effect/vitest";
import {
  EnvironmentId,
  HeartbeatCronExpression,
  HeartbeatJobId,
  HeartbeatPrompt,
  HeartbeatTimeZone,
  ProviderInstanceId,
  ThreadId,
} from "@t3tools/contracts";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import * as McpSchema from "effect/unstable/ai/McpSchema";

import * as McpInvocationContext from "../../McpInvocationContext.ts";
import { HeartbeatCreateToolInput, HeartbeatToolkit, HeartbeatWakeupToolInput } from "./tools.ts";

const decodeCreateInput = Schema.decodeUnknownSync(HeartbeatCreateToolInput);
const isWakeup = Schema.is(HeartbeatWakeupToolInput);
const isCron = Schema.is(HeartbeatCronExpression);
const isJobId = Schema.is(HeartbeatJobId);
const isPrompt = Schema.is(HeartbeatPrompt);
const isTimeZone = Schema.is(HeartbeatTimeZone);
const initializePayload = {
  protocolVersion: "2025-06-18",
  capabilities: {},
  clientInfo: { name: "heartbeat-toolkit-test", version: "1.0.0" },
};

describe("heartbeat toolkit schemas", () => {
  it("exposes exactly the four heartbeat tools", () => {
    expect(Object.keys(HeartbeatToolkit.tools)).toEqual([
      "t3_heartbeat_create",
      "t3_heartbeat_list",
      "t3_heartbeat_delete",
      "t3_heartbeat_wakeup",
    ]);
  });

  it("defaults recurring without changing the durable compatibility field", () => {
    expect(
      decodeCreateInput({
        cron: "0 9 * * 1-5",
        prompt: "  preserve this prompt  ",
        durable: false,
      }),
    ).toMatchObject({
      cron: "0 9 * * 1-5",
      prompt: "  preserve this prompt  ",
      recurring: true,
      durable: false,
    });
  });

  it("validates public heartbeat scalar contracts", () => {
    expect(isJobId("01abcdef")).toBe(true);
    expect(isJobId("01ABCDEf")).toBe(false);
    expect(isJobId("abcdefg")).toBe(false);
    expect(isCron("*/5 * * * *")).toBe(true);
    expect(isCron("0 0 * * * *")).toBe(false);
    expect(isCron("@hourly")).toBe(false);
    expect(isTimeZone("Europe/London")).toBe(true);
    expect(isTimeZone("Not/A_Zone")).toBe(false);
    expect(isPrompt("  keep surrounding whitespace  ")).toBe(true);
    expect(isPrompt("   \n\t")).toBe(false);
  });

  it("accepts one wakeup mode and rejects contradictory or non-finite arms", () => {
    expect(isWakeup({ stop: true })).toBe(true);
    expect(isWakeup({ delaySeconds: 1, prompt: "continue", noop: true })).toBe(true);
    expect(isWakeup({ stop: true, delaySeconds: 1, prompt: "continue" })).toBe(false);
    expect(isWakeup({ stop: false })).toBe(false);
    expect(isWakeup({ delaySeconds: 0, prompt: "continue" })).toBe(false);
    expect(isWakeup({ delaySeconds: Number.POSITIVE_INFINITY, prompt: "continue" })).toBe(false);
  });

  it("documents lifecycle behavior in the tool text", () => {
    const createDescription = HeartbeatToolkit.tools.t3_heartbeat_create.description;
    const wakeupDescription = HeartbeatToolkit.tools.t3_heartbeat_wakeup.description;
    expect(createDescription).toContain("restored dormant after restart");
    expect(createDescription).toContain("seven days");
    expect(wakeupDescription).toContain("process-local");
    expect(wakeupDescription).toContain("Child-completion");
    expect(wakeupDescription).toContain("never suppresses");
  });

  it.effect.each([
    { route: "top-level", capabilities: ["heartbeat"] as const, visible: true },
    { route: "orchestration child", capabilities: ["heartbeat"] as const, visible: true },
    { route: "leaf with browser off", capabilities: [] as const, visible: false },
  ])(
    "$route credential sets heartbeat discovery visibility to $visible",
    ({ route, capabilities, visible }) => {
      const invocation: McpInvocationContext.McpInvocationScope = {
        environmentId: EnvironmentId.make("environment-heartbeat-discovery"),
        threadId: ThreadId.make(`thread-${route}`),
        providerSessionId: `provider-session-${route}`,
        providerInstanceId: ProviderInstanceId.make("codex"),
        capabilities: new Set(capabilities),
        issuedAt: 1,
      };
      return Effect.sync(() => {
        for (const tool of Object.values(HeartbeatToolkit.tools)) {
          const enabledWhen = Context.get(
            tool.annotations as Context.Context<McpSchema.EnabledWhen>,
            McpSchema.EnabledWhen,
          );
          expect(enabledWhen(initializePayload)).toBe(visible);
        }
      }).pipe(Effect.provideService(McpInvocationContext.McpInvocationContext, invocation));
    },
  );
});
