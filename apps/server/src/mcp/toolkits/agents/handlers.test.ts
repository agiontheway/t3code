import * as NodeServices from "@effect/platform-node/NodeServices";
import { expect, it } from "@effect/vitest";
import {
  EnvironmentId,
  ProjectId,
  ProviderDriverKind,
  ProviderInstanceId,
  ThreadId,
  type OrchestrationCommand,
  type OrchestrationThread,
  type ServerProvider,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Stream from "effect/Stream";

import * as McpInvocationContext from "../../McpInvocationContext.ts";
import * as OrchestrationEngine from "../../../orchestration/Services/OrchestrationEngine.ts";
import * as ProjectionSnapshotQuery from "../../../orchestration/Services/ProjectionSnapshotQuery.ts";
import * as ProviderRegistry from "../../../provider/Services/ProviderRegistry.ts";
import * as ServerSettings from "../../../serverSettings.ts";
import { __testing } from "./handlers.ts";

const parentThreadId = ThreadId.make("source-parent");
const parentProviderInstanceId = ProviderInstanceId.make("openrouter-glm");
const targetProviderInstanceId = ProviderInstanceId.make("codex");
const parent = {
  id: parentThreadId,
  projectId: ProjectId.make("scratch"),
  title: "GLM orchestrator",
  runtimeMode: "full-access",
  interactionMode: "default",
  branch: null,
  worktreePath: "/tmp/scratch",
  session: null,
  activities: [],
} as unknown as OrchestrationThread;

const provider = {
  instanceId: targetProviderInstanceId,
  driver: ProviderDriverKind.make("codex"),
  displayName: "OpenAI subscription",
  enabled: true,
  installed: true,
  version: "1.0.0",
  status: "ready",
  auth: { status: "authenticated" },
  checkedAt: "2026-09-04T00:00:00.000Z",
  models: [
    {
      slug: "gpt-5.6-luna",
      name: "GPT-5.6-Luna",
      isCustom: false,
      capabilities: null,
    },
  ],
  slashCommands: [],
  skills: [],
} satisfies ServerProvider;

const invocation: McpInvocationContext.McpInvocationScope = {
  environmentId: EnvironmentId.make("environment-1"),
  threadId: parentThreadId,
  providerSessionId: "glm-session",
  providerInstanceId: parentProviderInstanceId,
  capabilities: new Set(["agents"]),
  issuedAt: 1,
};

function testLayer(commands: OrchestrationCommand[], aliases = {}) {
  const snapshots = ProjectionSnapshotQuery.ProjectionSnapshotQuery.of({
    getThreadDetailById: () => Effect.succeed(Option.some(parent)),
  } as unknown as ProjectionSnapshotQuery.ProjectionSnapshotQueryShape);
  const engine = OrchestrationEngine.OrchestrationEngineService.of({
    dispatch: (command: OrchestrationCommand) =>
      Effect.sync(() => {
        commands.push(command);
        return { sequence: commands.length };
      }),
    subscribeDomainEvents: Effect.succeed(Stream.empty),
  } as unknown as OrchestrationEngine.OrchestrationEngineShape);
  const registry = ProviderRegistry.ProviderRegistry.of({
    getProviders: Effect.succeed([provider]),
  } as unknown as ProviderRegistry.ProviderRegistryShape);
  return Layer.mergeAll(
    Layer.succeed(McpInvocationContext.McpInvocationContext, invocation),
    Layer.succeed(ProjectionSnapshotQuery.ProjectionSnapshotQuery, snapshots),
    Layer.succeed(OrchestrationEngine.OrchestrationEngineService, engine),
    Layer.succeed(ProviderRegistry.ProviderRegistry, registry),
    ServerSettings.layerTest({ crossProviderAgentAliases: aliases }),
    NodeServices.layer,
  );
}

it.effect("spawns an ordinary linked T3 thread through the selected subscription", () => {
  const commands: OrchestrationCommand[] = [];
  return Effect.gen(function* () {
    const result = yield* __testing.handlers.agent_spawn({
      providerInstanceId: targetProviderInstanceId,
      model: "gpt-5.6-luna",
      prompt: "Research the issue.",
      title: "Researcher",
    });
    expect(result.providerInstanceId).toBe(targetProviderInstanceId);
    expect(commands.map((command) => command.type)).toEqual([
      "thread.create",
      "thread.activity.append",
      "thread.activity.append",
      "thread.turn.start",
    ]);
    expect(commands[0]).toMatchObject({
      type: "thread.create",
      modelSelection: { instanceId: "codex", model: "gpt-5.6-luna" },
    });
    expect(commands[1]).toMatchObject({
      type: "thread.activity.append",
      threadId: result.childThreadId,
      activity: { kind: "t3.agent.linked" },
    });
    expect(commands[2]).toMatchObject({
      type: "thread.activity.append",
      threadId: parentThreadId,
      activity: {
        kind: "task.started",
        payload: {
          taskId: result.agentId,
          agentKind: "agent",
          providerInstanceId: "codex",
          runHandles: { runId: result.childThreadId },
        },
      },
    });
  }).pipe(Effect.provide(testLayer(commands)));
});

it.effect("resolves editable aliases but rejects unadvertised models without fallback", () => {
  const commands: OrchestrationCommand[] = [];
  return Effect.gen(function* () {
    const catalog = yield* __testing.handlers.agent_catalog();
    expect(catalog.providers[0]).toMatchObject({
      instanceId: "codex",
      aliases: ["OpenAI"],
      models: [{ id: "gpt-5.6-luna" }],
    });
    const error = yield* __testing.handlers
      .agent_spawn({
        providerInstanceId: ProviderInstanceId.make("OPENAI"),
        model: "gpt-openrouter-fallback",
        prompt: "Must fail.",
      })
      .pipe(Effect.flip);
    expect(error.detail).toContain("not advertised");
    expect(commands).toEqual([]);
  }).pipe(Effect.provide(testLayer(commands, { OpenAI: targetProviderInstanceId })));
});
