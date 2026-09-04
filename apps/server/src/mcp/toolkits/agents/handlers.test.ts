import * as NodeServices from "@effect/platform-node/NodeServices";
import { expect, it } from "@effect/vitest";
import {
  EnvironmentId,
  ProjectId,
  ProviderInstanceId,
  ThreadId,
  type OrchestrationCommand,
  type OrchestrationThread,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";

import * as McpInvocationContext from "../../McpInvocationContext.ts";
import * as ProjectionSnapshotQuery from "../../../orchestration/Services/ProjectionSnapshotQuery.ts";
import * as OrchestrationEngine from "../../../orchestration/Services/OrchestrationEngine.ts";
import * as ProviderService from "../../../provider/Services/ProviderService.ts";
import { __testing } from "./handlers.ts";

const parentProviderInstanceId = ProviderInstanceId.make("source-provider");
const targetProviderInstanceId = ProviderInstanceId.make("codex");
const parentThreadId = ThreadId.make("source-parent");

const parent = {
  id: parentThreadId,
  projectId: ProjectId.make("scratch"),
  title: "Source orchestrator",
  runtimeMode: "full-access",
  interactionMode: "default",
  branch: null,
  worktreePath: "/tmp/scratch",
  session: null,
  activities: [],
} as unknown as OrchestrationThread;

it.effect("routes a spawned child through the explicitly selected provider instance", () => {
  const commands: OrchestrationCommand[] = [];
  const requestedActivityKinds: Array<ReadonlyArray<string> | undefined> = [];
  const invocation: McpInvocationContext.McpInvocationScope = {
    environmentId: EnvironmentId.make("environment-1"),
    threadId: parentThreadId,
    providerSessionId: "source-provider-session",
    providerInstanceId: parentProviderInstanceId,
    capabilities: new Set(["agents"]),
    issuedAt: 1,
  };
  const snapshots = ProjectionSnapshotQuery.ProjectionSnapshotQuery.of({
    getThreadDetailById: (
      _threadId: ThreadId,
      query?: ProjectionSnapshotQuery.ProjectionThreadDetailQuery,
    ) =>
      Effect.sync(() => {
        requestedActivityKinds.push(query?.activityKinds);
        return Option.some(parent);
      }),
  } as unknown as ProjectionSnapshotQuery.ProjectionSnapshotQueryShape);
  const engine = OrchestrationEngine.OrchestrationEngineService.of({
    dispatch: (command: OrchestrationCommand) =>
      Effect.sync(() => {
        commands.push(command);
        return { sequence: commands.length };
      }),
  } as unknown as OrchestrationEngine.OrchestrationEngineShape);
  const providers = ProviderService.ProviderService.of({
    getInstanceInfo: (instanceId: ProviderInstanceId) =>
      Effect.succeed({
        instanceId,
        driverKind: "codex",
        displayName: "Codex",
        enabled: true,
        continuationIdentity: "codex",
      }),
  } as unknown as ProviderService.ProviderServiceShape);

  return Effect.gen(function* () {
    const result = yield* __testing.handlers.agent_spawn({
      providerInstanceId: targetProviderInstanceId,
      model: "gpt-5.6-sol",
      prompt: "Inspect the repository.",
      title: "Repository inspector",
    });

    expect(result.providerInstanceId).toBe(targetProviderInstanceId);
    expect(requestedActivityKinds).toEqual([["task.started", "task.updated", "task.completed"]]);
    expect(commands.map((command) => command.type)).toEqual([
      "thread.create",
      "thread.activity.append",
      "thread.turn.start",
    ]);

    const create = commands[0];
    expect(create?.type).toBe("thread.create");
    if (create?.type === "thread.create") {
      expect(create.modelSelection).toMatchObject({
        instanceId: targetProviderInstanceId,
        model: "gpt-5.6-sol",
      });
      expect(create.modelSelection.instanceId).not.toBe(parentProviderInstanceId);
      expect(create.runtimeMode).toBe(parent.runtimeMode);
    }

    const activity = commands[1];
    expect(activity?.type).toBe("thread.activity.append");
    if (activity?.type === "thread.activity.append") {
      expect(activity.activity.payload).toMatchObject({
        taskId: result.agentId,
        agentKind: "agent",
        providerInstanceId: targetProviderInstanceId,
        runHandles: { runId: result.childThreadId },
      });
    }

    const turn = commands[2];
    expect(turn?.type).toBe("thread.turn.start");
    if (turn?.type === "thread.turn.start") {
      expect(turn.threadId).toBe(result.childThreadId);
      expect(turn.modelSelection).toMatchObject({
        instanceId: targetProviderInstanceId,
        model: "gpt-5.6-sol",
      });
      expect(turn.runtimeMode).toBe(parent.runtimeMode);
    }
  }).pipe(
    Effect.provideService(McpInvocationContext.McpInvocationContext, invocation),
    Effect.provideService(ProjectionSnapshotQuery.ProjectionSnapshotQuery, snapshots),
    Effect.provideService(OrchestrationEngine.OrchestrationEngineService, engine),
    Effect.provideService(ProviderService.ProviderService, providers),
    Effect.provide(NodeServices.layer),
  );
});

it.effect("fails closed when the exact target provider instance is disabled", () => {
  const commands: OrchestrationCommand[] = [];
  const invocation: McpInvocationContext.McpInvocationScope = {
    environmentId: EnvironmentId.make("environment-1"),
    threadId: parentThreadId,
    providerSessionId: "source-provider-session",
    providerInstanceId: parentProviderInstanceId,
    capabilities: new Set(["agents"]),
    issuedAt: 1,
  };
  const snapshots = ProjectionSnapshotQuery.ProjectionSnapshotQuery.of({
    getThreadDetailById: () => Effect.succeed(Option.some(parent)),
  } as unknown as ProjectionSnapshotQuery.ProjectionSnapshotQueryShape);
  const engine = OrchestrationEngine.OrchestrationEngineService.of({
    dispatch: (command: OrchestrationCommand) =>
      Effect.sync(() => {
        commands.push(command);
        return { sequence: commands.length };
      }),
  } as unknown as OrchestrationEngine.OrchestrationEngineShape);
  const providers = ProviderService.ProviderService.of({
    getInstanceInfo: (instanceId: ProviderInstanceId) =>
      Effect.succeed({
        instanceId,
        driverKind: "codex",
        displayName: "Codex",
        enabled: false,
        continuationIdentity: "codex",
      }),
  } as unknown as ProviderService.ProviderServiceShape);

  return Effect.gen(function* () {
    const error = yield* __testing.handlers
      .agent_spawn({
        providerInstanceId: targetProviderInstanceId,
        model: "gpt-5.6-sol",
        prompt: "This must not start.",
      })
      .pipe(Effect.flip);

    expect(error.detail).toContain("is disabled");
    expect(commands).toEqual([]);
  }).pipe(
    Effect.provideService(McpInvocationContext.McpInvocationContext, invocation),
    Effect.provideService(ProjectionSnapshotQuery.ProjectionSnapshotQuery, snapshots),
    Effect.provideService(OrchestrationEngine.OrchestrationEngineService, engine),
    Effect.provideService(ProviderService.ProviderService, providers),
    Effect.provide(NodeServices.layer),
  );
});

it.effect(
  "preserves an interrupted terminal state without appending a conflicting duplicate",
  () => {
    const childThreadId = ThreadId.make("interrupted-child");
    const agentId = `t3-agent:${childThreadId}`;
    const interruptedParent = {
      ...parent,
      activities: [
        {
          kind: "task.started",
          payload: {
            taskId: agentId,
            taskType: "t3_cross_provider_agent",
            providerInstanceId: targetProviderInstanceId,
            model: "gpt-5.6-sol",
            runHandles: { runId: childThreadId },
          },
        },
        {
          kind: "task.completed",
          payload: { taskId: agentId, status: "stopped" },
        },
        {
          kind: "task.completed",
          payload: { taskId: agentId, status: "completed" },
        },
      ],
    } as unknown as OrchestrationThread;
    const child = {
      ...parent,
      id: childThreadId,
      title: "Interrupted child",
      latestTurn: { state: "completed" },
      session: { status: "running", lastError: null },
      messages: [
        { role: "assistant", streaming: false, text: "Partial output before interruption" },
      ],
    } as unknown as OrchestrationThread;
    const commands: OrchestrationCommand[] = [];
    const invocation: McpInvocationContext.McpInvocationScope = {
      environmentId: EnvironmentId.make("environment-1"),
      threadId: parentThreadId,
      providerSessionId: "source-provider-session",
      providerInstanceId: parentProviderInstanceId,
      capabilities: new Set(["agents"]),
      issuedAt: 1,
    };
    const snapshots = ProjectionSnapshotQuery.ProjectionSnapshotQuery.of({
      getThreadDetailById: (threadId: ThreadId) =>
        Effect.succeed(Option.some(threadId === parentThreadId ? interruptedParent : child)),
    } as unknown as ProjectionSnapshotQuery.ProjectionSnapshotQueryShape);
    const engine = OrchestrationEngine.OrchestrationEngineService.of({
      dispatch: (command: OrchestrationCommand) =>
        Effect.sync(() => {
          commands.push(command);
          return { sequence: commands.length };
        }),
    } as unknown as OrchestrationEngine.OrchestrationEngineShape);

    return Effect.gen(function* () {
      const result = yield* __testing.handlers.agent_status({ agentId });

      expect(result.status).toBe("interrupted");
      expect(commands).toEqual([]);
    }).pipe(
      Effect.provideService(McpInvocationContext.McpInvocationContext, invocation),
      Effect.provideService(ProjectionSnapshotQuery.ProjectionSnapshotQuery, snapshots),
      Effect.provideService(OrchestrationEngine.OrchestrationEngineService, engine),
      Effect.provide(NodeServices.layer),
    );
  },
);
