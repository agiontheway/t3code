import {
  CommandId,
  EventId,
  MessageId,
  ProviderInstanceId,
  ThreadId,
  type ModelSelection,
  type OrchestrationThread,
  type RuntimeMode,
} from "@t3tools/contracts";
import * as Crypto from "effect/Crypto";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";

import * as McpInvocationContext from "../../McpInvocationContext.ts";
import * as ProjectionSnapshotQuery from "../../../orchestration/Services/ProjectionSnapshotQuery.ts";
import * as OrchestrationEngine from "../../../orchestration/Services/OrchestrationEngine.ts";
import * as ProviderService from "../../../provider/Services/ProviderService.ts";
import { ServerSettingsService } from "../../../serverSettings.ts";
import { AgentToolkit, CrossProviderAgentError } from "./tools.ts";

type Operation = "spawn" | "status" | "follow_up" | "interrupt";
type AgentStatus =
  | "pending"
  | "running"
  | "waiting"
  | "idle"
  | "completed"
  | "failed"
  | "interrupted";

const fail = (operation: Operation, detail: string) =>
  new CrossProviderAgentError({
    operation,
    detail: detail.trim() || "Cross-provider agent operation failed.",
  });

const errorText = (error: unknown): string =>
  error instanceof Error ? error.message : typeof error === "string" ? error : String(error);

const mapFailure = (operation: Operation) =>
  Effect.mapError((error: unknown) => fail(operation, errorText(error)));

const randomId = Effect.gen(function* () {
  const crypto = yield* Crypto.Crypto;
  return yield* crypto.randomUUIDv4.pipe(Effect.orDie);
});

const nowIso = DateTime.now.pipe(Effect.map(DateTime.formatIso));

const normalizeProviderAlias = (value: string): string => value.trim().toLocaleLowerCase();

const resolveProviderInstance = Effect.fn("AgentToolkit.resolveProviderInstance")(function* (
  requestedInstanceId: ProviderInstanceId,
) {
  const providers = yield* ProviderService.ProviderService;
  const direct = yield* providers.getInstanceInfo(requestedInstanceId).pipe(Effect.option);
  if (Option.isSome(direct)) {
    return direct.value;
  }

  const settings = yield* ServerSettingsService;
  const aliases = (yield* settings.getSettings.pipe(mapFailure("spawn"))).crossProviderAgentAliases;
  const normalizedRequested = normalizeProviderAlias(requestedInstanceId);
  const aliasEntry = Object.entries(aliases).find(
    ([alias]) => normalizeProviderAlias(alias) === normalizedRequested,
  );
  if (!aliasEntry) {
    const configuredAliases = Object.keys(aliases);
    return yield* fail(
      "spawn",
      `Provider instance '${requestedInstanceId}' was not found and does not match a configured alias.${configuredAliases.length > 0 ? ` Available aliases: ${configuredAliases.join(", ")}.` : ""}`,
    );
  }

  const resolvedInstanceId = aliasEntry[1];
  return yield* providers
    .getInstanceInfo(resolvedInstanceId)
    .pipe(
      Effect.mapError((error) =>
        fail(
          "spawn",
          `Provider alias '${requestedInstanceId}' resolves to unavailable instance '${resolvedInstanceId}': ${errorText(error)}`,
        ),
      ),
    );
});

const getParent = Effect.fn("AgentToolkit.getParent")(function* (operation: Operation) {
  const invocation = yield* McpInvocationContext.requireMcpCapability("agents").pipe(
    Effect.mapError(() =>
      fail(operation, "This provider session is not allowed to use T3 child agents."),
    ),
  );
  const snapshots = yield* ProjectionSnapshotQuery.ProjectionSnapshotQuery;
  const parent = yield* snapshots
    .getThreadDetailById(invocation.threadId, {
      // Ownership must survive the normal 500-activity detail window. An
      // explicit kind filter reads the persisted task lifecycle directly.
      activityKinds: ["task.started", "task.updated", "task.completed"],
    })
    .pipe(mapFailure(operation));
  if (Option.isNone(parent)) {
    return yield* fail(operation, `Parent thread '${invocation.threadId}' no longer exists.`);
  }
  return { invocation, parent: parent.value } as const;
});

const agentTaskId = (childThreadId: ThreadId): string => `t3-agent:${childThreadId}`;

const findOwnedChild = (parent: OrchestrationThread, operation: Operation, agentId: string) => {
  const activity = parent.activities.find((entry) => {
    if (
      entry.kind !== "task.started" ||
      typeof entry.payload !== "object" ||
      entry.payload === null
    ) {
      return false;
    }
    const payload = entry.payload as Record<string, unknown>;
    return payload.taskId === agentId && payload.taskType === "t3_cross_provider_agent";
  });
  if (!activity || typeof activity.payload !== "object" || activity.payload === null) {
    return Effect.fail(fail(operation, `Agent '${agentId}' is not owned by this parent thread.`));
  }
  const payload = activity.payload as Record<string, unknown>;
  const handles =
    typeof payload.runHandles === "object" && payload.runHandles !== null
      ? (payload.runHandles as Record<string, unknown>)
      : undefined;
  const childThreadId = typeof handles?.runId === "string" ? handles.runId : undefined;
  const providerInstanceId =
    typeof payload.providerInstanceId === "string" ? payload.providerInstanceId : undefined;
  const model = typeof payload.model === "string" ? payload.model : undefined;
  if (!childThreadId || !providerInstanceId || !model) {
    return Effect.fail(
      fail(operation, `Agent '${agentId}' has incomplete persisted routing metadata.`),
    );
  }
  return Effect.succeed({
    agentId,
    childThreadId: ThreadId.make(childThreadId),
    providerInstanceId: ProviderInstanceId.make(providerInstanceId),
    model,
  });
};

const persistedTerminalStatus = (
  parent: OrchestrationThread,
  agentId: string,
): "completed" | "failed" | "interrupted" | undefined => {
  const lifecycle = parent.activities.filter(
    (entry) =>
      (entry.kind === "task.started" ||
        entry.kind === "task.updated" ||
        entry.kind === "task.completed") &&
      typeof entry.payload === "object" &&
      entry.payload !== null &&
      (entry.payload as Record<string, unknown>).taskId === agentId,
  );
  let runStart = -1;
  for (const [index, entry] of lifecycle.entries()) {
    if (entry.kind === "task.started" || entry.kind === "task.updated") runStart = index;
  }
  const latestLifecycle = lifecycle
    .slice(runStart + 1)
    .find((entry) => entry.kind === "task.completed");
  if (
    latestLifecycle?.kind !== "task.completed" ||
    typeof latestLifecycle.payload !== "object" ||
    latestLifecycle.payload === null
  ) {
    return undefined;
  }
  const status = (latestLifecycle.payload as Record<string, unknown>).status;
  if (status === "stopped") return "interrupted";
  if (status === "failed") return "failed";
  if (status === "completed") return "completed";
  return undefined;
};

const appendAgentActivity = Effect.fn("AgentToolkit.appendActivity")(function* (input: {
  readonly operation: Operation;
  readonly parent: OrchestrationThread;
  readonly kind: "task.started" | "task.updated" | "task.completed";
  readonly summary: string;
  readonly payload: Record<string, unknown>;
}) {
  const engine = yield* OrchestrationEngine.OrchestrationEngineService;
  const id = yield* randomId;
  const commandId = yield* randomId;
  const createdAt = yield* nowIso;
  yield* engine
    .dispatch({
      type: "thread.activity.append",
      commandId: CommandId.make(commandId),
      threadId: input.parent.id,
      activity: {
        id: EventId.make(id),
        tone:
          input.kind === "task.completed" && input.payload.status === "failed" ? "error" : "info",
        kind: input.kind,
        summary: input.summary,
        payload: input.payload,
        turnId: input.parent.session?.activeTurnId ?? null,
        createdAt,
      },
      createdAt,
    })
    .pipe(mapFailure(input.operation));
});

const readChildState = Effect.fn("AgentToolkit.readChildState")(function* (
  operation: Operation,
  handle: {
    readonly agentId: string;
    readonly childThreadId: ThreadId;
    readonly providerInstanceId: ProviderInstanceId;
    readonly model: string;
  },
) {
  const snapshots = yield* ProjectionSnapshotQuery.ProjectionSnapshotQuery;
  const childOption = yield* snapshots
    .getThreadDetailById(handle.childThreadId)
    .pipe(mapFailure(operation));
  if (Option.isNone(childOption)) {
    return yield* fail(operation, `Child thread '${handle.childThreadId}' no longer exists.`);
  }
  const child = childOption.value;
  const latest = child.latestTurn;
  const session = child.session;
  let status: AgentStatus = "pending";
  if (latest?.state === "completed") {
    status = "completed";
  } else if (latest?.state === "error" || session?.status === "error") {
    status = "failed";
  } else if (
    latest?.state === "interrupted" ||
    session?.status === "interrupted" ||
    session?.status === "stopped"
  ) {
    status = "interrupted";
  } else if (
    latest?.state === "running" ||
    session?.status === "running" ||
    session?.status === "starting"
  ) {
    status = "running";
  } else if (session?.status === "ready" || session?.status === "idle") {
    status = latest ? "completed" : "idle";
  }
  const output = child.messages
    .toReversed()
    .find((message) => message.role === "assistant" && !message.streaming)?.text;
  const error = session?.lastError ?? undefined;
  return { ...handle, child, status, ...(output ? { output } : {}), ...(error ? { error } : {}) };
});

const synchronizeParent = Effect.fn("AgentToolkit.synchronizeParent")(function* (
  operation: Operation,
  parent: OrchestrationThread,
  state: Effect.Success<ReturnType<typeof readChildState>>,
) {
  const base = {
    taskId: state.agentId,
    taskType: "t3_cross_provider_agent",
    agentKind: "agent",
    title: state.child.title,
    model: state.model,
    providerInstanceId: state.providerInstanceId,
    runHandles: { runId: state.childThreadId },
    timelineBypass: true,
  };
  if (state.status === "completed" || state.status === "failed" || state.status === "interrupted") {
    yield* appendAgentActivity({
      operation,
      parent,
      kind: "task.completed",
      summary: `Cross-provider agent ${state.status}`,
      payload: {
        ...base,
        status:
          state.status === "completed"
            ? "completed"
            : state.status === "failed"
              ? "failed"
              : "stopped",
        ...(state.output ? { summary: state.output.slice(0, 4_000) } : {}),
        ...(state.error ? { error: state.error } : {}),
      },
    });
  } else {
    yield* appendAgentActivity({
      operation,
      parent,
      kind: "task.updated",
      summary: "Cross-provider agent status updated",
      payload: { ...base, status: state.status },
    });
  }
});

const handlers = {
  agent_spawn: (input) =>
    Effect.gen(function* () {
      const { parent } = yield* getParent("spawn");
      const provider = yield* resolveProviderInstance(input.providerInstanceId);
      const providerInstanceId = provider.instanceId;
      if (!provider.enabled) {
        return yield* fail("spawn", `Provider instance '${providerInstanceId}' is disabled.`);
      }
      const childThreadId = ThreadId.make(yield* randomId);
      const agentId = agentTaskId(childThreadId);
      const createdAt = yield* nowIso;
      const title = input.title ?? input.prompt.slice(0, 80).trim();
      const modelSelection: ModelSelection = {
        instanceId: providerInstanceId,
        model: input.model,
        ...(input.options ? { options: input.options } : {}),
      };
      const runtimeMode: RuntimeMode = parent.runtimeMode;
      const engine = yield* OrchestrationEngine.OrchestrationEngineService;
      yield* engine
        .dispatch({
          type: "thread.create",
          commandId: CommandId.make(yield* randomId),
          threadId: childThreadId,
          projectId: parent.projectId,
          title,
          modelSelection,
          runtimeMode,
          interactionMode: "default",
          branch: parent.branch,
          worktreePath: parent.worktreePath,
          createdAt,
        })
        .pipe(mapFailure("spawn"));
      yield* appendAgentActivity({
        operation: "spawn",
        parent,
        kind: "task.started",
        summary: "Cross-provider agent started",
        payload: {
          taskId: agentId,
          taskType: "t3_cross_provider_agent",
          agentKind: "agent",
          detail: input.prompt,
          title,
          ...(input.role ? { role: input.role } : {}),
          model: input.model,
          providerInstanceId,
          runHandles: { runId: childThreadId },
          timelineBypass: true,
        },
      });
      yield* engine
        .dispatch({
          type: "thread.turn.start",
          commandId: CommandId.make(yield* randomId),
          threadId: childThreadId,
          message: {
            messageId: MessageId.make(yield* randomId),
            role: "user",
            text: input.prompt,
            attachments: [],
          },
          modelSelection,
          titleSeed: title,
          runtimeMode,
          interactionMode: "default",
          createdAt,
        })
        .pipe(
          mapFailure("spawn"),
          Effect.tapError((error) =>
            appendAgentActivity({
              operation: "spawn",
              parent,
              kind: "task.completed",
              summary: "Cross-provider agent failed to start",
              payload: {
                taskId: agentId,
                taskType: "t3_cross_provider_agent",
                agentKind: "agent",
                title,
                model: input.model,
                providerInstanceId,
                runHandles: { runId: childThreadId },
                timelineBypass: true,
                status: "failed",
                error: error.message,
              },
            }),
          ),
        );
      return {
        agentId,
        childThreadId,
        providerInstanceId,
        model: input.model,
      };
    }),
  agent_status: (input) =>
    Effect.gen(function* () {
      const { parent } = yield* getParent("status");
      const handle = yield* findOwnedChild(parent, "status", input.agentId);
      const observedState = yield* readChildState("status", handle);
      const terminalStatus = persistedTerminalStatus(parent, input.agentId);
      const state = terminalStatus ? { ...observedState, status: terminalStatus } : observedState;
      if (!terminalStatus) yield* synchronizeParent("status", parent, state);
      const { child: _child, ...result } = state;
      return result;
    }),
  agent_follow_up: (input) =>
    Effect.gen(function* () {
      const { parent } = yield* getParent("follow_up");
      const handle = yield* findOwnedChild(parent, "follow_up", input.agentId);
      const snapshots = yield* ProjectionSnapshotQuery.ProjectionSnapshotQuery;
      const childOption = yield* snapshots
        .getThreadDetailById(handle.childThreadId)
        .pipe(mapFailure("follow_up"));
      if (Option.isNone(childOption))
        return yield* fail("follow_up", `Child thread '${handle.childThreadId}' no longer exists.`);
      const child = childOption.value;
      if (child.latestTurn?.state === "running")
        return yield* fail("follow_up", `Agent '${input.agentId}' is still running.`);
      yield* appendAgentActivity({
        operation: "follow_up",
        parent,
        kind: "task.updated",
        summary: "Cross-provider agent reactivated",
        payload: {
          taskId: handle.agentId,
          taskType: "t3_cross_provider_agent",
          agentKind: "agent",
          title: child.title,
          model: handle.model,
          providerInstanceId: handle.providerInstanceId,
          runHandles: { runId: handle.childThreadId },
          timelineBypass: true,
          status: "running",
        },
      });
      const engine = yield* OrchestrationEngine.OrchestrationEngineService;
      yield* engine
        .dispatch({
          type: "thread.turn.start",
          commandId: CommandId.make(yield* randomId),
          threadId: handle.childThreadId,
          message: {
            messageId: MessageId.make(yield* randomId),
            role: "user",
            text: input.prompt,
            attachments: [],
          },
          modelSelection: child.modelSelection,
          runtimeMode: child.runtimeMode,
          interactionMode: child.interactionMode,
          createdAt: yield* nowIso,
        })
        .pipe(
          mapFailure("follow_up"),
          Effect.tapError((error) =>
            appendAgentActivity({
              operation: "follow_up",
              parent,
              kind: "task.completed",
              summary: "Cross-provider agent follow-up failed to start",
              payload: {
                taskId: handle.agentId,
                taskType: "t3_cross_provider_agent",
                agentKind: "agent",
                title: child.title,
                model: handle.model,
                providerInstanceId: handle.providerInstanceId,
                runHandles: { runId: handle.childThreadId },
                timelineBypass: true,
                status: "failed",
                error: error.message,
              },
            }),
          ),
        );
      return handle;
    }),
  agent_interrupt: (input) =>
    Effect.gen(function* () {
      const { parent } = yield* getParent("interrupt");
      const handle = yield* findOwnedChild(parent, "interrupt", input.agentId);
      const engine = yield* OrchestrationEngine.OrchestrationEngineService;
      yield* engine
        .dispatch({
          type: "thread.turn.interrupt",
          commandId: CommandId.make(yield* randomId),
          threadId: handle.childThreadId,
          createdAt: yield* nowIso,
        })
        .pipe(mapFailure("interrupt"));
      const state = yield* readChildState("interrupt", handle);
      yield* synchronizeParent("interrupt", parent, { ...state, status: "interrupted" });
      const { child: _child, ...result } = state;
      return { ...result, status: "interrupted" as const };
    }),
} satisfies Parameters<typeof AgentToolkit.toLayer>[0];

export const AgentToolkitHandlersLive = AgentToolkit.toLayer(handlers);

/** Direct handler access for focused service-boundary tests. */
export const __testing = { handlers };
