import {
  CommandId,
  EventId,
  MessageId,
  ProviderInstanceId,
  ThreadId,
  type ModelSelection,
  type OrchestrationEvent,
  type OrchestrationThread,
} from "@t3tools/contracts";
import * as Crypto from "effect/Crypto";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as Stream from "effect/Stream";

import * as McpInvocationContext from "../../McpInvocationContext.ts";
import * as OrchestrationEngine from "../../../orchestration/Services/OrchestrationEngine.ts";
import * as ProjectionSnapshotQuery from "../../../orchestration/Services/ProjectionSnapshotQuery.ts";
import * as ProviderRegistry from "../../../provider/Services/ProviderRegistry.ts";
import { ServerSettingsService } from "../../../serverSettings.ts";
import { AgentToolkit, CrossProviderAgentError } from "./tools.ts";

type Operation = "catalog" | "spawn" | "wait" | "follow_up" | "interrupt";

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
const agentTaskId = (childThreadId: ThreadId): string => `t3-agent:${childThreadId}`;
const normalizeAlias = (value: string): string => value.trim().toLocaleLowerCase();

const getParent = Effect.fn("AgentToolkit.getParent")(function* (operation: Operation) {
  const invocation = yield* McpInvocationContext.requireMcpCapability("agents").pipe(
    Effect.mapError(() =>
      fail(operation, "This session is not allowed to use cross-provider agents."),
    ),
  );
  const snapshots = yield* ProjectionSnapshotQuery.ProjectionSnapshotQuery;
  const parent = yield* snapshots
    .getThreadDetailById(invocation.threadId, {
      // Ownership must survive the normal recent-activity detail window.
      activityKinds: ["task.started", "task.updated"],
    })
    .pipe(mapFailure(operation));
  if (Option.isNone(parent)) {
    return yield* fail(operation, `Parent thread '${invocation.threadId}' no longer exists.`);
  }
  return { invocation, parent: parent.value } as const;
});

const providerCatalog = Effect.fn("AgentToolkit.providerCatalog")(function* () {
  const registry = yield* ProviderRegistry.ProviderRegistry;
  const settings = yield* ServerSettingsService;
  const [providers, currentSettings] = yield* Effect.all([
    registry.getProviders,
    settings.getSettings.pipe(mapFailure("catalog")),
  ]);
  return providers
    .filter(
      (provider) =>
        provider.enabled &&
        provider.installed &&
        provider.status !== "disabled" &&
        provider.availability !== "unavailable",
    )
    .map((provider) => ({
      provider,
      aliases: Object.entries(currentSettings.crossProviderAgentAliases)
        .filter(([, instanceId]) => instanceId === provider.instanceId)
        .map(([alias]) => alias),
    }));
});

const resolveProvider = Effect.fn("AgentToolkit.resolveProvider")(function* (
  requested: ProviderInstanceId,
  operation: Operation,
) {
  const catalog = yield* providerCatalog().pipe(
    Effect.mapError((error) => fail(operation, errorText(error))),
  );
  const direct = catalog.find(({ provider }) => provider.instanceId === requested);
  if (direct) return direct.provider;
  const normalized = normalizeAlias(requested);
  const matches = catalog.filter(({ aliases }) =>
    aliases.some((alias) => normalizeAlias(alias) === normalized),
  );
  if (matches.length !== 1) {
    return yield* fail(
      operation,
      matches.length > 1
        ? `Provider alias '${requested}' is ambiguous.`
        : `Provider instance or alias '${requested}' is not enabled and available. Call agent_catalog for valid routes.`,
    );
  }
  return matches[0]!.provider;
});

const appendActivity = Effect.fn("AgentToolkit.appendActivity")(function* (input: {
  readonly operation: Operation;
  readonly threadId: ThreadId;
  readonly turnId: string | null;
  readonly kind: string;
  readonly summary: string;
  readonly payload: Record<string, unknown>;
}) {
  const engine = yield* OrchestrationEngine.OrchestrationEngineService;
  const createdAt = yield* nowIso;
  yield* engine
    .dispatch({
      type: "thread.activity.append",
      commandId: CommandId.make(yield* randomId),
      threadId: input.threadId,
      activity: {
        id: EventId.make(yield* randomId),
        tone: "info",
        kind: input.kind,
        summary: input.summary,
        payload: input.payload,
        turnId: input.turnId as never,
        createdAt,
      },
      createdAt,
    })
    .pipe(mapFailure(input.operation));
});

interface OwnedAgent {
  readonly agentId: string;
  readonly childThreadId: ThreadId;
  readonly providerInstanceId: ProviderInstanceId;
  readonly model: string;
}

const findOwnedAgent = (parent: OrchestrationThread, operation: Operation, agentId: string) => {
  const start = parent.activities.find(
    (activity) =>
      activity.kind === "task.started" &&
      typeof activity.payload === "object" &&
      activity.payload !== null &&
      (activity.payload as Record<string, unknown>).taskId === agentId &&
      (activity.payload as Record<string, unknown>).taskType === "t3_cross_provider_agent",
  );
  const payload =
    start && typeof start.payload === "object" && start.payload !== null
      ? (start.payload as Record<string, unknown>)
      : undefined;
  const handles =
    payload && typeof payload.runHandles === "object" && payload.runHandles !== null
      ? (payload.runHandles as Record<string, unknown>)
      : undefined;
  if (
    typeof handles?.runId !== "string" ||
    typeof payload?.providerInstanceId !== "string" ||
    typeof payload.model !== "string"
  ) {
    return Effect.fail(fail(operation, `Agent '${agentId}' is not owned by this parent thread.`));
  }
  return Effect.succeed({
    agentId,
    childThreadId: ThreadId.make(handles.runId),
    providerInstanceId: ProviderInstanceId.make(payload.providerInstanceId),
    model: payload.model,
  } satisfies OwnedAgent);
};

const readAgentResult = Effect.fn("AgentToolkit.readAgentResult")(function* (
  operation: Operation,
  handle: OwnedAgent,
) {
  const snapshots = yield* ProjectionSnapshotQuery.ProjectionSnapshotQuery;
  const childOption = yield* snapshots
    .getThreadDetailById(handle.childThreadId)
    .pipe(mapFailure(operation));
  if (Option.isNone(childOption)) {
    return yield* fail(operation, `Child thread '${handle.childThreadId}' no longer exists.`);
  }
  const child = childOption.value;
  const output = child.messages
    .toReversed()
    .find((message) => message.role === "assistant" && !message.streaming)?.text;
  const failed = child.latestTurn?.state === "error" || child.session?.status === "error";
  const interrupted =
    child.latestTurn?.state === "interrupted" ||
    child.session?.status === "interrupted" ||
    child.session?.status === "stopped";
  const settled = failed || interrupted || child.latestTurn?.state === "completed";
  return {
    child,
    settled,
    result: {
      ...handle,
      status: failed
        ? ("failed" as const)
        : interrupted
          ? ("interrupted" as const)
          : ("idle" as const),
      ...(output ? { output } : {}),
      ...(child.session?.lastError ? { error: child.session.lastError } : {}),
    },
  };
});

const isAgentSettledEvent = (event: OrchestrationEvent, parentId: ThreadId, taskId: string) => {
  const candidate = event as {
    type?: string;
    payload?: { threadId?: string; activity?: { kind?: string; payload?: unknown } };
  };
  if (candidate.type !== "thread.activity-appended" || candidate.payload?.threadId !== parentId)
    return false;
  const activity = candidate.payload.activity;
  if (
    activity?.kind !== "task.updated" ||
    typeof activity.payload !== "object" ||
    activity.payload === null
  )
    return false;
  const payload = activity.payload as Record<string, unknown>;
  return (
    payload.taskId === taskId &&
    (payload.status === "idle" || payload.status === "failed" || payload.status === "interrupted")
  );
};

const handlers = {
  agent_catalog: () =>
    Effect.gen(function* () {
      yield* getParent("catalog");
      const catalog = yield* providerCatalog();
      return {
        providers: catalog.map(({ provider, aliases }) => ({
          instanceId: provider.instanceId,
          displayName: provider.displayName ?? provider.instanceId,
          driver: provider.driver,
          aliases,
          models: provider.models.map((model) => ({ id: model.slug, name: model.name })),
        })),
      };
    }),

  agent_spawn: (input) =>
    Effect.gen(function* () {
      const { invocation, parent } = yield* getParent("spawn");
      const provider = yield* resolveProvider(input.providerInstanceId, "spawn");
      if (provider.instanceId === invocation.providerInstanceId) {
        return yield* fail(
          "spawn",
          `Provider '${provider.instanceId}' is this session's provider. Use its native spawnAgent tool instead.`,
        );
      }
      if (!provider.models.some((model) => model.slug === input.model)) {
        return yield* fail(
          "spawn",
          `Model '${input.model}' is not advertised by '${provider.instanceId}'. Call agent_catalog for valid models.`,
        );
      }
      const childThreadId = ThreadId.make(yield* randomId);
      const agentId = agentTaskId(childThreadId);
      const title = input.title ?? input.prompt.slice(0, 80).trim();
      const modelSelection: ModelSelection = {
        instanceId: provider.instanceId,
        model: input.model,
      };
      const engine = yield* OrchestrationEngine.OrchestrationEngineService;
      const createdAt = yield* nowIso;
      yield* engine
        .dispatch({
          type: "thread.create",
          commandId: CommandId.make(yield* randomId),
          threadId: childThreadId,
          projectId: parent.projectId,
          title,
          modelSelection,
          runtimeMode: parent.runtimeMode,
          interactionMode: "default",
          branch: parent.branch,
          worktreePath: parent.worktreePath,
          createdAt,
        })
        .pipe(mapFailure("spawn"));
      const linkage = {
        taskId: agentId,
        taskType: "t3_cross_provider_agent",
        agentKind: "agent",
        title,
        ...(input.role ? { role: input.role } : {}),
        model: input.model,
        providerInstanceId: provider.instanceId,
        parentThreadId: parent.id,
        childThreadId,
        runHandles: { runId: childThreadId },
        timelineBypass: true,
      };
      yield* appendActivity({
        operation: "spawn",
        threadId: childThreadId,
        turnId: null,
        kind: "t3.agent.linked",
        summary: "Linked cross-provider child",
        payload: linkage,
      });
      yield* appendActivity({
        operation: "spawn",
        threadId: parent.id,
        turnId: parent.session?.activeTurnId ?? null,
        kind: "task.started",
        summary: "Cross-provider agent started",
        payload: { ...linkage, detail: input.prompt },
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
          runtimeMode: parent.runtimeMode,
          interactionMode: "default",
          createdAt: yield* nowIso,
        })
        .pipe(
          mapFailure("spawn"),
          Effect.tapError((error) =>
            appendActivity({
              operation: "spawn",
              threadId: parent.id,
              turnId: parent.session?.activeTurnId ?? null,
              kind: "task.updated",
              summary: "Cross-provider agent failed to start",
              payload: { ...linkage, status: "failed", error: error.message },
            }),
          ),
        );
      return {
        agentId,
        childThreadId,
        providerInstanceId: provider.instanceId,
        model: input.model,
      };
    }),

  agent_wait: (input) =>
    Effect.scoped(
      Effect.gen(function* () {
        const { parent } = yield* getParent("wait");
        const handle = yield* findOwnedAgent(parent, "wait", input.agentId);
        const engine = yield* OrchestrationEngine.OrchestrationEngineService;
        const events = yield* engine.subscribeDomainEvents;
        const current = yield* readAgentResult("wait", handle);
        if (!current.settled) {
          yield* events.pipe(
            Stream.filter((event) => isAgentSettledEvent(event, parent.id, handle.agentId)),
            Stream.runHead,
          );
        }
        return (yield* readAgentResult("wait", handle)).result;
      }),
    ),

  agent_follow_up: (input) =>
    Effect.gen(function* () {
      const { parent } = yield* getParent("follow_up");
      const handle = yield* findOwnedAgent(parent, "follow_up", input.agentId);
      const state = yield* readAgentResult("follow_up", handle);
      if (!state.settled)
        return yield* fail("follow_up", `Agent '${input.agentId}' is still running.`);
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
          modelSelection: state.child.modelSelection,
          runtimeMode: state.child.runtimeMode,
          interactionMode: state.child.interactionMode,
          createdAt: yield* nowIso,
        })
        .pipe(
          mapFailure("follow_up"),
          Effect.tapError((error) =>
            appendActivity({
              operation: "follow_up",
              threadId: parent.id,
              turnId: parent.session?.activeTurnId ?? null,
              kind: "task.updated",
              summary: "Cross-provider agent follow-up failed",
              payload: {
                taskId: handle.agentId,
                taskType: "t3_cross_provider_agent",
                agentKind: "agent",
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
      const handle = yield* findOwnedAgent(parent, "interrupt", input.agentId);
      const engine = yield* OrchestrationEngine.OrchestrationEngineService;
      yield* engine
        .dispatch({
          type: "thread.turn.interrupt",
          commandId: CommandId.make(yield* randomId),
          threadId: handle.childThreadId,
          createdAt: yield* nowIso,
        })
        .pipe(mapFailure("interrupt"));
      return handle;
    }),
} satisfies Parameters<typeof AgentToolkit.toLayer>[0];

export const AgentToolkitHandlersLive = AgentToolkit.toLayer(handlers);
export const __testing = { handlers };
