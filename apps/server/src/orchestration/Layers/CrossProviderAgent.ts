/**
 * CrossProviderAgentLive - provider-neutral cross-provider agent service.
 *
 * Children are ordinary threads created through the engine's own commands;
 * ownership is the durable `spawn` metadata on the child thread; Direct
 * Spawns is driven by canonical `task.*` activities appended to the parent.
 * Nothing here talks to a provider protocol.
 *
 * @module CrossProviderAgentLive
 */
import {
  CROSS_PROVIDER_AGENT_SUPPORTED_DRIVERS,
  CROSS_PROVIDER_AGENT_TOOL_INPUTS,
  CROSS_PROVIDER_AGENT_TOOL_NAMES,
  classifyTaskAgentKind,
  CommandId,
  crossProviderAgentEffortOptionId,
  EventId,
  isCrossProviderAgentErrorOutput,
  MessageId,
  ProviderInstanceId,
  RuntimeTaskId,
  ThreadId,
  type CrossProviderAgentChildState,
  type CrossProviderAgentError,
  type CrossProviderAgentErrorOutput,
  type CrossProviderAgentToolName,
  type CrossProviderAgentWaitChild,
  type ModelSelection,
  type OrchestrationEvent,
  type OrchestrationThread,
  type OrchestrationThreadActivity,
  type OrchestrationThreadShell,
  type ProviderDriverKind,
  type RuntimeTaskUsage,
  type ServerProvider,
  type ServerProviderModel,
  type ServerSettings,
  type TurnId,
} from "@t3tools/contracts";
import { resolveCrossProviderAgentRoutes } from "@t3tools/shared/crossProviderAgentRoutes";
import {
  getModelSelectionStringOptionValue,
  getProviderOptionCurrentValue,
  getProviderOptionDescriptors,
} from "@t3tools/shared/model";
import * as Cause from "effect/Cause";
import * as Clock from "effect/Clock";
import * as Crypto from "effect/Crypto";
import * as DateTime from "effect/DateTime";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Predicate from "effect/Predicate";
import * as Schema from "effect/Schema";
import * as Scope from "effect/Scope";
import * as Stream from "effect/Stream";
// @effect-diagnostics-next-line nodeBuiltinImport:off
import * as NodeCrypto from "node:crypto";

import { ServerSettingsService } from "../../serverSettings.ts";
import { OrchestrationCommandReceiptRepository } from "../../persistence/Services/OrchestrationCommandReceipts.ts";
import {
  clearCrossProviderAgentToolHost,
  setCrossProviderAgentToolHost,
  type CrossProviderToolResult,
} from "../../provider/CrossProviderAgentToolHost.ts";
import { CROSS_PROVIDER_TOOL_SPECS } from "../../provider/crossProviderToolSchema.ts";
import { ProviderRegistry } from "../../provider/Services/ProviderRegistry.ts";
import {
  CrossProviderAgentService,
  type CrossProviderAgentServiceShape,
} from "../Services/CrossProviderAgent.ts";
import { OrchestrationEngineService } from "../Services/OrchestrationEngine.ts";
import { ProjectionSnapshotQuery } from "../Services/ProjectionSnapshotQuery.ts";

/** Task type stamped on the parent's Direct Spawns row for a cross-provider child. */
export const CROSS_PROVIDER_AGENT_TASK_TYPE = "cross_provider_agent";
const CROSS_PROVIDER_AGENT_ROLE = "cross-provider";
const MAX_OWNERSHIP_HOPS = 32;
const MAX_TITLE_CHARS = 60;
const MAX_SUMMARY_CHARS = 200;
/** Parent-row telemetry is coalesced to at most one mirror per child per interval. */
export const PROGRESS_MIRROR_INTERVAL_MS = 1000;
/** Child-thread activities that carry the telemetry a native row would show. */
const CHILD_TELEMETRY_ACTIVITY_KINDS: ReadonlySet<string> = new Set([
  "context-window.updated",
  "tool.started",
]);

const fail = (error: CrossProviderAgentError): CrossProviderAgentErrorOutput => ({ error });
const isThreadId = Schema.is(ThreadId);
const isProviderInstanceId = Schema.is(ProviderInstanceId);

/** Where a child stands, judged from its own projection only. */
export interface CrossProviderChildStatus {
  readonly settled: boolean;
  readonly state: CrossProviderAgentChildState;
  readonly turnId: TurnId | null;
}

/**
 * The projector creates `latestTurn` only once the provider reports the
 * turn running, so a freshly requested turn (spawn or follow-up) shows up
 * as a user message newer than the last started turn. A session that errors
 * before ever running settles the request as an error.
 */
export function evaluateCrossProviderChild(
  shell: Pick<OrchestrationThreadShell, "latestTurn" | "latestUserMessageAt" | "session">,
): CrossProviderChildStatus {
  const { latestTurn, latestUserMessageAt, session } = shell;
  const pendingStart =
    latestTurn === null ||
    (latestUserMessageAt !== null && latestUserMessageAt > latestTurn.requestedAt);
  if (pendingStart) {
    if (
      session?.status === "error" &&
      (latestUserMessageAt === null || session.updatedAt >= latestUserMessageAt)
    ) {
      return { settled: true, state: "error", turnId: latestTurn?.turnId ?? null };
    }
    return { settled: false, state: "running", turnId: latestTurn?.turnId ?? null };
  }
  if (latestTurn.state === "running") {
    return { settled: false, state: "running", turnId: latestTurn.turnId };
  }
  return { settled: true, state: latestTurn.state, turnId: latestTurn.turnId };
}

/** Final assistant text of the settled turn (falls back to the last assistant message). */
export function finalAssistantText(thread: OrchestrationThread, turnId: TurnId | null): string {
  const assistant = thread.messages.filter((message) => message.role === "assistant");
  const forTurn = turnId === null ? [] : assistant.filter((message) => message.turnId === turnId);
  return (forTurn.length > 0 ? forTurn : assistant).at(-1)?.text ?? "";
}

/**
 * The only permitted reduction of child output: above the cap, return a
 * deterministic head + tail and say so. Never a silent cut.
 */
export function applyOutputCap(
  text: string,
  cap: number,
  childId: string,
): { readonly output: string; readonly truncated: boolean; readonly totalChars: number } {
  const totalChars = text.length;
  if (totalChars <= cap) {
    return { output: text, truncated: false, totalChars };
  }
  const marker = (head: number, omitted: number) =>
    `\n[… ${omitted} characters omitted; call agent_result with childId "${childId}", offset ${head}, limit ${omitted} to read them …]\n`;
  // The marker is part of the returned string, so it comes out of the cap.
  // Its length depends on the digits it prints; two passes settle that.
  let budget = cap - marker(0, totalChars).length;
  for (let pass = 0; pass < 2; pass += 1) {
    const head = Math.max(0, Math.floor(budget / 2));
    budget = cap - marker(head, totalChars - Math.max(0, budget)).length;
  }
  budget = Math.max(0, budget);
  const head = Math.floor(budget / 2);
  const tail = budget - head;
  const omitted = totalChars - head - tail;
  return {
    output: `${text.slice(0, head)}${marker(head, omitted)}${text.slice(totalChars - tail)}`,
    truncated: true,
    totalChars,
  };
}

/**
 * Deterministic identity for one tool invocation. Everything a mutating call
 * creates (child thread id, message id, command ids) derives from it, so a
 * provider re-issuing the same call replays through command receipts instead
 * of creating a second child or turn. Without a provider call id there is
 * nothing to key on and a fresh identity is minted.
 */
export function crossProviderCallKey(input: {
  readonly callerThreadId: ThreadId;
  readonly tool: string;
  readonly callId: string;
}): string {
  return NodeCrypto.createHash("sha256")
    .update(`${input.callerThreadId}\u0000${input.tool}\u0000${input.callId}`)
    .digest("hex")
    .slice(0, 32);
}

/** A UUID-shaped thread id from a call key, so ids look like every other thread's. */
export function threadIdFromCallKey(key: string): ThreadId {
  const hex = key.padEnd(32, "0").slice(0, 32);
  return ThreadId.make(
    `${hex.slice(0, 8)}-${hex.slice(8, 12)}-4${hex.slice(13, 16)}-8${hex.slice(17, 20)}-${hex.slice(20, 32)}`,
  );
}

/** ISO stamp strictly after `floor` so a follow-up never ties with the last started turn. */
export function stampAfter(now: string, floor: string | null | undefined): string {
  if (floor === undefined || floor === null || now > floor) return now;
  return DateTime.formatIso(DateTime.add(DateTime.makeUnsafe(floor), { milliseconds: 1 }));
}

function boundedSummary(text: string): string | undefined {
  const trimmed = text.trim().replace(/\s+/g, " ");
  if (trimmed.length === 0) return undefined;
  return trimmed.length <= MAX_SUMMARY_CHARS
    ? trimmed
    : `${trimmed.slice(0, MAX_SUMMARY_CHARS - 1)}…`;
}

function threadTitle(title: string | undefined, prompt: string): string {
  const explicit = title?.trim();
  if (explicit) return explicit;
  const fromPrompt = prompt.trim().replace(/\s+/g, " ").slice(0, MAX_TITLE_CHARS).trim();
  return fromPrompt.length > 0 ? fromPrompt : "Cross-provider agent";
}

function effortDescriptor(model: ServerProviderModel | undefined, optionId: string | undefined) {
  if (model?.capabilities === null || model?.capabilities === undefined || optionId === undefined) {
    return undefined;
  }
  const descriptor = getProviderOptionDescriptors({ caps: model.capabilities }).find(
    (candidate) => candidate.id === optionId,
  );
  return descriptor?.type === "select" ? descriptor : undefined;
}

/** A thread's effective effort: its explicit selection resolved against its model's choices, else the model default. */
function effectiveThreadEffort(input: {
  readonly driver: ProviderDriverKind;
  readonly model: ServerProviderModel | undefined;
  readonly modelSelection: ModelSelection;
}): string | undefined {
  const optionId = crossProviderAgentEffortOptionId(input.driver);
  if (optionId === undefined) return undefined;
  const raw = getModelSelectionStringOptionValue(input.modelSelection, optionId);
  if (input.model?.capabilities === null || input.model?.capabilities === undefined) return raw;
  const descriptor = getProviderOptionDescriptors({
    caps: input.model.capabilities,
    ...(raw !== undefined ? { selections: [{ id: optionId, value: raw }] } : {}),
  }).find((candidate) => candidate.id === optionId);
  const value = getProviderOptionCurrentValue(descriptor);
  return typeof value === "string" ? value : undefined;
}

/**
 * The effort a child runs at, the way a native spawn resolves it: an
 * explicit request wins (and must be one the target model offers), else the
 * parent's effective effort when the target offers it, else the target's
 * default. A target without an effort option gets none.
 */
export function resolveCrossProviderEffort(input: {
  readonly explicit: string | undefined;
  readonly parent: {
    readonly driver: ProviderDriverKind;
    readonly model: ServerProviderModel | undefined;
    readonly modelSelection: ModelSelection;
  };
  readonly target: { readonly driver: ProviderDriverKind; readonly model: ServerProviderModel };
}):
  | { readonly selection: { readonly id: string; readonly value: string } | undefined }
  | { readonly unsupported: ReadonlyArray<string> } {
  const optionId = crossProviderAgentEffortOptionId(input.target.driver);
  const descriptor = effortDescriptor(input.target.model, optionId);
  const choices = descriptor?.options.map((choice) => choice.id) ?? [];
  const explicit = input.explicit?.trim();
  if (explicit !== undefined && explicit.length > 0) {
    return optionId !== undefined && choices.includes(explicit)
      ? { selection: { id: optionId, value: explicit } }
      : { unsupported: choices };
  }
  if (optionId === undefined || descriptor === undefined) return { selection: undefined };
  const inherited = effectiveThreadEffort(input.parent);
  if (inherited !== undefined && choices.includes(inherited)) {
    return { selection: { id: optionId, value: inherited } };
  }
  const fallback = getProviderOptionCurrentValue(descriptor);
  return {
    selection: typeof fallback === "string" ? { id: optionId, value: fallback } : undefined,
  };
}

/** The effort a child was created with, read under its own driver's option id. */
function childEffort(
  driver: ProviderDriverKind | undefined,
  modelSelection: ModelSelection,
): string | undefined {
  const optionId = driver === undefined ? undefined : crossProviderAgentEffortOptionId(driver);
  return optionId === undefined
    ? undefined
    : getModelSelectionStringOptionValue(modelSelection, optionId);
}

function finiteCount(value: unknown): number | undefined {
  return typeof value === "number" && Number.isFinite(value) && value >= 0 ? value : undefined;
}

function nonEmptyString(value: unknown): string | undefined {
  return typeof value === "string" && value.trim().length > 0 ? value.trim() : undefined;
}

/** Tool name the way a native row shows it: the provider's tool name, else the item title. */
function toolNameFromActivity(payload: unknown): string | undefined {
  if (!Predicate.isObject(payload)) return undefined;
  const data = payload.data;
  return (
    (Predicate.isObject(data) ? nonEmptyString(data.toolName) : undefined) ??
    nonEmptyString(payload.title)
  );
}

/**
 * What a child has reported so far. `totalTokens` is the provider's own
 * cumulative processed count and stays undefined until one has been
 * reported (the row shows "— tok" rather than a wrong number); `toolUses`
 * is a monotonic count of the child's tool calls.
 */
export interface CrossProviderChildTelemetry {
  readonly totalTokens: number | undefined;
  readonly toolUses: number;
  readonly lastToolName: string | undefined;
}

export const EMPTY_CHILD_TELEMETRY: CrossProviderChildTelemetry = {
  totalTokens: undefined,
  toolUses: 0,
  lastToolName: undefined,
};

/**
 * The cumulative token count on a `context-window.updated` activity. Only
 * `totalProcessedTokens` qualifies: Codex fills it from the thread's
 * cumulative `total` usage and Claude from the SDK result's cumulative
 * `total_tokens`, both only once it exceeds the current context size.
 * `usedTokens` is context occupancy (it shrinks on compaction) and is never
 * a row total.
 */
export function processedTokensFromActivity(payload: unknown): number | undefined {
  return Predicate.isObject(payload) ? finiteCount(payload.totalProcessedTokens) : undefined;
}

/** Fold one child activity into the telemetry; unrelated kinds pass through. */
export function foldChildTelemetry(
  current: CrossProviderChildTelemetry,
  activity: Pick<OrchestrationThreadActivity, "kind" | "payload">,
): CrossProviderChildTelemetry {
  if (activity.kind === "context-window.updated") {
    const total = processedTokensFromActivity(activity.payload);
    return total === undefined
      ? current
      : { ...current, totalTokens: Math.max(current.totalTokens ?? 0, Math.round(total)) };
  }
  if (activity.kind === "tool.started") {
    return {
      ...current,
      toolUses: current.toolUses + 1,
      lastToolName: toolNameFromActivity(activity.payload) ?? current.lastToolName,
    };
  }
  return current;
}

/** Field-wise maximum, so two observers of the same child agree. */
export function mergeChildTelemetry(
  a: CrossProviderChildTelemetry,
  b: CrossProviderChildTelemetry,
): CrossProviderChildTelemetry {
  const totalTokens =
    a.totalTokens === undefined
      ? b.totalTokens
      : b.totalTokens === undefined
        ? a.totalTokens
        : Math.max(a.totalTokens, b.totalTokens);
  return {
    totalTokens,
    toolUses: Math.max(a.toolUses, b.toolUses),
    lastToolName: a.lastToolName ?? b.lastToolName,
  };
}

/** The row's `typedUsage`; absent until the provider has reported a cumulative total. */
export function typedUsageFromTelemetry(
  telemetry: CrossProviderChildTelemetry,
): RuntimeTaskUsage | undefined {
  return telemetry.totalTokens === undefined
    ? undefined
    : {
        totalTokens: telemetry.totalTokens,
        ...(telemetry.toolUses > 0 ? { toolUses: telemetry.toolUses } : {}),
      };
}

const toolInputDecoders = Object.fromEntries(
  CROSS_PROVIDER_AGENT_TOOL_NAMES.map((name) => [
    name,
    Schema.decodeUnknownEffect(CROSS_PROVIDER_AGENT_TOOL_INPUTS[name]),
  ]),
) as {
  readonly [Name in CrossProviderAgentToolName]: ReturnType<
    typeof Schema.decodeUnknownEffect<(typeof CROSS_PROVIDER_AGENT_TOOL_INPUTS)[Name]>
  >;
};

const isToolName = (value: string): value is CrossProviderAgentToolName =>
  (CROSS_PROVIDER_AGENT_TOOL_NAMES as ReadonlyArray<string>).includes(value);

const makeCrossProviderAgent = Effect.gen(function* () {
  const engine = yield* OrchestrationEngineService;
  const projection = yield* ProjectionSnapshotQuery;
  const providerRegistry = yield* ProviderRegistry;
  const settingsService = yield* ServerSettingsService;
  const commandReceipts = yield* OrchestrationCommandReceiptRepository;
  const crypto = yield* Crypto.Crypto;
  const serviceScope = yield* Scope.Scope;

  /**
   * A call-derived command that already has an accepted receipt is a replay
   * of a call whose response was lost. Answer it the same way as the first
   * time instead of re-running lifecycle checks the mutation itself changed.
   */
  const alreadyAccepted = (commandId: CommandId) =>
    Effect.map(
      commandReceipts.getByCommandId({ commandId }),
      (receipt) => Option.isSome(receipt) && receipt.value.status === "accepted",
    );

  const nowIso = Effect.map(DateTime.now, DateTime.formatIso);
  const uuid = crypto.randomUUIDv4;

  /** Internal failures become a structured result; the cause is logged, never surfaced. */
  const guard = <A, E>(
    operation: string,
    effect: Effect.Effect<A, E>,
  ): Effect.Effect<A | CrossProviderAgentErrorOutput> =>
    effect.pipe(
      Effect.catchCause((cause) =>
        Cause.hasInterruptsOnly(cause)
          ? // Interrupt-only: no failure to surface, so the narrowing is sound.
            Effect.failCause(cause as Cause.Cause<never>)
          : Effect.logError("cross-provider agent operation failed", {
              operation,
              cause: Cause.pretty(cause),
            }).pipe(
              Effect.as(
                fail({
                  code: "internal",
                  message: `${operation} failed inside T3; see the server log.`,
                }),
              ),
            ),
      ),
      Effect.withSpan(`CrossProviderAgent.${operation}`),
    );

  const readShell = (threadId: ThreadId) => projection.getThreadShellById(threadId);

  interface CallerContext {
    readonly settings: ServerSettings;
    readonly shell: OrchestrationThreadShell;
    readonly providers: ReadonlyArray<ServerProvider>;
    readonly callerInstanceId: ProviderInstanceId;
    readonly callerDepth: number;
  }

  const callerContext = Effect.fn("CrossProviderAgent.callerContext")(function* (
    callerThreadId: ThreadId,
  ) {
    const settings = yield* settingsService.getSettings;
    if (!settings.enableCrossProviderAgentAccess) {
      return fail({
        code: "access_disabled",
        message: "Cross-provider agent access is disabled in Settings → Integrations → Agents.",
      });
    }
    const shell = yield* readShell(callerThreadId);
    if (Option.isNone(shell)) {
      return fail({ code: "unsupported_caller", message: "The calling thread does not exist." });
    }
    const providers = yield* providerRegistry.getProviders;
    const callerInstanceId = shell.value.modelSelection.instanceId;
    const callerProvider = providers.find((provider) => provider.instanceId === callerInstanceId);
    if (
      callerProvider === undefined ||
      !CROSS_PROVIDER_AGENT_SUPPORTED_DRIVERS.includes(callerProvider.driver)
    ) {
      return fail({
        code: "unsupported_caller",
        message: "Only Claude and Codex threads can use cross-provider agent tools.",
        providerInstanceId: callerInstanceId,
      });
    }
    return {
      settings,
      shell: shell.value,
      providers,
      callerInstanceId,
      callerDepth: shell.value.spawn?.depth ?? 0,
    } satisfies CallerContext;
  });

  /** The caller owns a child when it is the parent or any ancestor in the spawn chain. */
  const readOwnedChild = Effect.fn("CrossProviderAgent.readOwnedChild")(function* (
    callerThreadId: ThreadId,
    rawChildId: string,
  ) {
    const notOwner = fail({
      code: "not_owner",
      message: "No child with that id is owned by this thread.",
      childId: rawChildId,
    });
    if (!isThreadId(rawChildId)) return notOwner;
    const childId = ThreadId.make(rawChildId);
    const child = yield* readShell(childId);
    if (Option.isNone(child) || child.value.spawn === undefined) return notOwner;
    let cursor: OrchestrationThreadShell = child.value;
    for (let hops = 0; hops < MAX_OWNERSHIP_HOPS && cursor.spawn !== undefined; hops += 1) {
      if (cursor.spawn.parentThreadId === callerThreadId) return child.value;
      const parent = yield* readShell(cursor.spawn.parentThreadId);
      if (Option.isNone(parent)) break;
      cursor = parent.value;
    }
    return notOwner;
  });

  /**
   * Lifecycle rows keep one activity per command; telemetry rows pass a
   * stable `activityId` (the ids ingestion uses for native rows) so each
   * mirror replaces the previous one in the projection instead of growing it.
   */
  const appendParentActivity = (input: {
    readonly parentThreadId: ThreadId;
    readonly commandId: CommandId;
    readonly activityId?: string;
    readonly kind:
      | "task.started"
      | "task.updated"
      | "task.completed"
      | "task.progress"
      | "tool.progress";
    readonly summary: string;
    readonly payload: Record<string, unknown>;
  }) =>
    Effect.gen(function* () {
      const createdAt = yield* nowIso;
      yield* engine.dispatch({
        type: "thread.activity.append",
        commandId: input.commandId,
        threadId: input.parentThreadId,
        activity: {
          id: EventId.make(input.activityId ?? `xp-agent:${input.commandId}`),
          tone: "info",
          kind: input.kind,
          summary: input.summary,
          payload: input.payload,
          turnId: null,
          createdAt,
        },
        createdAt,
      });
    });

  const driverOf = (instanceId: ProviderInstanceId) =>
    Effect.map(
      providerRegistry.getProviders,
      (providers) => providers.find((provider) => provider.instanceId === instanceId)?.driver,
    );

  /** Identity fields repeated on every Direct Spawns row for this child. */
  const rowLinkage = (child: OrchestrationThreadShell) =>
    Effect.map(driverOf(child.modelSelection.instanceId), (driver) => {
      const effort = childEffort(driver, child.modelSelection);
      return {
        taskId: child.spawn!.taskId,
        taskType: CROSS_PROVIDER_AGENT_TASK_TYPE,
        agentKind: classifyTaskAgentKind({ taskType: CROSS_PROVIDER_AGENT_TASK_TYPE }),
        title: child.title,
        role: CROSS_PROVIDER_AGENT_ROLE,
        model: child.modelSelection.model,
        ...(effort !== undefined ? { effort } : {}),
        timelineBypass: true,
        childThreadId: child.id,
        providerInstanceId: child.modelSelection.instanceId,
      };
    });

  /**
   * The child's telemetry from the projection: the latest reported
   * cumulative total (the server retains only the newest context-window
   * row per turn, so this read is small) and an exact COUNT of its tool
   * calls, never the bounded detail window. Used once to seed a watcher and
   * once at settlement.
   */
  const readChildTelemetry = (childId: ThreadId) =>
    Effect.gen(function* () {
      const detail = yield* projection.getThreadDetailById(childId, {
        activityKinds: ["context-window.updated"],
      });
      const toolUses = yield* projection.countThreadActivitiesByKind({
        threadId: childId,
        kind: "tool.started",
      });
      const fromActivities = (Option.isSome(detail) ? detail.value.activities : []).reduce(
        foldChildTelemetry,
        EMPTY_CHILD_TELEMETRY,
      );
      return {
        ...fromActivities,
        toolUses,
        lastToolName: undefined,
      } satisfies CrossProviderChildTelemetry;
    });

  /**
   * Mirror a settled child onto its parent's row exactly once per turn: the
   * command id carries the turn, so a wait and the background watcher can
   * both observe the same settlement without a duplicate row.
   */
  const mirrorSettlement = (
    child: OrchestrationThreadShell,
    status: CrossProviderChildStatus,
    observed: CrossProviderChildTelemetry = EMPTY_CHILD_TELEMETRY,
  ) =>
    Effect.gen(function* () {
      if (!status.settled || child.spawn === undefined) return;
      const detail = yield* projection.getThreadDetailById(child.id, { activityKinds: [] });
      const output = Option.isSome(detail) ? finalAssistantText(detail.value, status.turnId) : "";
      const summary = boundedSummary(output);
      const typedUsage = typedUsageFromTelemetry(
        mergeChildTelemetry(yield* readChildTelemetry(child.id), observed),
      );
      const linkage = yield* rowLinkage(child);
      const completedStatus =
        status.state === "completed"
          ? "completed"
          : status.state === "error"
            ? "failed"
            : "stopped";
      yield* appendParentActivity({
        parentThreadId: child.spawn.parentThreadId,
        commandId: CommandId.make(
          `server:xp-agent:task-settled:${child.spawn.parentThreadId}:${child.id}:${status.turnId ?? "no-turn"}`,
        ),
        kind: "task.completed",
        summary: `Cross-provider agent ${completedStatus}`,
        payload: {
          ...linkage,
          status: completedStatus,
          ...(summary ? { summary } : {}),
          ...(typedUsage ? { typedUsage } : {}),
          ...(status.state === "error" && child.session?.lastError
            ? { error: child.session.lastError }
            : {}),
        },
      });
    });

  /** Thread id of the events that can move a child toward a readable settlement. */
  const settlementEventThreadId = (event: OrchestrationEvent): ThreadId | undefined => {
    switch (event.type) {
      case "thread.session-set":
      case "thread.turn-diff-completed":
      case "thread.message-sent":
        return event.payload.threadId;
      default:
        return undefined;
    }
  };

  /**
   * A settled turn whose assistant message is still streaming has not been
   * finalized yet (ingestion completes it in the same handler that settles
   * the session); reading now would return partial text.
   */
  const settledOutputReadable = (childId: ThreadId, status: CrossProviderChildStatus) =>
    Effect.gen(function* () {
      if (!status.settled || status.turnId === null) return true;
      const detail = yield* projection.getThreadDetailById(childId, { activityKinds: [] });
      if (Option.isNone(detail)) return true;
      return !detail.value.messages.some(
        (message) =>
          message.role === "assistant" && message.turnId === status.turnId && message.streaming,
      );
    });

  interface ProgressMirrorState {
    readonly parentThreadId: ThreadId;
    readonly taskId: RuntimeTaskId;
    /** Row identity captured at seed time; settlement re-reads the shell. */
    readonly linkage: Record<string, unknown>;
    telemetry: CrossProviderChildTelemetry;
    dirty: boolean;
    lastFlushMs: number;
    lastSequence: number;
    mirrored: CrossProviderChildTelemetry;
    fiber: Fiber.Fiber<void> | null;
  }

  /**
   * Dispatch the child's current telemetry onto its parent row with the same
   * stable activity ids ingestion gives native rows; nothing is read here.
   * Command ids embed the last child event folded in, so two flushes of the
   * same state replay one receipt, and a later flush with new events writes
   * a new command that the stable activity id then upserts into place.
   */
  const flushProgress = (childId: ThreadId, state: ProgressMirrorState) =>
    Effect.gen(function* () {
      const { parentThreadId, taskId, linkage, telemetry } = state;
      let mirrored = false;
      const mint = (op: string) =>
        CommandId.make(`server:xp-agent:${op}:${parentThreadId}:${childId}:${state.lastSequence}`);
      const typedUsage = typedUsageFromTelemetry(telemetry);
      if (
        typedUsage !== undefined &&
        (telemetry.totalTokens !== state.mirrored.totalTokens ||
          telemetry.toolUses !== state.mirrored.toolUses)
      ) {
        yield* appendParentActivity({
          parentThreadId,
          commandId: mint("task-usage"),
          activityId: `task-usage:${parentThreadId}:${taskId}`,
          kind: "task.progress",
          summary: "Task usage updated",
          payload: { ...linkage, usageSnapshot: true, typedUsage },
        });
        mirrored = true;
      }
      if (
        telemetry.lastToolName !== undefined &&
        telemetry.lastToolName !== state.mirrored.lastToolName
      ) {
        yield* appendParentActivity({
          parentThreadId,
          commandId: mint("task-progress"),
          activityId: `task-progress:${parentThreadId}:${taskId}`,
          kind: "task.progress",
          summary: `Cross-provider agent used ${telemetry.lastToolName}`,
          payload: { ...linkage, lastToolName: telemetry.lastToolName },
        });
        yield* appendParentActivity({
          parentThreadId,
          commandId: mint("tool-progress"),
          activityId: `tool-progress:${parentThreadId}:${taskId}`,
          kind: "tool.progress",
          summary: telemetry.lastToolName,
          payload: { taskId, toolName: telemetry.lastToolName },
        });
        mirrored = true;
      }
      state.mirrored = telemetry;
      return mirrored;
    });

  /**
   * Subscribe first, then read: any settlement committed before the read is
   * visible in the projection, anything after it arrives on the stream.
   * With `mirrorProgress`, the child's usage and tool activity are mirrored
   * onto the parent row as they arrive, coalesced to one mirror per
   * `PROGRESS_MIRROR_INTERVAL_MS`; settlement always carries the final values.
   */
  const awaitSettlement = (
    childIds: ReadonlyArray<ThreadId>,
    timeout: Duration.Duration | undefined,
    options?: { readonly mirrorProgress?: boolean },
  ) =>
    Effect.scoped(
      Effect.gen(function* () {
        const events = yield* engine.subscribeDomainEvents;
        const pending = new Set<string>(childIds);
        const statuses = new Map<ThreadId, CrossProviderChildStatus>();
        const shells = new Map<ThreadId, OrchestrationThreadShell>();
        const progress = new Map<ThreadId, ProgressMirrorState>();
        const refresh = (childId: ThreadId) =>
          Effect.gen(function* () {
            const shell = yield* readShell(childId);
            if (Option.isNone(shell)) {
              pending.delete(childId);
              return;
            }
            shells.set(childId, shell.value);
            const status = evaluateCrossProviderChild(shell.value);
            if (status.settled && !(yield* settledOutputReadable(childId, status))) {
              return;
            }
            statuses.set(childId, status);
            if (status.settled) {
              pending.delete(childId);
              const state = progress.get(childId);
              progress.delete(childId);
              if (state?.fiber) yield* Fiber.interrupt(state.fiber);
              yield* mirrorSettlement(shell.value, status, state?.telemetry);
            }
          });
        /** One projection read per child at watcher start; everything after folds from events. */
        const seed = (childId: ThreadId) =>
          Effect.gen(function* () {
            const shell = shells.get(childId);
            if (shell === undefined || shell.spawn === undefined) return;
            const telemetry = yield* readChildTelemetry(childId);
            progress.set(childId, {
              parentThreadId: shell.spawn.parentThreadId,
              taskId: shell.spawn.taskId,
              linkage: yield* rowLinkage(shell),
              telemetry,
              dirty: false,
              lastFlushMs: Number.NEGATIVE_INFINITY,
              lastSequence: 0,
              mirrored: telemetry,
              fiber: null,
            });
          });
        // One scheduler per child: the first event mirrors at once, later
        // ones wait out the interval and mirror the latest state. The final
        // dirty check and the fiber hand-back share one synchronous step so
        // an event landing between them cannot be stranded.
        const runScheduler = (childId: ThreadId, state: ProgressMirrorState) =>
          Effect.gen(function* () {
            for (;;) {
              const now = yield* Clock.currentTimeMillis;
              const wait = state.lastFlushMs + PROGRESS_MIRROR_INTERVAL_MS - now;
              if (wait > 0) yield* Effect.sleep(Duration.millis(wait));
              if (!pending.has(childId)) {
                state.fiber = null;
                return;
              }
              state.dirty = false;
              // A settlement interrupt waits for an in-flight mirror rather
              // than cutting a parent dispatch short. Only a flush that wrote
              // something starts the interval: an event that changed nothing
              // visible must not delay the next real mirror.
              if (yield* Effect.uninterruptible(flushProgress(childId, state))) {
                state.lastFlushMs = yield* Clock.currentTimeMillis;
              }
              if (!state.dirty) {
                state.fiber = null;
                return;
              }
            }
          }).pipe(
            Effect.catchCause((cause) =>
              Cause.hasInterruptsOnly(cause)
                ? Effect.void
                : Effect.logWarning("cross-provider agent telemetry mirror failed", {
                    childThreadId: childId,
                    cause: Cause.pretty(cause),
                  }).pipe(
                    Effect.andThen(
                      Effect.sync(() => {
                        state.fiber = null;
                      }),
                    ),
                  ),
            ),
          );
        const observe = (event: OrchestrationEvent) =>
          Effect.gen(function* () {
            if (event.type === "thread.activity-appended") {
              if (options?.mirrorProgress !== true) return;
              const childId = event.payload.threadId;
              const activity = event.payload.activity;
              if (!pending.has(childId) || !CHILD_TELEMETRY_ACTIVITY_KINDS.has(activity.kind)) {
                return;
              }
              const state = progress.get(childId);
              if (state === undefined) return;
              state.telemetry = foldChildTelemetry(state.telemetry, activity);
              state.dirty = true;
              state.lastSequence = Math.max(state.lastSequence, event.sequence);
              if (state.fiber === null) {
                state.fiber = yield* Effect.forkScoped(runScheduler(childId, state));
              }
              return;
            }
            const threadId = settlementEventThreadId(event);
            if (threadId !== undefined && pending.has(threadId)) {
              yield* refresh(threadId);
            }
          });
        yield* Effect.forEach(childIds, refresh, { discard: true });
        if (options?.mirrorProgress === true) {
          yield* Effect.forEach([...pending], (childId) => seed(ThreadId.make(childId)), {
            discard: true,
          });
        }
        if (pending.size > 0) {
          const drain = events.pipe(
            Stream.mapEffect(observe),
            Stream.takeUntil(() => pending.size === 0),
            Stream.runDrain,
          );
          yield* timeout === undefined ? drain : Effect.timeoutOption(drain, timeout);
        }
        return statuses;
      }),
    );

  /** Keep the parent's row honest even when nobody waits on the child. */
  const watchSettlement = (childId: ThreadId) =>
    awaitSettlement([childId], undefined, { mirrorProgress: true }).pipe(
      Effect.catchCause((cause) =>
        Cause.hasInterruptsOnly(cause)
          ? Effect.void
          : Effect.logWarning("cross-provider agent settlement watcher stopped", {
              childThreadId: childId,
              cause: Cause.pretty(cause),
            }),
      ),
      Effect.forkIn(serviceScope),
    );

  const catalog: CrossProviderAgentServiceShape["catalog"] = (callerThreadId) =>
    guard(
      "catalog",
      Effect.gen(function* () {
        const context = yield* callerContext(callerThreadId);
        if (isCrossProviderAgentErrorOutput(context)) return context;
        const routes = resolveCrossProviderAgentRoutes(
          context.settings.crossProviderAgentRoutes,
          context.providers,
        );
        return {
          routes: routes.map((route) => ({
            providerInstanceId: route.providerInstanceId,
            displayName: route.provider.displayName ?? route.providerInstanceId,
            driver: route.provider.driver,
            models: route.provider.models
              .filter((model) => route.models.length === 0 || route.models.includes(model.slug))
              .map((model) => ({ slug: model.slug, name: model.name })),
            isCallerInstance: route.providerInstanceId === context.callerInstanceId,
          })),
          maxDepth: context.settings.crossProviderAgentMaxDepth,
          callerDepth: context.callerDepth,
        };
      }),
    );

  /** Identity for a mutating call: the provider's call id when given, else fresh. */
  const callIdentity = (callerThreadId: ThreadId, tool: string, callId: string | undefined) =>
    callId === undefined
      ? Effect.map(uuid, (id) => id.replaceAll("-", ""))
      : Effect.succeed(crossProviderCallKey({ callerThreadId, tool, callId }));

  const spawn: CrossProviderAgentServiceShape["spawn"] = (callerThreadId, input, options) =>
    guard(
      "spawn",
      Effect.gen(function* () {
        const context = yield* callerContext(callerThreadId);
        if (isCrossProviderAgentErrorOutput(context)) return context;
        const { settings, shell: parent, providers } = context;

        if (!isProviderInstanceId(input.providerInstanceId)) {
          return fail({
            code: "route_not_eligible",
            message: "Unknown provider instance; use an id from agent_catalog.",
            providerInstanceId: input.providerInstanceId,
          });
        }
        const targetInstanceId = ProviderInstanceId.make(input.providerInstanceId);
        if (targetInstanceId === context.callerInstanceId) {
          return fail({
            code: "same_instance",
            message:
              "That is your own provider instance; use your native spawn tool for same-provider children.",
            providerInstanceId: targetInstanceId,
          });
        }
        const target = providers.find((provider) => provider.instanceId === targetInstanceId);
        if (
          target !== undefined &&
          CROSS_PROVIDER_AGENT_SUPPORTED_DRIVERS.includes(target.driver) &&
          !target.enabled
        ) {
          return fail({
            code: "instance_disabled",
            message: "That provider instance is disabled.",
            providerInstanceId: targetInstanceId,
          });
        }
        if (
          target !== undefined &&
          CROSS_PROVIDER_AGENT_SUPPORTED_DRIVERS.includes(target.driver) &&
          target.enabled &&
          target.auth.status === "unauthenticated"
        ) {
          return fail({
            code: "instance_unauthenticated",
            message: "That provider instance is signed out.",
            providerInstanceId: targetInstanceId,
          });
        }
        const route = resolveCrossProviderAgentRoutes(
          settings.crossProviderAgentRoutes,
          providers,
        ).find((candidate) => candidate.providerInstanceId === targetInstanceId);
        if (route === undefined) {
          return fail({
            code: "route_not_eligible",
            message: "That provider instance is not an eligible cross-provider route.",
            providerInstanceId: targetInstanceId,
          });
        }
        const modelOffered =
          route.provider.models.some((model) => model.slug === input.model) &&
          (route.models.length === 0 || route.models.includes(input.model));
        if (!modelOffered) {
          return fail({
            code: "model_not_offered",
            message: "That exact model is not offered on that provider instance.",
            providerInstanceId: targetInstanceId,
            model: input.model,
          });
        }
        const targetModel = route.provider.models.find((model) => model.slug === input.model)!;
        const callerProvider = providers.find(
          (provider) => provider.instanceId === context.callerInstanceId,
        )!;
        const effort = resolveCrossProviderEffort({
          explicit: input.effort,
          parent: {
            driver: callerProvider.driver,
            model: callerProvider.models.find(
              (model) => model.slug === parent.modelSelection.model,
            ),
            modelSelection: parent.modelSelection,
          },
          target: { driver: route.provider.driver, model: targetModel },
        });
        if ("unsupported" in effort) {
          return fail({
            code: "invalid_input",
            message:
              effort.unsupported.length === 0
                ? `Model ${input.model} on that instance has no effort setting; omit effort.`
                : `Model ${input.model} on that instance does not offer effort "${input.effort}"; offered: ${effort.unsupported.join(", ")}.`,
            providerInstanceId: targetInstanceId,
            model: input.model,
          });
        }
        const allowOrchestration = input.allowOrchestration === true;
        const childDepth = context.callerDepth + 1;
        const maxDepth = settings.crossProviderAgentMaxDepth;
        if (childDepth > maxDepth || (allowOrchestration && childDepth >= maxDepth)) {
          return fail({
            code: "depth_exceeded",
            message: allowOrchestration
              ? `A sub-orchestrator at depth ${childDepth} could never spawn within the depth cap of ${maxDepth}.`
              : `Spawning at depth ${childDepth} exceeds the depth cap of ${maxDepth}.`,
            providerInstanceId: targetInstanceId,
            model: input.model,
          });
        }

        // Everything below derives from the call identity, so a re-issued
        // call (same callId after a lost response) replays every command
        // through its receipt instead of creating a second child or turn.
        const key = yield* callIdentity(callerThreadId, "agent_spawn", options?.callId);
        const childId = threadIdFromCallKey(key);
        const taskId = RuntimeTaskId.make(`xp-agent:${childId}`);
        const mint = (op: string) =>
          CommandId.make(`server:xp-agent:${op}:${callerThreadId}:${key}`);
        const createCommandId = mint("thread-create");
        const startedCommandId = mint("task-started");
        const turnCommandId = mint("turn-start");
        const createdAt = yield* nowIso;
        const title = threadTitle(input.title, input.prompt);

        yield* engine.dispatch({
          type: "thread.create",
          commandId: createCommandId,
          threadId: childId,
          projectId: parent.projectId,
          title,
          modelSelection: {
            instanceId: targetInstanceId,
            model: input.model,
            ...(effort.selection !== undefined ? { options: [effort.selection] } : {}),
          },
          runtimeMode: parent.runtimeMode,
          interactionMode: parent.interactionMode,
          branch: parent.branch,
          worktreePath: parent.worktreePath,
          createdAt,
          spawn: {
            parentThreadId: callerThreadId,
            allowOrchestration,
            depth: childDepth,
            taskId,
          },
        });
        const child = yield* readShell(childId);
        if (Option.isNone(child)) {
          return fail({
            code: "internal",
            message: "The child thread was created but is not yet readable.",
            childId,
          });
        }
        yield* appendParentActivity({
          parentThreadId: callerThreadId,
          commandId: startedCommandId,
          kind: "task.started",
          summary: `Cross-provider agent started on ${target?.displayName ?? targetInstanceId}`,
          payload: { ...(yield* rowLinkage(child.value)), status: "running" },
        });
        yield* engine.dispatch({
          type: "thread.turn.start",
          commandId: turnCommandId,
          threadId: childId,
          message: {
            messageId: MessageId.make(`xp-agent:spawn:${key}`),
            role: "user",
            text: input.prompt,
            attachments: [],
          },
          runtimeMode: parent.runtimeMode,
          interactionMode: parent.interactionMode,
          createdAt: yield* nowIso,
        });
        yield* watchSettlement(childId);
        yield* Effect.logInfo("cross-provider agent spawned", {
          parentThreadId: callerThreadId,
          childThreadId: childId,
          providerInstanceId: targetInstanceId,
          model: input.model,
          effort: effort.selection?.value ?? null,
          depth: childDepth,
          allowOrchestration,
        });
        return {
          childId,
          taskId,
          providerInstanceId: targetInstanceId,
          model: input.model,
          status: "running" as const,
        };
      }),
    );

  const wait: CrossProviderAgentServiceShape["wait"] = (callerThreadId, input) =>
    guard(
      "wait",
      Effect.gen(function* () {
        const context = yield* callerContext(callerThreadId);
        if (isCrossProviderAgentErrorOutput(context)) return context;
        const children: ThreadId[] = [];
        for (const rawChildId of input.childIds) {
          const child = yield* readOwnedChild(callerThreadId, rawChildId);
          if (isCrossProviderAgentErrorOutput(child)) return child;
          children.push(child.id);
        }
        const statuses = yield* awaitSettlement(
          children,
          input.timeoutSeconds === undefined ? undefined : Duration.seconds(input.timeoutSeconds),
        );
        const cap = context.settings.crossProviderAgentOutputCapChars;
        const entries: CrossProviderAgentWaitChild[] = [];
        for (const childId of children) {
          const status = statuses.get(childId) ?? {
            settled: false,
            state: "running" as const,
            turnId: null,
          };
          if (!status.settled) {
            entries.push({ childId, settled: false, state: status.state });
            continue;
          }
          const detail = yield* projection.getThreadDetailById(childId, { activityKinds: [] });
          const text = Option.isSome(detail) ? finalAssistantText(detail.value, status.turnId) : "";
          entries.push({
            childId,
            settled: true,
            state: status.state,
            ...applyOutputCap(text, cap, childId),
          });
        }
        return { children: entries };
      }),
    );

  const result: CrossProviderAgentServiceShape["result"] = (callerThreadId, input) =>
    guard(
      "result",
      Effect.gen(function* () {
        const context = yield* callerContext(callerThreadId);
        if (isCrossProviderAgentErrorOutput(context)) return context;
        const child = yield* readOwnedChild(callerThreadId, input.childId);
        if (isCrossProviderAgentErrorOutput(child)) return child;
        const status = evaluateCrossProviderChild(child);
        if (!status.settled) {
          return fail({
            code: "child_not_settled",
            message: "The child is still running; wait for it first.",
            childId: child.id,
          });
        }
        const detail = yield* projection.getThreadDetailById(child.id, { activityKinds: [] });
        const text = Option.isSome(detail) ? finalAssistantText(detail.value, status.turnId) : "";
        if (input.offset === undefined && input.limit === undefined) {
          const capped = applyOutputCap(
            text,
            context.settings.crossProviderAgentOutputCapChars,
            child.id,
          );
          return {
            childId: child.id,
            state: status.state,
            ...capped,
            offset: 0,
            length: capped.output.length,
          };
        }
        const offset = Math.min(input.offset ?? 0, text.length);
        const slice = text.slice(
          offset,
          input.limit === undefined ? undefined : offset + input.limit,
        );
        return {
          childId: child.id,
          state: status.state,
          output: slice,
          truncated: slice.length < text.length,
          totalChars: text.length,
          offset,
          length: slice.length,
        };
      }),
    );

  const followUp: CrossProviderAgentServiceShape["followUp"] = (callerThreadId, input, options) =>
    guard(
      "followUp",
      Effect.gen(function* () {
        const context = yield* callerContext(callerThreadId);
        if (isCrossProviderAgentErrorOutput(context)) return context;
        const child = yield* readOwnedChild(callerThreadId, input.childId);
        if (isCrossProviderAgentErrorOutput(child)) return child;
        const key = yield* callIdentity(callerThreadId, "agent_follow_up", options?.callId);
        const turnCommandId = CommandId.make(`server:xp-agent:follow-up:${callerThreadId}:${key}`);
        const runningCommandId = CommandId.make(
          `server:xp-agent:task-running:${callerThreadId}:${key}`,
        );
        if (options?.callId !== undefined && (yield* alreadyAccepted(turnCommandId))) {
          return { childId: child.id, status: "running" as const };
        }
        if (!evaluateCrossProviderChild(child).settled) {
          return fail({
            code: "child_not_settled",
            message: "The child is still running; wait for it before following up.",
            childId: child.id,
          });
        }
        yield* engine.dispatch({
          type: "thread.turn.start",
          commandId: turnCommandId,
          threadId: child.id,
          message: {
            messageId: MessageId.make(`xp-agent:follow-up:${key}`),
            role: "user",
            text: input.prompt,
            attachments: [],
          },
          runtimeMode: child.runtimeMode,
          interactionMode: child.interactionMode,
          // Strictly after the last started turn so the pending-start check
          // in evaluateCrossProviderChild cannot tie on the millisecond.
          createdAt: stampAfter(yield* nowIso, child.latestTurn?.requestedAt),
        });
        yield* appendParentActivity({
          parentThreadId: child.spawn!.parentThreadId,
          commandId: runningCommandId,
          kind: "task.updated",
          summary: "Cross-provider agent follow-up started",
          payload: { ...(yield* rowLinkage(child)), status: "running" },
        });
        yield* watchSettlement(child.id);
        return { childId: child.id, status: "running" as const };
      }),
    );

  const interrupt: CrossProviderAgentServiceShape["interrupt"] = (callerThreadId, input, options) =>
    guard(
      "interrupt",
      Effect.gen(function* () {
        const context = yield* callerContext(callerThreadId);
        if (isCrossProviderAgentErrorOutput(context)) return context;
        const child = yield* readOwnedChild(callerThreadId, input.childId);
        if (isCrossProviderAgentErrorOutput(child)) return child;
        const key = yield* callIdentity(callerThreadId, "agent_interrupt", options?.callId);
        const interruptCommandId = CommandId.make(
          `server:xp-agent:interrupt:${callerThreadId}:${key}`,
        );
        if (options?.callId !== undefined && (yield* alreadyAccepted(interruptCommandId))) {
          return { childId: child.id, status: "interrupting" as const };
        }
        const status = evaluateCrossProviderChild(child);
        if (status.settled) {
          return fail({
            code: "child_not_running",
            message: "The child is not running.",
            childId: child.id,
          });
        }
        yield* engine.dispatch({
          type: "thread.turn.interrupt",
          commandId: interruptCommandId,
          threadId: child.id,
          ...(status.turnId === null ? {} : { turnId: status.turnId }),
          createdAt: yield* nowIso,
        });
        return { childId: child.id, status: "interrupting" as const };
      }),
    );

  const toolsForThread: CrossProviderAgentServiceShape["toolsForThread"] = (threadId) =>
    Effect.gen(function* () {
      const settings = yield* settingsService.getSettings;
      if (!settings.enableCrossProviderAgentAccess) return Option.none();
      const shell = yield* readShell(threadId);
      if (Option.isNone(shell)) return Option.none();
      if (shell.value.spawn !== undefined) {
        return shell.value.spawn.allowOrchestration
          ? Option.some(CROSS_PROVIDER_TOOL_SPECS)
          : Option.none();
      }
      const providers = yield* providerRegistry.getProviders;
      const provider = providers.find(
        (candidate) => candidate.instanceId === shell.value.modelSelection.instanceId,
      );
      return provider !== undefined &&
        CROSS_PROVIDER_AGENT_SUPPORTED_DRIVERS.includes(provider.driver)
        ? Option.some(CROSS_PROVIDER_TOOL_SPECS)
        : Option.none();
    }).pipe(
      Effect.catchCause((cause) =>
        Effect.logWarning("cross-provider agent tool grant check failed; withholding tools", {
          threadId,
          cause: Cause.pretty(cause),
        }).pipe(Effect.as(Option.none())),
      ),
    );

  const call: CrossProviderAgentServiceShape["call"] = (threadId, tool, args, callId) =>
    Effect.gen(function* () {
      if (!isToolName(tool)) {
        return fail({ code: "invalid_input", message: `Unknown tool ${tool}.` });
      }
      const decoded = yield* Effect.result(toolInputDecoders[tool](args));
      if (decoded._tag === "Failure") {
        return fail({
          code: "invalid_input",
          message: `Invalid ${tool} arguments: ${decoded.failure.message}`,
        });
      }
      const input = decoded.success;
      switch (tool) {
        case "agent_catalog":
          return yield* catalog(threadId);
        case "agent_spawn":
          return yield* spawn(
            threadId,
            input as Schema.Schema.Type<typeof CROSS_PROVIDER_AGENT_TOOL_INPUTS.agent_spawn>,
            { callId },
          );
        case "agent_wait":
          return yield* wait(
            threadId,
            input as Schema.Schema.Type<typeof CROSS_PROVIDER_AGENT_TOOL_INPUTS.agent_wait>,
          );
        case "agent_result":
          return yield* result(
            threadId,
            input as Schema.Schema.Type<typeof CROSS_PROVIDER_AGENT_TOOL_INPUTS.agent_result>,
          );
        case "agent_follow_up":
          return yield* followUp(
            threadId,
            input as Schema.Schema.Type<typeof CROSS_PROVIDER_AGENT_TOOL_INPUTS.agent_follow_up>,
            { callId },
          );
        case "agent_interrupt":
          return yield* interrupt(
            threadId,
            input as Schema.Schema.Type<typeof CROSS_PROVIDER_AGENT_TOOL_INPUTS.agent_interrupt>,
            { callId },
          );
      }
    }).pipe(
      Effect.map((output): CrossProviderToolResult => ({
        output,
        isError: isCrossProviderAgentErrorOutput(output),
      })),
    );

  const service = CrossProviderAgentService.of({
    catalog,
    spawn,
    wait,
    result,
    followUp,
    interrupt,
    toolsForThread,
    call,
  });

  // The adapters read the host registry at session start; register for the
  // layer's lifetime so a rebuilt runtime never leaves a stale host behind.
  setCrossProviderAgentToolHost(service);
  yield* Effect.addFinalizer(() => Effect.sync(clearCrossProviderAgentToolHost));

  return service;
});

export const CrossProviderAgentLive = Layer.effect(
  CrossProviderAgentService,
  makeCrossProviderAgent,
);
