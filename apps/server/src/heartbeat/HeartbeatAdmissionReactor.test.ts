import {
  CommandId,
  EventId,
  HeartbeatJobId,
  HeartbeatOccurrenceId,
  HeartbeatPrompt,
  MessageId,
  NodeId,
  ProviderInstanceId,
  ProviderThreadId,
  RunId,
  ThreadId,
  TurnItemId,
  type HeartbeatOccurrenceOutcome,
  type OrchestrationV2Actor,
  type OrchestrationV2Run,
  type OrchestrationV2RunStatus,
  type OrchestrationV2ServerCommand,
  type OrchestrationV2StoredEvent,
} from "@t3tools/contracts";
import { assert, it } from "@effect/vitest";
import * as Cause from "effect/Cause";
import * as DateTime from "effect/DateTime";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Ref from "effect/Ref";
import { TestClock } from "effect/testing";

import { EventSinkV2 } from "../orchestration-v2/EventSink.ts";
import { DelegationGrants } from "../orchestration-v2/fork/DelegationGrants.ts";
import {
  OrchestratorDispatchError,
  OrchestratorThreadBusyError,
  type OrchestratorV2DispatchResult,
} from "../orchestration-v2/Orchestrator.ts";
import { ThreadManagementService } from "../orchestration-v2/ThreadManagementService.ts";
import { OrchestrationCommandReceiptRepository } from "../persistence/OrchestrationCommandReceipts.ts";
import * as McpToolAccessTestkit from "../mcp/McpToolAccess.testkit.ts";
import * as ServerSettings from "../serverSettings.ts";
import * as HeartbeatAdmissionReactor from "./HeartbeatAdmissionReactor.ts";
import * as HeartbeatReactor from "./HeartbeatReactor.ts";
import { HeartbeatRepository } from "./HeartbeatRepository.ts";
import { HeartbeatScheduler, type ReservedHeartbeatOccurrence } from "./HeartbeatService.ts";

const THREAD_ID = ThreadId.make("heartbeat-admission-thread");
const OCCURRENCE_ID = HeartbeatOccurrenceId.make("heartbeat-admission-occurrence");
const NOW_ISO = "2026-09-17T10:00:00.000Z";
const NOW = DateTime.makeUnsafe(NOW_ISO);
const ORIGIN = 40;

const occurrence: ReservedHeartbeatOccurrence = {
  occurrenceId: OCCURRENCE_ID,
  threadId: THREAD_ID,
  publicId: HeartbeatJobId.make("deadbeef"),
  prompt: HeartbeatPrompt.make("  continue the authorized work\n"),
  generation: 1,
  nominalAtMs: Date.parse(NOW_ISO),
  dueAtMs: Date.parse(NOW_ISO),
  reservedAtMs: Date.parse(NOW_ISO),
  kind: "relative",
  noop: false,
  originSequence: ORIGIN,
};

type SchedulerCall =
  | { readonly method: "activate"; readonly threadId: ThreadId }
  | {
      readonly method: "cancelRelative" | "cancelThread";
      readonly threadId: ThreadId;
      readonly reason: string;
      readonly beforeSequence: number | undefined;
    };

const dispatched: OrchestratorV2DispatchResult = { sequence: 41, storedEvents: [] };

const makeAdmission = (
  dispatch: ThreadManagementService["Service"]["dispatch"],
  options: {
    /** Successive `latestCancellationSequence` answers; the last one repeats. */
    readonly cancellationSequences?: ReadonlyArray<number | undefined>;
    readonly enabled?: boolean;
    readonly receipts?: Readonly<Record<string, { commandType: string; status?: string }>>;
  } = {},
) =>
  Effect.gen(function* () {
    const outcomes = yield* Ref.make<ReadonlyArray<HeartbeatOccurrenceOutcome>>([]);
    const outcomeIds = yield* Ref.make<ReadonlyArray<HeartbeatOccurrenceId>>([]);
    const calls = yield* Ref.make<ReadonlyArray<SchedulerCall>>([]);
    const push = (call: SchedulerCall) => Ref.update(calls, (current) => [...current, call]);
    const cancellationReads = yield* Ref.make(0);
    const cancellationSequences = options.cancellationSequences ?? [undefined];
    const scheduler = Layer.mock(HeartbeatScheduler)({
      schedulerSnapshot: Effect.die("unused scheduler snapshot"),
      reserveDue: () => Effect.die("unused reserve"),
      subscribeChanges: Effect.die("unused changes"),
      pruneExpired: Effect.die("unused prune"),
      validateOccurrence: () => Effect.succeed(true),
      withValidOccurrence: (_occurrenceId, admission) => admission.pipe(Effect.asSome),
      setOccurrenceOutcome: (occurrenceId, outcome) =>
        Ref.update(outcomes, (current) => [...current, outcome]).pipe(
          Effect.andThen(Ref.update(outcomeIds, (current) => [...current, occurrenceId])),
        ),
      activateOnAcceptedTurn: (threadId) => push({ method: "activate", threadId }),
      cancelRelative: (threadId, reason, beforeSequence) =>
        push({ method: "cancelRelative", threadId, reason, beforeSequence }),
      cancelThread: (threadId, reason, beforeSequence) =>
        push({ method: "cancelThread", threadId, reason, beforeSequence }),
    });
    const layers = Layer.mergeAll(
      scheduler,
      Layer.mock(HeartbeatReactor.HeartbeatReactor)({
        start: () => Effect.die("unused reactor start"),
      }),
      Layer.mock(HeartbeatRepository)({
        latestCancellationSequence: () =>
          Ref.getAndUpdate(cancellationReads, (count) => count + 1).pipe(
            Effect.map(
              (count) => cancellationSequences[Math.min(count, cancellationSequences.length - 1)],
            ),
          ),
      }),
      Layer.mock(ThreadManagementService)({
        dispatch,
        getThreadShell: (threadId) =>
          Effect.succeed(McpToolAccessTestkit.liveThreadShell(threadId)),
      }),
      Layer.mock(EventSinkV2)({}),
      Layer.mock(OrchestrationCommandReceiptRepository)({
        getByCommandId: ({ commandId }) => {
          const receipt = options.receipts?.[commandId];
          return Effect.succeed(
            receipt === undefined
              ? Option.none()
              : Option.some({
                  commandId,
                  aggregateKind: "thread" as const,
                  aggregateId: THREAD_ID,
                  commandType: receipt.commandType,
                  acceptedAt: NOW_ISO,
                  resultSequence: 0,
                  status: receipt.status === "rejected" ? "rejected" : "accepted",
                  error: null,
                }),
          );
        },
      }),
      Layer.mock(DelegationGrants)({ read: () => Effect.succeedNone }),
      ServerSettings.layerTest({ enableHeartbeatAccess: options.enabled ?? true }),
    );
    const service = yield* HeartbeatAdmissionReactor.make.pipe(Effect.provide(layers));
    return { service, outcomes, outcomeIds, calls } as const;
  });

const run = (status: OrchestrationV2RunStatus, userMessageId: string): OrchestrationV2Run => ({
  id: RunId.make("run-1"),
  threadId: THREAD_ID,
  ordinal: 1,
  providerInstanceId: ProviderInstanceId.make("codex"),
  modelSelection: { instanceId: ProviderInstanceId.make("codex"), model: "gpt-5" },
  providerThreadId: null,
  userMessageId: MessageId.make(userMessageId),
  rootNodeId: null,
  activeAttemptId: null,
  status,
  requestedAt: NOW,
  startedAt: null,
  completedAt: null,
  checkpointId: null,
  contextHandoffId: null,
});

const runUpdated = (
  sequence: number,
  commandId: string | null,
  payload: OrchestrationV2Run,
): OrchestrationV2StoredEvent => ({
  sequence,
  commandId: commandId === null ? null : CommandId.make(commandId),
  event: {
    id: EventId.make(`event-${sequence}`),
    threadId: THREAD_ID,
    occurredAt: NOW,
    type: "run.updated",
    payload,
  },
});

const interruptRequested = (sequence: number, commandId: string): OrchestrationV2StoredEvent => ({
  sequence,
  commandId: CommandId.make(commandId),
  event: {
    id: EventId.make(`event-${sequence}`),
    threadId: THREAD_ID,
    occurredAt: NOW,
    type: "turn-item.updated",
    payload: {
      id: TurnItemId.make("item-interrupt-request"),
      threadId: THREAD_ID,
      runId: RunId.make("run-1"),
      nodeId: NodeId.make("node-1"),
      providerThreadId: ProviderThreadId.make("provider-thread-1"),
      providerTurnId: null,
      nativeItemRef: null,
      parentItemId: null,
      ordinal: 1,
      status: "completed",
      title: "Interrupt requested",
      startedAt: NOW,
      completedAt: NOW,
      updatedAt: NOW,
      type: "run_interrupt_request",
      message: "Interrupt requested",
    },
  },
});

const userMessage = (
  sequence: number,
  messageId: string,
  createdBy: OrchestrationV2Actor = "user",
): OrchestrationV2StoredEvent => ({
  sequence,
  commandId: CommandId.make(`command-${sequence}`),
  event: {
    id: EventId.make(`event-${sequence}`),
    threadId: THREAD_ID,
    occurredAt: NOW,
    type: "message.updated",
    payload: {
      id: MessageId.make(messageId),
      threadId: THREAD_ID,
      runId: null,
      nodeId: null,
      role: "user",
      text: "hello",
      attachments: [],
      streaming: false,
      createdAt: NOW,
      updatedAt: NOW,
      createdBy,
      creationSource: createdBy === "user" ? "web" : "server",
    },
  },
});

const threadLifecycle = (
  sequence: number,
  type: "thread.archived" | "thread.deleted",
): OrchestrationV2StoredEvent => {
  const shell = McpToolAccessTestkit.liveThreadShell(THREAD_ID);
  return {
    sequence,
    commandId: CommandId.make(`command-${sequence}`),
    event: {
      id: EventId.make(`event-${sequence}`),
      threadId: THREAD_ID,
      occurredAt: NOW,
      type,
      payload: McpToolAccessTestkit.idleThreadProjection(shell).thread,
    },
  };
};

it.effect("classifies a typed busy failure as retryable and keeps the command ID stable", () =>
  Effect.gen(function* () {
    yield* TestClock.setTime(Date.parse(NOW_ISO));
    const firstAttempt = yield* Deferred.make<void>();
    const commands = yield* Ref.make<ReadonlyArray<OrchestrationV2ServerCommand>>([]);
    const attempts = yield* Ref.make(0);
    const { service, outcomes } = yield* makeAdmission((command) =>
      Effect.gen(function* () {
        yield* Ref.update(commands, (current) => [...current, command]);
        const attempt = yield* Ref.getAndUpdate(attempts, (count) => count + 1);
        if (attempt === 0) {
          yield* Deferred.succeed(firstAttempt, undefined);
          return yield* new OrchestratorThreadBusyError({
            commandId: command.commandId,
            threadId: THREAD_ID,
          });
        }
        return dispatched;
      }),
    );

    const fiber = yield* service.processReserved(occurrence).pipe(Effect.forkChild);
    yield* Deferred.await(firstAttempt);
    yield* TestClock.adjust("1 second");
    yield* Fiber.join(fiber);

    const sent = yield* Ref.get(commands);
    assert.strictEqual(sent.length, 2);
    assert.strictEqual(sent[0]?.commandId, sent[1]?.commandId);
    assert.deepStrictEqual(yield* Ref.get(outcomes), [{ status: "admitted" }]);
  }),
);

it.effect("terminalizes a permanent admission rejection without retrying", () =>
  Effect.gen(function* () {
    const attempts = yield* Ref.make(0);
    const { service, outcomes, calls } = yield* makeAdmission((command) =>
      Ref.update(attempts, (count) => count + 1).pipe(
        Effect.andThen(
          new OrchestratorDispatchError({
            commandId: command.commandId,
            commandType: command.type,
            cause: "Thread is deleted.",
          }),
        ),
      ),
    );

    yield* service.processReserved(occurrence);
    assert.strictEqual(yield* Ref.get(attempts), 1);
    assert.deepStrictEqual(yield* Ref.get(outcomes), [{ status: "failed" }]);
    // No Stop receipt lies past the origin, so the thread's other jobs stay.
    assert.deepStrictEqual(yield* Ref.get(calls), []);
  }),
);

it.effect("propagates interruption instead of retrying or writing an outcome", () =>
  Effect.gen(function* () {
    const { service, outcomes } = yield* makeAdmission(() => Effect.interrupt);
    const exit = yield* Effect.exit(service.processReserved(occurrence));
    assert.ok(Exit.isFailure(exit));
    if (Exit.isFailure(exit)) assert.ok(Cause.hasInterrupts(exit.cause));
    assert.deepStrictEqual(yield* Ref.get(outcomes), []);
  }),
);

it.effect("replays an uncertain acknowledgement with the stable command ID", () =>
  Effect.gen(function* () {
    yield* TestClock.setTime(Date.parse(NOW_ISO));
    const firstAttempt = yield* Deferred.make<void>();
    const commandIds = yield* Ref.make<ReadonlyArray<string>>([]);
    const attempts = yield* Ref.make(0);
    const { service, outcomes } = yield* makeAdmission((command) =>
      Effect.gen(function* () {
        yield* Ref.update(commandIds, (current) => [...current, command.commandId]);
        const attempt = yield* Ref.getAndUpdate(attempts, (count) => count + 1);
        if (attempt === 0) {
          yield* Deferred.succeed(firstAttempt, undefined);
          return yield* Effect.die("accepted acknowledgement was lost");
        }
        return dispatched;
      }),
    );

    const fiber = yield* service.processReserved(occurrence).pipe(Effect.forkChild);
    yield* Deferred.await(firstAttempt);
    yield* TestClock.adjust("1 second");
    yield* Fiber.join(fiber);

    const ids = yield* Ref.get(commandIds);
    assert.strictEqual(ids.length, 2);
    assert.strictEqual(ids[0], ids[1]);
    assert.deepStrictEqual(yield* Ref.get(outcomes), [{ status: "admitted" }]);
  }),
);

it.effect("delivers the prompt verbatim as a fenced server-only user message", () =>
  Effect.gen(function* () {
    const commands = yield* Ref.make<ReadonlyArray<OrchestrationV2ServerCommand>>([]);
    const { service } = yield* makeAdmission((command) =>
      Ref.update(commands, (current) => [...current, command]).pipe(Effect.as(dispatched)),
    );

    yield* service.processReserved(occurrence);
    yield* service.processReserved({
      ...occurrence,
      occurrenceId: HeartbeatOccurrenceId.make("cron-occurrence"),
      kind: "cron",
    });

    const [relative, cron] = yield* Ref.get(commands);
    assert.deepStrictEqual(relative, {
      type: "message.dispatch",
      commandId: CommandId.make(`server:heartbeat:${OCCURRENCE_ID}`),
      threadId: THREAD_ID,
      messageId: MessageId.make(`heartbeat:${OCCURRENCE_ID}`),
      text: "  continue the authorized work\n",
      attachments: [],
      createdBy: "user",
      creationSource: "server",
      dispatchMode: {
        type: "start_if_idle",
        fence: { afterSequence: ORIGIN, kinds: ["stop", "interrupt"] },
      },
    });
    // A cron job survives an interrupt, so only Stop fences it.
    assert.deepStrictEqual(cron?.type === "message.dispatch" ? cron.dispatchMode : undefined, {
      type: "start_if_idle",
      fence: { afterSequence: ORIGIN, kinds: ["stop"] },
    });
  }),
);

it.effect("cancels the occurrence without dispatching once access is revoked", () =>
  Effect.gen(function* () {
    const attempts = yield* Ref.make(0);
    const { service, outcomes } = yield* makeAdmission(
      () => Ref.update(attempts, (count) => count + 1).pipe(Effect.as(dispatched)),
      { enabled: false },
    );

    yield* service.processReserved(occurrence);
    assert.strictEqual(yield* Ref.get(attempts), 0);
    assert.deepStrictEqual(yield* Ref.get(outcomes), [{ status: "canceled" }]);
  }),
);

it.effect("drops the thread's schedules when a Stop without events lies past the origin", () =>
  Effect.gen(function* () {
    const attempts = yield* Ref.make(0);
    const { service, outcomes, calls } = yield* makeAdmission(
      () => Ref.update(attempts, (count) => count + 1).pipe(Effect.as(dispatched)),
      // The repository places a Stop that wrote no event one past the head it saw.
      { cancellationSequences: [ORIGIN + 1] },
    );

    yield* service.processReserved(occurrence);
    assert.strictEqual(yield* Ref.get(attempts), 0);
    assert.deepStrictEqual(yield* Ref.get(calls), [
      {
        method: "cancelThread",
        threadId: THREAD_ID,
        reason: "session stopped",
        beforeSequence: ORIGIN + 1,
      },
    ]);
    assert.deepStrictEqual(yield* Ref.get(outcomes), [{ status: "canceled" }]);
  }),
);

it.effect("still dispatches a job armed after the latest Stop", () =>
  Effect.gen(function* () {
    const attempts = yield* Ref.make(0);
    const { service, outcomes, calls } = yield* makeAdmission(
      () => Ref.update(attempts, (count) => count + 1).pipe(Effect.as(dispatched)),
      { cancellationSequences: [ORIGIN] },
    );

    yield* service.processReserved(occurrence);
    assert.strictEqual(yield* Ref.get(attempts), 1);
    assert.deepStrictEqual(yield* Ref.get(calls), []);
    assert.deepStrictEqual(yield* Ref.get(outcomes), [{ status: "admitted" }]);
  }),
);

it.effect("cleans up the thread's schedules when the fence rejects a Stop it raced", () =>
  Effect.gen(function* () {
    const { service, outcomes, calls } = yield* makeAdmission(
      (command) =>
        Effect.fail(
          new OrchestratorDispatchError({
            commandId: command.commandId,
            commandType: command.type,
            cause: `start_if_idle fence: thread was stopped or interrupted at sequence ${ORIGIN}, not before ${ORIGIN}.`,
          }),
        ),
      // The Stop lands between the pre-dispatch check and the dispatch.
      { cancellationSequences: [undefined, ORIGIN + 1] },
    );

    yield* service.processReserved(occurrence);
    assert.deepStrictEqual(yield* Ref.get(outcomes), [{ status: "failed" }]);
    assert.deepStrictEqual(yield* Ref.get(calls), [
      {
        method: "cancelThread",
        threadId: THREAD_ID,
        reason: "session stopped",
        beforeSequence: ORIGIN + 1,
      },
    ]);
  }),
);

it.effect("reactivates on a normal user message but never on a heartbeat or agent message", () =>
  Effect.gen(function* () {
    const { service, calls } = yield* makeAdmission(() => Effect.succeed(dispatched));

    yield* service.handleStoredEvent(userMessage(50, `heartbeat:${OCCURRENCE_ID}`));
    yield* service.handleStoredEvent(userMessage(51, "message:delegated", "agent"));
    assert.deepStrictEqual(yield* Ref.get(calls), []);

    yield* service.handleStoredEvent(userMessage(52, "message:typed-by-user"));
    assert.deepStrictEqual(yield* Ref.get(calls), [{ method: "activate", threadId: THREAD_ID }]);
  }),
);

it.effect("tells an interrupt from a Stop by the command receipt", () =>
  Effect.gen(function* () {
    const { service, calls } = yield* makeAdmission(() => Effect.succeed(dispatched), {
      receipts: {
        "command-interrupt": { commandType: "run.interrupt" },
        "command-stop": { commandType: "thread.stop" },
        "command-detach": { commandType: "provider-session.detach" },
        "command-rejected-stop": { commandType: "thread.stop", status: "rejected" },
      },
    });

    yield* service.handleStoredEvent(
      runUpdated(60, "command-interrupt", run("interrupted", "message:user")),
    );
    yield* service.handleStoredEvent(interruptRequested(61, "command-stop"));
    // Settle-driven detach and anything not accepted never cancel.
    yield* service.handleStoredEvent(
      runUpdated(62, "command-detach", run("interrupted", "message:user")),
    );
    yield* service.handleStoredEvent(interruptRequested(63, "command-rejected-stop"));
    yield* service.handleStoredEvent(runUpdated(64, null, run("interrupted", "message:user")));

    assert.deepStrictEqual(yield* Ref.get(calls), [
      {
        method: "cancelRelative",
        threadId: THREAD_ID,
        reason: "turn interrupted",
        beforeSequence: 60,
      },
      {
        method: "cancelThread",
        threadId: THREAD_ID,
        reason: "session stopped",
        beforeSequence: 61,
      },
    ]);
  }),
);

it.effect("removes every schedule when the thread is archived or deleted", () =>
  Effect.gen(function* () {
    const { service, calls } = yield* makeAdmission(() => Effect.succeed(dispatched));

    yield* service.handleStoredEvent(threadLifecycle(70, "thread.archived"));
    yield* service.handleStoredEvent(threadLifecycle(71, "thread.deleted"));

    assert.deepStrictEqual(yield* Ref.get(calls), [
      {
        method: "cancelThread",
        threadId: THREAD_ID,
        reason: "thread.archived",
        beforeSequence: 70,
      },
      {
        method: "cancelThread",
        threadId: THREAD_ID,
        reason: "thread.deleted",
        beforeSequence: 71,
      },
    ]);
  }),
);

it.effect("records sent once the heartbeat run starts and failed when it never does", () =>
  Effect.gen(function* () {
    const { service, outcomes, outcomeIds } = yield* makeAdmission(() =>
      Effect.succeed(dispatched),
    );

    yield* service.handleStoredEvent(
      runUpdated(80, null, run("starting", `heartbeat:${OCCURRENCE_ID}`)),
    );
    yield* service.handleStoredEvent(
      runUpdated(81, null, run("running", `heartbeat:${OCCURRENCE_ID}`)),
    );
    yield* service.handleStoredEvent(runUpdated(82, null, run("failed", "heartbeat:other")));
    yield* service.handleStoredEvent(runUpdated(83, null, run("running", "message:user")));

    const recorded = yield* Ref.get(outcomes);
    assert.deepStrictEqual(yield* Ref.get(outcomeIds), [
      OCCURRENCE_ID,
      HeartbeatOccurrenceId.make("other"),
    ]);
    assert.strictEqual(recorded[0]?.status, "sent");
    assert.isString(recorded[0]?.providerAcceptedAt);
    assert.strictEqual(recorded[1]?.status, "failed");
  }),
);
