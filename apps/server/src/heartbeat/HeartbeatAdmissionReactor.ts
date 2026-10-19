import {
  CommandId,
  HeartbeatOccurrenceId,
  MessageId,
  type OrchestrationV2Run,
  type OrchestrationV2StoredEvent,
  type ThreadId,
} from "@t3tools/contracts";
import * as Cause from "effect/Cause";
import * as Context from "effect/Context";
import * as DateTime from "effect/DateTime";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Scope from "effect/Scope";
import * as Stream from "effect/Stream";

import { EventSinkV2 } from "../orchestration-v2/EventSink.ts";
import { DelegationGrants } from "../orchestration-v2/fork/DelegationGrants.ts";
import { startIfIdleFenceCommandTypes } from "../orchestration-v2/fork/StartIfIdleAdmission.ts";
import { ThreadManagementService } from "../orchestration-v2/ThreadManagementService.ts";
import { OrchestrationCommandReceiptRepository } from "../persistence/OrchestrationCommandReceipts.ts";
import * as ServerSettings from "../serverSettings.ts";
import { forkParked } from "../serverActivation.ts";
import { heartbeatAccessAllowed } from "./HeartbeatAccess.ts";
import * as HeartbeatReactor from "./HeartbeatReactor.ts";
import { HeartbeatRepository } from "./HeartbeatRepository.ts";
import { HeartbeatScheduler, type ReservedHeartbeatOccurrence } from "./HeartbeatService.ts";

/** Heartbeat deliveries carry this message id prefix; the server refuses it from clients. */
export const HEARTBEAT_MESSAGE_ID_PREFIX = "heartbeat:";

/** Errors after which the command id is settled, so a retry cannot start it. */
const DEFINITE_REJECTIONS: ReadonlySet<string> = new Set([
  "OrchestratorDispatchError",
  "OrchestratorCommandPreviouslyRejectedError",
  "OrchestratorCommandRejectedError",
  "ThreadManagementProviderInstanceDisabledError",
]);

const errorTag = (error: unknown): string | undefined =>
  typeof error === "object" && error !== null && "_tag" in error && typeof error._tag === "string"
    ? error._tag
    : undefined;

export class HeartbeatAdmissionReactor extends Context.Service<
  HeartbeatAdmissionReactor,
  {
    readonly start: () => Effect.Effect<void, never, Scope.Scope>;
    readonly attemptReserved: (
      occurrence: ReservedHeartbeatOccurrence,
    ) => Effect.Effect<HeartbeatReactor.ReservationAttemptResult, never>;
    readonly processReserved: (
      occurrence: ReservedHeartbeatOccurrence,
    ) => Effect.Effect<void, never>;
    readonly handleStoredEvent: (stored: OrchestrationV2StoredEvent) => Effect.Effect<void, never>;
  }
>()("t3/heartbeat/HeartbeatAdmissionReactor") {}

/**
 * Delivers due heartbeats as `start_if_idle` message dispatches and applies
 * thread lifecycle to schedules: a normal user message reactivates dormant
 * jobs, an interrupt drops the relative wakeup, and Stop, archive or delete
 * drop every job armed before them.
 */
export const make = Effect.gen(function* () {
  const reactor = yield* HeartbeatReactor.HeartbeatReactor;
  const scheduler = yield* HeartbeatScheduler;
  const repository = yield* HeartbeatRepository;
  const threads = yield* ThreadManagementService;
  const eventSink = yield* EventSinkV2;
  const receipts = yield* OrchestrationCommandReceiptRepository;
  const settingsService = yield* ServerSettings.ServerSettingsService;
  const grants = yield* DelegationGrants;

  const logFailure =
    (message: string, fields: Record<string, unknown>) =>
    <A, E, R>(effect: Effect.Effect<A, E, R>) =>
      effect.pipe(
        Effect.asVoid,
        Effect.catchCause((cause) =>
          Cause.hasInterruptsOnly(cause)
            ? Effect.interrupt
            : Effect.logWarning(message, { ...fields, cause: Cause.pretty(cause) }),
        ),
      );

  const accessFor = (threadId: ThreadId) =>
    Effect.gen(function* () {
      const thread = yield* threads.getThreadShell(threadId);
      if (thread === null || thread.deletedAt !== null) return false;
      return yield* heartbeatAccessAllowed(yield* settingsService.getSettings, thread);
    }).pipe(
      Effect.provideService(DelegationGrants, grants),
      Effect.catchCause(() => Effect.succeed(false)),
    );

  const outcomeForOccurrence = (
    occurrence: ReservedHeartbeatOccurrence,
    outcome: Parameters<HeartbeatScheduler["Service"]["setOccurrenceOutcome"]>[1],
  ) =>
    scheduler.setOccurrenceOutcome(occurrence.occurrenceId, outcome).pipe(
      Effect.catchCause((cause) =>
        Cause.hasInterrupts(cause)
          ? Effect.interrupt
          : Effect.logWarning("failed to record heartbeat occurrence outcome", {
              occurrenceId: occurrence.occurrenceId,
              outcome,
              cause: Cause.pretty(cause),
            }),
      ),
      Effect.asVoid,
    );

  // A Stop that ends nothing writes no event, so the lifecycle stream never
  // sees it; its accepted receipt is the only record. The sequence follows
  // the restore rule: jobs armed before it are cancelled.
  const stoppedSince = (occurrence: ReservedHeartbeatOccurrence) =>
    repository
      .latestCancellationSequence(occurrence.threadId)
      .pipe(
        Effect.map((sequence) =>
          sequence !== undefined && occurrence.originSequence < sequence ? sequence : undefined,
        ),
      );

  // The admission fence refuses every job armed at or before the latest
  // accepted Stop, so after a rejection those jobs are removed with the same
  // inclusive rule; otherwise a recurring job armed at an event-carrying
  // Stop's own sequence would fail on every occurrence.
  const fencedByStop = (occurrence: ReservedHeartbeatOccurrence) =>
    receipts
      .latestAcceptedSequence({
        threadId: occurrence.threadId,
        commandTypes: startIfIdleFenceCommandTypes(["stop"]),
      })
      .pipe(
        Effect.map((sequence) =>
          sequence !== null && sequence >= occurrence.originSequence ? sequence + 1 : undefined,
        ),
      );

  const cancelAfterStop = (threadId: ThreadId, sequence: number) =>
    scheduler
      .cancelThread(threadId, "session stopped", sequence)
      .pipe(logFailure("failed to cancel heartbeat schedules", { threadId }));

  const dispatch = (occurrence: ReservedHeartbeatOccurrence) =>
    threads.dispatch({
      type: "message.dispatch",
      commandId: CommandId.make(`server:heartbeat:${occurrence.occurrenceId}`),
      threadId: occurrence.threadId,
      messageId: MessageId.make(`${HEARTBEAT_MESSAGE_ID_PREFIX}${occurrence.occurrenceId}`),
      text: occurrence.prompt,
      attachments: [],
      createdBy: "user",
      creationSource: "server",
      // Cron jobs survive an interrupt; a relative wakeup does not.
      dispatchMode: {
        type: "start_if_idle",
        fence: {
          afterSequence: occurrence.originSequence,
          kinds: occurrence.kind === "relative" ? ["stop", "interrupt"] : ["stop"],
        },
      },
    });

  const attemptReserved: HeartbeatAdmissionReactor["Service"]["attemptReserved"] = (occurrence) =>
    Effect.gen(function* () {
      // Validation and serialized dispatch share the scheduler mutation lock.
      const attempt = yield* scheduler
        .withValidOccurrence(
          occurrence.occurrenceId,
          Effect.gen(function* () {
            if (!(yield* accessFor(occurrence.threadId))) return { _tag: "AccessRevoked" } as const;
            const stoppedAt = yield* stoppedSince(occurrence);
            if (stoppedAt !== undefined) return { _tag: "Stopped", sequence: stoppedAt } as const;
            return {
              _tag: "Dispatched",
              exit: yield* Effect.exit(dispatch(occurrence)),
            } as const;
          }),
        )
        .pipe(Effect.orElseSucceed(() => ({ _tag: "SchedulerError" }) as const));
      if (attempt._tag === "SchedulerError") {
        yield* Effect.logWarning("heartbeat admission will retry after a scheduler error", {
          occurrenceId: occurrence.occurrenceId,
        });
        return "defer";
      }
      if (Option.isNone(attempt)) {
        yield* outcomeForOccurrence(occurrence, { status: "canceled" });
        return "complete";
      }
      if (attempt.value._tag === "AccessRevoked") {
        yield* outcomeForOccurrence(occurrence, { status: "canceled" });
        return "complete";
      }
      if (attempt.value._tag === "Stopped") {
        yield* cancelAfterStop(occurrence.threadId, attempt.value.sequence);
        yield* outcomeForOccurrence(occurrence, { status: "canceled" });
        return "complete";
      }
      const exit = attempt.value.exit;
      if (Exit.isSuccess(exit)) {
        yield* outcomeForOccurrence(occurrence, { status: "admitted" });
        return "complete";
      }

      const cause = exit.cause;
      // A boolean check cannot narrow Cause<E>; interrupts must leave the
      // typed failure channel untouched.
      if (Cause.hasInterrupts(cause)) return yield* Effect.interrupt;
      const tag = Option.map(Cause.findErrorOption(cause), errorTag).pipe(Option.getOrUndefined);
      if (tag === "OrchestratorThreadBusyError") return "defer";
      if (tag !== undefined && DEFINITE_REJECTIONS.has(tag)) {
        yield* outcomeForOccurrence(occurrence, { status: "failed" });
        // A fence rejection means a Stop landed since this job was armed. When
        // that Stop wrote no event, or tied the job's origin, nothing else
        // removes the thread's jobs.
        const stoppedAt = yield* Effect.gen(function* () {
          return (yield* fencedByStop(occurrence)) ?? (yield* stoppedSince(occurrence));
        }).pipe(Effect.orElseSucceed(() => undefined));
        if (stoppedAt !== undefined) yield* cancelAfterStop(occurrence.threadId, stoppedAt);
        return "complete";
      }
      yield* Effect.logWarning("heartbeat admission will retry after an uncertain error", {
        occurrenceId: occurrence.occurrenceId,
        cause: Cause.pretty(cause),
      });
      return "defer";
    });

  const processReserved: HeartbeatAdmissionReactor["Service"]["processReserved"] = (occurrence) =>
    Effect.gen(function* () {
      while ((yield* attemptReserved(occurrence)) === "defer") {
        yield* Effect.sleep(Duration.seconds(1));
      }
    });

  // The heartbeat's own run: running means its provider accepted the turn.
  const recordSendOutcome = (run: OrchestrationV2Run) =>
    Effect.gen(function* () {
      const occurrenceId = HeartbeatOccurrenceId.make(
        run.userMessageId.slice(HEARTBEAT_MESSAGE_ID_PREFIX.length),
      );
      switch (run.status) {
        case "running":
        case "waiting":
        case "completed":
          return yield* scheduler.setOccurrenceOutcome(occurrenceId, {
            status: "sent",
            providerAcceptedAt: DateTime.formatIso(yield* DateTime.now),
          });
        case "failed":
        case "interrupted":
        case "cancelled":
        case "rolled_back":
          return yield* scheduler.setOccurrenceOutcome(occurrenceId, {
            status: "failed",
            error: `Heartbeat run ended ${run.status} before its provider turn started.`,
          });
      }
    });

  // Interrupt and Stop write the same events; the command's receipt tells them apart.
  const cancelForCommand = (stored: OrchestrationV2StoredEvent) =>
    Effect.gen(function* () {
      if (stored.commandId === null) return;
      const receipt = yield* receipts.getByCommandId({ commandId: stored.commandId });
      if (Option.isNone(receipt) || receipt.value.status !== "accepted") return;
      const threadId = stored.event.threadId;
      if (receipt.value.commandType === "run.interrupt") {
        return yield* scheduler.cancelRelative(threadId, "turn interrupted", stored.sequence);
      }
      if (receipt.value.commandType === "thread.stop") {
        return yield* scheduler.cancelThread(threadId, "session stopped", stored.sequence);
      }
    });

  const handleStoredEvent: HeartbeatAdmissionReactor["Service"]["handleStoredEvent"] = (stored) => {
    const event = stored.event;
    const fields = { eventType: event.type, threadId: event.threadId };
    switch (event.type) {
      case "message.updated": {
        const message = event.payload;
        if (
          message.role !== "user" ||
          message.createdBy !== "user" ||
          message.id.startsWith(HEARTBEAT_MESSAGE_ID_PREFIX)
        ) {
          return Effect.void;
        }
        return scheduler
          .activateOnAcceptedTurn(event.threadId)
          .pipe(logFailure("failed to activate heartbeat schedules", fields));
      }
      case "run.updated": {
        const run = event.payload;
        return Effect.all([
          run.userMessageId.startsWith(HEARTBEAT_MESSAGE_ID_PREFIX)
            ? recordSendOutcome(run)
            : Effect.void,
          run.status === "interrupted" ? cancelForCommand(stored) : Effect.void,
        ]).pipe(logFailure("heartbeat lifecycle reactor failed", fields));
      }
      case "turn-item.updated":
        return event.payload.type === "run_interrupt_request"
          ? cancelForCommand(stored).pipe(logFailure("heartbeat lifecycle reactor failed", fields))
          : Effect.void;
      case "thread.archived":
      case "thread.deleted":
        return scheduler
          .cancelThread(event.threadId, event.type, stored.sequence)
          .pipe(logFailure("failed to cancel heartbeat schedules", fields));
      default:
        return Effect.void;
    }
  };

  const start: HeartbeatAdmissionReactor["Service"]["start"] = () =>
    forkParked(
      Effect.gen(function* () {
        // The head is read before any admission, so the lifecycle stream also
        // replays whatever a heartbeat dispatch commits while it subscribes.
        const afterSequence = yield* eventSink.latestSequence();
        yield* Stream.runForEach(eventSink.stream({ afterSequence }), handleStoredEvent).pipe(
          Effect.catchCause((cause) =>
            Effect.logWarning("heartbeat lifecycle event stream failed", { cause }),
          ),
          Effect.forkScoped,
        );
        yield* reactor.start(attemptReserved);
      }).pipe(
        Effect.catchCause((cause) =>
          Effect.logWarning("heartbeat admission reactor failed to start", { cause }),
        ),
      ),
    );

  return HeartbeatAdmissionReactor.of({
    start,
    attemptReserved,
    processReserved,
    handleStoredEvent,
  });
});

export const layer = Layer.effect(HeartbeatAdmissionReactor, make);
