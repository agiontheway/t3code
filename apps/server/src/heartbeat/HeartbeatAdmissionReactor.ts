import { CommandId, type OrchestrationEvent } from "@t3tools/contracts";
import * as Cause from "effect/Cause";
import * as Context from "effect/Context";
import * as DateTime from "effect/DateTime";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as Scope from "effect/Scope";
import * as Stream from "effect/Stream";
import * as Exit from "effect/Exit";

import {
  HeartbeatThreadBusyError,
  OrchestrationCommandInvariantError,
  OrchestrationCommandPreviouslyRejectedError,
} from "../orchestration/Errors.ts";
import { OrchestrationEngineService } from "../orchestration/Services/OrchestrationEngine.ts";
import * as ProjectionSnapshotQuery from "../orchestration/Services/ProjectionSnapshotQuery.ts";
import * as ServerSettings from "../serverSettings.ts";
import { forkParked } from "../serverActivation.ts";
import { heartbeatAccessAllowed } from "./HeartbeatAccess.ts";
import * as HeartbeatReactor from "./HeartbeatReactor.ts";
import { HeartbeatScheduler, type ReservedHeartbeatOccurrence } from "./HeartbeatService.ts";

const isHeartbeatThreadBusyError = Schema.is(HeartbeatThreadBusyError);
const isOrchestrationCommandInvariantError = Schema.is(OrchestrationCommandInvariantError);
const isOrchestrationCommandPreviouslyRejectedError = Schema.is(
  OrchestrationCommandPreviouslyRejectedError,
);

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
  }
>()("t3/heartbeat/HeartbeatAdmissionReactor") {}

/** @public The reactor installs native event subscriptions before starting dispatch. */
export const make = Effect.gen(function* () {
  const reactor = yield* HeartbeatReactor.HeartbeatReactor;
  const engine = yield* OrchestrationEngineService;
  const scheduler = yield* HeartbeatScheduler;
  const snapshots = yield* ProjectionSnapshotQuery.ProjectionSnapshotQuery;
  const settingsService = yield* ServerSettings.ServerSettingsService;

  const accessFor = (threadId: ReservedHeartbeatOccurrence["threadId"]) =>
    Effect.gen(function* () {
      const thread = yield* snapshots.getThreadShellById(threadId);
      if (Option.isNone(thread)) return false;
      return heartbeatAccessAllowed(yield* settingsService.getSettings, thread.value);
    }).pipe(Effect.catchCause(() => Effect.succeed(false)));

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

  const dispatch = (occurrence: ReservedHeartbeatOccurrence, createdAt: string) => {
    const iso = (epochMillis: number) => DateTime.formatIso(DateTime.makeUnsafe(epochMillis));
    const base = {
      type: "thread.heartbeat.due",
      commandId: CommandId.make(`server:heartbeat:${occurrence.occurrenceId}`),
      threadId: occurrence.threadId,
      jobId: occurrence.publicId,
      occurrenceId: occurrence.occurrenceId,
      reservedAt: iso(occurrence.reservedAtMs),
      scheduledAfterSequence: occurrence.originSequence,
      prompt: occurrence.prompt,
      dueAt: iso(occurrence.dueAtMs),
      createdAt,
    } as const;
    return occurrence.kind === "relative"
      ? engine.dispatch({
          ...base,
          jobKind: "relative",
        })
      : engine.dispatch({ ...base, jobKind: "cron" });
  };

  const attemptReserved: HeartbeatAdmissionReactor["Service"]["attemptReserved"] = (occurrence) =>
    Effect.gen(function* () {
      const createdAt = DateTime.formatIso(yield* DateTime.now);
      // Validation and serialized dispatch share the scheduler mutation lock.
      const attempt = yield* scheduler
        .withValidOccurrence(
          occurrence.occurrenceId,
          Effect.gen(function* () {
            if (!(yield* accessFor(occurrence.threadId))) return { _tag: "AccessRevoked" } as const;
            return {
              _tag: "Dispatched",
              exit: yield* Effect.exit(dispatch(occurrence, createdAt)),
            } as const;
          }),
        )
        .pipe(Effect.catch(() => Effect.succeed({ _tag: "SchedulerError" } as const)));
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
      const exit = attempt.value.exit;
      if (Exit.isSuccess(exit)) {
        yield* outcomeForOccurrence(occurrence, { status: "admitted" });
        return "complete";
      }

      const cause = exit.cause;
      // A boolean check cannot narrow Cause<E>; interrupts must leave the
      // typed failure channel untouched.
      if (Cause.hasInterrupts(cause)) return yield* Effect.interrupt;
      const failure = Cause.findErrorOption(cause);
      if (Option.isSome(failure) && isHeartbeatThreadBusyError(failure.value)) {
        return "defer";
      }
      if (
        Option.isSome(failure) &&
        (isOrchestrationCommandInvariantError(failure.value) ||
          isOrchestrationCommandPreviouslyRejectedError(failure.value))
      ) {
        yield* outcomeForOccurrence(occurrence, { status: "failed" });
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

  const start: HeartbeatAdmissionReactor["Service"]["start"] = Effect.fn("start")(function* () {
    const domainEvents = yield* engine.subscribeDomainEvents;
    yield* reactor.start(attemptReserved);
    const processLifecycleEvent = (event: OrchestrationEvent) =>
      Effect.gen(function* () {
        if (event.type === "thread.message-sent") {
          if (event.payload.role !== "user" || event.payload.heartbeat !== undefined) {
            return;
          }
          return yield* scheduler.activateOnAcceptedTurn(event.payload.threadId).pipe(
            Effect.catchCauseIf(
              (cause) => !Cause.hasInterruptsOnly(cause),
              (cause) =>
                Effect.logWarning("failed to activate heartbeat schedules", {
                  threadId: event.payload.threadId,
                  cause: Cause.pretty(cause),
                }),
            ),
          );
        }
        if (event.type === "thread.turn-interrupt-requested") {
          return yield* scheduler
            .cancelRelative(event.payload.threadId, "turn interrupted", event.sequence)
            .pipe(
              Effect.catchCauseIf(
                (cause) => !Cause.hasInterruptsOnly(cause),
                (cause) =>
                  Effect.logWarning("failed to cancel relative heartbeat", {
                    threadId: event.payload.threadId,
                    cause: Cause.pretty(cause),
                  }),
              ),
            );
        }
        if (
          event.type === "thread.session-stop-requested" &&
          event.payload.onlyIfSettled !== true
        ) {
          return yield* scheduler
            .cancelThread(event.payload.threadId, "session stopped", event.sequence)
            .pipe(
              Effect.catchCauseIf(
                (cause) => !Cause.hasInterruptsOnly(cause),
                (cause) =>
                  Effect.logWarning("failed to cancel heartbeat schedules", {
                    threadId: event.payload.threadId,
                    cause: Cause.pretty(cause),
                  }),
              ),
            );
        }
        if (event.type === "thread.archived" || event.type === "thread.deleted") {
          return yield* scheduler
            .cancelThread(event.payload.threadId, event.type, event.sequence)
            .pipe(
              Effect.catchCauseIf(
                (cause) => !Cause.hasInterruptsOnly(cause),
                (cause) =>
                  Effect.logWarning("failed to cancel heartbeat schedules", {
                    threadId: event.payload.threadId,
                    cause: Cause.pretty(cause),
                  }),
              ),
            );
        }
      });

    yield* forkParked(
      Stream.runForEach(domainEvents, (event) =>
        processLifecycleEvent(event).pipe(
          Effect.catchCauseIf(
            (cause) => !Cause.hasInterruptsOnly(cause),
            (cause) =>
              Effect.logWarning("heartbeat lifecycle reactor failed", {
                eventType: event.type,
                cause: Cause.pretty(cause),
              }),
          ),
        ),
      ),
    );
  });

  return HeartbeatAdmissionReactor.of({ start, attemptReserved, processReserved });
});

export const layer = Layer.effect(HeartbeatAdmissionReactor, make);
