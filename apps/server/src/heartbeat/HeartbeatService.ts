import {
  HeartbeatCronExpression,
  HeartbeatInvalidCronError,
  HeartbeatInvalidTimeZoneError,
  HeartbeatJobId,
  HeartbeatJobLimitExceededError,
  HeartbeatOccurrenceId,
  HeartbeatOperationError,
  HeartbeatThreadNotFoundError,
  HeartbeatTimeZone,
  type HeartbeatCreateInput,
  type HeartbeatCreateResult,
  type HeartbeatCronJob,
  type HeartbeatDeleteInput,
  type HeartbeatDeleteResult,
  type HeartbeatListResult,
  type HeartbeatOccurrenceOutcome,
  type HeartbeatRelativeJob,
  type HeartbeatServiceError,
  type HeartbeatWakeupInput,
  type HeartbeatWakeupResult,
  type ThreadId,
} from "@t3tools/contracts";
import * as Clock from "effect/Clock";
import * as Context from "effect/Context";
import * as Cron from "effect/Cron";
import * as Crypto from "effect/Crypto";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as PubSub from "effect/PubSub";
import * as Result from "effect/Result";
import * as Schema from "effect/Schema";
import * as Scope from "effect/Scope";
import * as Semaphore from "effect/Semaphore";

import {
  HEARTBEAT_CRON_JOB_LIMIT,
  HeartbeatRepository,
  HeartbeatRepositoryError,
  type PendingHeartbeatOccurrence,
  type StoredHeartbeatJob,
} from "./HeartbeatRepository.ts";
import {
  cronDueTime,
  nextNominalOccurrence,
  parseHeartbeatCron,
  relativeDueTime,
} from "./heartbeatSchedule.ts";

const RECURRING_LIFETIME_MS = 7 * 24 * 60 * 60 * 1_000;
const OCCURRENCE_RETENTION_MS = 7 * 24 * 60 * 60 * 1_000;

export interface RelativeSlot {
  readonly threadId: ThreadId;
  readonly id: HeartbeatJobId;
  readonly prompt: HeartbeatRelativeJob["prompt"];
  readonly dueAtMs: number;
  /** Authoritative orchestration head observed when this arm was created. */
  readonly originSequence: number;
  readonly reason?: HeartbeatRelativeJob["reason"];
  readonly noop: boolean;
  readonly pendingOccurrenceId?: HeartbeatOccurrenceId;
}

interface CachedCronJob {
  readonly job: StoredHeartbeatJob;
  readonly cron: Cron.Cron;
  readonly originSequence: number;
  readonly pendingOccurrenceId?: HeartbeatOccurrenceId;
  readonly removeAfterPending: boolean;
}

type OccurrenceStatus = HeartbeatOccurrenceOutcome["status"] | "pending";

interface OccurrenceToken {
  readonly kind: "cron" | "relative";
  readonly key: string;
  readonly status: OccurrenceStatus;
}

export interface SchedulerSnapshot {
  readonly cronJobs: ReadonlyArray<StoredHeartbeatJob>;
  readonly relativeSlots: ReadonlyArray<RelativeSlot>;
  readonly hasPendingReservations: boolean;
}

export interface ReservedHeartbeatOccurrence extends PendingHeartbeatOccurrence {
  readonly kind: "cron" | "relative";
  readonly noop: boolean;
  readonly originSequence: number;
}

export interface HeartbeatServiceShape {
  readonly create: (
    threadId: ThreadId,
    input: HeartbeatCreateInput,
  ) => Effect.Effect<HeartbeatCreateResult, HeartbeatServiceError>;
  readonly list: (threadId: ThreadId) => Effect.Effect<HeartbeatListResult, HeartbeatServiceError>;
  readonly delete: (
    threadId: ThreadId,
    input: HeartbeatDeleteInput,
  ) => Effect.Effect<HeartbeatDeleteResult, HeartbeatServiceError>;
  readonly wakeup: (
    threadId: ThreadId,
    input: HeartbeatWakeupInput,
  ) => Effect.Effect<HeartbeatWakeupResult, HeartbeatServiceError>;
  readonly activateOnAcceptedTurn: (
    threadId: ThreadId,
  ) => Effect.Effect<void, HeartbeatServiceError>;
  readonly cancelRelative: (
    threadId: ThreadId,
    reason: string,
    beforeSequence?: number,
  ) => Effect.Effect<void, HeartbeatServiceError>;
  readonly cancelThread: (
    threadId: ThreadId,
    reason: string,
    beforeSequence?: number,
  ) => Effect.Effect<void, HeartbeatServiceError>;
  readonly setOccurrenceOutcome: (
    occurrenceId: HeartbeatOccurrenceId,
    outcome: HeartbeatOccurrenceOutcome,
  ) => Effect.Effect<void, HeartbeatServiceError>;
}

export class HeartbeatService extends Context.Service<HeartbeatService, HeartbeatServiceShape>()(
  "t3/heartbeat/HeartbeatService",
) {}

export interface HeartbeatServiceImplementation extends HeartbeatServiceShape {
  readonly schedulerSnapshot: Effect.Effect<SchedulerSnapshot, HeartbeatServiceError>;
  readonly reserveDue: (
    nowMs: number,
  ) => Effect.Effect<ReadonlyArray<ReservedHeartbeatOccurrence>, HeartbeatServiceError>;
  readonly subscribeChanges: Effect.Effect<PubSub.Subscription<void>, never, Scope.Scope>;
  readonly pruneExpired: Effect.Effect<void, HeartbeatServiceError>;
  readonly validateOccurrence: (
    occurrenceId: HeartbeatOccurrenceId,
  ) => Effect.Effect<boolean, HeartbeatServiceError>;
  readonly withValidOccurrence: <A, E, R>(
    occurrenceId: HeartbeatOccurrenceId,
    admission: Effect.Effect<A, E, R>,
  ) => Effect.Effect<Option.Option<A>, E | HeartbeatServiceError, R>;
}

export class HeartbeatScheduler extends Context.Service<
  HeartbeatScheduler,
  Pick<
    HeartbeatServiceImplementation,
    | "schedulerSnapshot"
    | "reserveDue"
    | "subscribeChanges"
    | "pruneExpired"
    | "validateOccurrence"
    | "withValidOccurrence"
    | "setOccurrenceOutcome"
    | "activateOnAcceptedTurn"
    | "cancelRelative"
    | "cancelThread"
  >
>()("t3/heartbeat/HeartbeatService/HeartbeatScheduler") {}

const iso = (epochMillis: number) => DateTime.formatIso(DateTime.makeUnsafe(epochMillis));
const jobKey = (threadId: ThreadId, publicId: HeartbeatJobId) => `${threadId}\0${publicId}`;

const cronJobView = (cached: CachedCronJob, active: boolean): HeartbeatCronJob => ({
  id: cached.job.publicId,
  kind: "cron",
  prompt: cached.job.prompt,
  status: cached.pendingOccurrenceId === undefined ? (active ? "active" : "dormant") : "pending",
  cron: HeartbeatCronExpression.make(cached.job.cronExpression),
  recurring: cached.job.recurring,
  timezone: HeartbeatTimeZone.make(cached.job.timezone),
  scheduleDescription: `${cached.job.cronExpression} (${cached.job.timezone})`,
  nextNominalAt: iso(cached.job.nextNominalAtMs),
  nextFireAt: iso(cached.job.nextDueAtMs),
  expiresAt: cached.job.recurring ? iso(cached.job.expiresAtMs) : null,
  restartPolicy: "restore-on-resume",
});

const relativeJobView = (slot: RelativeSlot, active: boolean): HeartbeatRelativeJob => ({
  id: slot.id,
  kind: "relative",
  prompt: slot.prompt,
  status: slot.pendingOccurrenceId === undefined ? (active ? "active" : "dormant") : "pending",
  dueAt: iso(slot.dueAtMs),
  ...(slot.reason === undefined ? {} : { reason: slot.reason }),
  noop: slot.noop,
  restartPolicy: "process-local",
});

const nextRecurringSchedule = (cached: CachedCronJob, nowMs: number) => {
  const { job, cron } = cached;
  if (job.nextNominalAtMs >= job.expiresAtMs || nowMs >= job.expiresAtMs) return null;
  const nextNominal = nextNominalOccurrence(cron, Math.max(nowMs, job.nextNominalAtMs));
  const nominalMs = Math.min(nextNominal, job.expiresAtMs);
  return {
    nominalMs,
    dueMs:
      nominalMs === job.expiresAtMs
        ? nominalMs
        : cronDueTime({
            cron,
            publicId: job.publicId,
            nominalMs,
            createdAtMs: job.createdAtMs,
            recurring: true,
          }),
  };
};

/** @public Constructs the contract service plus scheduler-facing hooks. */
export const make = Effect.gen(function* () {
  const repository = yield* HeartbeatRepository;
  const crypto = yield* Crypto.Crypto;
  const changes = yield* PubSub.sliding<void>(1);
  const lock = yield* Semaphore.make(1);
  const activeThreads = new Set<string>();
  const cronJobs = new Map<string, CachedCronJob>();
  const relativeSlots = new Map<string, RelativeSlot>();
  const occurrences = new Map<HeartbeatOccurrenceId, OccurrenceToken>();

  const publishChange = PubSub.publish(changes, undefined).pipe(Effect.asVoid);
  const isServiceError = Schema.is(
    Schema.Union([
      HeartbeatInvalidCronError,
      HeartbeatInvalidTimeZoneError,
      HeartbeatJobLimitExceededError,
      HeartbeatThreadNotFoundError,
      HeartbeatOperationError,
    ]),
  );
  const operationError = (operation: HeartbeatOperationError["operation"]) => (cause: unknown) =>
    isServiceError(cause) ? cause : new HeartbeatOperationError({ operation, cause });
  const isRepositoryError = Schema.is(HeartbeatRepositoryError);
  const locked = <A, E, R>(effect: Effect.Effect<A, E, R>) => lock.withPermits(1)(effect);

  const assertThread = Effect.fn("HeartbeatService.assertThread")(function* (threadId: ThreadId) {
    if (!(yield* repository.threadExists(threadId))) {
      return yield* new HeartbeatThreadNotFoundError({ threadId });
    }
  });

  const nextPublicId = Effect.fn("HeartbeatService.nextPublicId")(function* () {
    const uuid = yield* crypto.randomUUIDv4;
    return HeartbeatJobId.make(uuid.replaceAll("-", "").slice(0, 8));
  });

  const nextOccurrenceId = Effect.fn("HeartbeatService.nextOccurrenceId")(function* () {
    return HeartbeatOccurrenceId.make(yield* crypto.randomUUIDv4);
  });

  const nextAvailableRelativeId = Effect.fn("HeartbeatService.nextAvailableRelativeId")(function* (
    threadId: ThreadId,
  ) {
    for (let attempt = 0; attempt < 16; attempt += 1) {
      const id = yield* nextPublicId();
      if (
        relativeSlots.get(threadId)?.id !== id &&
        !(yield* repository.publicIdExists(threadId, id))
      ) {
        return id;
      }
    }
    return yield* new HeartbeatOperationError({
      operation: "wakeup",
      cause: new Error("Could not allocate a collision-free heartbeat ID"),
    });
  });

  const invalidateOccurrence = (occurrenceId: HeartbeatOccurrenceId | undefined) => {
    if (occurrenceId !== undefined) occurrences.delete(occurrenceId);
  };

  // A restart makes old admission state uncertain. Preserve its durable audit
  // identity as expired, then rebuild one parsed in-memory calendar per job.
  yield* locked(
    Effect.gen(function* () {
      const nowMs = yield* Clock.currentTimeMillis;
      yield* repository.expirePendingOccurrences(nowMs);
      const stored = yield* repository.listAll;
      const cancellationSequences = new Map<string, number | undefined>();
      for (const original of stored) {
        let cancellationSequence = cancellationSequences.get(original.threadId);
        if (!cancellationSequences.has(original.threadId)) {
          cancellationSequence = yield* repository.latestCancellationSequence(original.threadId);
          cancellationSequences.set(original.threadId, cancellationSequence);
        }
        if (cancellationSequence !== undefined && original.originSequence < cancellationSequence) {
          yield* repository.delete(original.threadId, original.publicId, nowMs);
          continue;
        }
        const parsed = parseHeartbeatCron(original.cronExpression, original.timezone);
        if (Result.isFailure(parsed)) {
          yield* repository.delete(original.threadId, original.publicId, nowMs, "expired");
          continue;
        }
        if (!original.recurring) {
          if (original.nextDueAtMs <= nowMs) {
            yield* repository.delete(original.threadId, original.publicId, nowMs, "expired");
          } else {
            cronJobs.set(jobKey(original.threadId, original.publicId), {
              job: { ...original, pending: false },
              cron: parsed.success.cron,
              originSequence: original.originSequence,
              removeAfterPending: false,
            });
          }
          continue;
        }
        if (nowMs >= original.expiresAtMs) {
          yield* repository.delete(original.threadId, original.publicId, nowMs, "expired");
          continue;
        }
        const nominal = nextNominalOccurrence(parsed.success.cron, nowMs);
        const nominalMs = Math.min(nominal, original.expiresAtMs);
        const dueMs =
          nominalMs === original.expiresAtMs
            ? nominalMs
            : cronDueTime({
                cron: parsed.success.cron,
                publicId: original.publicId,
                nominalMs,
                createdAtMs: original.createdAtMs,
                recurring: true,
              });
        const job = {
          ...original,
          nextNominalAtMs: nominalMs,
          nextDueAtMs: dueMs,
          pending: false,
        };
        if (
          nominalMs !== original.nextNominalAtMs ||
          dueMs !== original.nextDueAtMs ||
          original.pending
        ) {
          yield* repository.replaceSchedule({
            threadId: job.threadId,
            publicId: job.publicId,
            nextNominalAtMs: nominalMs,
            nextDueAtMs: dueMs,
          });
        }
        cronJobs.set(jobKey(job.threadId, job.publicId), {
          job,
          cron: parsed.success.cron,
          originSequence: original.originSequence,
          removeAfterPending: false,
        });
      }
    }),
  );

  const create: HeartbeatServiceShape["create"] = (threadId, input) =>
    locked(
      Effect.gen(function* () {
        yield* assertThread(threadId);
        const logicalJobCount = [...cronJobs.values()].filter(
          (cached) => cached.job.threadId === threadId,
        ).length;
        if (logicalJobCount >= HEARTBEAT_CRON_JOB_LIMIT) {
          return yield* new HeartbeatJobLimitExceededError({ limit: HEARTBEAT_CRON_JOB_LIMIT });
        }
        const timezone =
          input.timezone === undefined
            ? DateTime.zoneToString(DateTime.zoneMakeLocal())
            : input.timezone;
        if (Option.isNone(DateTime.zoneMakeNamed(timezone))) {
          return yield* new HeartbeatInvalidTimeZoneError({ timezone });
        }
        const parsed = parseHeartbeatCron(input.cron, timezone);
        if (Result.isFailure(parsed)) {
          return yield* new HeartbeatInvalidCronError({ reason: parsed.failure.message });
        }
        const createdAtMs = yield* Clock.currentTimeMillis;
        const expiresAtMs = input.recurring
          ? createdAtMs + RECURRING_LIFETIME_MS
          : Number.POSITIVE_INFINITY;
        const firstNominal = nextNominalOccurrence(parsed.success.cron, createdAtMs);
        const nominalMs = input.recurring ? Math.min(firstNominal, expiresAtMs) : firstNominal;
        const originSequence = yield* repository.latestThreadSequence(threadId);

        for (let attempt = 0; attempt < 16; attempt += 1) {
          const publicId = yield* nextPublicId();
          if (relativeSlots.get(threadId)?.id === publicId) continue;
          const nextDueAtMs =
            input.recurring && nominalMs === expiresAtMs
              ? nominalMs
              : cronDueTime({
                  cron: parsed.success.cron,
                  publicId,
                  nominalMs,
                  createdAtMs,
                  recurring: input.recurring,
                });
          const job: StoredHeartbeatJob = {
            threadId,
            publicId,
            prompt: input.prompt,
            cronExpression: parsed.success.expression,
            timezone: parsed.success.timezone,
            recurring: input.recurring,
            createdAtMs,
            expiresAtMs: input.recurring ? expiresAtMs : nominalMs,
            nextNominalAtMs: nominalMs,
            nextDueAtMs,
            generation: 0,
            originSequence,
            pending: false,
          };
          const result = yield* repository.create(job);
          if (result === "limit") {
            return yield* new HeartbeatJobLimitExceededError({ limit: HEARTBEAT_CRON_JOB_LIMIT });
          }
          if (result === "created") {
            cronJobs.set(jobKey(threadId, publicId), {
              job,
              cron: parsed.success.cron,
              originSequence,
              removeAfterPending: false,
            });
            yield* publishChange;
            return cronJobView(
              cronJobs.get(jobKey(threadId, publicId))!,
              activeThreads.has(threadId),
            );
          }
        }
        return yield* new HeartbeatOperationError({
          operation: "create",
          cause: new Error("Could not allocate a collision-free heartbeat ID"),
        });
      }),
    ).pipe(Effect.mapError(operationError("create")));

  const list: HeartbeatServiceShape["list"] = (threadId) =>
    locked(
      Effect.gen(function* () {
        yield* assertThread(threadId);
        const active = activeThreads.has(threadId);
        const persisted = [...cronJobs.values()]
          .filter((cached) => cached.job.threadId === threadId)
          .sort(
            (left, right) =>
              left.job.createdAtMs - right.job.createdAtMs ||
              left.job.publicId.localeCompare(right.job.publicId),
          )
          .map((cached) => cronJobView(cached, active));
        const relative = relativeSlots.get(threadId);
        return {
          jobs: [
            ...persisted,
            ...(relative === undefined ? [] : [relativeJobView(relative, active)]),
          ],
        };
      }),
    ).pipe(Effect.mapError(operationError("list")));

  const deleteJob: HeartbeatServiceShape["delete"] = (threadId, input) =>
    locked(
      Effect.gen(function* () {
        yield* assertThread(threadId);
        const relative = relativeSlots.get(threadId);
        if (relative?.id === input.id) {
          invalidateOccurrence(relative.pendingOccurrenceId);
          relativeSlots.delete(threadId);
          yield* publishChange;
          return { id: input.id, deleted: true };
        }
        const key = jobKey(threadId, input.id);
        const cached = cronJobs.get(key);
        if (cached === undefined) return { id: input.id, deleted: false };
        invalidateOccurrence(cached.pendingOccurrenceId);
        yield* repository.delete(threadId, input.id, yield* Clock.currentTimeMillis);
        cronJobs.delete(key);
        yield* publishChange;
        return { id: input.id, deleted: true };
      }),
    ).pipe(Effect.mapError(operationError("delete")));

  const wakeup: HeartbeatServiceShape["wakeup"] = (threadId, input) =>
    locked(
      Effect.gen(function* () {
        yield* assertThread(threadId);
        const prior = relativeSlots.get(threadId);
        if (input.type === "stop") {
          invalidateOccurrence(prior?.pendingOccurrenceId);
          const stopped = relativeSlots.delete(threadId);
          if (stopped) yield* publishChange;
          return { action: "stopped", stopped, cronJobsUnaffected: true } as const;
        }
        const nowMs = yield* Clock.currentTimeMillis;
        const originSequence = yield* repository.latestThreadSequence(threadId);
        // Allocate before invalidating the previous generation so a failed
        // replacement attempt leaves the existing arm intact.
        const id = yield* nextAvailableRelativeId(threadId);
        const slot: RelativeSlot = {
          threadId,
          id,
          prompt: input.prompt,
          dueAtMs: relativeDueTime(nowMs, input.delaySeconds),
          originSequence,
          ...(input.reason === undefined ? {} : { reason: input.reason }),
          noop: input.noop,
        };
        invalidateOccurrence(prior?.pendingOccurrenceId);
        relativeSlots.set(threadId, slot);
        yield* publishChange;
        return {
          action: "armed",
          job: relativeJobView(slot, activeThreads.has(threadId)),
          replaced: prior !== undefined,
        } as const;
      }),
    ).pipe(Effect.mapError(operationError("wakeup")));

  const activateOnAcceptedTurn: HeartbeatServiceShape["activateOnAcceptedTurn"] = (threadId) =>
    locked(
      Effect.gen(function* () {
        yield* assertThread(threadId);
        if (!activeThreads.has(threadId)) {
          const nowMs = yield* Clock.currentTimeMillis;
          for (const [key, cached] of cronJobs) {
            if (cached.job.threadId !== threadId || cached.pendingOccurrenceId !== undefined)
              continue;
            if (!cached.job.recurring) {
              if (cached.job.nextDueAtMs <= nowMs) {
                yield* repository.delete(threadId, cached.job.publicId, nowMs, "expired");
                cronJobs.delete(key);
              }
              continue;
            }
            if (nowMs >= cached.job.expiresAtMs) {
              yield* repository.delete(threadId, cached.job.publicId, nowMs, "expired");
              cronJobs.delete(key);
              continue;
            }
            const nominal = nextNominalOccurrence(cached.cron, nowMs);
            const nominalMs = Math.min(nominal, cached.job.expiresAtMs);
            const dueMs =
              nominalMs === cached.job.expiresAtMs
                ? nominalMs
                : cronDueTime({
                    cron: cached.cron,
                    publicId: cached.job.publicId,
                    nominalMs,
                    createdAtMs: cached.job.createdAtMs,
                    recurring: true,
                  });
            if (nominalMs !== cached.job.nextNominalAtMs || dueMs !== cached.job.nextDueAtMs) {
              yield* repository.replaceSchedule({
                threadId,
                publicId: cached.job.publicId,
                nextNominalAtMs: nominalMs,
                nextDueAtMs: dueMs,
              });
              cronJobs.set(key, {
                ...cached,
                job: {
                  ...cached.job,
                  nextNominalAtMs: nominalMs,
                  nextDueAtMs: dueMs,
                },
              });
            }
          }
        }
        activeThreads.add(threadId);
        yield* publishChange;
      }),
    ).pipe(Effect.mapError(operationError("activate")));

  const cancelRelative: HeartbeatServiceShape["cancelRelative"] = (
    threadId,
    _reason,
    beforeSequence,
  ) =>
    locked(
      Effect.gen(function* () {
        const slot = relativeSlots.get(threadId);
        if (
          slot === undefined ||
          (beforeSequence !== undefined && slot.originSequence >= beforeSequence)
        ) {
          return;
        }
        invalidateOccurrence(slot?.pendingOccurrenceId);
        if (relativeSlots.delete(threadId)) yield* publishChange;
      }),
    ).pipe(Effect.mapError(operationError("cancel-relative")));

  const cancelThread: HeartbeatServiceShape["cancelThread"] = (threadId, _reason, beforeSequence) =>
    locked(
      Effect.gen(function* () {
        const relative = relativeSlots.get(threadId);
        if (
          relative !== undefined &&
          (beforeSequence === undefined || relative.originSequence < beforeSequence)
        ) {
          invalidateOccurrence(relative.pendingOccurrenceId);
          relativeSlots.delete(threadId);
        }
        const nowMs = yield* Clock.currentTimeMillis;
        for (const [key, cached] of cronJobs) {
          if (
            cached.job.threadId !== threadId ||
            (beforeSequence !== undefined && cached.originSequence >= beforeSequence)
          ) {
            continue;
          }
          invalidateOccurrence(cached.pendingOccurrenceId);
          yield* repository.delete(threadId, cached.job.publicId, nowMs);
          cronJobs.delete(key);
        }
        const hasSurvivors =
          relativeSlots.has(threadId) ||
          [...cronJobs.values()].some((cached) => cached.job.threadId === threadId);
        if (!hasSurvivors) activeThreads.delete(threadId);
        yield* publishChange;
      }),
    ).pipe(Effect.mapError(operationError("cancel-thread")));

  const setOccurrenceOutcome: HeartbeatServiceShape["setOccurrenceOutcome"] = (
    occurrenceId,
    outcome,
  ) =>
    locked(
      Effect.gen(function* () {
        const token = occurrences.get(occurrenceId);
        if (token === undefined) return;
        if (token.kind === "cron") {
          yield* repository.setOccurrenceOutcome(
            occurrenceId,
            outcome,
            yield* Clock.currentTimeMillis,
          );
        }
        if (outcome.status === "admitted") {
          occurrences.set(occurrenceId, { ...token, status: "admitted" });
          return;
        }
        occurrences.delete(occurrenceId);
        if (token.kind === "relative") {
          const slot = relativeSlots.get(token.key);
          if (slot?.pendingOccurrenceId === occurrenceId) relativeSlots.delete(token.key);
        } else {
          const cached = cronJobs.get(token.key);
          if (cached?.pendingOccurrenceId === occurrenceId) {
            if (cached.removeAfterPending) {
              cronJobs.delete(token.key);
            } else {
              cronJobs.set(token.key, {
                job: { ...cached.job, pending: false },
                cron: cached.cron,
                originSequence: cached.originSequence,
                removeAfterPending: false,
              });
            }
          }
        }
        yield* publishChange;
      }),
    ).pipe(
      Effect.mapError((error) =>
        isRepositoryError(error) ? operationError("set-occurrence-outcome")(error) : error,
      ),
    );

  const schedulerSnapshot: HeartbeatServiceImplementation["schedulerSnapshot"] = locked(
    Effect.sync(() => ({
      cronJobs: [...cronJobs.values()]
        .filter(
          (cached) =>
            activeThreads.has(cached.job.threadId) && cached.pendingOccurrenceId === undefined,
        )
        .map((cached) => cached.job),
      relativeSlots: [...relativeSlots.values()].filter(
        (slot) => activeThreads.has(slot.threadId) && slot.pendingOccurrenceId === undefined,
      ),
      hasPendingReservations: occurrences.size > 0,
    })),
  );

  const reserveDue: HeartbeatServiceImplementation["reserveDue"] = (nowMs) =>
    locked(
      Effect.gen(function* () {
        const reserved: ReservedHeartbeatOccurrence[] = [];
        const dueCron = [...cronJobs.entries()]
          .filter(
            ([, cached]) =>
              activeThreads.has(cached.job.threadId) &&
              cached.pendingOccurrenceId === undefined &&
              cached.job.nextDueAtMs <= nowMs,
          )
          .sort(
            ([, left], [, right]) =>
              left.job.nextDueAtMs - right.job.nextDueAtMs ||
              left.job.threadId.localeCompare(right.job.threadId) ||
              left.job.publicId.localeCompare(right.job.publicId),
          );
        for (const [key, cached] of dueCron) {
          const next = cached.job.recurring ? nextRecurringSchedule(cached, nowMs) : null;
          const occurrenceId = yield* nextOccurrenceId();
          const occurrence = yield* repository.reserve({
            job: cached.job,
            occurrenceId,
            nowMs,
            nextNominalAtMs: next?.nominalMs ?? null,
            nextDueAtMs: next?.dueMs ?? null,
          });
          if (occurrence === null) continue;
          const nextJob =
            next === null
              ? cached.job
              : {
                  ...cached.job,
                  generation: occurrence.generation,
                  nextNominalAtMs: next.nominalMs,
                  nextDueAtMs: next.dueMs,
                  pending: true,
                };
          cronJobs.set(key, {
            job: nextJob,
            cron: cached.cron,
            originSequence: cached.originSequence,
            pendingOccurrenceId: occurrenceId,
            removeAfterPending: next === null,
          });
          occurrences.set(occurrenceId, { kind: "cron", key, status: "pending" });
          reserved.push({
            ...occurrence,
            kind: "cron",
            noop: false,
            originSequence: cached.originSequence,
          });
        }

        const dueRelative = [...relativeSlots.values()]
          .filter(
            (slot) =>
              activeThreads.has(slot.threadId) &&
              slot.pendingOccurrenceId === undefined &&
              slot.dueAtMs <= nowMs,
          )
          .sort(
            (left, right) =>
              left.dueAtMs - right.dueAtMs ||
              left.threadId.localeCompare(right.threadId) ||
              left.id.localeCompare(right.id),
          );
        for (const dueSlot of dueRelative) {
          if (relativeSlots.get(dueSlot.threadId)?.id !== dueSlot.id) continue;
          const occurrenceId = yield* nextOccurrenceId();
          const slot = { ...dueSlot, pendingOccurrenceId: occurrenceId };
          relativeSlots.set(slot.threadId, slot);
          occurrences.set(occurrenceId, {
            kind: "relative",
            key: slot.threadId,
            status: "pending",
          });
          reserved.push({
            occurrenceId,
            threadId: slot.threadId,
            publicId: slot.id,
            prompt: slot.prompt,
            generation: 1,
            nominalAtMs: slot.dueAtMs,
            dueAtMs: slot.dueAtMs,
            reservedAtMs: nowMs,
            kind: "relative",
            noop: slot.noop,
            originSequence: slot.originSequence,
          });
        }
        if (reserved.length > 0) yield* publishChange;
        return reserved.sort(
          (left, right) =>
            left.dueAtMs - right.dueAtMs ||
            left.threadId.localeCompare(right.threadId) ||
            left.publicId.localeCompare(right.publicId),
        );
      }),
    ).pipe(Effect.mapError(operationError("wakeup")));

  const validateOccurrence: HeartbeatServiceImplementation["validateOccurrence"] = (occurrenceId) =>
    locked(
      Effect.gen(function* () {
        const token = occurrences.get(occurrenceId);
        if (token?.status !== "pending") return false;
        if (token.kind === "relative") return true;
        return (yield* repository.occurrenceStatus(occurrenceId)) === "pending";
      }),
    ).pipe(Effect.mapError(operationError("set-occurrence-outcome")));

  const withValidOccurrence: HeartbeatServiceImplementation["withValidOccurrence"] = (
    occurrenceId,
    admission,
  ) =>
    locked(
      Effect.gen(function* () {
        const token = occurrences.get(occurrenceId);
        if (token?.status !== "pending") return Option.none();
        if (
          token.kind === "cron" &&
          (yield* repository.occurrenceStatus(occurrenceId)) !== "pending"
        ) {
          return Option.none();
        }
        // Cancellation/replacement uses this same lock. Whichever operation
        // acquires it first owns the admission decision; stale work can no
        // longer slip through a validate-then-dispatch gap.
        return Option.some(yield* admission);
      }),
    ).pipe(
      Effect.mapError((error) =>
        isRepositoryError(error) ? operationError("set-occurrence-outcome")(error) : error,
      ),
    );

  const pruneExpired = locked(
    Effect.gen(function* () {
      yield* repository.pruneOccurrences(
        (yield* Clock.currentTimeMillis) - OCCURRENCE_RETENTION_MS,
      );
    }),
  ).pipe(Effect.mapError(operationError("wakeup")));

  return {
    create,
    list,
    delete: deleteJob,
    wakeup,
    activateOnAcceptedTurn,
    cancelRelative,
    cancelThread,
    setOccurrenceOutcome,
    schedulerSnapshot,
    reserveDue,
    subscribeChanges: PubSub.subscribe(changes),
    pruneExpired,
    validateOccurrence,
    withValidOccurrence,
  } satisfies HeartbeatServiceImplementation;
});

export const layer = Layer.effectContext(
  make.pipe(
    Effect.map((service) =>
      Context.make(HeartbeatService, service).pipe(Context.add(HeartbeatScheduler, service)),
    ),
  ),
);
