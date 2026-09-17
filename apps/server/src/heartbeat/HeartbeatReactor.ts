import { makeDrainableWorker, type DrainableWorker } from "@t3tools/shared/DrainableWorker";
import * as Clock from "effect/Clock";
import * as Context from "effect/Context";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as PubSub from "effect/PubSub";
import * as Queue from "effect/Queue";
import * as Scope from "effect/Scope";
import * as Semaphore from "effect/Semaphore";

import { HeartbeatScheduler, type ReservedHeartbeatOccurrence } from "./HeartbeatService.ts";

export interface HeartbeatReactorHandle {
  /** Force one wall-clock comparison. Tests use this after moving TestClock. */
  readonly pulse: Effect.Effect<void>;
  /** Wait until all comparisons already enqueued have completed. */
  readonly drain: Effect.Effect<void>;
  /** Reserved occurrences for the future orchestration admission worker. */
  readonly subscribeReserved: Effect.Effect<
    PubSub.Subscription<ReservedHeartbeatOccurrence>,
    never,
    Scope.Scope
  >;
}

export type ReservationAttemptResult = "complete" | "defer";

type ReservationConsumer = (
  occurrence: ReservedHeartbeatOccurrence,
) => Effect.Effect<ReservationAttemptResult, never>;

export class HeartbeatReactor extends Context.Service<
  HeartbeatReactor,
  {
    readonly start: (
      onReservation?: ReservationConsumer | undefined,
    ) => Effect.Effect<HeartbeatReactorHandle, never, Scope.Scope>;
  }
>()("t3/heartbeat/HeartbeatReactor") {}

/** @public The reactor is inert until its public start effect is run. */
export const make = Effect.gen(function* () {
  const scheduler = yield* HeartbeatScheduler;

  const start: HeartbeatReactor["Service"]["start"] = (onReservation) =>
    Effect.gen(function* () {
      const changes = yield* scheduler.subscribeChanges;
      const reserved = yield* Effect.acquireRelease(
        PubSub.unbounded<ReservedHeartbeatOccurrence>(),
        PubSub.shutdown,
      );
      // Install the admission handoff before any pulse can publish. The returned
      // subscription is deliberately non-dropping; orchestration will consume it
      // once that integration is wired.
      const reservedSubscription = yield* PubSub.subscribe(reserved);
      if (onReservation !== undefined) {
        const ingress = yield* Queue.bounded<ReservedHeartbeatOccurrence>(256);
        const admissionPermits = yield* Semaphore.make(8);
        const threadQueues = new Map<string, Queue.Queue<ReservedHeartbeatOccurrence>>();

        const queueForThread = Effect.fn("HeartbeatReactor.queueForThread")(function* (
          occurrence: ReservedHeartbeatOccurrence,
        ) {
          const existing = threadQueues.get(occurrence.threadId);
          if (existing !== undefined) return existing;
          const queue = yield* Queue.bounded<ReservedHeartbeatOccurrence>(64);
          threadQueues.set(occurrence.threadId, queue);
          // One parked worker per observed thread preserves FIFO ordering.
          // The shared semaphore bounds active admissions without allowing one
          // indefinitely busy thread to block unrelated due work.
          yield* Effect.forkScoped(
            Effect.forever(
              Effect.gen(function* () {
                const next = yield* Queue.take(queue);
                while (true) {
                  const result = yield* admissionPermits.withPermits(1)(onReservation(next));
                  if (result === "complete") break;
                  // Deferred work retains its per-thread position but releases
                  // the global attempt permit while waiting to retry.
                  yield* Effect.sleep(Duration.seconds(1));
                }
              }),
            ),
          );
          return queue;
        });

        yield* Effect.forkScoped(
          Effect.forever(
            Queue.take(ingress).pipe(
              Effect.flatMap((occurrence) =>
                queueForThread(occurrence).pipe(
                  Effect.flatMap((queue) => Queue.offer(queue, occurrence)),
                ),
              ),
            ),
          ),
        );
        // Install the handoff before the initial pulse. The bounded ingress
        // queue applies backpressure while per-thread workers admit in order.
        yield* Effect.forkScoped(
          Effect.forever(
            PubSub.take(reservedSubscription).pipe(
              Effect.flatMap((occurrence) => Queue.offer(ingress, occurrence)),
            ),
          ),
        );
      }

      const worker: DrainableWorker<number> = yield* makeDrainableWorker((nowMs: number) =>
        scheduler.reserveDue(nowMs).pipe(
          Effect.flatMap((occurrences) => PubSub.publishAll(reserved, occurrences)),
          Effect.asVoid,
          Effect.catchCause((cause) =>
            Effect.logError("heartbeat scheduler comparison failed", { cause }),
          ),
        ),
      );

      const pulse = Clock.currentTimeMillis.pipe(Effect.flatMap(worker.enqueue));

      // Startup restoration parks persisted jobs, but active in-process jobs may
      // already be due. The queue subscription must exist before this pulse.
      yield* pulse;
      yield* worker.drain;

      yield* Effect.gen(function* () {
        while (true) {
          const snapshot = yield* scheduler.schedulerSnapshot;
          const nextDueAtMs = Math.min(
            ...snapshot.cronJobs.map((job) => job.nextDueAtMs),
            ...snapshot.relativeSlots.map((slot) => slot.dueAtMs),
          );

          if (!Number.isFinite(nextDueAtMs) && !snapshot.hasPendingReservations) {
            yield* PubSub.take(changes);
          } else {
            const nowMs = yield* Clock.currentTimeMillis;
            const waitMs = Number.isFinite(nextDueAtMs)
              ? Math.min(Math.max(0, nextDueAtMs - nowMs), 1_000)
              : 1_000;
            if (waitMs > 0) {
              yield* Effect.raceFirst(Effect.sleep(Duration.millis(waitMs)), PubSub.take(changes));
            }
          }

          yield* pulse;
          yield* worker.drain;
        }
      }).pipe(
        Effect.catchCause((cause) => Effect.logError("heartbeat scheduler loop failed", { cause })),
        Effect.forkScoped,
      );

      return {
        pulse,
        drain: worker.drain,
        subscribeReserved: Effect.succeed(reservedSubscription),
      } satisfies HeartbeatReactorHandle;
    });
  return HeartbeatReactor.of({ start });
});

export const layer = Layer.effect(HeartbeatReactor, make);
