import {
  HeartbeatCronExpression,
  HeartbeatJobId,
  HeartbeatOccurrenceId,
  HeartbeatPrompt,
  HeartbeatTimeZone,
  ThreadId,
} from "@t3tools/contracts";
import { assert, it } from "@effect/vitest";
import * as Crypto from "effect/Crypto";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as PubSub from "effect/PubSub";
import * as Ref from "effect/Ref";
import * as SqlClient from "effect/unstable/sql/SqlClient";
import { TestClock } from "effect/testing";
import * as NodeSqliteClient from "@t3tools/shared/nodeSqliteClient";

import { runMigrations } from "../persistence/Migrations.ts";
import * as HeartbeatReactor from "./HeartbeatReactor.ts";
import * as HeartbeatRepository from "./HeartbeatRepository.ts";
import * as HeartbeatService from "./HeartbeatService.ts";

const START_MS = Date.parse("2026-09-17T10:00:10.000Z");
const THREAD_ID = ThreadId.make("heartbeat-reactor-thread");

const crypto = (() => {
  let counter = 0;
  const base = Crypto.make({
    randomBytes: (size) => new Uint8Array(size).fill(1),
    digest: (_algorithm, data) => Effect.succeed(data),
  });
  return {
    ...base,
    randomUUIDv4: Effect.sync(() => {
      counter += 1;
      return `${counter.toString(16).padStart(8, "0")}-0000-4000-8000-000000000000`;
    }),
  } satisfies Crypto.Crypto;
})();

it.effect("reserves due work as pending without pretending admission or delivery succeeded", () =>
  Effect.scoped(
    Effect.gen(function* () {
      yield* TestClock.setTime(START_MS);
      yield* runMigrations();
      const sql = yield* SqlClient.SqlClient;
      yield* sql`
        INSERT INTO projection_threads (
          thread_id, project_id, title, model_selection_json, created_at, updated_at
        ) VALUES (
          ${THREAD_ID}, 'heartbeat-project', 'Heartbeat reactor',
          '{"instanceId":"codex","model":"gpt-5"}',
          '2026-09-17T10:00:00.000Z', '2026-09-17T10:00:00.000Z'
        )
      `;

      const repository = yield* HeartbeatRepository.make;
      const service = yield* HeartbeatService.make.pipe(
        Effect.provideService(HeartbeatRepository.HeartbeatRepository, repository),
        Effect.provideService(Crypto.Crypto, crypto),
      );
      const job = yield* service.create(THREAD_ID, {
        cron: HeartbeatCronExpression.make("1 10 * * *"),
        prompt: HeartbeatPrompt.make("inspect the build"),
        recurring: false,
        timezone: HeartbeatTimeZone.make("UTC"),
      });
      yield* service.activateOnAcceptedTurn(THREAD_ID);

      const reactor = yield* HeartbeatReactor.make.pipe(
        Effect.provideService(HeartbeatService.HeartbeatScheduler, service),
      );
      const handle = yield* reactor.start();
      const reservations = yield* handle.subscribeReserved;

      yield* TestClock.setTime(Date.parse(job.nextFireAt));
      yield* handle.pulse;
      yield* handle.drain;
      const occurrence = yield* PubSub.take(reservations);

      assert.strictEqual(occurrence.publicId, job.id);
      assert.deepStrictEqual(yield* sql`SELECT occurrence_id, status FROM heartbeat_occurrences`, [
        { occurrence_id: occurrence.occurrenceId, status: "pending" },
      ]);
    }),
  ).pipe(Effect.provide(NodeSqliteClient.layer({ filename: ":memory:" }))),
);

it.effect("reconciles a wall-clock jump within one second without rescanning storage", () =>
  Effect.scoped(
    Effect.gen(function* () {
      yield* TestClock.setTime(START_MS);
      yield* runMigrations();
      const sql = yield* SqlClient.SqlClient;
      yield* sql`
        INSERT INTO projection_threads (
          thread_id, project_id, title, model_selection_json, created_at, updated_at
        ) VALUES (
          ${THREAD_ID}, 'heartbeat-project', 'Heartbeat reactor',
          '{"instanceId":"codex","model":"gpt-5"}',
          '2026-09-17T10:00:00.000Z', '2026-09-17T10:00:00.000Z'
        )
      `;

      const repository = yield* HeartbeatRepository.make;
      const listAllCalls = yield* Ref.make(0);
      const observedRepository = {
        ...repository,
        listAll: repository.listAll.pipe(Effect.tap(() => Ref.update(listAllCalls, (n) => n + 1))),
      } satisfies HeartbeatRepository.HeartbeatRepository["Service"];
      const service = yield* HeartbeatService.make.pipe(
        Effect.provideService(HeartbeatRepository.HeartbeatRepository, observedRepository),
        Effect.provideService(Crypto.Crypto, crypto),
      );
      const job = yield* service.create(THREAD_ID, {
        cron: HeartbeatCronExpression.make("1 10 * * *"),
        prompt: HeartbeatPrompt.make("recover after a clock jump"),
        recurring: false,
        timezone: HeartbeatTimeZone.make("UTC"),
      });
      yield* service.activateOnAcceptedTurn(THREAD_ID);

      const reactor = yield* HeartbeatReactor.make.pipe(
        Effect.provideService(HeartbeatService.HeartbeatScheduler, service),
      );
      const handle = yield* reactor.start();
      const reservations = yield* handle.subscribeReserved;
      const take = yield* Effect.forkScoped(PubSub.take(reservations));

      // Moving wall time alone does not wake a monotonic timer. The bounded
      // reconciliation wake does, no later than one second afterward.
      yield* TestClock.setTime(Date.parse(job.nextFireAt));
      yield* TestClock.adjust("1 second");
      const occurrence = yield* Fiber.join(take);

      assert.strictEqual(occurrence.publicId, job.id);
      assert.strictEqual(yield* Ref.get(listAllCalls), 1);
    }),
  ).pipe(Effect.provide(NodeSqliteClient.layer({ filename: ":memory:" }))),
);

it.effect(
  "defers a busy thread without blocking another thread and preserves per-thread order",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const changes = yield* PubSub.sliding<void>(1);
        const gateA = yield* Deferred.make<void>();
        const sawA2 = yield* Deferred.make<void>();
        const sawB = yield* Deferred.make<void>();
        const reservations = [
          {
            occurrenceId: HeartbeatOccurrenceId.make("occ-a1"),
            threadId: ThreadId.make("thread-a"),
            publicId: HeartbeatJobId.make("aaaaaaaa"),
            prompt: HeartbeatPrompt.make("A1"),
            generation: 1,
            nominalAtMs: START_MS,
            dueAtMs: START_MS,
            reservedAtMs: START_MS,
            kind: "relative",
            noop: false,
            originSequence: 0,
          },
          {
            occurrenceId: HeartbeatOccurrenceId.make("occ-a2"),
            threadId: ThreadId.make("thread-a"),
            publicId: HeartbeatJobId.make("aaaaaaab"),
            prompt: HeartbeatPrompt.make("A2"),
            generation: 1,
            nominalAtMs: START_MS,
            dueAtMs: START_MS,
            reservedAtMs: START_MS,
            kind: "relative",
            noop: false,
            originSequence: 0,
          },
          {
            occurrenceId: HeartbeatOccurrenceId.make("occ-b1"),
            threadId: ThreadId.make("thread-b"),
            publicId: HeartbeatJobId.make("bbbbbbbb"),
            prompt: HeartbeatPrompt.make("B1"),
            generation: 1,
            nominalAtMs: START_MS,
            dueAtMs: START_MS,
            reservedAtMs: START_MS,
            kind: "relative",
            noop: false,
            originSequence: 0,
          },
        ] as const;
        const emitted = yield* Ref.make(false);
        const scheduler = HeartbeatService.HeartbeatScheduler.of({
          schedulerSnapshot: Effect.succeed({
            cronJobs: [],
            relativeSlots: [],
            hasPendingReservations: false,
          }),
          reserveDue: () =>
            Ref.getAndSet(emitted, true).pipe(
              Effect.map((alreadyEmitted) => (alreadyEmitted ? [] : reservations)),
            ),
          subscribeChanges: PubSub.subscribe(changes),
          pruneExpired: Effect.void,
          validateOccurrence: () => Effect.succeed(true),
          withValidOccurrence: (_occurrenceId, admission) => admission.pipe(Effect.asSome),
          setOccurrenceOutcome: () => Effect.void,
          activateOnAcceptedTurn: () => Effect.void,
          cancelRelative: () => Effect.void,
          cancelThread: () => Effect.void,
        });
        const reactor = yield* HeartbeatReactor.make.pipe(
          Effect.provideService(HeartbeatService.HeartbeatScheduler, scheduler),
        );

        yield* reactor.start((occurrence) => {
          if (occurrence.occurrenceId === "occ-a1")
            return Deferred.await(gateA).pipe(Effect.as("complete" as const));
          if (occurrence.occurrenceId === "occ-a2")
            return Deferred.succeed(sawA2, undefined).pipe(Effect.as("complete" as const));
          return Deferred.succeed(sawB, undefined).pipe(Effect.as("complete" as const));
        });

        // B must run while A1 is held; A2 must remain behind A1.
        yield* Deferred.await(sawB);
        assert.strictEqual(yield* Deferred.isDone(sawA2), false);
        yield* Deferred.succeed(gateA, undefined);
        yield* Deferred.await(sawA2);
      }),
    ),
);

it.effect("releases all eight attempt permits so an idle ninth thread is admitted", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const changes = yield* PubSub.sliding<void>(1);
      const admittedNinth = yield* Deferred.make<void>();
      const emitted = yield* Ref.make(false);
      const reservations = Array.from({ length: 9 }, (_, index) => ({
        occurrenceId: HeartbeatOccurrenceId.make(`occ-${index + 1}`),
        threadId: ThreadId.make(`thread-${index + 1}`),
        publicId: HeartbeatJobId.make((index + 1).toString(16).padStart(8, "0")),
        prompt: HeartbeatPrompt.make(`prompt-${index + 1}`),
        generation: 1,
        nominalAtMs: START_MS,
        dueAtMs: START_MS,
        reservedAtMs: START_MS,
        kind: "relative" as const,
        noop: false,
        originSequence: 0,
      }));
      const scheduler = HeartbeatService.HeartbeatScheduler.of({
        schedulerSnapshot: Effect.succeed({
          cronJobs: [],
          relativeSlots: [],
          hasPendingReservations: false,
        }),
        reserveDue: () =>
          Ref.getAndSet(emitted, true).pipe(
            Effect.map((alreadyEmitted) => (alreadyEmitted ? [] : reservations)),
          ),
        subscribeChanges: PubSub.subscribe(changes),
        pruneExpired: Effect.void,
        validateOccurrence: () => Effect.succeed(true),
        withValidOccurrence: (_occurrenceId, admission) => admission.pipe(Effect.asSome),
        setOccurrenceOutcome: () => Effect.void,
        activateOnAcceptedTurn: () => Effect.void,
        cancelRelative: () => Effect.void,
        cancelThread: () => Effect.void,
      });
      const reactor = yield* HeartbeatReactor.make.pipe(
        Effect.provideService(HeartbeatService.HeartbeatScheduler, scheduler),
      );

      yield* reactor.start((next) =>
        next.threadId === "thread-9"
          ? Deferred.succeed(admittedNinth, undefined).pipe(Effect.as("complete" as const))
          : Effect.succeed("defer" as const),
      );

      // No clock adjustment is needed: the ninth attempt runs after the first
      // eight return "defer" and release their permits.
      yield* Deferred.await(admittedNinth);
    }),
  ),
);
