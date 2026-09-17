import {
  HeartbeatCronExpression,
  HeartbeatDelaySeconds,
  HeartbeatJobId,
  HeartbeatOccurrenceId,
  HeartbeatPrompt,
  HeartbeatTimeZone,
  ThreadId,
} from "@t3tools/contracts";
import { assert, it } from "@effect/vitest";
import * as Crypto from "effect/Crypto";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Ref from "effect/Ref";
import * as SqlClient from "effect/unstable/sql/SqlClient";
import { TestClock } from "effect/testing";
import * as NodeSqliteClient from "@t3tools/shared/nodeSqliteClient";

import { runMigrations } from "../persistence/Migrations.ts";
import * as HeartbeatRepository from "./HeartbeatRepository.ts";
import * as HeartbeatService from "./HeartbeatService.ts";

const THREAD_ID = ThreadId.make("heartbeat-thread");
const START_MS = Date.parse("2026-09-17T10:00:10.000Z");

const makeCrypto = () => {
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
};

const seedThread = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  yield* sql`
    INSERT INTO projection_threads (
      thread_id, project_id, title, model_selection_json, created_at, updated_at
    ) VALUES (
      ${THREAD_ID}, 'heartbeat-project', 'Heartbeat',
      '{"instanceId":"codex","model":"gpt-5"}',
      '2026-09-17T10:00:00.000Z', '2026-09-17T10:00:00.000Z'
    )
  `;
});

// Plain test fixture serialization; kept outside Effect code so the JSON
// global stays out of effect generators.
const serializePayload = (payload: Readonly<Record<string, unknown>>): string =>
  JSON.stringify(payload);

const appendThreadEvent = (
  eventId: string,
  eventType: string,
  payload: Readonly<Record<string, unknown>> = {},
) =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    const rows = yield* sql<{ readonly sequence: number }>`
      INSERT INTO orchestration_events (
        event_id, aggregate_kind, stream_id, stream_version, event_type,
        occurred_at, command_id, actor_kind, payload_json, metadata_json
      ) VALUES (
        ${eventId}, 'thread', ${THREAD_ID},
        COALESCE((SELECT MAX(stream_version) + 1 FROM orchestration_events
          WHERE aggregate_kind = 'thread' AND stream_id = ${THREAD_ID}), 0),
        ${eventType}, '2026-09-17T10:00:00.000Z', ${`cmd-${eventId}`},
        'server', ${serializePayload(payload)}, '{}'
      )
      RETURNING sequence
    `;
    return rows[0]!.sequence;
  });

const withService = <A, E>(
  use: (
    service: HeartbeatService.HeartbeatServiceImplementation,
    repository: HeartbeatRepository.HeartbeatRepository["Service"],
  ) => Effect.Effect<A, E, SqlClient.SqlClient>,
  crypto: Crypto.Crypto = makeCrypto(),
) =>
  Effect.gen(function* () {
    yield* TestClock.setTime(START_MS);
    yield* runMigrations();
    yield* seedThread;
    const repository = yield* HeartbeatRepository.make;
    const service = yield* HeartbeatService.make.pipe(
      Effect.provideService(HeartbeatRepository.HeartbeatRepository, repository),
      Effect.provideService(Crypto.Crypto, crypto),
    );
    return yield* use(service, repository);
  }).pipe(Effect.provide(NodeSqliteClient.layerMemory()));

const recurringInput = {
  cron: HeartbeatCronExpression.make("0 * * * *"),
  prompt: HeartbeatPrompt.make("check the build"),
  recurring: true,
  timezone: HeartbeatTimeZone.make("UTC"),
} as const;

it.effect("enforces the per-thread cron cap with collision-checked public IDs", () =>
  withService((service) =>
    Effect.gen(function* () {
      for (let index = 0; index < 50; index += 1) {
        const job = yield* service.create(THREAD_ID, recurringInput);
        assert.match(job.id, /^[0-9a-f]{8}$/);
      }
      const failure = yield* Effect.exit(service.create(THREAD_ID, recurringInput));
      assert.ok(Exit.isFailure(failure));
      assert.strictEqual((yield* service.list(THREAD_ID)).jobs.length, 50);
    }),
  ),
);

it.effect("counts fifty pending one-shots toward the logical cap until delete or finish", () =>
  withService((service) =>
    Effect.gen(function* () {
      const oneShot = {
        ...recurringInput,
        recurring: false,
        cron: HeartbeatCronExpression.make("1 10 * * *"),
      } as const;
      const jobs = yield* Effect.forEach(
        Array.from({ length: 50 }),
        () => service.create(THREAD_ID, oneShot),
        { concurrency: 1 },
      );
      yield* service.activateOnAcceptedTurn(THREAD_ID);
      const pending = yield* service.reserveDue(Date.parse(jobs[0]!.nextFireAt));
      assert.strictEqual(pending.length, 50);
      assert.strictEqual((yield* service.list(THREAD_ID)).jobs.length, 50);

      assert.ok(Exit.isFailure(yield* Effect.exit(service.create(THREAD_ID, oneShot))));

      yield* service.delete(THREAD_ID, { id: pending[0]!.publicId });
      yield* service.create(THREAD_ID, oneShot);
      assert.ok(Exit.isFailure(yield* Effect.exit(service.create(THREAD_ID, oneShot))));

      yield* service.setOccurrenceOutcome(pending[1]!.occurrenceId, { status: "sent" });
      yield* service.create(THREAD_ID, oneShot);
      assert.strictEqual((yield* service.list(THREAD_ID)).jobs.length, 50);
    }),
  ),
);

it.effect("sequence cutoffs preserve jobs created after delayed interrupt and stop events", () =>
  withService((service) =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      const appendLifecycleEvent = (eventId: string, eventType: string) => sql`
        INSERT INTO orchestration_events (
          event_id, aggregate_kind, stream_id, stream_version, event_type,
          occurred_at, command_id, actor_kind, payload_json, metadata_json
        ) VALUES (
          ${eventId}, 'thread', ${THREAD_ID},
          COALESCE((SELECT MAX(stream_version) + 1 FROM orchestration_events
            WHERE aggregate_kind = 'thread' AND stream_id = ${THREAD_ID}), 1),
          ${eventType}, '2026-09-17T10:00:00.000Z', ${`cmd-${eventId}`},
          'server', '{}', '{}'
        )
      `;

      yield* appendLifecycleEvent("evt-old-interrupt", "thread.turn-interrupt-requested");
      const relative = yield* service.wakeup(THREAD_ID, {
        type: "arm",
        delaySeconds: HeartbeatDelaySeconds.make(60),
        prompt: HeartbeatPrompt.make("new relative"),
        noop: false,
      });
      if (relative.action !== "armed") assert.fail("expected relative arm");
      yield* service.cancelRelative(THREAD_ID, "delayed interrupt", 1);
      assert.strictEqual((yield* service.list(THREAD_ID)).jobs[0]?.id, relative.job.id);

      yield* appendLifecycleEvent("evt-old-stop", "thread.session-stop-requested");
      const cron = yield* service.create(THREAD_ID, recurringInput);
      yield* service.cancelThread(THREAD_ID, "delayed stop", 2);
      const listed = (yield* service.list(THREAD_ID)).jobs;
      assert.ok(!listed.some((job) => job.id === relative.job.id));
      assert.ok(listed.some((job) => job.id === cron.id));
    }),
  ),
);

it.effect("atomically replaces and stops the single process-local relative slot", () =>
  withService((service) =>
    Effect.gen(function* () {
      const prompt = HeartbeatPrompt.make("look again");
      const [first, second] = yield* Effect.all(
        [
          service.wakeup(THREAD_ID, {
            type: "arm",
            delaySeconds: HeartbeatDelaySeconds.make(1),
            prompt,
            noop: false,
          }),
          service.wakeup(THREAD_ID, {
            type: "arm",
            delaySeconds: HeartbeatDelaySeconds.make(2),
            prompt,
            reason: "replacement",
            noop: true,
          }),
        ],
        { concurrency: "unbounded" },
      );
      assert.strictEqual(first.action, "armed");
      assert.strictEqual(second.action, "armed");
      assert.strictEqual((yield* service.list(THREAD_ID)).jobs.length, 1);

      const stopped = yield* service.wakeup(THREAD_ID, { type: "stop" });
      assert.deepStrictEqual(stopped, {
        action: "stopped",
        stopped: true,
        cronJobsUnaffected: true,
      });
      assert.deepStrictEqual((yield* service.list(THREAD_ID)).jobs, []);
    }),
  ),
);

it.effect("invalidates a reserved relative occurrence when stop wins the admission race", () =>
  withService((service) =>
    Effect.gen(function* () {
      const armed = yield* service.wakeup(THREAD_ID, {
        type: "arm",
        delaySeconds: HeartbeatDelaySeconds.make(1),
        prompt: HeartbeatPrompt.make("race me"),
        noop: false,
      });
      if (armed.action !== "armed") assert.fail("expected relative wakeup to arm");
      yield* service.activateOnAcceptedTurn(THREAD_ID);
      const occurrence = (yield* service.reserveDue(Date.parse(armed.job.dueAt)))[0]!;
      assert.strictEqual((yield* service.list(THREAD_ID)).jobs[0]?.status, "pending");

      const stopped = yield* service.wakeup(THREAD_ID, { type: "stop" });
      if (stopped.action !== "stopped") assert.fail("expected relative wakeup to stop");
      assert.ok(stopped.stopped);
      assert.strictEqual(yield* service.validateOccurrence(occurrence.occurrenceId), false);
      const called = yield* Ref.make(false);
      const admission = yield* service.withValidOccurrence(
        occurrence.occurrenceId,
        Ref.set(called, true),
      );
      assert.strictEqual(admission._tag, "None");
      assert.strictEqual(yield* Ref.get(called), false);
      assert.deepStrictEqual((yield* service.list(THREAD_ID)).jobs, []);
    }),
  ),
);

it.effect("keeps cron and relative display IDs unambiguous across deterministic collisions", () => {
  const ids = ["deadbeef", "deadbeef", "cafebabe"];
  let index = 0;
  const base = makeCrypto();
  const collisionCrypto = {
    ...base,
    randomUUIDv4: Effect.sync(
      () => `${ids[Math.min(index++, ids.length - 1)]}-0000-4000-8000-000000000000`,
    ),
  } satisfies Crypto.Crypto;
  return withService(
    (service) =>
      Effect.gen(function* () {
        const relative = yield* service.wakeup(THREAD_ID, {
          type: "arm",
          delaySeconds: HeartbeatDelaySeconds.make(1),
          prompt: HeartbeatPrompt.make("relative"),
          noop: false,
        });
        if (relative.action !== "armed") assert.fail("expected arm");
        assert.strictEqual(relative.job.id, "deadbeef");
        const cron = yield* service.create(THREAD_ID, recurringInput);
        assert.strictEqual(cron.id, "cafebabe");
      }),
    collisionCrypto,
  );
});

it.effect("preserves the prior relative arm when replacement ID allocation fails", () => {
  const base = makeCrypto();
  const fixedCrypto = {
    ...base,
    randomUUIDv4: Effect.succeed("deadbeef-0000-4000-8000-000000000000"),
  } satisfies Crypto.Crypto;
  return withService(
    (service) =>
      Effect.gen(function* () {
        const first = yield* service.wakeup(THREAD_ID, {
          type: "arm",
          delaySeconds: HeartbeatDelaySeconds.make(1),
          prompt: HeartbeatPrompt.make("keep me"),
          noop: false,
        });
        if (first.action !== "armed") assert.fail("expected arm");
        const failed = yield* Effect.exit(
          service.wakeup(THREAD_ID, {
            type: "arm",
            delaySeconds: HeartbeatDelaySeconds.make(2),
            prompt: HeartbeatPrompt.make("must not replace"),
            noop: false,
          }),
        );
        assert.ok(Exit.isFailure(failed));
        const listed = (yield* service.list(THREAD_ID)).jobs;
        assert.strictEqual(listed.length, 1);
        assert.strictEqual(listed[0]?.prompt, "keep me");
        assert.strictEqual(listed[0]?.id, first.job.id);
      }),
    fixedCrypto,
  );
});

it.effect("makes a pending cron occurrence invalid when explicit deletion wins", () =>
  withService((service, repository) =>
    Effect.gen(function* () {
      const created = yield* service.create(THREAD_ID, {
        ...recurringInput,
        recurring: false,
        cron: HeartbeatCronExpression.make("1 10 * * *"),
      });
      yield* service.activateOnAcceptedTurn(THREAD_ID);
      const occurrence = (yield* service.reserveDue(Date.parse(created.nextFireAt)))[0]!;

      assert.deepStrictEqual(yield* service.delete(THREAD_ID, { id: created.id }), {
        id: created.id,
        deleted: true,
      });
      assert.strictEqual(yield* service.validateOccurrence(occurrence.occurrenceId), false);
      assert.strictEqual(yield* repository.occurrenceStatus(occurrence.occurrenceId), "canceled");

      // A stale admission completion cannot resurrect the canceled occurrence.
      yield* service.setOccurrenceOutcome(occurrence.occurrenceId, { status: "sent" });
      assert.strictEqual(yield* repository.occurrenceStatus(occurrence.occurrenceId), "canceled");
    }),
  ),
);

it.effect("does not fire immediately and consumes a one-shot exactly once", () =>
  withService((service, repository) =>
    Effect.gen(function* () {
      const created = yield* service.create(THREAD_ID, {
        ...recurringInput,
        recurring: false,
        cron: HeartbeatCronExpression.make("1 10 * * *"),
      });
      yield* service.activateOnAcceptedTurn(THREAD_ID);
      assert.deepStrictEqual(yield* service.reserveDue(START_MS), []);

      const dueMs = Date.parse(created.nextFireAt);
      const occurrences = yield* service.reserveDue(dueMs);
      assert.strictEqual(occurrences.length, 1);
      assert.strictEqual(occurrences[0]?.publicId, created.id);
      assert.deepStrictEqual(yield* service.reserveDue(dueMs), []);
      assert.strictEqual((yield* service.list(THREAD_ID)).jobs[0]?.status, "pending");
      yield* service.setOccurrenceOutcome(occurrences[0]!.occurrenceId, { status: "sent" });
      assert.deepStrictEqual((yield* service.list(THREAD_ID)).jobs, []);
      assert.strictEqual(
        yield* repository.create({
          threadId: THREAD_ID,
          publicId: created.id,
          prompt: created.prompt,
          cronExpression: created.cron,
          timezone: created.timezone,
          recurring: false,
          createdAtMs: START_MS,
          expiresAtMs: dueMs,
          nextNominalAtMs: Date.parse(created.nextNominalAt),
          nextDueAtMs: dueMs,
          generation: 0,
          originSequence: 0,
        }),
        "collision",
      );
    }),
  ),
);

it.effect("orders simultaneous jobs deterministically and coalesces overdue recurrence", () =>
  withService((service) =>
    Effect.gen(function* () {
      const oneShot = {
        ...recurringInput,
        recurring: false,
        cron: HeartbeatCronExpression.make("1 10 * * *"),
      } as const;
      const first = yield* service.create(THREAD_ID, oneShot);
      const second = yield* service.create(THREAD_ID, oneShot);
      yield* service.activateOnAcceptedTurn(THREAD_ID);
      const simultaneous = yield* service.reserveDue(Date.parse(first.nextFireAt));
      assert.deepStrictEqual(
        simultaneous.map((occurrence) => occurrence.publicId),
        [first.id, second.id].sort(),
      );

      const recurring = yield* service.create(THREAD_ID, {
        ...recurringInput,
        cron: HeartbeatCronExpression.make("* * * * *"),
      });
      const overdueAt = Date.parse(recurring.nextFireAt) + 5 * 60_000;
      const coalesced = yield* service.reserveDue(overdueAt);
      assert.strictEqual(coalesced.length, 1);
      assert.strictEqual(coalesced[0]?.publicId, recurring.id);
      assert.deepStrictEqual(yield* service.reserveDue(overdueAt), []);
      const listed = (yield* service.list(THREAD_ID)).jobs;
      assert.strictEqual(listed.find((job) => job.id === recurring.id)?.status, "pending");
    }),
  ),
);

it.effect("keeps the final recurring fire inside seven days and then removes the job", () =>
  withService((service) =>
    Effect.gen(function* () {
      const created = yield* service.create(THREAD_ID, {
        ...recurringInput,
        cron: HeartbeatCronExpression.make("0 10 * * *"),
      });
      yield* service.activateOnAcceptedTurn(THREAD_ID);

      let occurrenceCount = 0;
      while ((yield* service.list(THREAD_ID)).jobs.length > 0) {
        const job = (yield* service.list(THREAD_ID)).jobs[0];
        assert.ok(job?.kind === "cron");
        const due = Date.parse(job.nextFireAt);
        const reserved = yield* service.reserveDue(due);
        assert.strictEqual(reserved.length, 1);
        occurrenceCount += 1;
        yield* service.setOccurrenceOutcome(reserved[0]!.occurrenceId, { status: "sent" });
      }

      // Seven nominal daily occurrences plus one final idle transition exactly
      // at the seven-day expiry boundary.
      assert.strictEqual(occurrenceCount, 8);
      assert.ok(Date.parse(created.expiresAt!) <= START_MS + 7 * 24 * 60 * 60 * 1_000);
    }),
  ),
);

it.effect("gives a sparse recurring job one final expiry-boundary transition", () =>
  withService((service) =>
    Effect.gen(function* () {
      const created = yield* service.create(THREAD_ID, {
        ...recurringInput,
        cron: HeartbeatCronExpression.make("0 0 1 * *"),
      });
      yield* service.activateOnAcceptedTurn(THREAD_ID);

      assert.strictEqual(Date.parse(created.nextFireAt), Date.parse(created.expiresAt!));
      const reserved = yield* service.reserveDue(Date.parse(created.expiresAt!));
      assert.strictEqual(reserved.length, 1);
      assert.deepStrictEqual(yield* service.reserveDue(Date.parse(created.expiresAt!)), []);
      yield* service.setOccurrenceOutcome(reserved[0]!.occurrenceId, { status: "sent" });
      assert.deepStrictEqual((yield* service.list(THREAD_ID)).jobs, []);
    }),
  ),
);

it.effect(
  "restores future cron jobs dormant, advances recurring jobs, and drops overdue one-shots",
  () =>
    Effect.gen(function* () {
      yield* TestClock.setTime(START_MS);
      yield* runMigrations();
      yield* seedThread;
      const repository = yield* HeartbeatRepository.make;
      const prompt = HeartbeatPrompt.make("restore me");
      yield* repository.create({
        threadId: THREAD_ID,
        publicId: HeartbeatJobId.make("00000001"),
        prompt,
        cronExpression: "0 * * * *",
        timezone: "UTC",
        recurring: true,
        createdAtMs: START_MS - 60_000,
        expiresAtMs: START_MS + 7 * 24 * 60 * 60 * 1_000,
        nextNominalAtMs: START_MS - 30_000,
        nextDueAtMs: START_MS - 30_000,
        generation: 0,
        originSequence: 0,
      });
      yield* repository.create({
        threadId: THREAD_ID,
        publicId: HeartbeatJobId.make("00000002"),
        prompt,
        cronExpression: "0 * * * *",
        timezone: "UTC",
        recurring: false,
        createdAtMs: START_MS - 120_000,
        expiresAtMs: START_MS - 60_000,
        nextNominalAtMs: START_MS - 60_000,
        nextDueAtMs: START_MS - 60_000,
        generation: 0,
        originSequence: 0,
      });

      const service = yield* HeartbeatService.make.pipe(
        Effect.provideService(HeartbeatRepository.HeartbeatRepository, repository),
        Effect.provideService(Crypto.Crypto, makeCrypto()),
      );
      const jobs = (yield* service.list(THREAD_ID)).jobs;
      assert.strictEqual(jobs.length, 1);
      assert.strictEqual(jobs[0]?.status, "dormant");
      assert.ok(jobs[0]?.kind === "cron" && Date.parse(jobs[0].nextNominalAt) > START_MS);
      assert.deepStrictEqual(yield* service.reserveDue(START_MS + 24 * 60 * 60 * 1_000), []);
      yield* TestClock.adjust("8 days");
      yield* service.activateOnAcceptedTurn(THREAD_ID);
      assert.deepStrictEqual((yield* service.list(THREAD_ID)).jobs, []);
    }).pipe(Effect.provide(NodeSqliteClient.layerMemory())),
);

it.effect("prunes a pre-stop cron on restart while retaining and running a post-stop cron", () =>
  withService((service, repository) =>
    Effect.gen(function* () {
      const oneShot = {
        ...recurringInput,
        recurring: false,
        cron: HeartbeatCronExpression.make("1 11 * * *"),
      } as const;
      const stoppedJob = yield* service.create(THREAD_ID, oneShot);
      const stopSequence = yield* appendThreadEvent(
        "evt-crash-stop",
        "thread.session-stop-requested",
        {
          threadId: THREAD_ID,
          createdAt: "2026-09-17T10:00:00.000Z",
        },
      );
      const survivor = yield* service.create(THREAD_ID, oneShot);

      assert.deepStrictEqual(
        (yield* repository.listByThread(THREAD_ID)).map((job) => [
          job.publicId,
          job.originSequence,
        ]),
        [
          [stoppedJob.id, 0],
          [survivor.id, stopSequence],
        ],
      );

      // Simulate a crash before the lifecycle subscriber observes the stop:
      // construct a fresh scheduler over the same durable database.
      const restored = yield* HeartbeatService.make.pipe(
        Effect.provideService(HeartbeatRepository.HeartbeatRepository, repository),
        Effect.provideService(Crypto.Crypto, makeCrypto()),
      );
      const restoredJobs = (yield* restored.list(THREAD_ID)).jobs;
      assert.deepStrictEqual(
        restoredJobs.map((job) => job.id),
        [survivor.id],
      );
      assert.strictEqual(restoredJobs[0]?.status, "dormant");

      yield* restored.activateOnAcceptedTurn(THREAD_ID);
      const due = yield* restored.reserveDue(Date.parse(survivor.nextFireAt));
      assert.deepStrictEqual(
        due.map((occurrence) => occurrence.publicId),
        [survivor.id],
      );
    }),
  ),
);

it.effect("does not revive an archived cron after restore and later unarchive", () =>
  withService((service, repository) =>
    Effect.gen(function* () {
      const job = yield* service.create(THREAD_ID, {
        ...recurringInput,
        recurring: false,
        cron: HeartbeatCronExpression.make("1 11 * * *"),
      });
      yield* appendThreadEvent("evt-crash-archive", "thread.archived", {
        threadId: THREAD_ID,
        archivedAt: "2026-09-17T10:00:00.000Z",
        updatedAt: "2026-09-17T10:00:00.000Z",
      });

      const restoredArchived = yield* HeartbeatService.make.pipe(
        Effect.provideService(HeartbeatRepository.HeartbeatRepository, repository),
        Effect.provideService(Crypto.Crypto, makeCrypto()),
      );
      assert.deepStrictEqual((yield* restoredArchived.list(THREAD_ID)).jobs, []);

      yield* appendThreadEvent("evt-crash-unarchive", "thread.unarchived", {
        threadId: THREAD_ID,
        updatedAt: "2026-09-17T10:00:01.000Z",
      });
      const restoredUnarchived = yield* HeartbeatService.make.pipe(
        Effect.provideService(HeartbeatRepository.HeartbeatRepository, repository),
        Effect.provideService(Crypto.Crypto, makeCrypto()),
      );
      yield* restoredUnarchived.activateOnAcceptedTurn(THREAD_ID);
      assert.deepStrictEqual((yield* restoredUnarchived.list(THREAD_ID)).jobs, []);
      assert.deepStrictEqual(yield* restoredUnarchived.reserveDue(Date.parse(job.nextFireAt)), []);

      const postArchiveJob = yield* restoredUnarchived.create(THREAD_ID, {
        ...recurringInput,
        recurring: false,
        cron: HeartbeatCronExpression.make("2 11 * * *"),
      });
      yield* appendThreadEvent("evt-crash-delete", "thread.deleted", {
        threadId: THREAD_ID,
        deletedAt: "2026-09-17T10:00:02.000Z",
      });
      const restoredDeleted = yield* HeartbeatService.make.pipe(
        Effect.provideService(HeartbeatRepository.HeartbeatRepository, repository),
        Effect.provideService(Crypto.Crypto, makeCrypto()),
      );
      assert.deepStrictEqual((yield* restoredDeleted.list(THREAD_ID)).jobs, []);
      yield* restoredDeleted.activateOnAcceptedTurn(THREAD_ID);
      assert.deepStrictEqual(
        yield* restoredDeleted.reserveDue(Date.parse(postArchiveJob.nextFireAt)),
        [],
      );
    }),
  ),
);

it.effect("terminalizes an unresolved cron occurrence on restart without resending it", () =>
  Effect.gen(function* () {
    yield* TestClock.setTime(START_MS);
    yield* runMigrations();
    yield* seedThread;
    const repository = yield* HeartbeatRepository.make;
    const publicId = HeartbeatJobId.make("feedface");
    const occurrenceId = HeartbeatOccurrenceId.make("00000000-0000-4000-8000-000000000123");
    const stored = {
      threadId: THREAD_ID,
      publicId,
      prompt: HeartbeatPrompt.make("recover conservatively"),
      cronExpression: "* * * * *",
      timezone: "UTC",
      recurring: true,
      createdAtMs: START_MS - 60_000,
      expiresAtMs: START_MS + 7 * 24 * 60 * 60 * 1_000,
      nextNominalAtMs: START_MS,
      nextDueAtMs: START_MS,
      generation: 0,
      originSequence: 0,
    } as const;
    assert.strictEqual(yield* repository.create(stored), "created");
    assert.ok(
      yield* repository.reserve({
        job: stored,
        occurrenceId,
        nowMs: START_MS,
        nextNominalAtMs: START_MS + 60_000,
        nextDueAtMs: START_MS + 60_000,
      }),
    );

    const restored = yield* HeartbeatService.make.pipe(
      Effect.provideService(HeartbeatRepository.HeartbeatRepository, repository),
      Effect.provideService(Crypto.Crypto, makeCrypto()),
    );

    assert.strictEqual(yield* repository.occurrenceStatus(occurrenceId), "expired");
    assert.strictEqual((yield* restored.list(THREAD_ID)).jobs[0]?.status, "dormant");
    yield* restored.activateOnAcceptedTurn(THREAD_ID);
    assert.deepStrictEqual(yield* restored.reserveDue(START_MS), []);
  }).pipe(Effect.provide(NodeSqliteClient.layerMemory())),
);

it.effect(
  "pruning settled occurrences preserves jobs while explicit thread cancellation deletes them",
  () =>
    withService((service) =>
      Effect.gen(function* () {
        yield* service.create(THREAD_ID, recurringInput);
        yield* service.wakeup(THREAD_ID, {
          type: "arm",
          delaySeconds: HeartbeatDelaySeconds.make(10),
          prompt: HeartbeatPrompt.make("temporary"),
          noop: false,
        });
        yield* TestClock.adjust("8 days");
        yield* service.pruneExpired;
        assert.strictEqual((yield* service.list(THREAD_ID)).jobs.length, 2);

        yield* service.cancelRelative(THREAD_ID, "settled");
        assert.strictEqual((yield* service.list(THREAD_ID)).jobs.length, 1);
        yield* service.cancelThread(THREAD_ID, "archived");
        assert.deepStrictEqual((yield* service.list(THREAD_ID)).jobs, []);
      }),
    ),
);
