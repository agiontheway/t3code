import { assert, it } from "@effect/vitest";
import { ThreadId } from "@t3tools/contracts";
import * as NodeSqliteClient from "@t3tools/shared/nodeSqliteClient";
import * as Effect from "effect/Effect";
import * as SqlClient from "effect/sql/SqlClient";

import { runMigrations } from "../persistence/Migrations.ts";
import * as HeartbeatRepository from "./HeartbeatRepository.ts";

const THREAD_ID = ThreadId.make("heartbeat-thread");
const OTHER_THREAD_ID = ThreadId.make("other-thread");

const seedThread = (threadId: ThreadId, deletedAt: string | null = null) =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    yield* sql`
      INSERT INTO orchestration_v2_projection_threads (
        thread_id, project_id, title, default_provider, runtime_mode, interaction_mode,
        created_at, updated_at, deleted_at, payload_json
      ) VALUES (
        ${threadId}, 'heartbeat-project', 'Heartbeat', 'codex', 'full-access', 'default',
        '2026-09-17T10:00:00.000Z', '2026-09-17T10:00:00.000Z', ${deletedAt}, '{}'
      )
    `;
  });

const appendEvent = (threadId: ThreadId, eventId: string, applicationEventVersion: 1 | 2 = 2) =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    const rows = yield* sql<{ readonly sequence: number }>`
      INSERT INTO orchestration_events (
        event_id, aggregate_kind, stream_id, stream_version, event_type,
        occurred_at, command_id, actor_kind, payload_json, metadata_json,
        application_event_version
      ) VALUES (
        ${eventId}, 'thread', ${threadId},
        COALESCE((SELECT MAX(stream_version) + 1 FROM orchestration_events
          WHERE aggregate_kind = 'thread' AND stream_id = ${threadId}), 0),
        'thread.updated', '2026-09-17T10:00:00.000Z', ${`cmd-${eventId}`},
        'server', '{}', '{}', ${applicationEventVersion}
      )
      RETURNING sequence
    `;
    return rows[0]!.sequence;
  });

const recordReceipt = (
  threadId: ThreadId,
  commandId: string,
  commandType: string,
  resultSequence: number,
  status: "accepted" | "rejected" = "accepted",
) =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    yield* sql`
      INSERT INTO orchestration_command_receipts (
        command_id, aggregate_kind, aggregate_id, command_type, accepted_at,
        result_sequence, status, error
      ) VALUES (
        ${commandId}, 'thread', ${threadId}, ${commandType}, '2026-09-17T10:00:00.000Z',
        ${resultSequence}, ${status}, NULL
      )
    `;
  });

const withRepository = <A, E>(
  use: (
    repository: HeartbeatRepository.HeartbeatRepository["Service"],
  ) => Effect.Effect<A, E, SqlClient.SqlClient>,
) =>
  Effect.gen(function* () {
    yield* runMigrations();
    return yield* use(yield* HeartbeatRepository.make);
  }).pipe(Effect.provide(NodeSqliteClient.layer({ filename: ":memory:" })));

it.effect("reads thread existence from the v2 projection and skips deleted threads", () =>
  withRepository((repository) =>
    Effect.gen(function* () {
      yield* seedThread(THREAD_ID);
      yield* seedThread(OTHER_THREAD_ID, "2026-09-17T10:00:01.000Z");
      assert.isTrue(yield* repository.threadExists(THREAD_ID));
      assert.isFalse(yield* repository.threadExists(OTHER_THREAD_ID));
      assert.isFalse(yield* repository.threadExists(ThreadId.make("missing")));
    }),
  ),
);

it.effect("takes the thread head from its own v2 events only", () =>
  withRepository((repository) =>
    Effect.gen(function* () {
      assert.strictEqual(yield* repository.latestThreadSequence(THREAD_ID), 0);
      const own = yield* appendEvent(THREAD_ID, "evt-own");
      yield* appendEvent(OTHER_THREAD_ID, "evt-other");
      yield* appendEvent(THREAD_ID, "evt-legacy", 1);
      assert.strictEqual(yield* repository.latestThreadSequence(THREAD_ID), own);
    }),
  ),
);

/** An accepted command that wrote one event; its receipt records that event. */
const acceptWithEvent = (threadId: ThreadId, eventId: string, commandType: string) =>
  Effect.gen(function* () {
    const sequence = yield* appendEvent(threadId, eventId);
    yield* recordReceipt(threadId, `cmd-${eventId}`, commandType, sequence);
    return sequence;
  });

it.effect("cancels on accepted stop, archive and delete receipts only", () =>
  withRepository((repository) =>
    Effect.gen(function* () {
      assert.isUndefined(yield* repository.latestCancellationSequence(THREAD_ID));
      yield* acceptWithEvent(THREAD_ID, "interrupt", "run.interrupt");
      yield* acceptWithEvent(THREAD_ID, "detach", "provider-session.detach");
      yield* acceptWithEvent(THREAD_ID, "unarchive", "thread.unarchive");
      const rejectedAt = yield* appendEvent(THREAD_ID, "before-rejected-stop");
      yield* recordReceipt(THREAD_ID, "cmd-rejected-stop", "thread.stop", rejectedAt, "rejected");
      yield* acceptWithEvent(OTHER_THREAD_ID, "other-stop", "thread.stop");
      assert.isUndefined(yield* repository.latestCancellationSequence(THREAD_ID));

      const stop = yield* acceptWithEvent(THREAD_ID, "stop", "thread.stop");
      assert.strictEqual(yield* repository.latestCancellationSequence(THREAD_ID), stop);
      const archive = yield* acceptWithEvent(THREAD_ID, "archive", "thread.archive");
      assert.strictEqual(yield* repository.latestCancellationSequence(THREAD_ID), archive);
      const deleted = yield* acceptWithEvent(THREAD_ID, "delete", "thread.delete");
      assert.strictEqual(yield* repository.latestCancellationSequence(THREAD_ID), deleted);
    }),
  ),
);

it.effect("places an event-less stop one past the head it recorded", () =>
  withRepository((repository) =>
    Effect.gen(function* () {
      const head = yield* appendEvent(THREAD_ID, "turn-completed");
      // A Stop that ended nothing records the thread head and writes no event.
      yield* recordReceipt(THREAD_ID, "cmd-idle-stop", "thread.stop", head);
      assert.strictEqual(yield* repository.latestCancellationSequence(THREAD_ID), head + 1);
    }),
  ),
);
