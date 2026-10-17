import { assert, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as SqlClient from "effect/sql/SqlClient";
import * as NodeSqliteClient from "@t3tools/shared/nodeSqliteClient";

import HeartbeatJobs from "./002_HeartbeatJobs.ts";
import HeartbeatV2SequenceSpace from "./003_HeartbeatV2SequenceSpace.ts";

const insertJob = (threadId: string, originSequence: number) =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    yield* sql`INSERT INTO heartbeat_jobs (
      thread_id, public_id, prompt, cron_expression, timezone, recurring,
      created_at_ms, expires_at_ms, next_nominal_at_ms, next_due_at_ms, generation,
      origin_sequence
    ) VALUES (
      ${threadId}, 'deadbeef', 'check the build', '0 * * * *', 'Europe/Berlin', 0,
      1000, 604801000, 3600000, 3600010, 2, ${originSequence}
    )`;
  });

it.effect("moves every legacy job origin below any v2 cancellation", () =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    yield* HeartbeatJobs;
    yield* insertJob("thread-a", 500000);
    yield* insertJob("thread-b", 17);
    const otherColumns = sql`SELECT thread_id, public_id, prompt, cron_expression, timezone, recurring,
      created_at_ms, expires_at_ms, next_nominal_at_ms, next_due_at_ms, generation
      FROM heartbeat_jobs ORDER BY thread_id`;
    const before = yield* otherColumns;

    yield* HeartbeatV2SequenceSpace;

    assert.deepStrictEqual(
      yield* sql`SELECT thread_id, origin_sequence FROM heartbeat_jobs ORDER BY thread_id`,
      [
        { thread_id: "thread-a", origin_sequence: 0 },
        { thread_id: "thread-b", origin_sequence: 0 },
      ],
    );
    assert.deepStrictEqual(yield* otherColumns, before);
  }).pipe(Effect.provide(NodeSqliteClient.layer({ filename: ":memory:" }))),
);

it.effect("is a no-op on a database without jobs", () =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    yield* HeartbeatJobs;
    yield* HeartbeatV2SequenceSpace;
    assert.deepStrictEqual(yield* sql`SELECT * FROM heartbeat_jobs`, []);
  }).pipe(Effect.provide(NodeSqliteClient.layer({ filename: ":memory:" }))),
);
