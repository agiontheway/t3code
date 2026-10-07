import { assert, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as SqlClient from "effect/unstable/sql/SqlClient";
import * as NodeSqliteClient from "@t3tools/shared/nodeSqliteClient";

import HeartbeatJobs from "./002_HeartbeatJobs.ts";

it.effect("creates durable cron jobs and collision-safe occurrence identities", () =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    yield* HeartbeatJobs;

    yield* sql`INSERT INTO heartbeat_jobs (
      thread_id, public_id, prompt, cron_expression, timezone, recurring,
      created_at_ms, expires_at_ms, next_nominal_at_ms, next_due_at_ms, generation,
      origin_sequence
    ) VALUES (
      'thread-a', 'deadbeef', 'check the build', '0 * * * *', 'UTC', 1,
      1000, 604801000, 3600000, 3600010, 0, 17
    )`;

    // Public IDs are scoped to a thread rather than globally unique.
    yield* sql`INSERT INTO heartbeat_jobs (
      thread_id, public_id, prompt, cron_expression, timezone, recurring,
      created_at_ms, expires_at_ms, next_nominal_at_ms, next_due_at_ms, generation,
      origin_sequence
    ) VALUES (
      'thread-b', 'deadbeef', 'check the build', '0 * * * *', 'UTC', 1,
      1000, 604801000, 3600000, 3600010, 0, 18
    )`;

    assert.deepStrictEqual(
      yield* sql`SELECT thread_id, origin_sequence FROM heartbeat_jobs ORDER BY thread_id`,
      [
        { thread_id: "thread-a", origin_sequence: 17 },
        { thread_id: "thread-b", origin_sequence: 18 },
      ],
    );

    yield* sql`INSERT INTO heartbeat_occurrences (
      occurrence_id, thread_id, public_id, generation, nominal_at_ms, due_at_ms,
      status, created_at_ms, updated_at_ms
    ) VALUES (
      '00000000-0000-4000-8000-000000000001', 'thread-a', 'deadbeef', 1,
      3600000, 3600010, 'pending', 3600010, 3600010
    )`;

    const duplicatePending = yield* Effect.exit(sql`INSERT INTO heartbeat_occurrences (
      occurrence_id, thread_id, public_id, generation, nominal_at_ms, due_at_ms,
      status, created_at_ms, updated_at_ms
    ) VALUES (
      '00000000-0000-4000-8000-000000000002', 'thread-a', 'deadbeef', 2,
      7200000, 7200010, 'pending', 7200010, 7200010
    )`);
    assert.ok(Exit.isFailure(duplicatePending));

    yield* sql`UPDATE heartbeat_occurrences SET status = 'sent' WHERE occurrence_id = '00000000-0000-4000-8000-000000000001'`;
    yield* sql`INSERT INTO heartbeat_occurrences (
      occurrence_id, thread_id, public_id, generation, nominal_at_ms, due_at_ms,
      status, created_at_ms, updated_at_ms
    ) VALUES (
      '00000000-0000-4000-8000-000000000002', 'thread-a', 'deadbeef', 2,
      7200000, 7200010, 'pending', 7200010, 7200010
    )`;

    assert.deepStrictEqual(
      yield* sql`SELECT occurrence_id, generation, status FROM heartbeat_occurrences ORDER BY generation`,
      [
        {
          occurrence_id: "00000000-0000-4000-8000-000000000001",
          generation: 1,
          status: "sent",
        },
        {
          occurrence_id: "00000000-0000-4000-8000-000000000002",
          generation: 2,
          status: "pending",
        },
      ],
    );
  }).pipe(Effect.provide(NodeSqliteClient.layer({ filename: ":memory:" }))),
);

it.effect("can be applied directly without touching an official migration ledger", () =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    yield* sql`CREATE TABLE effect_sql_migrations (migration_id INTEGER PRIMARY KEY, name TEXT NOT NULL)`;
    yield* HeartbeatJobs;
    assert.deepStrictEqual(yield* sql`SELECT * FROM effect_sql_migrations`, []);
  }).pipe(Effect.provide(NodeSqliteClient.layer({ filename: ":memory:" }))),
);
