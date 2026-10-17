import { assert, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Migrator from "effect/sql/Migrator";
import * as SqlClient from "effect/sql/SqlClient";
import * as NodeSqliteClient from "@t3tools/shared/nodeSqliteClient";

import { migrationManifest, runMigrations } from "./Migrations.ts";
import ProjectionThreadsSpawn from "./ForkMigrations/001_ProjectionThreadsSpawn.ts";
import HeartbeatJobs from "./ForkMigrations/002_HeartbeatJobs.ts";

const spawn =
  '{"parentThreadId":"parent","taskId":"xp-agent:child","depth":1,"allowOrchestration":true}';
const linked =
  '{"projectId":"project","repository":"pingdotgg/t3code","number":42,"url":"https://github.com/pingdotgg/t3code/pull/42"}';

const seedThread = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  yield* sql`INSERT INTO projection_threads (thread_id, project_id, title, model_selection_json, linked_pull_request_json, created_at, updated_at)
    VALUES ('child', 'project', 'Child', '{"instanceId":"codex","model":"gpt-5.4"}', ${linked}, '2026-09-01T00:00:00.000Z', '2026-09-01T00:00:00.000Z')`;
});

const seedFork50 = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  yield* runMigrations({ toMigrationInclusive: 49 });
  yield* ProjectionThreadsSpawn;
  yield* sql`INSERT INTO effect_sql_migrations (migration_id, name) VALUES (50, 'ProjectionThreadsSpawn')`;
  yield* seedThread;
  yield* sql`UPDATE projection_threads SET spawn_json = ${spawn} WHERE thread_id = 'child'`;
});

const assertCurrentSchema = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  const upstream =
    yield* sql`SELECT migration_id, name FROM effect_sql_migrations WHERE migration_id >= 50 ORDER BY migration_id`;
  assert.deepStrictEqual(upstream, [
    { migration_id: 50, name: "ProjectionThreadPullRequests" },
    { migration_id: 51, name: "ProjectionThreadMessageContext" },
    { migration_id: 52, name: "ProjectionThreadTitleState" },
    { migration_id: 53, name: "PullRequestFilesViewed" },
    { migration_id: 54, name: "ProjectionThreadsAutoSettleDisabledAt" },
    { migration_id: 55, name: "OrchestrationV2" },
    { migration_id: 56, name: "RemoveRedundantProjectionIndexes" },
    { migration_id: 57, name: "ScheduledTaskWebhooks" },
    { migration_id: 58, name: "WebhookRelayDeliveries" },
    { migration_id: 59, name: "McpAppModelContext" },
    { migration_id: 60, name: "ThreadSnapshotWindowIndexes" },
    { migration_id: 61, name: "PullRequestReviewGuides" },
    { migration_id: 62, name: "ThreadFailureLookupIndex" },
  ]);
  assert.deepStrictEqual(
    yield* sql`SELECT migration_id, name FROM t3_fork_migrations ORDER BY migration_id`,
    [
      { migration_id: 1, name: "ProjectionThreadsSpawn" },
      { migration_id: 2, name: "HeartbeatJobs" },
      { migration_id: 3, name: "HeartbeatV2SequenceSpace" },
      { migration_id: 4, name: "DelegationGrants" },
      { migration_id: 5, name: "AliasOwnedChildren" },
    ],
  );
  assert.deepStrictEqual(
    yield* sql`SELECT name FROM sqlite_master WHERE type = 'table' AND name IN ('heartbeat_jobs', 'heartbeat_occurrences', 'fork_delegation_grants', 'fork_alias_owned_children') ORDER BY name`,
    [
      { name: "fork_alias_owned_children" },
      { name: "fork_delegation_grants" },
      { name: "heartbeat_jobs" },
      { name: "heartbeat_occurrences" },
    ],
  );
  const threads = yield* sql<{ readonly name: string }>`PRAGMA table_info(projection_threads)`;
  assert.ok(threads.some((column) => column.name === "spawn_json"));
  assert.ok(threads.some((column) => column.name === "title_state_json"));
  assert.ok(threads.some((column) => column.name === "auto_settle_disabled_at"));
  const messages = yield* sql<{
    readonly name: string;
  }>`PRAGMA table_info(projection_thread_messages)`;
  assert.ok(messages.some((column) => column.name === "context_json"));
});

it.effect("upgrades legacy fork 50 without losing spawn ownership or upstream PR backfill", () =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    yield* seedFork50;
    yield* runMigrations();
    yield* assertCurrentSchema;
    assert.deepStrictEqual(
      yield* sql`SELECT spawn_json, linked_pull_request_json FROM projection_threads WHERE thread_id = 'child'`,
      [{ spawn_json: spawn, linked_pull_request_json: linked }],
    );
    const links =
      yield* sql`SELECT thread_id, host, repository, number, source FROM projection_thread_pull_requests`;
    assert.deepStrictEqual(links, [
      {
        thread_id: "child",
        host: "github.com",
        repository: "pingdotgg/t3code",
        number: 42,
        source: "manual",
      },
    ]);
    assert.deepStrictEqual(yield* runMigrations(), []);
    assert.deepStrictEqual(
      yield* sql`SELECT thread_id, host, repository, number, source FROM projection_thread_pull_requests`,
      links,
    );
    yield* assertCurrentSchema;
  }).pipe(Effect.provide(NodeSqliteClient.layer({ filename: ":memory:" }))),
);

it.effect("adds the fork schema to official 52 without changing existing PR data", () =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    yield* runMigrations({ toMigrationInclusive: 52 });
    yield* seedThread;
    yield* sql`INSERT INTO projection_thread_pull_requests (thread_id, host, repository, number, url, source, linked_at, snapshot_json, stack_json)
      VALUES ('child', 'github.com', 'pingdotgg/t3code', 42, 'https://github.com/pingdotgg/t3code/pull/42', 'stack-dismissed', '2026-09-02', '{"kept":true}', '{"stack":"kept"}')`;
    const before = yield* sql`SELECT * FROM projection_thread_pull_requests`;
    yield* runMigrations();
    yield* runMigrations();
    yield* assertCurrentSchema;
    assert.deepStrictEqual(yield* sql`SELECT * FROM projection_thread_pull_requests`, before);
    assert.deepStrictEqual(yield* sql`SELECT spawn_json FROM projection_threads`, [
      { spawn_json: null },
    ]);
  }).pipe(Effect.provide(NodeSqliteClient.layer({ filename: ":memory:" }))),
);

it.effect("initializes a fresh database and reruns both ledgers idempotently", () =>
  Effect.gen(function* () {
    yield* runMigrations();
    yield* assertCurrentSchema;
    assert.deepStrictEqual(yield* runMigrations(), []);
    yield* assertCurrentSchema;
  }).pipe(Effect.provide(NodeSqliteClient.layer({ filename: ":memory:" }))),
);

it.effect("commits the legacy repair on its own when a later upstream migration fails", () =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    yield* seedFork50;
    yield* sql`DROP TABLE projection_thread_messages`;
    yield* sql`CREATE VIEW projection_thread_messages AS SELECT 1 AS id`;
    assert.ok(Exit.isFailure(yield* Effect.exit(runMigrations())));
    // The repaired 50 is a consistent upstream history; the failed upstream run rolled back alone.
    assert.deepStrictEqual(
      yield* sql`SELECT migration_id, name FROM effect_sql_migrations WHERE migration_id >= 50`,
      [{ migration_id: 50, name: "ProjectionThreadPullRequests" }],
    );
    const links =
      yield* sql`SELECT thread_id, repository, number FROM projection_thread_pull_requests`;
    assert.deepStrictEqual(links, [
      { thread_id: "child", repository: "pingdotgg/t3code", number: 42 },
    ]);
    assert.deepStrictEqual(yield* sql`SELECT spawn_json FROM projection_threads`, [
      { spawn_json: spawn },
    ]);
    assert.deepStrictEqual(
      yield* sql`SELECT name FROM sqlite_master WHERE name = 't3_fork_migrations'`,
      [],
    );
    // A retry does not repeat the repair and fails at the same upstream step.
    assert.ok(Exit.isFailure(yield* Effect.exit(runMigrations())));
    assert.deepStrictEqual(
      yield* sql`SELECT thread_id, repository, number FROM projection_thread_pull_requests`,
      links,
    );
  }).pipe(Effect.provide(NodeSqliteClient.layer({ filename: ":memory:" }))),
);

it.effect("rolls back a legacy repair that fails part way", () =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    yield* seedFork50;
    yield* sql`CREATE TRIGGER block_ledger_rename BEFORE UPDATE ON effect_sql_migrations
      BEGIN SELECT RAISE(ABORT, 'blocked'); END`;
    assert.ok(Exit.isFailure(yield* Effect.exit(runMigrations())));
    assert.deepStrictEqual(
      yield* sql`SELECT name FROM effect_sql_migrations WHERE migration_id = 50`,
      [{ name: "ProjectionThreadsSpawn" }],
    );
    assert.deepStrictEqual(
      yield* sql`SELECT name FROM sqlite_master WHERE name = 'projection_thread_pull_requests'`,
      [],
    );
  }).pipe(Effect.provide(NodeSqliteClient.layer({ filename: ":memory:" }))),
);

it.effect("upgrades a 0.0.45 fork database and resets heartbeat origins to the v2 space", () =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    yield* runMigrations({ toMigrationInclusive: 54 });
    yield* seedThread;
    // The 0.0.45 fork ledger, exactly as that build recorded it.
    yield* Migrator.make({})({
      table: "t3_fork_migrations",
      loader: Migrator.fromRecord({
        "1_ProjectionThreadsSpawn": ProjectionThreadsSpawn,
        "2_HeartbeatJobs": HeartbeatJobs,
      }),
    });
    yield* sql`UPDATE projection_threads SET spawn_json = ${spawn} WHERE thread_id = 'child'`;
    yield* sql`INSERT INTO heartbeat_jobs (
      thread_id, public_id, prompt, cron_expression, timezone, recurring,
      created_at_ms, expires_at_ms, next_nominal_at_ms, next_due_at_ms, generation,
      origin_sequence
    ) VALUES (
      'child', 'deadbeef', 'check the build', '0 * * * *', 'UTC', 1,
      1000, 604801000, 3600000, 3600010, 3, 500000
    )`;
    yield* sql`INSERT INTO heartbeat_occurrences (
      occurrence_id, thread_id, public_id, generation, nominal_at_ms, due_at_ms,
      status, created_at_ms, updated_at_ms
    ) VALUES (
      '00000000-0000-4000-8000-000000000001', 'child', 'deadbeef', 3,
      3600000, 3600010, 'sent', 3600010, 3600010
    )`;

    assert.deepStrictEqual(
      yield* runMigrations(),
      migrationManifest.filter(([id]) => id >= 55),
    );
    yield* assertCurrentSchema;
    assert.deepStrictEqual(
      yield* sql`SELECT thread_id, public_id, prompt, generation, origin_sequence FROM heartbeat_jobs`,
      [
        {
          thread_id: "child",
          public_id: "deadbeef",
          prompt: "check the build",
          generation: 3,
          origin_sequence: 0,
        },
      ],
    );
    assert.deepStrictEqual(yield* sql`SELECT occurrence_id, status FROM heartbeat_occurrences`, [
      { occurrence_id: "00000000-0000-4000-8000-000000000001", status: "sent" },
    ]);
    assert.deepStrictEqual(yield* sql`SELECT spawn_json FROM projection_threads`, [
      { spawn_json: spawn },
    ]);

    // Jobs written after the upgrade keep their v2 origins on later starts.
    yield* sql`UPDATE heartbeat_jobs SET origin_sequence = 7`;
    assert.deepStrictEqual(yield* runMigrations(), []);
    assert.deepStrictEqual(yield* sql`SELECT origin_sequence FROM heartbeat_jobs`, [
      { origin_sequence: 7 },
    ]);
    yield* assertCurrentSchema;
  }).pipe(Effect.provide(NodeSqliteClient.layer({ filename: ":memory:" }))),
);

it.effect.each(["UnexpectedForkMigration", "ProjectionThreadPullRequests"])(
  "rejects inconsistent migration 50 (%s) without rewriting data",
  (name) =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      yield* seedFork50;
      yield* sql`UPDATE effect_sql_migrations SET name = ${name} WHERE migration_id = 50`;
      assert.ok(Exit.isFailure(yield* Effect.exit(runMigrations())));
      assert.deepStrictEqual(
        yield* sql`SELECT name FROM effect_sql_migrations WHERE migration_id = 50`,
        [{ name }],
      );
      assert.deepStrictEqual(yield* sql`SELECT spawn_json FROM projection_threads`, [
        { spawn_json: spawn },
      ]);
    }).pipe(Effect.provide(NodeSqliteClient.layer({ filename: ":memory:" }))),
);

it.effect("refuses a legacy fork ledger whose spawn column is missing", () =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    yield* seedFork50;
    yield* sql`ALTER TABLE projection_threads DROP COLUMN spawn_json`;
    assert.ok(Exit.isFailure(yield* Effect.exit(runMigrations())));
    assert.deepStrictEqual(
      yield* sql`SELECT name FROM effect_sql_migrations WHERE migration_id = 50`,
      [{ name: "ProjectionThreadsSpawn" }],
    );
  }).pipe(Effect.provide(NodeSqliteClient.layer({ filename: ":memory:" }))),
);

it.effect("refuses an upstream ledger with migration 50 missing below later entries", () =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    yield* runMigrations({ toMigrationInclusive: 52 });
    yield* sql`DELETE FROM effect_sql_migrations WHERE migration_id = 50`;
    assert.ok(Exit.isFailure(yield* Effect.exit(runMigrations())));
    assert.deepStrictEqual(
      yield* sql`SELECT name FROM sqlite_master WHERE name = 't3_fork_migrations'`,
      [],
    );
  }).pipe(Effect.provide(NodeSqliteClient.layer({ filename: ":memory:" }))),
);
