import { assert, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as SqlClient from "effect/unstable/sql/SqlClient";
import * as NodeSqliteClient from "@t3tools/shared/nodeSqliteClient";

import { runMigrations } from "./Migrations.ts";
import ProjectionThreadsSpawn from "./ForkMigrations/001_ProjectionThreadsSpawn.ts";

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
  ]);
  assert.deepStrictEqual(yield* sql`SELECT migration_id, name FROM t3_fork_migrations`, [
    { migration_id: 1, name: "ProjectionThreadsSpawn" },
  ]);
  const threads = yield* sql<{ readonly name: string }>`PRAGMA table_info(projection_threads)`;
  assert.ok(threads.some((column) => column.name === "spawn_json"));
  assert.ok(threads.some((column) => column.name === "title_state_json"));
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
  }).pipe(Effect.provide(NodeSqliteClient.layerMemory())),
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
  }).pipe(Effect.provide(NodeSqliteClient.layerMemory())),
);

it.effect("initializes a fresh database and reruns both ledgers idempotently", () =>
  Effect.gen(function* () {
    yield* runMigrations();
    yield* assertCurrentSchema;
    assert.deepStrictEqual(yield* runMigrations(), []);
    yield* assertCurrentSchema;
  }).pipe(Effect.provide(NodeSqliteClient.layerMemory())),
);

it.effect("rolls back legacy reconciliation when a later upstream migration fails", () =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    yield* seedFork50;
    yield* sql`DROP TABLE projection_thread_messages`;
    yield* sql`CREATE VIEW projection_thread_messages AS SELECT 1 AS id`;
    assert.ok(Exit.isFailure(yield* Effect.exit(runMigrations())));
    assert.deepStrictEqual(
      yield* sql`SELECT name FROM effect_sql_migrations WHERE migration_id = 50`,
      [{ name: "ProjectionThreadsSpawn" }],
    );
    assert.deepStrictEqual(
      yield* sql`SELECT name FROM sqlite_master WHERE name = 'projection_thread_pull_requests'`,
      [],
    );
    assert.deepStrictEqual(yield* sql`SELECT spawn_json FROM projection_threads`, [
      { spawn_json: spawn },
    ]);
  }).pipe(Effect.provide(NodeSqliteClient.layerMemory())),
);

for (const name of ["UnexpectedForkMigration", "ProjectionThreadPullRequests"]) {
  it.effect(`rejects inconsistent migration 50 (${name}) without rewriting data`, () =>
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
    }).pipe(Effect.provide(NodeSqliteClient.layerMemory())),
  );
}

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
  }).pipe(Effect.provide(NodeSqliteClient.layerMemory())),
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
  }).pipe(Effect.provide(NodeSqliteClient.layerMemory())),
);
