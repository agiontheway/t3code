import { assert, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as SqlClient from "effect/sql/SqlClient";
import * as NodeSqliteClient from "@t3tools/shared/nodeSqliteClient";

import DelegationGrants from "./004_DelegationGrants.ts";

it.effect("creates the grant table idempotently and keeps existing grants", () =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    yield* DelegationGrants;
    yield* sql`INSERT INTO fork_delegation_grants (
      child_thread_id, parent_thread_id, depth, allow_orchestration, created_at
    ) VALUES ('child', 'root', 1, 1, '2026-10-01T00:00:00.000Z')`;

    yield* DelegationGrants;

    assert.deepStrictEqual(yield* sql`SELECT * FROM fork_delegation_grants`, [
      {
        child_thread_id: "child",
        parent_thread_id: "root",
        depth: 1,
        allow_orchestration: 1,
        created_at: "2026-10-01T00:00:00.000Z",
      },
    ]);
  }).pipe(Effect.provide(NodeSqliteClient.layer({ filename: ":memory:" }))),
);

it.effect("keeps one grant per child and rejects impossible rows", () =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    yield* DelegationGrants;
    yield* sql`INSERT INTO fork_delegation_grants (
      child_thread_id, parent_thread_id, depth, allow_orchestration, created_at
    ) VALUES ('child', 'root', 1, 0, '2026-10-01T00:00:00.000Z')`;

    for (const insert of [
      sql`INSERT INTO fork_delegation_grants (
        child_thread_id, parent_thread_id, depth, allow_orchestration, created_at
      ) VALUES ('child', 'other', 1, 0, '2026-10-01T00:00:00.000Z')`,
      sql`INSERT INTO fork_delegation_grants (
        child_thread_id, parent_thread_id, depth, allow_orchestration, created_at
      ) VALUES ('root-row', 'root', 0, 0, '2026-10-01T00:00:00.000Z')`,
      sql`INSERT INTO fork_delegation_grants (
        child_thread_id, parent_thread_id, depth, allow_orchestration, created_at
      ) VALUES ('flag', 'root', 1, 2, '2026-10-01T00:00:00.000Z')`,
    ]) {
      assert.ok(Exit.isFailure(yield* Effect.exit(insert)));
    }
    assert.deepStrictEqual(yield* sql`SELECT child_thread_id FROM fork_delegation_grants`, [
      { child_thread_id: "child" },
    ]);
  }).pipe(Effect.provide(NodeSqliteClient.layer({ filename: ":memory:" }))),
);
