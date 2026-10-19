import { assert, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as SqlClient from "effect/sql/SqlClient";
import * as NodeSqliteClient from "@t3tools/shared/nodeSqliteClient";

import AliasOwnedChildren from "./005_AliasOwnedChildren.ts";

it.effect("creates the alias-owned table idempotently and keeps one row per child", () =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    yield* AliasOwnedChildren;
    yield* sql`INSERT INTO fork_alias_owned_children (child_thread_id, created_at)
      VALUES ('child', '2026-10-01T00:00:00.000Z')`;

    yield* AliasOwnedChildren;

    const duplicate = yield* Effect.exit(
      sql`INSERT INTO fork_alias_owned_children (child_thread_id, created_at)
        VALUES ('child', '2026-10-02T00:00:00.000Z')`,
    );
    assert.ok(Exit.isFailure(duplicate));
    assert.deepStrictEqual(yield* sql`SELECT * FROM fork_alias_owned_children`, [
      { child_thread_id: "child", created_at: "2026-10-01T00:00:00.000Z" },
    ]);
  }).pipe(Effect.provide(NodeSqliteClient.layer({ filename: ":memory:" }))),
);
