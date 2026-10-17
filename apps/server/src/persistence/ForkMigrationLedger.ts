/**
 * Fork schema that lives beside upstream's migrations.
 *
 * Fork migrations use their own `t3_fork_migrations` ledger so every upstream id stays available
 * to upstream. `runMigrations` calls `repairLegacyForkMigration50` before the upstream runner and
 * `runForkMigrations` after it on full startup.
 */
import * as Effect from "effect/Effect";
import * as Migrator from "effect/sql/Migrator";
import * as SqlClient from "effect/sql/SqlClient";

import ProjectionThreadsSpawn from "./ForkMigrations/001_ProjectionThreadsSpawn.ts";
import HeartbeatJobs from "./ForkMigrations/002_HeartbeatJobs.ts";
import HeartbeatV2SequenceSpace from "./ForkMigrations/003_HeartbeatV2SequenceSpace.ts";
import DelegationGrants from "./ForkMigrations/004_DelegationGrants.ts";
import AliasOwnedChildren from "./ForkMigrations/005_AliasOwnedChildren.ts";
import Migration0050 from "./Migrations/050_ProjectionThreadPullRequests.ts";

const PULL_REQUEST_COLUMNS = [
  "thread_id",
  "host",
  "repository",
  "number",
  "url",
  "source",
  "linked_at",
  "snapshot_json",
  "stack_json",
];

const columnNames = (table: string) =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    const columns = yield* sql<{
      readonly name: string;
    }>`SELECT name FROM pragma_table_info(${table})`;
    return new Set(columns.map((column) => column.name));
  });

/**
 * The 0.0.39 fork recorded its spawn schema under upstream's id 50. Run the real 50 and rename the
 * ledger row in one transaction, before the numeric upstream runner would skip it. Any other
 * unexpected history at 50 fails without rewriting anything.
 */
export const repairLegacyForkMigration50 = Effect.fn("repairLegacyForkMigration50")(function* () {
  const sql = yield* SqlClient.SqlClient;
  yield* sql.withTransaction(
    Effect.gen(function* () {
      const tables =
        yield* sql`SELECT name FROM sqlite_master WHERE type = 'table' AND name = 'effect_sql_migrations'`;
      if (tables.length === 0) return;
      const recorded = yield* sql<{
        readonly name: string;
      }>`SELECT name FROM effect_sql_migrations WHERE migration_id = 50`;
      const name = recorded[0]?.name;
      if (name === undefined) {
        const later =
          yield* sql`SELECT migration_id FROM effect_sql_migrations WHERE migration_id > 50 LIMIT 1`;
        if (later.length > 0) {
          return yield* new Migrator.MigrationError({
            kind: "BadState",
            message: "Migration history after 50 is missing migration 50",
          });
        }
        return;
      }
      if (name === "ProjectionThreadsSpawn") {
        if (!(yield* columnNames("projection_threads")).has("spawn_json")) {
          return yield* new Migrator.MigrationError({
            kind: "BadState",
            message: "Legacy fork migration 50 is recorded but spawn ownership schema is missing",
          });
        }
        yield* Migration0050;
        yield* sql`UPDATE effect_sql_migrations SET name = 'ProjectionThreadPullRequests' WHERE migration_id = 50 AND name = 'ProjectionThreadsSpawn'`;
        return;
      }
      if (name === "ProjectionThreadPullRequests") {
        const columns = yield* columnNames("projection_thread_pull_requests");
        if (PULL_REQUEST_COLUMNS.some((column) => !columns.has(column))) {
          return yield* new Migrator.MigrationError({
            kind: "BadState",
            message: "Migration 50 is recorded but the pull-request schema is incomplete",
          });
        }
        return;
      }
      return yield* new Migrator.MigrationError({
        kind: "BadState",
        message: `Unrecognized migration 50: ${name}`,
      });
    }),
  );
});

const runForkLedger = Migrator.make({});

/** Fork migrations, keyed in `t3_fork_migrations`. Append only; never reuse an id. */
export const runForkMigrations = Effect.fn("runForkMigrations")(function* () {
  return yield* runForkLedger({
    table: "t3_fork_migrations",
    loader: Migrator.fromRecord({
      "1_ProjectionThreadsSpawn": ProjectionThreadsSpawn,
      "2_HeartbeatJobs": HeartbeatJobs,
      "3_HeartbeatV2SequenceSpace": HeartbeatV2SequenceSpace,
      "4_DelegationGrants": DelegationGrants,
      "5_AliasOwnedChildren": AliasOwnedChildren,
    }),
  });
});
