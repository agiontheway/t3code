import * as Effect from "effect/Effect";
import * as SqlClient from "effect/unstable/sql/SqlClient";

/** Durable parent/child ownership for cross-provider spawned threads. */
export default Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  const columns = yield* sql<{ readonly name: string }>`
    PRAGMA table_info(projection_threads)
  `;
  if (!columns.some((column) => column.name === "spawn_json")) {
    yield* sql`
      ALTER TABLE projection_threads
      ADD COLUMN spawn_json TEXT
    `;
  }
});
