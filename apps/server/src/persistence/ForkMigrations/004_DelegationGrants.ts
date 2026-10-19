import * as Effect from "effect/Effect";
import * as SqlClient from "effect/sql/SqlClient";

/** Depth and orchestration grant of app-owned delegated child threads. Roots have no row. */
export default Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  yield* sql`
    CREATE TABLE IF NOT EXISTS fork_delegation_grants (
      child_thread_id TEXT PRIMARY KEY,
      parent_thread_id TEXT NOT NULL,
      depth INTEGER NOT NULL CHECK (depth >= 1),
      allow_orchestration INTEGER NOT NULL CHECK (allow_orchestration IN (0, 1)),
      created_at TEXT NOT NULL
    )
  `;
});
