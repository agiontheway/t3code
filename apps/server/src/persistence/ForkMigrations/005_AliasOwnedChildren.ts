import * as Effect from "effect/Effect";
import * as SqlClient from "effect/sql/SqlClient";

/**
 * Delegated children spawned or followed up through the `agent_*` aliases. Their parent wakes
 * carry the capped child result inline; children of native `delegate_task` have no row.
 */
export default Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  yield* sql`
    CREATE TABLE IF NOT EXISTS fork_alias_owned_children (
      child_thread_id TEXT PRIMARY KEY,
      created_at TEXT NOT NULL
    )
  `;
});
