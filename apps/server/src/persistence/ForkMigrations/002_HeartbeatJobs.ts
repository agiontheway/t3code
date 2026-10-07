import * as Effect from "effect/Effect";
import * as SqlClient from "effect/unstable/sql/SqlClient";

/** Durable cron schedules and their delivery identities. Relative wakeups never reach SQLite. */
export default Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;

  yield* sql`
    CREATE TABLE heartbeat_jobs (
      thread_id TEXT NOT NULL,
      public_id TEXT NOT NULL,
      prompt TEXT NOT NULL,
      cron_expression TEXT NOT NULL,
      timezone TEXT NOT NULL,
      recurring INTEGER NOT NULL CHECK (recurring IN (0, 1)),
      created_at_ms INTEGER NOT NULL,
      expires_at_ms INTEGER NOT NULL,
      next_nominal_at_ms INTEGER NOT NULL,
      next_due_at_ms INTEGER NOT NULL,
      generation INTEGER NOT NULL DEFAULT 0 CHECK (generation >= 0),
      origin_sequence INTEGER NOT NULL CHECK (origin_sequence >= 0),
      PRIMARY KEY (thread_id, public_id),
      CHECK (length(public_id) = 8 AND public_id NOT GLOB '*[^0-9a-f]*')
    )
  `;

  yield* sql`
    CREATE INDEX idx_heartbeat_jobs_next_due
    ON heartbeat_jobs (next_due_at_ms, thread_id, public_id)
  `;

  yield* sql`
    CREATE TABLE heartbeat_occurrences (
      occurrence_id TEXT PRIMARY KEY,
      thread_id TEXT NOT NULL,
      public_id TEXT NOT NULL,
      generation INTEGER NOT NULL CHECK (generation > 0),
      nominal_at_ms INTEGER NOT NULL,
      due_at_ms INTEGER NOT NULL,
      status TEXT NOT NULL CHECK (
        status IN ('pending', 'admitted', 'sent', 'failed', 'canceled', 'expired')
      ),
      created_at_ms INTEGER NOT NULL,
      updated_at_ms INTEGER NOT NULL,
      failure TEXT,
      UNIQUE (thread_id, public_id, generation)
    )
  `;

  yield* sql`
    CREATE UNIQUE INDEX idx_heartbeat_occurrences_unresolved_job
    ON heartbeat_occurrences (thread_id, public_id)
    WHERE status IN ('pending', 'admitted')
  `;

  yield* sql`
    CREATE INDEX idx_heartbeat_occurrences_retention
    ON heartbeat_occurrences (status, updated_at_ms)
  `;
});
