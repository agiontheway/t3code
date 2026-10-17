import * as Effect from "effect/Effect";
import * as SqlClient from "effect/sql/SqlClient";

/**
 * Stored job origins are v1 `orchestration_events` sequences. Cancellation fences now compare
 * against v2 sequences, a separate and smaller space, so a legacy origin would outrank every v2
 * Stop, Archive or Delete. Every v2 cancellation happens after every legacy job was created, so 0
 * keeps legacy jobs cancellable.
 */
export default Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  yield* sql`UPDATE heartbeat_jobs SET origin_sequence = 0`;
});
