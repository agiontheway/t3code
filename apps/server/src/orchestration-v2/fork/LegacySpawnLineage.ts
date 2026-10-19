/**
 * fork: carries 0.0.45 cross-provider children through the v1 -> v2 legacy import.
 *
 * 0.0.45 stored a child's parent, depth and orchestration grant in v1
 * `projection_threads.spawn_json`. The upstream importer gives every v1 thread a top-level
 * lineage, so without this the old children would flood the sidebar. A child with a spawn row is
 * imported as a `subagent` of its parent (rooted at the top of its spawn chain) and gets a
 * `fork_delegation_grants` row. No subagent task rows are synthesized on the parent, so the
 * child shows under the parent's Lineage but never triggers a completion wake.
 */
import { ThreadId } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import type * as SqlClient from "effect/sql/SqlClient";

const LegacySpawn = Schema.Struct({
  parentThreadId: ThreadId,
  allowOrchestration: Schema.Boolean,
  // `fork_delegation_grants.depth` requires >= 1; anything else stays a top-level thread.
  depth: Schema.Int.check(Schema.isGreaterThanOrEqualTo(1)),
});
type LegacySpawn = typeof LegacySpawn.Type;

const decodeLegacySpawn = Schema.decodeUnknownOption(Schema.fromJsonString(LegacySpawn));

export type LegacySpawns = ReadonlyMap<string, LegacySpawn>;

/**
 * Every decodable v1 spawn row, keyed by child thread id. Empty when the database never ran the
 * fork's spawn or grant migrations.
 */
export const loadLegacySpawns = (sql: SqlClient.SqlClient) =>
  Effect.gen(function* () {
    const columns = yield* sql<{ readonly name: string }>`PRAGMA table_info(projection_threads)`;
    const grantTables = yield* sql<{ readonly name: string }>`
      SELECT name FROM sqlite_master WHERE type = 'table' AND name = 'fork_delegation_grants'
    `;
    const spawns = new Map<string, LegacySpawn>();
    if (!columns.some((column) => column.name === "spawn_json") || grantTables.length === 0) {
      return spawns as LegacySpawns;
    }
    const rows = yield* sql<{ readonly thread_id: string; readonly spawn_json: string }>`
      SELECT thread_id, spawn_json
      FROM projection_threads
      WHERE spawn_json IS NOT NULL
    `;
    for (const row of rows) {
      const spawn = decodeLegacySpawn(row.spawn_json);
      if (Option.isSome(spawn) && spawn.value.parentThreadId !== row.thread_id) {
        spawns.set(row.thread_id, spawn.value);
      }
    }
    return spawns as LegacySpawns;
  });

/** The v2 lineage for a legacy child, or none for a v1 thread that was not spawned. */
export function legacySpawnLineage(threadId: ThreadId, spawns: LegacySpawns) {
  const spawn = spawns.get(threadId);
  if (spawn === undefined) return Option.none();
  const visited = new Set<string>([threadId]);
  let rootThreadId = spawn.parentThreadId;
  for (
    let next = spawns.get(rootThreadId);
    next !== undefined && !visited.has(rootThreadId);
    next = spawns.get(rootThreadId)
  ) {
    visited.add(rootThreadId);
    rootThreadId = next.parentThreadId;
  }
  return Option.some({
    parentThreadId: spawn.parentThreadId,
    relationshipToParent: "subagent" as const,
    rootThreadId,
  });
}

/** Writes the legacy child's grant; an existing grant for the child wins. */
export const recordLegacySpawnGrant = (
  sql: SqlClient.SqlClient,
  input: { readonly threadId: ThreadId; readonly spawns: LegacySpawns; readonly createdAt: string },
) => {
  const spawn = input.spawns.get(input.threadId);
  if (spawn === undefined) return Effect.void;
  return sql`
    INSERT INTO fork_delegation_grants (
      child_thread_id,
      parent_thread_id,
      depth,
      allow_orchestration,
      created_at
    )
    VALUES (
      ${input.threadId},
      ${spawn.parentThreadId},
      ${spawn.depth},
      ${spawn.allowOrchestration ? 1 : 0},
      ${input.createdAt}
    )
    ON CONFLICT (child_thread_id) DO NOTHING
  `.pipe(Effect.asVoid);
};
