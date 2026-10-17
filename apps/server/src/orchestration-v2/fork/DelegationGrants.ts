import { IsoDateTime, ThreadId } from "@t3tools/contracts";
import * as Context from "effect/Context";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/sql/SqlClient";
import * as SqlSchema from "effect/sql/SqlSchema";

import {
  PersistenceDecodeError,
  PersistenceSqlError,
  type ProjectionRepositoryError,
} from "../../persistence/Errors.ts";

/** How deep an app-owned delegated child sits and whether it may delegate in turn. */
export const DelegationGrant = Schema.Struct({
  childThreadId: ThreadId,
  parentThreadId: ThreadId,
  depth: Schema.Int,
  allowOrchestration: Schema.Boolean,
  createdAt: IsoDateTime,
});
export type DelegationGrant = typeof DelegationGrant.Type;

const DelegationGrantDbRow = Schema.Struct({
  ...DelegationGrant.fields,
  allowOrchestration: Schema.BooleanFromBit,
});

/**
 * Fork-owned depth and orchestration grants for delegated children, in `fork_delegation_grants`.
 *
 * v2 lineage records who spawned a child but has no home for depth or whether the child may
 * spawn, so the fork keeps them here, keyed by the child's deterministic thread id. A thread with
 * no row is a root (depth 0) or a child that was never granted orchestration.
 */
export class DelegationGrants extends Context.Service<
  DelegationGrants,
  {
    /** Write a child's grant before its delegation is dispatched. The first write for a child wins. */
    readonly record: (input: {
      readonly childThreadId: ThreadId;
      readonly parentThreadId: ThreadId;
      readonly depth: number;
      readonly allowOrchestration: boolean;
    }) => Effect.Effect<void, ProjectionRepositoryError>;
    readonly read: (
      threadId: ThreadId,
    ) => Effect.Effect<Option.Option<DelegationGrant>, ProjectionRepositoryError>;
    /** The thread's recorded depth, or 0 when it has no grant. */
    readonly depthOf: (threadId: ThreadId) => Effect.Effect<number, ProjectionRepositoryError>;
  }
>()("t3/orchestration-v2/fork/DelegationGrants") {}

const make = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;

  const insertRow = SqlSchema.void({
    Request: DelegationGrantDbRow,
    execute: (row) => sql`
      INSERT INTO fork_delegation_grants (
        child_thread_id,
        parent_thread_id,
        depth,
        allow_orchestration,
        created_at
      )
      VALUES (
        ${row.childThreadId},
        ${row.parentThreadId},
        ${row.depth},
        ${row.allowOrchestration},
        ${row.createdAt}
      )
      ON CONFLICT (child_thread_id) DO NOTHING
    `,
  });

  const selectRow = SqlSchema.findOneOption({
    Request: ThreadId,
    Result: DelegationGrantDbRow,
    execute: (threadId) => sql`
      SELECT
        child_thread_id AS "childThreadId",
        parent_thread_id AS "parentThreadId",
        depth,
        allow_orchestration AS "allowOrchestration",
        created_at AS "createdAt"
      FROM fork_delegation_grants
      WHERE child_thread_id = ${threadId}
    `,
  });

  const toRepositoryError =
    (operation: string) =>
    (cause: unknown): ProjectionRepositoryError =>
      Schema.isSchemaError(cause)
        ? PersistenceDecodeError.fromSchemaError(operation, cause)
        : new PersistenceSqlError({ operation, cause });

  const read: DelegationGrants["Service"]["read"] = (threadId) =>
    selectRow(threadId).pipe(Effect.mapError(toRepositoryError("DelegationGrants.read")));

  return DelegationGrants.of({
    record: (input) =>
      Effect.gen(function* () {
        const createdAt = DateTime.formatIso(yield* DateTime.now);
        yield* insertRow({ ...input, createdAt });
      }).pipe(Effect.mapError(toRepositoryError("DelegationGrants.record"))),
    read,
    depthOf: (threadId) =>
      read(threadId).pipe(
        Effect.map(
          Option.match({
            onNone: () => 0,
            onSome: (grant) => grant.depth,
          }),
        ),
      ),
  });
});

export const layer = Layer.effect(DelegationGrants, make);
