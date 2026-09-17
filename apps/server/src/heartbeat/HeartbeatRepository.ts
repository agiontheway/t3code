import type {
  HeartbeatJobId,
  HeartbeatOccurrenceId,
  HeartbeatOccurrenceOutcome,
  HeartbeatPrompt,
  ThreadId,
} from "@t3tools/contracts";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/unstable/sql/SqlClient";

import { toPersistenceSqlError } from "../persistence/Errors.ts";

export const HEARTBEAT_CRON_JOB_LIMIT = 50;

export interface StoredHeartbeatJob {
  readonly threadId: ThreadId;
  readonly publicId: HeartbeatJobId;
  readonly prompt: HeartbeatPrompt;
  readonly cronExpression: string;
  readonly timezone: string;
  readonly recurring: boolean;
  readonly createdAtMs: number;
  readonly expiresAtMs: number;
  readonly nextNominalAtMs: number;
  readonly nextDueAtMs: number;
  readonly generation: number;
  readonly originSequence: number;
  readonly pending?: boolean;
}

interface StoredHeartbeatJobRow extends Omit<StoredHeartbeatJob, "recurring" | "pending"> {
  readonly recurring: number;
  readonly pending: number;
}

export interface PendingHeartbeatOccurrence {
  readonly occurrenceId: HeartbeatOccurrenceId;
  readonly threadId: ThreadId;
  readonly publicId: HeartbeatJobId;
  readonly prompt: HeartbeatPrompt;
  readonly generation: number;
  readonly nominalAtMs: number;
  readonly dueAtMs: number;
  readonly reservedAtMs: number;
}

export type CreateHeartbeatJobResult = "created" | "collision" | "limit";

export class HeartbeatRepositoryError extends Schema.TaggedError<HeartbeatRepositoryError>()(
  "HeartbeatRepositoryError",
  { operation: Schema.String, cause: Schema.Defect() },
) {}

const mapRow = (row: StoredHeartbeatJobRow): StoredHeartbeatJob => ({
  ...row,
  recurring: row.recurring === 1,
  pending: row.pending === 1,
});

export class HeartbeatRepository extends Context.Service<
  HeartbeatRepository,
  {
    readonly threadExists: (threadId: ThreadId) => Effect.Effect<boolean, HeartbeatRepositoryError>;
    readonly latestThreadSequence: (
      threadId: ThreadId,
    ) => Effect.Effect<number, HeartbeatRepositoryError>;
    readonly latestCancellationSequence: (
      threadId: ThreadId,
    ) => Effect.Effect<number | undefined, HeartbeatRepositoryError>;
    readonly publicIdExists: (
      threadId: ThreadId,
      publicId: HeartbeatJobId,
    ) => Effect.Effect<boolean, HeartbeatRepositoryError>;
    readonly create: (
      job: StoredHeartbeatJob,
    ) => Effect.Effect<CreateHeartbeatJobResult, HeartbeatRepositoryError>;
    readonly listByThread: (
      threadId: ThreadId,
    ) => Effect.Effect<ReadonlyArray<StoredHeartbeatJob>, HeartbeatRepositoryError>;
    readonly listAll: Effect.Effect<ReadonlyArray<StoredHeartbeatJob>, HeartbeatRepositoryError>;
    readonly replaceSchedule: (
      job: Pick<StoredHeartbeatJob, "threadId" | "publicId" | "nextNominalAtMs" | "nextDueAtMs">,
    ) => Effect.Effect<void, HeartbeatRepositoryError>;
    readonly delete: (
      threadId: ThreadId,
      publicId: HeartbeatJobId,
      nowMs: number,
      occurrenceStatus?: "canceled" | "expired",
    ) => Effect.Effect<boolean, HeartbeatRepositoryError>;
    readonly deleteThread: (
      threadId: ThreadId,
      nowMs: number,
    ) => Effect.Effect<void, HeartbeatRepositoryError>;
    readonly reserve: (input: {
      readonly job: StoredHeartbeatJob;
      readonly occurrenceId: HeartbeatOccurrenceId;
      readonly nowMs: number;
      readonly nextNominalAtMs: number | null;
      readonly nextDueAtMs: number | null;
    }) => Effect.Effect<PendingHeartbeatOccurrence | null, HeartbeatRepositoryError>;
    readonly setOccurrenceOutcome: (
      occurrenceId: HeartbeatOccurrenceId,
      outcome: HeartbeatOccurrenceOutcome,
      nowMs: number,
    ) => Effect.Effect<void, HeartbeatRepositoryError>;
    readonly occurrenceStatus: (
      occurrenceId: HeartbeatOccurrenceId,
    ) => Effect.Effect<
      "pending" | "admitted" | "sent" | "failed" | "canceled" | "expired" | undefined,
      HeartbeatRepositoryError
    >;
    readonly expirePendingOccurrences: (
      nowMs: number,
    ) => Effect.Effect<void, HeartbeatRepositoryError>;
    readonly pruneOccurrences: (
      terminalBeforeMs: number,
    ) => Effect.Effect<void, HeartbeatRepositoryError>;
  }
>()("t3/heartbeat/HeartbeatRepository") {}

/** @public Service construction is used by focused tests and the server layer. */
export const make = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  const mapError = (operation: string) => (cause: unknown) =>
    new HeartbeatRepositoryError({
      operation,
      cause: toPersistenceSqlError(`HeartbeatRepository.${operation}`)(cause),
    });

  const threadExists: HeartbeatRepository["Service"]["threadExists"] = (threadId) =>
    sql`
      SELECT thread_id FROM projection_threads WHERE thread_id = ${threadId} LIMIT 1
    `.pipe(
      Effect.map((rows) => rows.length > 0),
      Effect.mapError(mapError("threadExists")),
    );

  const latestThreadSequence: HeartbeatRepository["Service"]["latestThreadSequence"] = (threadId) =>
    sql<{ readonly sequence: number | null }>`
      SELECT MAX(sequence) AS sequence FROM orchestration_events
      WHERE aggregate_kind = 'thread' AND stream_id = ${threadId}
    `.pipe(
      Effect.map((rows) => rows[0]?.sequence ?? 0),
      Effect.mapError(mapError("latestThreadSequence")),
    );

  const latestCancellationSequence: HeartbeatRepository["Service"]["latestCancellationSequence"] = (
    threadId,
  ) =>
    sql<{ readonly sequence: number | null }>`
        SELECT MAX(sequence) AS sequence FROM orchestration_events
        WHERE aggregate_kind = 'thread'
          AND stream_id = ${threadId}
          AND (
            event_type IN ('thread.archived', 'thread.deleted')
            OR (
              event_type = 'thread.session-stop-requested'
              AND COALESCE(json_extract(payload_json, '$.onlyIfSettled'), 0) <> 1
            )
          )
      `.pipe(
      Effect.map((rows) => rows[0]?.sequence ?? undefined),
      Effect.mapError(mapError("latestCancellationSequence")),
    );

  const publicIdExists: HeartbeatRepository["Service"]["publicIdExists"] = (threadId, publicId) =>
    sql`
      SELECT public_id FROM heartbeat_jobs
      WHERE thread_id = ${threadId} AND public_id = ${publicId}
      UNION ALL
      SELECT public_id FROM heartbeat_occurrences
      WHERE thread_id = ${threadId} AND public_id = ${publicId}
      LIMIT 1
    `.pipe(
      Effect.map((rows) => rows.length > 0),
      Effect.mapError(mapError("publicIdExists")),
    );

  const create: HeartbeatRepository["Service"]["create"] = (job) =>
    sql
      .withTransaction(
        Effect.gen(function* () {
          // Retained occurrence identities permanently reserve their display
          // ID until retention pruning removes that history.
          const retainedIdentity = yield* sql`
            SELECT public_id FROM heartbeat_occurrences
            WHERE thread_id = ${job.threadId} AND public_id = ${job.publicId}
            LIMIT 1
          `;
          if (retainedIdentity.length > 0) return "collision" as const;
          const count = yield* sql<{ readonly count: number }>`
          SELECT COUNT(*) AS count FROM heartbeat_jobs WHERE thread_id = ${job.threadId}
        `;
          if ((count[0]?.count ?? 0) >= HEARTBEAT_CRON_JOB_LIMIT) return "limit" as const;
          const inserted = yield* sql<{ readonly publicId: string }>`
          INSERT INTO heartbeat_jobs (
            thread_id, public_id, prompt, cron_expression, timezone, recurring,
            created_at_ms, expires_at_ms, next_nominal_at_ms, next_due_at_ms, generation,
            origin_sequence
          ) VALUES (
            ${job.threadId}, ${job.publicId}, ${job.prompt}, ${job.cronExpression},
            ${job.timezone}, ${job.recurring ? 1 : 0}, ${job.createdAtMs}, ${job.expiresAtMs},
            ${job.nextNominalAtMs}, ${job.nextDueAtMs}, ${job.generation}, ${job.originSequence}
          )
          ON CONFLICT (thread_id, public_id) DO NOTHING
          RETURNING public_id AS "publicId"
        `;
          return inserted.length === 0 ? ("collision" as const) : ("created" as const);
        }),
      )
      .pipe(Effect.mapError(mapError("create")));

  const listByThread: HeartbeatRepository["Service"]["listByThread"] = (threadId) =>
    sql<StoredHeartbeatJobRow>`
      SELECT
        thread_id AS "threadId", public_id AS "publicId", prompt,
        cron_expression AS "cronExpression", timezone, recurring,
        created_at_ms AS "createdAtMs", expires_at_ms AS "expiresAtMs",
        next_nominal_at_ms AS "nextNominalAtMs", next_due_at_ms AS "nextDueAtMs",
        generation, origin_sequence AS "originSequence",
        EXISTS (
          SELECT 1 FROM heartbeat_occurrences occurrence
          WHERE occurrence.thread_id = heartbeat_jobs.thread_id
            AND occurrence.public_id = heartbeat_jobs.public_id
            AND occurrence.status IN ('pending', 'admitted')
        ) AS pending
      FROM heartbeat_jobs
      WHERE thread_id = ${threadId}
      ORDER BY created_at_ms ASC, public_id ASC
    `.pipe(
      Effect.map((rows) => rows.map(mapRow)),
      Effect.mapError(mapError("listByThread")),
    );

  const listAll: HeartbeatRepository["Service"]["listAll"] = sql<StoredHeartbeatJobRow>`
    SELECT
      thread_id AS "threadId", public_id AS "publicId", prompt,
      cron_expression AS "cronExpression", timezone, recurring,
      created_at_ms AS "createdAtMs", expires_at_ms AS "expiresAtMs",
      next_nominal_at_ms AS "nextNominalAtMs", next_due_at_ms AS "nextDueAtMs",
      generation, origin_sequence AS "originSequence",
      EXISTS (
        SELECT 1 FROM heartbeat_occurrences occurrence
        WHERE occurrence.thread_id = heartbeat_jobs.thread_id
          AND occurrence.public_id = heartbeat_jobs.public_id
          AND occurrence.status IN ('pending', 'admitted')
      ) AS pending
    FROM heartbeat_jobs
    ORDER BY next_due_at_ms ASC, thread_id ASC, public_id ASC
  `.pipe(
    Effect.map((rows) => rows.map(mapRow)),
    Effect.mapError(mapError("listAll")),
  );

  const replaceSchedule: HeartbeatRepository["Service"]["replaceSchedule"] = (job) =>
    sql`
      UPDATE heartbeat_jobs
      SET next_nominal_at_ms = ${job.nextNominalAtMs}, next_due_at_ms = ${job.nextDueAtMs}
      WHERE thread_id = ${job.threadId} AND public_id = ${job.publicId}
    `.pipe(Effect.asVoid, Effect.mapError(mapError("replaceSchedule")));

  const deleteJob: HeartbeatRepository["Service"]["delete"] = (
    threadId,
    publicId,
    nowMs,
    occurrenceStatus = "canceled",
  ) =>
    sql
      .withTransaction(
        Effect.gen(function* () {
          yield* sql`
          UPDATE heartbeat_occurrences
          SET status = ${occurrenceStatus}, updated_at_ms = ${nowMs}
          WHERE thread_id = ${threadId} AND public_id = ${publicId}
            AND status IN ('pending', 'admitted')
        `;
          const deleted = yield* sql<{ readonly publicId: string }>`
          DELETE FROM heartbeat_jobs
          WHERE thread_id = ${threadId} AND public_id = ${publicId}
          RETURNING public_id AS "publicId"
        `;
          return deleted.length > 0;
        }),
      )
      .pipe(Effect.mapError(mapError("delete")));

  const deleteThread: HeartbeatRepository["Service"]["deleteThread"] = (threadId, nowMs) =>
    sql
      .withTransaction(
        Effect.gen(function* () {
          yield* sql`
          UPDATE heartbeat_occurrences
          SET status = 'canceled', updated_at_ms = ${nowMs}
          WHERE thread_id = ${threadId} AND status IN ('pending', 'admitted')
        `;
          yield* sql`DELETE FROM heartbeat_jobs WHERE thread_id = ${threadId}`;
        }),
      )
      .pipe(Effect.mapError(mapError("deleteThread")));

  const reserve: HeartbeatRepository["Service"]["reserve"] = (input) =>
    sql
      .withTransaction(
        Effect.gen(function* () {
          const unresolved = yield* sql`
          SELECT occurrence_id FROM heartbeat_occurrences
          WHERE thread_id = ${input.job.threadId} AND public_id = ${input.job.publicId}
            AND status IN ('pending', 'admitted')
          LIMIT 1
        `;
          if (unresolved.length > 0) return null;

          const claimed = yield* sql<{ readonly generation: number }>`
          UPDATE heartbeat_jobs
          SET generation = generation + 1
          WHERE thread_id = ${input.job.threadId}
            AND public_id = ${input.job.publicId}
            AND generation = ${input.job.generation}
            AND next_due_at_ms <= ${input.nowMs}
          RETURNING generation
        `;
          const generation = claimed[0]?.generation;
          if (generation === undefined) return null;

          yield* sql`
          INSERT INTO heartbeat_occurrences (
            occurrence_id, thread_id, public_id, generation, nominal_at_ms, due_at_ms,
            status, created_at_ms, updated_at_ms
          ) VALUES (
            ${input.occurrenceId}, ${input.job.threadId}, ${input.job.publicId}, ${generation},
            ${input.job.nextNominalAtMs}, ${input.job.nextDueAtMs}, 'pending',
            ${input.nowMs}, ${input.nowMs}
          )
        `;

          if (input.nextNominalAtMs === null || input.nextDueAtMs === null) {
            yield* sql`
            DELETE FROM heartbeat_jobs
            WHERE thread_id = ${input.job.threadId} AND public_id = ${input.job.publicId}
          `;
          } else {
            yield* sql`
            UPDATE heartbeat_jobs
            SET next_nominal_at_ms = ${input.nextNominalAtMs},
                next_due_at_ms = ${input.nextDueAtMs}
            WHERE thread_id = ${input.job.threadId} AND public_id = ${input.job.publicId}
          `;
          }

          return {
            occurrenceId: input.occurrenceId,
            threadId: input.job.threadId,
            publicId: input.job.publicId,
            prompt: input.job.prompt,
            generation,
            nominalAtMs: input.job.nextNominalAtMs,
            dueAtMs: input.job.nextDueAtMs,
            reservedAtMs: input.nowMs,
          } satisfies PendingHeartbeatOccurrence;
        }),
      )
      .pipe(Effect.mapError(mapError("reserve")));

  const setOccurrenceOutcome: HeartbeatRepository["Service"]["setOccurrenceOutcome"] = (
    occurrenceId,
    outcome,
    nowMs,
  ) =>
    sql`
      UPDATE heartbeat_occurrences
      SET status = ${outcome.status}, updated_at_ms = ${nowMs}, failure = ${outcome.error ?? null}
      WHERE occurrence_id = ${occurrenceId}
        AND status IN ('pending', 'admitted')
    `.pipe(Effect.asVoid, Effect.mapError(mapError("setOccurrenceOutcome")));

  const occurrenceStatus: HeartbeatRepository["Service"]["occurrenceStatus"] = (occurrenceId) =>
    sql<{
      readonly status: "pending" | "admitted" | "sent" | "failed" | "canceled" | "expired";
    }>`
      SELECT status FROM heartbeat_occurrences WHERE occurrence_id = ${occurrenceId} LIMIT 1
    `.pipe(
      Effect.map((rows) => rows[0]?.status),
      Effect.mapError(mapError("occurrenceStatus")),
    );

  const expirePendingOccurrences: HeartbeatRepository["Service"]["expirePendingOccurrences"] = (
    nowMs,
  ) =>
    sql`
        UPDATE heartbeat_occurrences
        SET status = 'expired', updated_at_ms = ${nowMs}
        WHERE status IN ('pending', 'admitted')
      `.pipe(Effect.asVoid, Effect.mapError(mapError("expirePendingOccurrences")));

  const pruneOccurrences: HeartbeatRepository["Service"]["pruneOccurrences"] = (terminalBeforeMs) =>
    sql`
      DELETE FROM heartbeat_occurrences
      WHERE status IN ('sent', 'failed', 'canceled', 'expired')
        AND updated_at_ms < ${terminalBeforeMs}
    `.pipe(Effect.asVoid, Effect.mapError(mapError("pruneOccurrences")));

  return HeartbeatRepository.of({
    threadExists,
    latestThreadSequence,
    latestCancellationSequence,
    publicIdExists,
    create,
    listByThread,
    listAll,
    replaceSchedule,
    delete: deleteJob,
    deleteThread,
    reserve,
    setOccurrenceOutcome,
    occurrenceStatus,
    expirePendingOccurrences,
    pruneOccurrences,
  });
});

export const layer = Layer.effect(HeartbeatRepository, make);
