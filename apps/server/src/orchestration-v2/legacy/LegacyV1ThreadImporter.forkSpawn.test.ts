import { assert, it } from "@effect/vitest";
import { ThreadId } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as SqlClient from "effect/sql/SqlClient";

import * as SqlitePersistence from "../../persistence/Sqlite.ts";
import * as EventSink from "../EventSink.ts";
import * as EventStore from "../EventStore.ts";
import * as DelegationGrants from "../fork/DelegationGrants.ts";
import * as LegacyV1ThreadImporter from "./LegacyV1ThreadImporter.ts";
import * as ProjectionMaintenance from "../ProjectionMaintenance.ts";
import * as ProjectionStore from "../ProjectionStore.ts";

const layerDatabase = SqlitePersistence.layerMemory;
const layerStoresProvided = Layer.mergeAll(
  layerDatabase,
  EventStore.layer.pipe(Layer.provideMerge(layerDatabase)),
  ProjectionStore.layer.pipe(Layer.provideMerge(layerDatabase)),
);
const layerEventSinkProvided = EventSink.layer.pipe(Layer.provide(layerStoresProvided));
const layerTest = Layer.mergeAll(
  layerStoresProvided,
  layerEventSinkProvided,
  LegacyV1ThreadImporter.layer.pipe(
    Layer.provide(Layer.mergeAll(layerStoresProvided, layerEventSinkProvided)),
  ),
  ProjectionMaintenance.layer.pipe(Layer.provide(layerStoresProvided)),
  DelegationGrants.layer.pipe(Layer.provide(layerDatabase)),
);

const insertProject = (projectId: string) =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    yield* sql`
      INSERT INTO projection_projects (
        project_id, title, workspace_root, scripts_json, created_at, updated_at
      ) VALUES (
        ${projectId},
        'Legacy project',
        '/tmp/legacy-spawn',
        '[]',
        '2026-01-01T00:00:00.000Z',
        '2026-01-01T00:00:00.000Z'
      )
    `;
  });

const insertLegacyThread = (input: {
  readonly threadId: ThreadId;
  readonly projectId: string;
  readonly createdAt: string;
  readonly spawnJson: string | null;
}) =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    yield* sql`
      INSERT INTO projection_threads (
        thread_id, project_id, title, model_selection_json, runtime_mode,
        interaction_mode, created_at, updated_at, spawn_json
      ) VALUES (
        ${input.threadId},
        ${input.projectId},
        'Legacy thread',
        '{"instanceId":"codex","model":"gpt-5.4"}',
        'full-access',
        'default',
        ${input.createdAt},
        ${input.createdAt},
        ${input.spawnJson}
      )
    `;
  });

const spawnJson = (parentThreadId: ThreadId, depth: number, allowOrchestration: boolean) =>
  JSON.stringify({
    parentThreadId,
    allowOrchestration,
    depth,
    taskId: `xp-agent:task-${depth}`,
  });

it.layer(layerTest)("LegacyV1ThreadImporter 0.0.45 spawn rows", (it) => {
  it.effect("imports a spawn chain as subagent lineage with delegation grants", () =>
    Effect.gen(function* () {
      const importer = yield* LegacyV1ThreadImporter.LegacyV1ThreadImporter;
      const projections = yield* ProjectionStore.ProjectionStoreV2;
      const maintenance = yield* ProjectionMaintenance.ProjectionMaintenanceV2;
      const grants = yield* DelegationGrants.DelegationGrants;
      const projectId = "project:legacy-spawn-chain";
      const rootId = ThreadId.make("thread:legacy-spawn-root");
      const childId = ThreadId.make("thread:legacy-spawn-child");
      const grandchildId = ThreadId.make("thread:legacy-spawn-grandchild");

      yield* insertProject(projectId);
      yield* insertLegacyThread({
        threadId: rootId,
        projectId,
        createdAt: "2026-01-01T00:00:00.000Z",
        spawnJson: null,
      });
      yield* insertLegacyThread({
        threadId: childId,
        projectId,
        createdAt: "2026-01-01T01:00:00.000Z",
        spawnJson: spawnJson(rootId, 1, true),
      });
      yield* insertLegacyThread({
        threadId: grandchildId,
        projectId,
        createdAt: "2026-01-01T02:00:00.000Z",
        spawnJson: spawnJson(childId, 2, false),
      });

      assert.deepStrictEqual(yield* importer.reconcileShells, {
        importedThreadCount: 3,
        importedMessageCount: 0,
      });

      const lineageOf = (threadId: ThreadId) =>
        projections.getThreadProjection(threadId).pipe(Effect.map((p) => p.thread.lineage));
      assert.deepStrictEqual(yield* lineageOf(rootId), {
        parentThreadId: null,
        relationshipToParent: null,
        rootThreadId: rootId,
      });
      assert.deepStrictEqual(yield* lineageOf(childId), {
        parentThreadId: rootId,
        relationshipToParent: "subagent",
        rootThreadId: rootId,
      });
      assert.deepStrictEqual(yield* lineageOf(grandchildId), {
        parentThreadId: childId,
        relationshipToParent: "subagent",
        rootThreadId: rootId,
      });
      // No task rows are synthesized, so a legacy child never schedules a completion wake.
      assert.isNull((yield* projections.getThreadProjection(childId)).thread.forkedFrom);
      assert.deepStrictEqual((yield* projections.getThreadProjection(rootId)).subagents, []);

      const childGrant = yield* grants.read(childId);
      assert.isTrue(Option.isSome(childGrant));
      if (Option.isSome(childGrant)) {
        assert.equal(childGrant.value.parentThreadId, rootId);
        assert.equal(childGrant.value.depth, 1);
        assert.isTrue(childGrant.value.allowOrchestration);
        assert.equal(childGrant.value.createdAt, "2026-01-01T01:00:00.000Z");
      }
      const grandchildGrant = yield* grants.read(grandchildId);
      assert.isTrue(Option.isSome(grandchildGrant));
      if (Option.isSome(grandchildGrant)) {
        assert.equal(grandchildGrant.value.parentThreadId, childId);
        assert.equal(grandchildGrant.value.depth, 2);
        assert.isFalse(grandchildGrant.value.allowOrchestration);
      }
      assert.isTrue(Option.isNone(yield* grants.read(rootId)));

      // Lineage survives a projection rebuild from the imported events, and a re-run is a no-op.
      assert.isTrue((yield* maintenance.rebuild).valid);
      assert.equal((yield* lineageOf(grandchildId)).rootThreadId, rootId);
      assert.deepStrictEqual(yield* importer.reconcileShells, {
        importedThreadCount: 0,
        importedMessageCount: 0,
      });
    }),
  );

  it.effect("leaves threads without a usable spawn_json top-level and without a grant", () =>
    Effect.gen(function* () {
      const importer = yield* LegacyV1ThreadImporter.LegacyV1ThreadImporter;
      const projections = yield* ProjectionStore.ProjectionStoreV2;
      const grants = yield* DelegationGrants.DelegationGrants;
      const projectId = "project:legacy-no-spawn";
      const plainId = ThreadId.make("thread:legacy-no-spawn");
      const depthZeroId = ThreadId.make("thread:legacy-spawn-depth-zero");
      const malformedId = ThreadId.make("thread:legacy-spawn-malformed");

      yield* insertProject(projectId);
      yield* insertLegacyThread({
        threadId: plainId,
        projectId,
        createdAt: "2026-02-01T00:00:00.000Z",
        spawnJson: null,
      });
      yield* insertLegacyThread({
        threadId: depthZeroId,
        projectId,
        createdAt: "2026-02-01T01:00:00.000Z",
        spawnJson: spawnJson(plainId, 0, false),
      });
      yield* insertLegacyThread({
        threadId: malformedId,
        projectId,
        createdAt: "2026-02-01T02:00:00.000Z",
        spawnJson: "{not json",
      });

      assert.deepStrictEqual(yield* importer.reconcileShells, {
        importedThreadCount: 3,
        importedMessageCount: 0,
      });
      for (const threadId of [plainId, depthZeroId, malformedId]) {
        const projection = yield* projections.getThreadProjection(threadId);
        assert.deepStrictEqual(projection.thread.lineage, {
          parentThreadId: null,
          relationshipToParent: null,
          rootThreadId: threadId,
        });
        assert.isNull(projection.thread.forkedFrom);
        assert.equal(projection.thread.historyOrigin, "v1_import");
        assert.isTrue(Option.isNone(yield* grants.read(threadId)));
      }
    }),
  );
});
