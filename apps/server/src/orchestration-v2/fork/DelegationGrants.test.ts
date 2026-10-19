// @effect-diagnostics nodeBuiltinImport:off
import * as NodeFS from "node:fs";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";

import { assert, it } from "@effect/vitest";
import { ThreadId } from "@t3tools/contracts";
import * as NodeSqliteClient from "@t3tools/shared/nodeSqliteClient";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";

import DelegationGrantsMigration from "../../persistence/ForkMigrations/004_DelegationGrants.ts";
import { runMigrations } from "../../persistence/Migrations.ts";
import * as DelegationGrants from "./DelegationGrants.ts";

const root = ThreadId.make("root");
const child = ThreadId.make("child");
const grandchild = ThreadId.make("grandchild");
const greatGrandchild = ThreadId.make("great-grandchild");

const layerInMemory = DelegationGrants.layer.pipe(
  Layer.provideMerge(NodeSqliteClient.layer({ filename: ":memory:" })),
);

/** Each child's depth is its parent's plus one, the rule the delegation policy applies. */
const delegate = (parentThreadId: ThreadId, childThreadId: ThreadId, allowOrchestration: boolean) =>
  Effect.gen(function* () {
    const grants = yield* DelegationGrants.DelegationGrants;
    const depth = (yield* grants.depthOf(parentThreadId)) + 1;
    yield* grants.record({ childThreadId, parentThreadId, depth, allowOrchestration });
  });

it.effect("derives depth over a three-level chain from a root without a grant", () =>
  Effect.gen(function* () {
    yield* DelegationGrantsMigration;
    const grants = yield* DelegationGrants.DelegationGrants;

    yield* delegate(root, child, true);
    yield* delegate(child, grandchild, true);
    yield* delegate(grandchild, greatGrandchild, false);

    assert.strictEqual(yield* grants.depthOf(root), 0);
    assert.strictEqual(yield* grants.depthOf(child), 1);
    assert.strictEqual(yield* grants.depthOf(grandchild), 2);
    assert.strictEqual(yield* grants.depthOf(greatGrandchild), 3);
    assert.ok(Option.isNone(yield* grants.read(root)));

    const leaf = yield* grants.read(greatGrandchild);
    assert.ok(Option.isSome(leaf));
    assert.strictEqual(leaf.value.parentThreadId, grandchild);
    assert.strictEqual(leaf.value.depth, 3);
    assert.strictEqual(leaf.value.allowOrchestration, false);
    assert.ok(Number.isFinite(Date.parse(leaf.value.createdAt)));
  }).pipe(Effect.provide(layerInMemory)),
);

it.effect("keeps the first grant when a delegation is recorded again", () =>
  Effect.gen(function* () {
    yield* DelegationGrantsMigration;
    const grants = yield* DelegationGrants.DelegationGrants;

    yield* grants.record({
      childThreadId: child,
      parentThreadId: root,
      depth: 1,
      allowOrchestration: false,
    });
    yield* grants.record({
      childThreadId: child,
      parentThreadId: root,
      depth: 1,
      allowOrchestration: true,
    });

    const grant = yield* grants.read(child);
    assert.ok(Option.isSome(grant));
    assert.strictEqual(grant.value.allowOrchestration, false);
  }).pipe(Effect.provide(layerInMemory)),
);

it.effect("reads grants written before a restart", () => {
  const directory = NodeFS.mkdtempSync(NodePath.join(NodeOS.tmpdir(), "t3-delegation-grants-"));
  const layerFile = () =>
    DelegationGrants.layer.pipe(
      Layer.provideMerge(
        NodeSqliteClient.layer({ filename: NodePath.join(directory, "statev2.sqlite") }),
      ),
    );

  return Effect.gen(function* () {
    yield* Effect.gen(function* () {
      yield* runMigrations();
      yield* delegate(root, child, true);
      yield* delegate(child, grandchild, false);
    }).pipe(Effect.provide(layerFile()));

    yield* Effect.gen(function* () {
      assert.deepStrictEqual(yield* runMigrations(), []);
      const grants = yield* DelegationGrants.DelegationGrants;
      assert.strictEqual(yield* grants.depthOf(grandchild), 2);
      const grant = yield* grants.read(child);
      assert.ok(Option.isSome(grant));
      assert.strictEqual(grant.value.allowOrchestration, true);
    }).pipe(Effect.provide(layerFile()));
  }).pipe(
    Effect.ensuring(Effect.sync(() => NodeFS.rmSync(directory, { recursive: true, force: true }))),
  );
});
