import { assert, it } from "@effect/vitest";
import { EnvironmentId, ProviderInstanceId, ThreadId } from "@t3tools/contracts";
import * as NodeSqliteClient from "@t3tools/shared/nodeSqliteClient";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";

import type { McpCapability, McpInvocationScope } from "../../mcp/McpInvocationContext.ts";
import DelegationGrantsMigration from "../../persistence/ForkMigrations/004_DelegationGrants.ts";
import * as ServerSettings from "../../serverSettings.ts";
import * as DelegationGrants from "./DelegationGrants.ts";
import * as DelegationPolicy from "./DelegationPolicy.ts";

const root = ThreadId.make("root");
const child = ThreadId.make("child");
const grandchild = ThreadId.make("grandchild");
const greatGrandchild = ThreadId.make("great-grandchild");

const scopeFor = (
  threadId: ThreadId,
  capabilities: ReadonlyArray<McpCapability> = ["orchestration", "delegation"],
): McpInvocationScope => ({
  environmentId: EnvironmentId.make("environment"),
  requestNamespace: `session:${threadId}`,
  thread: {
    threadId,
    providerSessionId: `session:${threadId}`,
    providerInstanceId: ProviderInstanceId.make("codex"),
  },
  client: undefined,
  capabilities: new Set(capabilities),
  issuedAt: 1,
});

const layerPolicy = (maxDepth: number) =>
  DelegationPolicy.layer.pipe(
    Layer.provideMerge(DelegationGrants.layer),
    Layer.provideMerge(NodeSqliteClient.layer({ filename: ":memory:" })),
    Layer.provide(
      ServerSettings.layerTest({ crossProviderAgentMaxDepth: maxDepth }).pipe(Layer.orDie),
    ),
  );

const admit = (input: {
  readonly parent: ThreadId;
  readonly child: ThreadId;
  readonly allowOrchestration: boolean;
  readonly capabilities?: ReadonlyArray<McpCapability>;
  readonly replay?: boolean;
}) =>
  Effect.gen(function* () {
    const policy = yield* DelegationPolicy.DelegationPolicy;
    return yield* policy
      .admit({
        scope: scopeFor(input.parent, input.capabilities),
        parentThreadId: input.parent,
        childThreadId: input.child,
        allowOrchestration: input.allowOrchestration,
        replay: input.replay ?? false,
      })
      .pipe(Effect.flip, Effect.option);
  });

it.effect("records a native child of a root at depth 1 without orchestration", () =>
  Effect.gen(function* () {
    yield* DelegationGrantsMigration;
    const grants = yield* DelegationGrants.DelegationGrants;

    assert.ok(Option.isNone(yield* admit({ parent: root, child, allowOrchestration: false })));

    const grant = yield* grants.read(child);
    assert.ok(Option.isSome(grant));
    assert.strictEqual(grant.value.parentThreadId, root);
    assert.strictEqual(grant.value.depth, 1);
    assert.strictEqual(grant.value.allowOrchestration, false);
  }).pipe(Effect.provide(layerPolicy(2))),
);

it.effect("refuses a caller without the delegation capability and records nothing", () =>
  Effect.gen(function* () {
    yield* DelegationGrantsMigration;
    const grants = yield* DelegationGrants.DelegationGrants;

    const refused = yield* admit({
      parent: child,
      child: grandchild,
      allowOrchestration: false,
      capabilities: ["orchestration"],
    });
    assert.ok(Option.isSome(refused));
    assert.strictEqual(refused.value.code, "capability_denied");
    assert.ok(Option.isNone(yield* grants.read(grandchild)));
  }).pipe(Effect.provide(layerPolicy(2))),
);

it.effect("applies the depth cap the way 0.0.45 spawns did", () =>
  Effect.gen(function* () {
    yield* DelegationGrantsMigration;
    const grants = yield* DelegationGrants.DelegationGrants;

    // Depth 1 sub-orchestrator under a cap of 2 can still spawn leaves at depth 2.
    assert.ok(Option.isNone(yield* admit({ parent: root, child, allowOrchestration: true })));

    // A sub-orchestrator at depth 2 could never spawn within the cap.
    const orchestrator = yield* admit({
      parent: child,
      child: grandchild,
      allowOrchestration: true,
    });
    assert.ok(Option.isSome(orchestrator));
    assert.strictEqual(orchestrator.value.code, "depth_exceeded");
    assert.include(orchestrator.value.message, "depth 2");
    assert.ok(Option.isNone(yield* grants.read(grandchild)));

    // A leaf at depth 2 fits.
    assert.ok(
      Option.isNone(yield* admit({ parent: child, child: grandchild, allowOrchestration: false })),
    );
    assert.strictEqual(yield* grants.depthOf(grandchild), 2);

    // Depth 3 exceeds the cap even for a leaf.
    const tooDeep = yield* admit({
      parent: grandchild,
      child: greatGrandchild,
      allowOrchestration: false,
    });
    assert.ok(Option.isSome(tooDeep));
    assert.strictEqual(tooDeep.value.code, "depth_exceeded");
    assert.include(tooDeep.value.message, "exceeds the depth cap of 2");
  }).pipe(Effect.provide(layerPolicy(2))),
);

it.effect("lets a replay of an accepted delegation through after the cap was lowered", () =>
  Effect.gen(function* () {
    yield* DelegationGrantsMigration;
    const grants = yield* DelegationGrants.DelegationGrants;

    const fresh = yield* admit({ parent: root, child, allowOrchestration: true });
    assert.ok(Option.isSome(fresh));
    assert.strictEqual(fresh.value.code, "depth_exceeded");

    assert.ok(
      Option.isNone(yield* admit({ parent: root, child, allowOrchestration: true, replay: true })),
    );
    assert.ok(Option.isNone(yield* grants.read(child)));

    const deniedReplay = yield* admit({
      parent: root,
      child,
      allowOrchestration: false,
      capabilities: ["orchestration"],
      replay: true,
    });
    assert.ok(Option.isSome(deniedReplay));
    assert.strictEqual(deniedReplay.value.code, "capability_denied");
  }).pipe(Effect.provide(layerPolicy(1))),
);
