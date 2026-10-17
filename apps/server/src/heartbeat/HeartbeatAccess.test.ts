import { assert, it } from "@effect/vitest";
import { DEFAULT_SERVER_SETTINGS, ProjectId, ThreadId } from "@t3tools/contracts";
import { applyServerSettingsPatch } from "@t3tools/shared/serverSettings";
import * as NodeSqliteClient from "@t3tools/shared/nodeSqliteClient";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";

import DelegationGrantsMigration from "../persistence/ForkMigrations/004_DelegationGrants.ts";
import * as DelegationGrants from "../orchestration-v2/fork/DelegationGrants.ts";
import { heartbeatAccessAllowed } from "./HeartbeatAccess.ts";

const projectId = ProjectId.make("heartbeat-project");
const root = ThreadId.make("root");
const fork = ThreadId.make("fork");
const orchestrator = ThreadId.make("orchestrating-child");
const leaf = ThreadId.make("leaf-child");
const ungranted = ThreadId.make("ungranted-child");
const native = ThreadId.make("native-subagent");

const layerInMemory = DelegationGrants.layer.pipe(
  Layer.provideMerge(NodeSqliteClient.layer({ filename: ":memory:" })),
);

const thread = (
  id: ThreadId,
  lineage: {
    readonly parentThreadId: ThreadId | null;
    readonly relationshipToParent: "fork" | "subagent" | null;
  },
  creationSource: "web" | "mcp" | "provider" = "web",
) => ({
  id,
  projectId,
  lineage: { ...lineage, rootThreadId: root },
  creationSource,
});

const seedGrants = Effect.gen(function* () {
  yield* DelegationGrantsMigration;
  const grants = yield* DelegationGrants.DelegationGrants;
  yield* grants.record({
    childThreadId: orchestrator,
    parentThreadId: root,
    depth: 1,
    allowOrchestration: true,
  });
  yield* grants.record({
    childThreadId: leaf,
    parentThreadId: root,
    depth: 1,
    allowOrchestration: false,
  });
});

it.effect("allows roots and orchestrating children, denies leaves and native subagents", () =>
  Effect.gen(function* () {
    yield* seedGrants;
    const allowed = (input: ReturnType<typeof thread>) =>
      heartbeatAccessAllowed(DEFAULT_SERVER_SETTINGS, input);

    assert.isTrue(
      yield* allowed(thread(root, { parentThreadId: null, relationshipToParent: null })),
    );
    assert.isTrue(
      yield* allowed(thread(fork, { parentThreadId: root, relationshipToParent: "fork" })),
    );
    assert.isTrue(
      yield* allowed(
        thread(orchestrator, { parentThreadId: root, relationshipToParent: "subagent" }, "mcp"),
      ),
    );
    assert.isFalse(
      yield* allowed(
        thread(leaf, { parentThreadId: root, relationshipToParent: "subagent" }, "mcp"),
      ),
    );
    assert.isFalse(
      yield* allowed(
        thread(ungranted, { parentThreadId: root, relationshipToParent: "subagent" }, "mcp"),
      ),
    );
    assert.isFalse(
      yield* allowed(
        thread(native, { parentThreadId: root, relationshipToParent: "subagent" }, "provider"),
      ),
    );
  }).pipe(Effect.provide(layerInMemory)),
);

it.effect("denies every thread in a project whose setting is off", () =>
  Effect.gen(function* () {
    yield* seedGrants;
    const settings = applyServerSettingsPatch(DEFAULT_SERVER_SETTINGS, {
      projectSettingsOverrides: { [projectId]: { enableHeartbeatAccess: false } },
    });

    assert.isFalse(
      yield* heartbeatAccessAllowed(
        settings,
        thread(root, { parentThreadId: null, relationshipToParent: null }),
      ),
    );
    assert.isFalse(
      yield* heartbeatAccessAllowed(
        settings,
        thread(orchestrator, { parentThreadId: root, relationshipToParent: "subagent" }, "mcp"),
      ),
    );
  }).pipe(Effect.provide(layerInMemory)),
);
