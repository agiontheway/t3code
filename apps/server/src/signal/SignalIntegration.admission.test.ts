// @effect-diagnostics-next-line nodeBuiltinImport:off -- Effect's Crypto has no sync sha256.
import * as NodeCrypto from "node:crypto";
import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, it } from "@effect/vitest";
import {
  CommandId,
  EnvironmentId,
  EventId,
  MessageId,
  ProjectId,
  ProviderDriverKind,
  ProviderInstanceId,
  ServerProvider,
  ThreadId,
} from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import type * as ProviderAdapter from "@t3tools/provider-core/server/ProviderAdapter";

import { ServerConfig } from "../config.ts";
import { ServerEnvironment } from "../environment/ServerEnvironment.ts";
import * as SqlitePersistence from "../persistence/Sqlite.ts";
import { ProjectService } from "../project/ProjectService.ts";
import { ProviderRegistry } from "../provider/ProviderRegistry.ts";
import * as ServerSettings from "../serverSettings.ts";
import { CodexProviderCapabilitiesV2 } from "../orchestration-v2/Adapters/CodexAdapterV2.ts";
import * as CommandReceiptStore from "../orchestration-v2/CommandReceiptStore.ts";
import * as EffectOutbox from "../orchestration-v2/EffectOutbox.ts";
import * as EventStore from "../orchestration-v2/EventStore.ts";
import * as Orchestrator from "../orchestration-v2/Orchestrator.ts";
import * as ProjectionStore from "../orchestration-v2/ProjectionStore.ts";
import * as ProviderAdapterRegistry from "../orchestration-v2/ProviderAdapterRegistry.ts";
import * as ThreadManagementService from "../orchestration-v2/ThreadManagementService.ts";
import * as ProviderReplayHarness from "../orchestration-v2/testkit/ProviderReplayHarness.ts";
import * as SignalIntegration from "./SignalIntegration.ts";

// Signal delivery against the real v2 orchestrator: a busy thread answers
// `thread_busy` without recording the command, and the worker's retry with the
// same request id starts exactly one run.

const instanceId = ProviderInstanceId.make("codex");
const adapter = {
  instanceId,
  driver: ProviderDriverKind.make("codex"),
  getCapabilities: () => Effect.succeed(CodexProviderCapabilitiesV2),
  planSelectionTransition: () => Effect.succeed({ type: "apply_on_next_turn" as const }),
  openSession: () => Effect.die("Runs here never reach a provider"),
} as ProviderAdapter.ProviderAdapterV2["Service"];
const layerDatabase = SqlitePersistence.layerMemory;
// No effect worker: a started run stays "starting", which is a blocking run.
const layerOrchestration = ThreadManagementService.layer.pipe(
  Layer.provideMerge(
    Layer.mergeAll(
      layerDatabase,
      ProjectionStore.layer.pipe(Layer.provide(layerDatabase)),
      EventStore.layer.pipe(Layer.provide(layerDatabase)),
      EffectOutbox.layer.pipe(Layer.provide(layerDatabase)),
      ProviderReplayHarness.layerWithRegistry(
        { name: "signal-admission" },
        ProviderAdapterRegistry.layerFromAdapters([adapter]),
        { databaseLayer: layerDatabase, runEffectWorker: false },
      ),
    ),
  ),
  Layer.provideMerge(ServerSettings.layerTest()),
  Layer.provideMerge(CommandReceiptStore.layer.pipe(Layer.provide(layerDatabase))),
);

const NOW = "2026-10-13T00:00:00.000Z";
const provider = Schema.decodeSync(ServerProvider)({
  instanceId: "codex",
  driver: "codex",
  enabled: true,
  installed: true,
  version: null,
  status: "ready",
  auth: { status: "authenticated" },
  checkedAt: NOW,
  models: [],
  skills: [{ name: "review-risk", path: "/test/SKILL.md", enabled: true }],
});

it.effect("a busy thread answers thread_busy, and the same request starts once idle", () =>
  Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    const stateDir = yield* fs.makeTempDirectoryScoped();
    yield* Effect.gen(function* () {
      const orchestrator = yield* Orchestrator.OrchestratorV2;
      const projections = yield* ProjectionStore.ProjectionStoreV2;
      const receipts = yield* CommandReceiptStore.CommandReceiptStoreV2;
      const signal = yield* SignalIntegration.SignalIntegration;
      const threadId = ThreadId.make("thread:signal");
      yield* orchestrator.dispatch({
        type: "thread.create",
        commandId: CommandId.make("create:signal"),
        threadId,
        projectId: ProjectId.make("project:signal"),
        title: "Signal",
        modelSelection: { instanceId, model: "gpt-5.1-codex" },
        runtimeMode: "full-access",
        interactionMode: "default",
        branch: null,
        worktreePath: null,
        createdBy: "user",
        creationSource: "web",
      });
      yield* orchestrator.dispatch({
        type: "message.dispatch",
        commandId: CommandId.make("send:human"),
        threadId,
        messageId: MessageId.make("message:human"),
        text: "human work",
        attachments: [],
        dispatchMode: { type: "start_immediately" },
        createdBy: "user",
        creationSource: "web",
      });

      const configured = yield* signal.configure({
        commands: [{ command: "/reviews", threadId, skill: "review-risk" }],
      });
      const requestId = "f".repeat(64);
      const input = {
        connectionId: configured.binding.connectionId,
        environmentId: "environment",
        requestId,
        command: "/reviews",
      };
      const key = NodeCrypto.createHash("sha256")
        .update(`${configured.binding.connectionId}:${requestId}`)
        .digest("hex");
      const commandId = CommandId.make(`signal-${key}`);

      const busy = yield* signal.trigger(input, configured.token).pipe(Effect.flip);
      assert.equal(busy.reason, "thread_busy");
      assert.isTrue(Option.isNone(yield* receipts.getByCommandId(commandId)));

      // The human run ends, as the provider would report it.
      const now = yield* DateTime.now;
      for (const run of (yield* orchestrator.getThreadProjection(threadId)).runs) {
        yield* projections.apply({
          id: EventId.make(`event:${run.id}:completed`),
          type: "run.updated",
          threadId,
          runId: run.id,
          occurredAt: now,
          payload: { ...run, status: "completed", completedAt: now },
        });
      }

      const accepted = yield* signal.trigger(input, configured.token);
      assert.deepEqual(accepted, { threadId, messageId: `signal-${key}` });
      const replayed = yield* signal.trigger(input, configured.token);
      assert.deepEqual(replayed, accepted);
      const runs = (yield* orchestrator.getThreadProjection(threadId)).runs;
      assert.deepEqual(
        runs.map((run) => [run.status, run.userMessageId]),
        [
          ["completed", "message:human"],
          ["starting", `signal-${key}`],
        ],
      );
      const receipt = yield* receipts.getByCommandId(commandId);
      assert.equal(Option.getOrNull(receipt)?.status, "accepted");
    }).pipe(
      Effect.provide(
        SignalIntegration.layer.pipe(
          Layer.provideMerge(layerOrchestration),
          Layer.provide(
            Layer.mergeAll(
              ServerConfig.layerTest(stateDir, stateDir),
              Layer.mock(ServerEnvironment)({
                getEnvironmentId: Effect.succeed(EnvironmentId.make("environment")),
              }),
              Layer.mock(ProjectService)({
                getById: (id) => Effect.succeedSome({ id, workspaceRoot: "/tmp/project" } as never),
              }),
              Layer.mock(ProviderRegistry)({
                refreshWorkspaceSnapshot: () => Effect.succeed([provider]),
              }),
            ),
          ),
        ),
      ),
    );
  }).pipe(Effect.provide(NodeServices.layer)),
);
