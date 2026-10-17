import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, it } from "@effect/vitest";
import {
  CommandId,
  EventId,
  MessageId,
  ProjectId,
  ProviderDriverKind,
  ProviderInstanceId,
  RuntimeRequestId,
  ThreadId,
  type OrchestrationV2Command,
} from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import type * as ProviderAdapter from "@t3tools/provider-core/server/ProviderAdapter";

import * as ServerConfig from "../../config.ts";
import * as SqlitePersistence from "../../persistence/Sqlite.ts";
import * as ServerSettings from "../../serverSettings.ts";
import { CodexProviderCapabilitiesV2 } from "../Adapters/CodexAdapterV2.ts";
import * as CommandReceiptStore from "../CommandReceiptStore.ts";
import * as EffectOutbox from "../EffectOutbox.ts";
import * as EventStore from "../EventStore.ts";
import * as Orchestrator from "../Orchestrator.ts";
import * as ProjectionStore from "../ProjectionStore.ts";
import * as ProviderAdapterRegistry from "../ProviderAdapterRegistry.ts";
import * as ThreadManagementService from "../ThreadManagementService.ts";
import * as ThreadMessageIntake from "../ThreadMessageIntake.ts";
import * as ProviderReplayHarness from "../testkit/ProviderReplayHarness.ts";

// Ports the fork's v1 idle-admission coverage (decider.heartbeat.test.ts busy
// cases, decider.userMessageAppend.test.ts "integration idle admission", and the
// OrchestrationEngine heartbeat busy/fence cases) to v2 `start_if_idle`.

const instanceId = ProviderInstanceId.make("codex");
const modelSelection = { instanceId, model: "gpt-5.1-codex" };
const adapter = {
  instanceId,
  driver: ProviderDriverKind.make("codex"),
  getCapabilities: () => Effect.succeed(CodexProviderCapabilitiesV2),
  planSelectionTransition: () => Effect.succeed({ type: "apply_on_next_turn" as const }),
  openSession: () => Effect.die("Runs here never reach a provider"),
} as ProviderAdapter.ProviderAdapterV2["Service"];
const layerDatabase = SqlitePersistence.layerMemory;
// No effect worker: a started run stays "starting", which is a blocking run.
const layerTest = ThreadManagementService.layer.pipe(
  Layer.provideMerge(
    Layer.mergeAll(
      layerDatabase,
      ProjectionStore.layer.pipe(Layer.provide(layerDatabase)),
      EventStore.layer.pipe(Layer.provide(layerDatabase)),
      EffectOutbox.layer.pipe(Layer.provide(layerDatabase)),
      ProviderReplayHarness.layerWithRegistry(
        { name: "start-if-idle" },
        ProviderAdapterRegistry.layerFromAdapters([adapter]),
        { databaseLayer: layerDatabase, runEffectWorker: false },
      ),
    ),
  ),
  Layer.provideMerge(ServerSettings.layerTest()),
  Layer.provideMerge(CommandReceiptStore.layer.pipe(Layer.provide(layerDatabase))),
);

// Client intake reads server config only on paths these refusals never reach.
const layerIntake = ServerConfig.layerTest(process.cwd(), { prefix: "t3-start-if-idle-" }).pipe(
  Layer.provideMerge(NodeServices.layer),
);

type MessageDispatch = Extract<OrchestrationV2Command, { readonly type: "message.dispatch" }>;

const createThread = (threadId: ThreadId) =>
  Effect.gen(function* () {
    const orchestrator = yield* Orchestrator.OrchestratorV2;
    return yield* orchestrator.dispatch({
      type: "thread.create",
      commandId: CommandId.make(`create:${threadId}`),
      threadId,
      projectId: ProjectId.make("project:start-if-idle"),
      title: threadId,
      modelSelection,
      runtimeMode: "full-access",
      interactionMode: "default",
      branch: null,
      worktreePath: null,
      createdBy: "user",
      creationSource: "web",
    });
  });

const userMessage = (
  threadId: ThreadId,
  text: string,
  type: "start_immediately" | "queue_after_active" = "start_immediately",
): MessageDispatch => ({
  type: "message.dispatch",
  commandId: CommandId.make(`send:${threadId}:${text}`),
  threadId,
  messageId: MessageId.make(`message:${threadId}:${text}`),
  text,
  attachments: [],
  dispatchMode: { type },
  createdBy: "user",
  creationSource: "web",
});

const heartbeat = (
  threadId: ThreadId,
  occurrence: string,
  fence?: { readonly afterSequence: number; readonly kinds: ReadonlyArray<"stop" | "interrupt"> },
): MessageDispatch => ({
  type: "message.dispatch",
  commandId: CommandId.make(`server:heartbeat:${occurrence}`),
  threadId,
  messageId: MessageId.make(`heartbeat:${occurrence}`),
  text: `heartbeat ${occurrence}`,
  attachments: [],
  dispatchMode: { type: "start_if_idle", ...(fence === undefined ? {} : { fence }) },
  createdBy: "user",
  creationSource: "server",
});

const receiptFor = (commandId: CommandId) =>
  Effect.gen(function* () {
    const receipts = yield* CommandReceiptStore.CommandReceiptStoreV2;
    return Option.getOrNull(yield* receipts.getByCommandId(commandId));
  });

/** Ends the thread's runs that are still blocking, as the provider would. */
const finishActiveRuns = (threadId: ThreadId) =>
  Effect.gen(function* () {
    const orchestrator = yield* Orchestrator.OrchestratorV2;
    const projections = yield* ProjectionStore.ProjectionStoreV2;
    const now = yield* DateTime.now;
    for (const run of (yield* orchestrator.getThreadProjection(threadId)).runs) {
      if (run.status !== "starting" && run.status !== "running") continue;
      yield* projections.apply({
        id: EventId.make(`event:${run.id}:completed`),
        type: "run.updated",
        threadId,
        runId: run.id,
        occurredAt: now,
        payload: { ...run, status: "completed", completedAt: now },
      });
    }
  });

const runStatuses = (threadId: ThreadId) =>
  Effect.gen(function* () {
    const orchestrator = yield* Orchestrator.OrchestratorV2;
    return (yield* orchestrator.getThreadProjection(threadId)).runs.map((run) => run.status);
  });

it.effect("an idle thread starts the message and the receipt replays it", () =>
  Effect.gen(function* () {
    const orchestrator = yield* Orchestrator.OrchestratorV2;
    const threadId = ThreadId.make("thread:start-if-idle:idle");
    yield* createThread(threadId);

    const command = heartbeat(threadId, "idle");
    const started = yield* orchestrator.dispatch(command);
    assert.deepEqual(yield* runStatuses(threadId), ["starting"]);
    const projection = yield* orchestrator.getThreadProjection(threadId);
    assert.equal(projection.runs[0]?.userMessageId, command.messageId);
    assert.equal((yield* receiptFor(command.commandId))?.status, "accepted");

    const replayed = yield* orchestrator.dispatch(command);
    assert.equal(replayed.sequence, started.sequence);
    assert.deepEqual(yield* runStatuses(threadId), ["starting"]);
  }).pipe(Effect.provide(layerTest)),
);

it.effect("a blocking run makes it busy without a receipt, and the same id starts later", () =>
  Effect.gen(function* () {
    const orchestrator = yield* Orchestrator.OrchestratorV2;
    const events = yield* EventStore.EventStoreV2;
    const threadId = ThreadId.make("thread:start-if-idle:blocking");
    yield* createThread(threadId);
    yield* orchestrator.dispatch(userMessage(threadId, "active"));

    const command = heartbeat(threadId, "blocking");
    const before = yield* events.latestSequence({ threadId });
    const busy = yield* orchestrator.dispatch(command).pipe(Effect.flip);
    assert.equal(busy._tag, "OrchestratorThreadBusyError");
    assert.isNull(yield* receiptFor(command.commandId));
    assert.equal(yield* events.latestSequence({ threadId }), before);
    assert.deepEqual(yield* runStatuses(threadId), ["starting"]);

    yield* finishActiveRuns(threadId);
    yield* orchestrator.dispatch(command);
    assert.deepEqual(yield* runStatuses(threadId), ["completed", "starting"]);
    assert.equal((yield* receiptFor(command.commandId))?.status, "accepted");
  }).pipe(Effect.provide(layerTest)),
);

it.effect("queued human work keeps the thread busy after its active run ends", () =>
  Effect.gen(function* () {
    const orchestrator = yield* Orchestrator.OrchestratorV2;
    const threadId = ThreadId.make("thread:start-if-idle:queued");
    yield* createThread(threadId);
    yield* orchestrator.dispatch(userMessage(threadId, "active"));
    yield* orchestrator.dispatch(userMessage(threadId, "human work", "queue_after_active"));
    yield* finishActiveRuns(threadId);
    assert.deepEqual(yield* runStatuses(threadId), ["completed", "queued"]);

    const command = heartbeat(threadId, "queued");
    const busy = yield* orchestrator.dispatch(command).pipe(Effect.flip);
    assert.equal(busy._tag, "OrchestratorThreadBusyError");
    assert.isNull(yield* receiptFor(command.commandId));
  }).pipe(Effect.provide(layerTest)),
);

it.effect("a pending runtime request keeps the thread busy", () =>
  Effect.gen(function* () {
    const orchestrator = yield* Orchestrator.OrchestratorV2;
    const projections = yield* ProjectionStore.ProjectionStoreV2;
    const threadId = ThreadId.make("thread:start-if-idle:request");
    yield* createThread(threadId);
    yield* orchestrator.dispatch(userMessage(threadId, "active"));
    const run = (yield* orchestrator.getThreadProjection(threadId)).runs[0]!;
    yield* finishActiveRuns(threadId);
    const now = yield* DateTime.now;
    yield* projections.apply({
      id: EventId.make("event:start-if-idle:request"),
      type: "runtime-request.updated",
      threadId,
      runId: run.id,
      occurredAt: now,
      payload: {
        id: RuntimeRequestId.make("request:start-if-idle"),
        nodeId: run.rootNodeId!,
        providerTurnId: null,
        nativeRequestRef: null,
        kind: "user_input",
        status: "pending",
        responseCapability: { type: "message" },
        createdAt: now,
        resolvedAt: null,
      },
    });

    const command = heartbeat(threadId, "request");
    const busy = yield* orchestrator.dispatch(command).pipe(Effect.flip);
    assert.equal(busy._tag, "OrchestratorThreadBusyError");
    assert.isNull(yield* receiptFor(command.commandId));
  }).pipe(Effect.provide(layerTest)),
);

it.effect("an archived thread is busy", () =>
  Effect.gen(function* () {
    const orchestrator = yield* Orchestrator.OrchestratorV2;
    const threadId = ThreadId.make("thread:start-if-idle:archived");
    yield* createThread(threadId);
    yield* orchestrator.dispatch({
      type: "thread.archive",
      commandId: CommandId.make("archive:start-if-idle"),
      threadId,
    });

    const command = heartbeat(threadId, "archived");
    const busy = yield* orchestrator.dispatch(command).pipe(Effect.flip);
    assert.equal(busy._tag, "OrchestratorThreadBusyError");
    assert.isNull(yield* receiptFor(command.commandId));
  }).pipe(Effect.provide(layerTest)),
);

it.effect("an interrupt fence rejects a stale relative wakeup with a receipt", () =>
  Effect.gen(function* () {
    const orchestrator = yield* Orchestrator.OrchestratorV2;
    const events = yield* EventStore.EventStoreV2;
    const threadId = ThreadId.make("thread:start-if-idle:interrupt");
    yield* createThread(threadId);
    yield* orchestrator.dispatch(userMessage(threadId, "active"));
    const armedAt = yield* events.latestSequence({ threadId });
    const run = (yield* orchestrator.getThreadProjection(threadId)).runs[0]!;
    yield* orchestrator.dispatch({
      type: "run.interrupt",
      commandId: CommandId.make("interrupt:start-if-idle"),
      threadId,
      runId: run.id,
      holdQueue: true,
    });
    assert.deepEqual(yield* runStatuses(threadId), ["interrupted"]);

    const stale = heartbeat(threadId, "interrupt-stale", {
      afterSequence: armedAt,
      kinds: ["stop", "interrupt"],
    });
    const error = yield* orchestrator.dispatch(stale).pipe(Effect.flip);
    assert.equal(error._tag, "OrchestratorDispatchError");
    assert.equal((yield* receiptFor(stale.commandId))?.status, "rejected");
    const retried = yield* orchestrator.dispatch(stale).pipe(Effect.flip);
    assert.equal(retried._tag, "OrchestratorCommandPreviouslyRejectedError");

    // A cron wakeup fences only on Stop, so an interrupt does not cancel it.
    yield* orchestrator.dispatch(
      heartbeat(threadId, "interrupt-cron", { afterSequence: armedAt, kinds: ["stop"] }),
    );
    assert.deepEqual(yield* runStatuses(threadId), ["interrupted", "starting"]);
    yield* finishActiveRuns(threadId);

    // Re-armed after the interrupt, the relative wakeup starts.
    yield* orchestrator.dispatch(
      heartbeat(threadId, "interrupt-rearmed", {
        afterSequence: yield* events.latestSequence({ threadId }),
        kinds: ["stop", "interrupt"],
      }),
    );
    assert.deepEqual(yield* runStatuses(threadId), ["interrupted", "completed", "starting"]);
  }).pipe(Effect.provide(layerTest)),
);

it.effect("a Stop that ends nothing still fences a wakeup armed at that sequence", () =>
  Effect.gen(function* () {
    const orchestrator = yield* Orchestrator.OrchestratorV2;
    const events = yield* EventStore.EventStoreV2;
    const threadId = ThreadId.make("thread:start-if-idle:idle-stop");
    yield* createThread(threadId);
    const armedAt = yield* events.latestSequence({ threadId });
    const stopped = yield* orchestrator.dispatch({
      type: "thread.stop",
      commandId: CommandId.make("stop:start-if-idle:idle"),
      threadId,
    });
    assert.lengthOf(stopped.storedEvents, 0);
    assert.equal(stopped.sequence, armedAt);

    const stale = heartbeat(threadId, "idle-stop-stale", {
      afterSequence: armedAt,
      kinds: ["stop"],
    });
    const error = yield* orchestrator.dispatch(stale).pipe(Effect.flip);
    assert.equal(error._tag, "OrchestratorDispatchError");
    assert.equal((yield* receiptFor(stale.commandId))?.status, "rejected");
    assert.deepEqual(yield* runStatuses(threadId), []);
  }).pipe(Effect.provide(layerTest)),
);

it.effect("a stop fence holds across an ordinary user turn after the Stop", () =>
  Effect.gen(function* () {
    const orchestrator = yield* Orchestrator.OrchestratorV2;
    const events = yield* EventStore.EventStoreV2;
    const threadId = ThreadId.make("thread:start-if-idle:stop");
    yield* createThread(threadId);
    yield* orchestrator.dispatch(userMessage(threadId, "active"));
    const armedAt = yield* events.latestSequence({ threadId });
    yield* orchestrator.dispatch({
      type: "thread.stop",
      commandId: CommandId.make("stop:start-if-idle"),
      threadId,
    });
    // An ordinary human turn after the Stop must not erase the fence.
    yield* orchestrator.dispatch(userMessage(threadId, "continue after stop"));
    yield* finishActiveRuns(threadId);

    const stale = heartbeat(threadId, "stop-stale", { afterSequence: armedAt, kinds: ["stop"] });
    const error = yield* orchestrator.dispatch(stale).pipe(Effect.flip);
    assert.equal(error._tag, "OrchestratorDispatchError");
    assert.equal((yield* receiptFor(stale.commandId))?.status, "rejected");

    yield* orchestrator.dispatch(
      heartbeat(threadId, "stop-fresh", {
        afterSequence: yield* events.latestSequence({ threadId }),
        kinds: ["stop"],
      }),
    );
    assert.deepEqual(yield* runStatuses(threadId), ["interrupted", "completed", "starting"]);
  }).pipe(Effect.provide(layerTest)),
);

it.effect("a concurrent human message and start_if_idle never both start", () =>
  Effect.gen(function* () {
    const orchestrator = yield* Orchestrator.OrchestratorV2;
    for (const round of [0, 1, 2, 3]) {
      const threadId = ThreadId.make(`thread:start-if-idle:race:${round}`);
      yield* createThread(threadId);
      const [human, admitted] = yield* Effect.all(
        [
          Effect.exit(orchestrator.dispatch(userMessage(threadId, "human"))),
          Effect.exit(orchestrator.dispatch(heartbeat(threadId, `race-${round}`))),
        ],
        { concurrency: "unbounded" },
      );
      assert.equal(human._tag, "Success");
      const statuses = yield* runStatuses(threadId);
      assert.equal(statuses.filter((status) => status === "starting").length, 1);
      if (admitted._tag === "Success") {
        // The heartbeat won the lock; the human message queued behind it.
        assert.deepEqual([...statuses].sort(), ["queued", "starting"]);
      } else {
        assert.deepEqual(statuses, ["starting"]);
      }
    }
  }).pipe(Effect.provide(layerTest)),
);

it.effect("start_if_idle and reserved message ids are refused from non-server origins", () =>
  Effect.gen(function* () {
    const orchestrator = yield* Orchestrator.OrchestratorV2;
    const threadId = ThreadId.make("thread:start-if-idle:origins");
    yield* createThread(threadId);

    const fromWeb: MessageDispatch = {
      ...heartbeat(threadId, "web"),
      createdBy: "user",
      creationSource: "web",
    };
    const webError = yield* orchestrator.dispatch(fromWeb).pipe(Effect.flip);
    assert.equal(webError._tag, "OrchestratorDispatchError");

    for (const messageId of ["heartbeat:forged", "signal-forged"]) {
      const fromMcp: MessageDispatch = {
        ...userMessage(threadId, messageId),
        messageId: MessageId.make(messageId),
        createdBy: "agent",
        creationSource: "mcp",
      };
      const mcpError = yield* orchestrator.dispatch(fromMcp).pipe(Effect.flip);
      assert.equal(mcpError._tag, "OrchestratorDispatchError");
    }

    // Clients choose their own creationSource, so intake refuses even "server".
    for (const command of [
      heartbeat(threadId, "client"),
      {
        ...userMessage(threadId, "client signal"),
        messageId: MessageId.make("signal-client"),
        creationSource: "server" as const,
      },
    ]) {
      const intakeError = yield* ThreadMessageIntake.dispatchCommand(command).pipe(
        Effect.provide(layerIntake),
        Effect.flip,
      );
      assert.equal(intakeError._tag, "OrchestratorDispatchError");
      assert.isNull(yield* receiptFor(command.commandId));
    }
    assert.deepEqual(yield* runStatuses(threadId), []);
  }).pipe(Effect.provide(layerTest)),
);
