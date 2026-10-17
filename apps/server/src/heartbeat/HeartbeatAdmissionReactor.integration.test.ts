import { assert, it } from "@effect/vitest";
import {
  CommandId,
  HeartbeatJobId,
  HeartbeatOccurrenceId,
  HeartbeatPrompt,
  MessageId,
  ProjectId,
  ProviderDriverKind,
  ProviderInstanceId,
  ThreadId,
  type HeartbeatOccurrenceOutcome,
  type OrchestrationV2StoredEvent,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Ref from "effect/Ref";
import type * as ProviderAdapter from "@t3tools/provider-core/server/ProviderAdapter";

import { CodexProviderCapabilitiesV2 } from "../orchestration-v2/Adapters/CodexAdapterV2.ts";
import * as CommandReceiptStore from "../orchestration-v2/CommandReceiptStore.ts";
import * as EffectOutbox from "../orchestration-v2/EffectOutbox.ts";
import { EventSinkV2 } from "../orchestration-v2/EventSink.ts";
import * as EventStore from "../orchestration-v2/EventStore.ts";
import { DelegationGrants } from "../orchestration-v2/fork/DelegationGrants.ts";
import * as Orchestrator from "../orchestration-v2/Orchestrator.ts";
import * as ProjectionStore from "../orchestration-v2/ProjectionStore.ts";
import * as ProviderAdapterRegistry from "../orchestration-v2/ProviderAdapterRegistry.ts";
import * as ProviderReplayHarness from "../orchestration-v2/testkit/ProviderReplayHarness.ts";
import * as ThreadManagementService from "../orchestration-v2/ThreadManagementService.ts";
import * as OrchestrationCommandReceipts from "../persistence/OrchestrationCommandReceipts.ts";
import * as SqlitePersistence from "../persistence/Sqlite.ts";
import * as ServerSettings from "../serverSettings.ts";
import * as HeartbeatAdmissionReactor from "./HeartbeatAdmissionReactor.ts";
import * as HeartbeatReactor from "./HeartbeatReactor.ts";
import * as HeartbeatRepository from "./HeartbeatRepository.ts";
import { HeartbeatScheduler, type ReservedHeartbeatOccurrence } from "./HeartbeatService.ts";

// The admission reactor against the real v2 orchestrator: start_if_idle
// admission, the receipts it leaves, and the events Stop and interrupt write.

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
        { name: "heartbeat-admission" },
        ProviderAdapterRegistry.layerFromAdapters([adapter]),
        { databaseLayer: layerDatabase, runEffectWorker: false },
      ),
    ),
  ),
  Layer.provideMerge(ServerSettings.layerTest()),
  Layer.provideMerge(CommandReceiptStore.layer.pipe(Layer.provide(layerDatabase))),
  Layer.provideMerge(
    Layer.mergeAll(HeartbeatRepository.layer, OrchestrationCommandReceipts.layer).pipe(
      Layer.provide(layerDatabase),
    ),
  ),
);

type Recorded =
  | { readonly method: "outcome"; readonly outcome: HeartbeatOccurrenceOutcome }
  | { readonly method: "activate" }
  | {
      readonly method: "cancelRelative" | "cancelThread";
      readonly beforeSequence: number | undefined;
    };

const makeReactor = Effect.gen(function* () {
  const recorded = yield* Ref.make<ReadonlyArray<Recorded>>([]);
  const push = (entry: Recorded) => Ref.update(recorded, (current) => [...current, entry]);
  const layerHeartbeat = Layer.mergeAll(
    Layer.mock(HeartbeatScheduler)({
      withValidOccurrence: (_occurrenceId, admission) => admission.pipe(Effect.asSome),
      setOccurrenceOutcome: (_occurrenceId, outcome) => push({ method: "outcome", outcome }),
      activateOnAcceptedTurn: () => push({ method: "activate" }),
      cancelRelative: (_threadId, _reason, beforeSequence) =>
        push({ method: "cancelRelative", beforeSequence }),
      cancelThread: (_threadId, _reason, beforeSequence) =>
        push({ method: "cancelThread", beforeSequence }),
    }),
    Layer.mock(HeartbeatReactor.HeartbeatReactor)({}),
    Layer.mock(EventSinkV2)({}),
    Layer.mock(DelegationGrants)({ read: () => Effect.succeedNone }),
  );
  const reactor = yield* HeartbeatAdmissionReactor.make.pipe(Effect.provide(layerHeartbeat));
  return { reactor, recorded };
});

const createThread = (threadId: ThreadId) =>
  Effect.gen(function* () {
    const orchestrator = yield* Orchestrator.OrchestratorV2;
    yield* orchestrator.dispatch({
      type: "thread.create",
      commandId: CommandId.make(`create:${threadId}`),
      threadId,
      projectId: ProjectId.make("project:heartbeat-admission"),
      title: threadId,
      modelSelection: { instanceId, model: "gpt-5.1-codex" },
      runtimeMode: "full-access",
      interactionMode: "default",
      branch: null,
      worktreePath: null,
      createdBy: "user",
      creationSource: "web",
    });
  });

const sendUserMessage = (threadId: ThreadId, text: string) =>
  Effect.flatMap(Orchestrator.OrchestratorV2, (orchestrator) =>
    orchestrator.dispatch({
      type: "message.dispatch",
      commandId: CommandId.make(`send:${threadId}:${text}`),
      threadId,
      messageId: MessageId.make(`message:${threadId}:${text}`),
      text,
      attachments: [],
      dispatchMode: { type: "start_immediately" },
      createdBy: "user",
      creationSource: "web",
    }),
  );

/** A reservation armed now, at the thread's current event head. */
const armed = (threadId: ThreadId, name: string, kind: "cron" | "relative") =>
  Effect.gen(function* () {
    const repository = yield* HeartbeatRepository.HeartbeatRepository;
    return {
      occurrenceId: HeartbeatOccurrenceId.make(`occurrence-${name}`),
      threadId,
      publicId: HeartbeatJobId.make("deadbeef"),
      prompt: HeartbeatPrompt.make(`  heartbeat ${name}\n`),
      generation: 1,
      nominalAtMs: 0,
      dueAtMs: 0,
      reservedAtMs: 0,
      kind,
      noop: false,
      originSequence: yield* repository.latestThreadSequence(threadId),
    } satisfies ReservedHeartbeatOccurrence;
  });

const replay = (
  reactor: HeartbeatAdmissionReactor.HeartbeatAdmissionReactor["Service"],
  events: ReadonlyArray<OrchestrationV2StoredEvent>,
) => Effect.forEach(events, reactor.handleStoredEvent, { discard: true });

it.effect("starts an idle thread with the prompt as a user message and defers while busy", () =>
  Effect.gen(function* () {
    const { reactor, recorded } = yield* makeReactor;
    const orchestrator = yield* Orchestrator.OrchestratorV2;
    const threadId = ThreadId.make("thread:heartbeat-admission:idle");
    yield* createThread(threadId);

    const first = yield* armed(threadId, "idle", "cron");
    assert.equal(yield* reactor.attemptReserved(first), "complete");
    const projection = yield* orchestrator.getThreadProjection(threadId);
    const message = projection.messages.find(
      (candidate) => candidate.id === `heartbeat:${first.occurrenceId}`,
    );
    assert.equal(message?.text, "  heartbeat idle\n");
    assert.equal(message?.role, "user");
    assert.equal(message?.createdBy, "user");
    assert.deepEqual(
      projection.runs.map((run) => [run.status, run.userMessageId]),
      [["starting", `heartbeat:${first.occurrenceId}`]],
    );

    // The heartbeat's own run is now blocking, so the next one waits.
    const second = yield* armed(threadId, "busy", "cron");
    assert.equal(yield* reactor.attemptReserved(second), "defer");
    const receipts = yield* CommandReceiptStore.CommandReceiptStoreV2;
    assert.isTrue(
      Option.isNone(
        yield* receipts.getByCommandId(CommandId.make(`server:heartbeat:${second.occurrenceId}`)),
      ),
    );
    assert.deepEqual(yield* Ref.get(recorded), [
      { method: "outcome", outcome: { status: "admitted" } },
    ]);
  }).pipe(Effect.provide(layerOrchestration)),
);

it.effect("a Stop that wrote no event cancels the thread's jobs instead of delivering", () =>
  Effect.gen(function* () {
    const { reactor, recorded } = yield* makeReactor;
    const orchestrator = yield* Orchestrator.OrchestratorV2;
    const threadId = ThreadId.make("thread:heartbeat-admission:idle-stop");
    yield* createThread(threadId);
    const occurrence = yield* armed(threadId, "idle-stop", "cron");
    const stopped = yield* orchestrator.dispatch({
      type: "thread.stop",
      commandId: CommandId.make("stop:heartbeat-admission:idle"),
      threadId,
    });
    assert.lengthOf(stopped.storedEvents, 0);

    assert.equal(yield* reactor.attemptReserved(occurrence), "complete");
    assert.deepEqual((yield* orchestrator.getThreadProjection(threadId)).runs, []);
    assert.deepEqual(yield* Ref.get(recorded), [
      { method: "cancelThread", beforeSequence: occurrence.originSequence + 1 },
      { method: "outcome", outcome: { status: "canceled" } },
    ]);

    // A job armed after that Stop is delivered once the thread has moved on.
    yield* sendUserMessage(threadId, "wake up");
    yield* Ref.set(recorded, []);
    const runs = (yield* orchestrator.getThreadProjection(threadId)).runs;
    yield* orchestrator.dispatch({
      type: "run.interrupt",
      commandId: CommandId.make("interrupt:heartbeat-admission:idle-stop"),
      threadId,
      runId: runs[0]!.id,
      holdQueue: true,
    });
    const later = yield* armed(threadId, "after-stop", "cron");
    assert.equal(yield* reactor.attemptReserved(later), "complete");
    assert.deepEqual(yield* Ref.get(recorded), [
      { method: "outcome", outcome: { status: "admitted" } },
    ]);
  }).pipe(Effect.provide(layerOrchestration)),
);

it.effect(
  "a job armed at an event-carrying Stop's sequence is removed after the fence refuses it",
  () =>
    Effect.gen(function* () {
      const { reactor, recorded } = yield* makeReactor;
      const orchestrator = yield* Orchestrator.OrchestratorV2;
      const threadId = ThreadId.make("thread:heartbeat-admission:stop-tie");
      yield* createThread(threadId);
      yield* sendUserMessage(threadId, "active");
      const stopped = yield* orchestrator.dispatch({
        type: "thread.stop",
        commandId: CommandId.make("stop:heartbeat-admission:tie"),
        threadId,
      });
      assert.isAbove(stopped.storedEvents.length, 0);

      // Armed at the Stop's last event, which is still the thread head.
      const occurrence = yield* armed(threadId, "stop-tie", "cron");
      assert.equal(occurrence.originSequence, stopped.sequence);
      assert.equal(yield* reactor.attemptReserved(occurrence), "complete");
      assert.deepEqual(yield* Ref.get(recorded), [
        { method: "outcome", outcome: { status: "failed" } },
        { method: "cancelThread", beforeSequence: occurrence.originSequence + 1 },
      ]);
    }).pipe(Effect.provide(layerOrchestration)),
);

it.effect("the events of an interrupt and of a Stop cancel relative and all schedules", () =>
  Effect.gen(function* () {
    const { reactor, recorded } = yield* makeReactor;
    const orchestrator = yield* Orchestrator.OrchestratorV2;
    const threadId = ThreadId.make("thread:heartbeat-admission:lifecycle");
    yield* createThread(threadId);

    yield* replay(reactor, (yield* sendUserMessage(threadId, "first turn")).storedEvents);
    assert.deepEqual(yield* Ref.get(recorded), [{ method: "activate" }]);
    yield* Ref.set(recorded, []);

    const relativeOrigin = (yield* armed(threadId, "relative", "relative")).originSequence;
    const run = (yield* orchestrator.getThreadProjection(threadId)).runs[0]!;
    const interrupted = yield* orchestrator.dispatch({
      type: "run.interrupt",
      commandId: CommandId.make("interrupt:heartbeat-admission"),
      threadId,
      runId: run.id,
      holdQueue: true,
    });
    yield* replay(reactor, interrupted.storedEvents);
    const afterInterrupt = yield* Ref.get(recorded);
    assert.isAbove(afterInterrupt.length, 0);
    for (const entry of afterInterrupt) {
      assert.equal(entry.method, "cancelRelative");
      if (entry.method === "cancelRelative") assert.isAbove(entry.beforeSequence!, relativeOrigin);
    }
    yield* Ref.set(recorded, []);

    yield* replay(reactor, (yield* sendUserMessage(threadId, "second turn")).storedEvents);
    yield* Ref.set(recorded, []);
    const stopOrigin = (yield* armed(threadId, "cron", "cron")).originSequence;
    const stopped = yield* orchestrator.dispatch({
      type: "thread.stop",
      commandId: CommandId.make("stop:heartbeat-admission:active"),
      threadId,
    });
    yield* replay(reactor, stopped.storedEvents);
    const afterStop = yield* Ref.get(recorded);
    assert.isAbove(afterStop.length, 0);
    for (const entry of afterStop) {
      assert.equal(entry.method, "cancelThread");
      if (entry.method === "cancelThread") assert.isAbove(entry.beforeSequence!, stopOrigin);
    }
  }).pipe(Effect.provide(layerOrchestration)),
);
