import {
  DEFAULT_SERVER_SETTINGS,
  OrchestrationThreadShell,
  HeartbeatJobId,
  HeartbeatOccurrenceId,
  HeartbeatPrompt,
  ProjectId,
  ThreadId,
  type HeartbeatOccurrenceOutcome,
  type OrchestrationCommand,
} from "@t3tools/contracts";
import { assert, it } from "@effect/vitest";
import * as Cause from "effect/Cause";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as Ref from "effect/Ref";
import * as Stream from "effect/Stream";
import * as Schema from "effect/Schema";
import { TestClock } from "effect/testing";

import {
  HeartbeatThreadBusyError,
  OrchestrationCommandInvariantError,
} from "../orchestration/Errors.ts";
import { OrchestrationEngineService } from "../orchestration/Services/OrchestrationEngine.ts";
import * as ProjectionSnapshotQuery from "../orchestration/Services/ProjectionSnapshotQuery.ts";
import * as ServerSettings from "../serverSettings.ts";
import * as HeartbeatAdmissionReactor from "./HeartbeatAdmissionReactor.ts";
import * as HeartbeatReactor from "./HeartbeatReactor.ts";
import { HeartbeatScheduler, type ReservedHeartbeatOccurrence } from "./HeartbeatService.ts";

const THREAD_ID = ThreadId.make("heartbeat-admission-thread");
const OCCURRENCE_ID = HeartbeatOccurrenceId.make("heartbeat-admission-occurrence");
const NOW = "2026-09-17T10:00:00.000Z";
const decodeThreadShell = Schema.decodeUnknownSync(OrchestrationThreadShell);

const occurrence: ReservedHeartbeatOccurrence = {
  occurrenceId: OCCURRENCE_ID,
  threadId: THREAD_ID,
  publicId: HeartbeatJobId.make("deadbeef"),
  prompt: HeartbeatPrompt.make("continue the authorized work"),
  generation: 1,
  nominalAtMs: Date.parse(NOW),
  dueAtMs: Date.parse(NOW),
  reservedAtMs: Date.parse(NOW),
  kind: "relative",
  noop: false,
  originSequence: 0,
};

const makeAdmission = (dispatch: OrchestrationEngineService["Service"]["dispatch"]) =>
  Effect.gen(function* () {
    const outcomes = yield* Ref.make<ReadonlyArray<HeartbeatOccurrenceOutcome>>([]);
    const scheduler = Layer.mock(HeartbeatScheduler)({
      schedulerSnapshot: Effect.die("unused scheduler snapshot"),
      reserveDue: () => Effect.die("unused reserve"),
      subscribeChanges: Effect.die("unused changes"),
      pruneExpired: Effect.die("unused prune"),
      validateOccurrence: () => Effect.succeed(true),
      withValidOccurrence: (_occurrenceId, admission) => admission.pipe(Effect.asSome),
      setOccurrenceOutcome: (_occurrenceId, outcome) =>
        Ref.update(outcomes, (current) => [...current, outcome]),
      activateOnAcceptedTurn: () => Effect.void,
      cancelRelative: () => Effect.void,
      cancelThread: () => Effect.void,
    });
    const layers = Layer.mergeAll(
      scheduler,
      Layer.mock(HeartbeatReactor.HeartbeatReactor)({
        start: () => Effect.die("unused reactor start"),
      }),
      Layer.mock(OrchestrationEngineService)({ dispatch }),
      Layer.mock(ProjectionSnapshotQuery.ProjectionSnapshotQuery)({
        getThreadShellById: () =>
          Effect.succeedSome(
            decodeThreadShell({
              id: THREAD_ID,
              projectId: ProjectId.make("heartbeat-admission-project"),
              title: "Heartbeat admission test",
              modelSelection: { instanceId: "codex", model: "gpt-5" },
              runtimeMode: "full-access",
              branch: null,
              worktreePath: null,
              latestTurn: null,
              createdAt: NOW,
              updatedAt: NOW,
              session: null,
              latestUserMessageAt: null,
              hasPendingApprovals: false,
              hasPendingUserInput: false,
              hasActionableProposedPlan: false,
            }),
          ),
      }),
      Layer.mock(ServerSettings.ServerSettingsService)({
        getSettings: Effect.succeed(DEFAULT_SERVER_SETTINGS),
        streamChanges: Stream.empty,
      }),
    );
    const service = yield* HeartbeatAdmissionReactor.make.pipe(Effect.provide(layers));
    return { service, outcomes } as const;
  });

it.effect("classifies a typed busy failure as retryable and keeps the command ID stable", () =>
  Effect.gen(function* () {
    yield* TestClock.setTime(Date.parse(NOW));
    const firstAttempt = yield* Deferred.make<void>();
    const commands = yield* Ref.make<ReadonlyArray<OrchestrationCommand>>([]);
    const attempts = yield* Ref.make(0);
    const { service, outcomes } = yield* makeAdmission((command) =>
      Effect.gen(function* () {
        yield* Ref.update(commands, (current) => [...current, command]);
        const attempt = yield* Ref.getAndUpdate(attempts, (count) => count + 1);
        if (attempt === 0) {
          yield* Deferred.succeed(firstAttempt, undefined);
          return yield* new HeartbeatThreadBusyError({
            threadId: THREAD_ID,
            jobId: occurrence.publicId,
            occurrenceId: OCCURRENCE_ID,
            detail: "active turn",
          });
        }
        return { sequence: 1 };
      }),
    );

    const fiber = yield* service.processReserved(occurrence).pipe(Effect.forkChild);
    yield* Deferred.await(firstAttempt);
    yield* TestClock.adjust("1 second");
    yield* Fiber.join(fiber);

    const dispatched = yield* Ref.get(commands);
    assert.strictEqual(dispatched.length, 2);
    assert.strictEqual(dispatched[0]?.commandId, dispatched[1]?.commandId);
    assert.deepStrictEqual(yield* Ref.get(outcomes), [{ status: "admitted" }]);
  }),
);

it.effect("terminalizes a permanent admission rejection without retrying", () =>
  Effect.gen(function* () {
    const attempts = yield* Ref.make(0);
    const { service, outcomes } = yield* makeAdmission(() =>
      Ref.update(attempts, (count) => count + 1).pipe(
        Effect.andThen(
          new OrchestrationCommandInvariantError({
            commandType: "thread.heartbeat.due",
            detail: "explicit stop fence",
          }),
        ),
      ),
    );

    yield* service.processReserved(occurrence);
    assert.strictEqual(yield* Ref.get(attempts), 1);
    assert.deepStrictEqual(yield* Ref.get(outcomes), [{ status: "failed" }]);
  }),
);

it.effect("propagates interruption instead of retrying or writing an outcome", () =>
  Effect.gen(function* () {
    const { service, outcomes } = yield* makeAdmission(() => Effect.interrupt);
    const exit = yield* Effect.exit(service.processReserved(occurrence));
    assert.ok(Exit.isFailure(exit));
    if (Exit.isFailure(exit)) assert.ok(Cause.hasInterrupts(exit.cause));
    assert.deepStrictEqual(yield* Ref.get(outcomes), []);
  }),
);

it.effect("replays an uncertain acknowledgement with the stable command ID", () =>
  Effect.gen(function* () {
    yield* TestClock.setTime(Date.parse(NOW));
    const firstAttempt = yield* Deferred.make<void>();
    const commandIds = yield* Ref.make<ReadonlyArray<string>>([]);
    const attempts = yield* Ref.make(0);
    const { service, outcomes } = yield* makeAdmission((command) =>
      Effect.gen(function* () {
        yield* Ref.update(commandIds, (current) => [...current, command.commandId]);
        const attempt = yield* Ref.getAndUpdate(attempts, (count) => count + 1);
        if (attempt === 0) {
          yield* Deferred.succeed(firstAttempt, undefined);
          return yield* Effect.die("accepted acknowledgement was lost");
        }
        return { sequence: 7 };
      }),
    );

    const fiber = yield* service.processReserved(occurrence).pipe(Effect.forkChild);
    yield* Deferred.await(firstAttempt);
    yield* TestClock.adjust("1 second");
    yield* Fiber.join(fiber);

    const ids = yield* Ref.get(commandIds);
    assert.strictEqual(ids.length, 2);
    assert.strictEqual(ids[0], ids[1]);
    assert.deepStrictEqual(yield* Ref.get(outcomes), [{ status: "admitted" }]);
  }),
);
