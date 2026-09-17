import {
  EnvironmentId,
  HeartbeatCronExpression,
  HeartbeatInvalidCronError,
  HeartbeatJobId,
  HeartbeatPrompt,
  HeartbeatTimeZone,
  OrchestrationThreadShell,
  ProjectId,
  ProviderInstanceId,
  ThreadId,
  type HeartbeatServiceError,
} from "@t3tools/contracts";
import { describe, expect, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Ref from "effect/Ref";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";
import { HeartbeatService } from "../../../heartbeat/HeartbeatService.ts";
import type { Tool } from "effect/unstable/ai";

import * as ProjectionSnapshotQuery from "../../../orchestration/Services/ProjectionSnapshotQuery.ts";
import * as ServerSettings from "../../../serverSettings.ts";
import * as McpInvocationContext from "../../McpInvocationContext.ts";
import { HeartbeatToolkitHandlersLive } from "./handlers.ts";
import { HeartbeatToolkit } from "./tools.ts";

const THREAD_ID = ThreadId.make("thread-bound-to-credential");
const JOB_ID = HeartbeatJobId.make("deadbeef");
const PROMPT = HeartbeatPrompt.make("continue");
const CRON = HeartbeatCronExpression.make("0 * * * *");
const TIME_ZONE = HeartbeatTimeZone.make("Europe/London");
const PROJECT_ID = ProjectId.make("project-heartbeat-toolkit");
const decodeThreadShell = Schema.decodeUnknownSync(OrchestrationThreadShell);

type Call = {
  readonly method: "create" | "list" | "delete" | "wakeup";
  readonly threadId: ThreadId;
  readonly input?: unknown;
};

const invocation = (
  capabilities: ReadonlyArray<McpInvocationContext.McpCapability>,
): McpInvocationContext.McpInvocationScope => ({
  environmentId: EnvironmentId.make("environment-1"),
  threadId: THREAD_ID,
  providerSessionId: "provider-session-1",
  providerInstanceId: ProviderInstanceId.make("codex"),
  capabilities: new Set(capabilities),
  issuedAt: 1,
});

const createResult = {
  id: JOB_ID,
  kind: "cron" as const,
  prompt: PROMPT,
  status: "active" as const,
  cron: CRON,
  recurring: true,
  timezone: TIME_ZONE,
  scheduleDescription: "hourly",
  nextNominalAt: "2026-09-17T14:00:00.000Z",
  nextFireAt: "2026-09-17T14:04:00.000Z",
  expiresAt: "2026-09-24T13:00:00.000Z",
  restartPolicy: "restore-on-resume" as const,
};

const relativeJob = {
  id: HeartbeatJobId.make("cafebabe"),
  kind: "relative" as const,
  prompt: PROMPT,
  status: "pending" as const,
  dueAt: "2026-09-17T13:02:00.000Z",
  noop: false,
  restartPolicy: "process-local" as const,
};

const makeHarness = Effect.fn("makeHeartbeatToolkitHarness")(function* (
  options: {
    readonly createFailure?: HeartbeatServiceError;
    readonly enabled?: boolean;
    readonly spawn?: { readonly allowOrchestration: boolean };
  } = {},
) {
  const calls = yield* Ref.make<ReadonlyArray<Call>>([]);
  const record = (call: Call) => Ref.update(calls, (current) => [...current, call]);
  const service = HeartbeatService.of({
    create: (threadId, input) =>
      Effect.gen(function* () {
        yield* record({ method: "create", threadId, input });
        if (options.createFailure !== undefined) return yield* options.createFailure;
        return createResult;
      }),
    list: (threadId) =>
      record({ method: "list", threadId }).pipe(Effect.as({ jobs: [createResult] })),
    delete: (threadId, input) =>
      record({ method: "delete", threadId, input }).pipe(
        Effect.as({ id: input.id, deleted: true }),
      ),
    wakeup: (threadId, input) =>
      record({ method: "wakeup", threadId, input }).pipe(
        Effect.as(
          input.type === "stop"
            ? ({ action: "stopped", stopped: true, cronJobsUnaffected: true } as const)
            : ({ action: "armed", job: relativeJob, replaced: false } as const),
        ),
      ),
    activateOnAcceptedTurn: () => Effect.void,
    cancelRelative: () => Effect.void,
    cancelThread: () => Effect.void,
    setOccurrenceOutcome: (_occurrenceId, _outcome) => Effect.void,
  });
  const thread = decodeThreadShell({
    id: THREAD_ID,
    projectId: PROJECT_ID,
    title: "Heartbeat toolkit test",
    modelSelection: { instanceId: "codex", model: "gpt-5" },
    runtimeMode: "full-access",
    branch: null,
    worktreePath: null,
    ...(options.spawn
      ? {
          spawn: {
            parentThreadId: "thread-parent",
            allowOrchestration: options.spawn.allowOrchestration,
            depth: 1,
            taskId: "xp-agent:heartbeat-toolkit-test",
          },
        }
      : {}),
    latestTurn: null,
    createdAt: "2026-09-17T12:00:00.000Z",
    updatedAt: "2026-09-17T12:00:00.000Z",
    session: null,
    latestUserMessageAt: null,
    hasPendingApprovals: false,
    hasPendingUserInput: false,
    hasActionableProposedPlan: false,
  });
  const dependencies = Layer.mergeAll(
    Layer.succeed(HeartbeatService, service),
    Layer.mock(ProjectionSnapshotQuery.ProjectionSnapshotQuery)({
      getThreadShellById: () => Effect.succeedSome(thread),
    }),
    ServerSettings.ServerSettingsService.layerTest({
      enableHeartbeatAccess: options.enabled ?? true,
    }),
  );
  const toolkit = yield* HeartbeatToolkit.pipe(
    Effect.provide(HeartbeatToolkitHandlersLive.pipe(Layer.provide(dependencies))),
  );
  const call = <Name extends keyof typeof HeartbeatToolkit.tools>(
    name: Name,
    params: Parameters<typeof toolkit.handle<Name>>[1],
    capabilities: ReadonlyArray<McpInvocationContext.McpCapability> = ["heartbeat"],
  ) =>
    toolkit.handle(name, params).pipe(
      Stream.unwrap,
      Stream.runCollect,
      Effect.map(
        (chunk) => chunk.at(-1)!.result as Tool.Success<(typeof HeartbeatToolkit.tools)[Name]>,
      ),
      Effect.provideService(McpInvocationContext.McpInvocationContext, invocation(capabilities)),
      Effect.provide(dependencies),
    );
  return { calls, call };
});

describe("heartbeat toolkit handlers", () => {
  it.effect("rejects every handler before invoking the service when capability is missing", () =>
    Effect.gen(function* () {
      const harness = yield* makeHarness();
      const errorCreate = yield* harness
        .call("t3_heartbeat_create", { cron: "0 * * * *", prompt: "continue" }, ["preview"])
        .pipe(Effect.flip);
      const errorList = yield* harness.call("t3_heartbeat_list", {}, ["preview"]).pipe(Effect.flip);
      const errorDelete = yield* harness
        .call("t3_heartbeat_delete", { id: "deadbeef" }, ["preview"])
        .pipe(Effect.flip);
      const errorWakeup = yield* harness
        .call("t3_heartbeat_wakeup", { stop: true }, ["preview"])
        .pipe(Effect.flip);
      for (const error of [errorCreate, errorList, errorDelete, errorWakeup]) {
        expect(error).toMatchObject({
          _tag: "McpCapabilityUnavailableError",
          capability: "heartbeat",
          threadId: THREAD_ID,
        });
      }
      expect(yield* Ref.get(harness.calls)).toEqual([]);
    }),
  );

  it.effect("binds every granted service call to the authenticated thread", () =>
    Effect.gen(function* () {
      const harness = yield* makeHarness();
      yield* harness.call("t3_heartbeat_create", {
        cron: "0 * * * *",
        prompt: "  retain whitespace  ",
        timezone: "Europe/London",
        durable: false,
      });
      yield* harness.call("t3_heartbeat_list", {});
      yield* harness.call("t3_heartbeat_delete", { id: "deadbeef" });
      yield* harness.call("t3_heartbeat_wakeup", {
        delaySeconds: 1.5,
        prompt: "continue",
        reason: "waiting for child",
        noop: true,
      });

      const recorded = yield* Ref.get(harness.calls);
      expect(recorded.map((call) => call.threadId)).toEqual([
        THREAD_ID,
        THREAD_ID,
        THREAD_ID,
        THREAD_ID,
      ]);
      expect(recorded[0]?.input).toEqual({
        cron: "0 * * * *",
        prompt: "  retain whitespace  ",
        recurring: true,
        timezone: "Europe/London",
      });
      expect(recorded[3]?.input).toEqual({
        type: "arm",
        delaySeconds: 1.5,
        prompt: "continue",
        reason: "waiting for child",
        noop: true,
      });
    }),
  );

  it.effect("allows a currently orchestration-enabled child to call the service", () =>
    Effect.gen(function* () {
      const harness = yield* makeHarness({ spawn: { allowOrchestration: true } });
      const result = yield* harness.call("t3_heartbeat_list", {});
      expect(result).toEqual({ jobs: [createResult] });
      expect(yield* Ref.get(harness.calls)).toEqual([{ method: "list", threadId: THREAD_ID }]);
    }),
  );

  it.effect("passes typed service failures through the tool error channel", () =>
    Effect.gen(function* () {
      const harness = yield* makeHarness({
        createFailure: new HeartbeatInvalidCronError({ reason: "minute is out of range" }),
      });
      const error = yield* harness
        .call("t3_heartbeat_create", { cron: "99 * * * *", prompt: "continue" })
        .pipe(Effect.flip);
      expect(error).toBeInstanceOf(HeartbeatInvalidCronError);
      expect(error.message).toContain("minute is out of range");
    }),
  );

  it.effect("rechecks settings and child grant before invoking the service", () =>
    Effect.gen(function* () {
      const disabled = yield* makeHarness({ enabled: false });
      const disabledError = yield* disabled.call("t3_heartbeat_list", {}).pipe(Effect.flip);
      expect(disabledError).toMatchObject({
        _tag: "McpCapabilityUnavailableError",
        capability: "heartbeat",
      });
      expect(yield* Ref.get(disabled.calls)).toEqual([]);

      const leaf = yield* makeHarness({ spawn: { allowOrchestration: false } });
      const leafError = yield* leaf.call("t3_heartbeat_list", {}).pipe(Effect.flip);
      expect(leafError).toMatchObject({
        _tag: "McpCapabilityUnavailableError",
        capability: "heartbeat",
      });
      expect(yield* Ref.get(leaf.calls)).toEqual([]);
    }),
  );
});
