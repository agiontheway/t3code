import {
  CommandId,
  DEFAULT_PROVIDER_INTERACTION_MODE,
  MessageId,
  ProjectId,
  ProviderDriverKind,
  ProviderInstanceId,
  RuntimeTaskId,
  ThreadId,
  TurnId,
  isCrossProviderAgentErrorOutput,
  type CrossProviderAgentErrorOutput,
  type OrchestrationEvent,
  type OrchestrationThreadShell,
  type ServerProvider,
  type ServerSettings,
} from "@t3tools/contracts";
import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, describe, it } from "@effect/vitest";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Stream from "effect/Stream";
import * as TestClock from "effect/testing/TestClock";

import { OrchestrationCommandReceiptRepositoryLive } from "../../persistence/Layers/OrchestrationCommandReceipts.ts";
import { OrchestrationCommandReceiptRepository } from "../../persistence/Services/OrchestrationCommandReceipts.ts";
import { OrchestrationEventStoreLive } from "../../persistence/Layers/OrchestrationEventStore.ts";
import { SqlitePersistenceMemory } from "../../persistence/Layers/Sqlite.ts";
import * as RepositoryIdentityResolver from "../../project/RepositoryIdentityResolver.ts";
import { ServerConfig } from "../../config.ts";
import { ServerSettingsService } from "../../serverSettings.ts";
import { readCrossProviderAgentToolHost } from "../../provider/CrossProviderAgentToolHost.ts";
import { ProviderRegistry } from "../../provider/Services/ProviderRegistry.ts";
import { OrchestrationEngineService } from "../Services/OrchestrationEngine.ts";
import { ProjectionSnapshotQuery } from "../Services/ProjectionSnapshotQuery.ts";
import { CrossProviderAgentService } from "../Services/CrossProviderAgent.ts";
import * as ThreadBackgroundLiveness from "../ThreadBackgroundLiveness.ts";
import * as ThreadPlanProgress from "../ThreadPlanProgress.ts";
import {
  applyOutputCap,
  CrossProviderAgentLive,
  evaluateCrossProviderChild,
} from "./CrossProviderAgent.ts";
import { OrchestrationEngineLive } from "./OrchestrationEngine.ts";
import { OrchestrationProjectionPipelineLive } from "./ProjectionPipeline.ts";
import { OrchestrationProjectionSnapshotQueryLive } from "./ProjectionSnapshotQuery.ts";

const CLAUDE = ProviderInstanceId.make("claudeAgent");
const CODEX = ProviderInstanceId.make("codex");
const CODEX_WORK = ProviderInstanceId.make("codex-work");
const CURSOR = ProviderInstanceId.make("cursor");
const PROJECT_ID = ProjectId.make("project-xp");
const ROOT = ThreadId.make("thread-root");
const NOW = "2026-09-07T00:00:00.000Z";

const provider = (
  instanceId: ProviderInstanceId,
  driver: string,
  models: ReadonlyArray<string>,
  overrides?: Partial<ServerProvider>,
): ServerProvider => ({
  instanceId,
  driver: ProviderDriverKind.make(driver),
  displayName: `${instanceId} display`,
  enabled: true,
  installed: true,
  version: "1.0.0",
  status: "ready",
  auth: { status: "authenticated" },
  checkedAt: NOW,
  models: models.map((slug) => ({
    slug,
    name: slug.toUpperCase(),
    isCustom: false,
    capabilities: null,
  })),
  slashCommands: [],
  skills: [],
  ...overrides,
});

const DEFAULT_PROVIDERS: ReadonlyArray<ServerProvider> = [
  provider(CLAUDE, "claudeAgent", ["claude-haiku-4-5", "claude-fable-5-1"]),
  provider(CODEX, "codex", ["gpt-5.6-sol", "gpt-5.6-luna"]),
  provider(CODEX_WORK, "codex", ["gpt-5.6-sol"], { auth: { status: "unauthenticated" } }),
  provider(CURSOR, "cursor", ["composer-1"]),
];

function makeLayer(input?: {
  readonly settings?: Partial<ServerSettings>;
  readonly providers?: ReadonlyArray<ServerProvider>;
}) {
  const providers = input?.providers ?? DEFAULT_PROVIDERS;
  const providerRegistry = Layer.succeed(
    ProviderRegistry,
    ProviderRegistry.of({
      getProviders: Effect.succeed(providers),
      refresh: () => Effect.succeed(providers),
      refreshInstance: () => Effect.succeed(providers),
      refreshWorkspaceSnapshot: () => Effect.succeed(providers),
      getProviderMaintenanceCapabilitiesForInstance: () => Effect.die("unused"),
      setProviderMaintenanceActionState: () => Effect.succeed(providers),
      streamChanges: Stream.empty,
    }),
  );
  const orchestration = Layer.mergeAll(
    OrchestrationEngineLive.pipe(
      Layer.provide(OrchestrationProjectionSnapshotQueryLive),
      Layer.provide(OrchestrationProjectionPipelineLive),
    ),
    OrchestrationProjectionSnapshotQueryLive,
  ).pipe(
    Layer.provideMerge(ThreadBackgroundLiveness.layer),
    Layer.provide(ThreadPlanProgress.layer),
    Layer.provide(OrchestrationEventStoreLive),
    Layer.provideMerge(OrchestrationCommandReceiptRepositoryLive),
    Layer.provide(RepositoryIdentityResolver.layer),
    Layer.provide(SqlitePersistenceMemory),
    Layer.provideMerge(
      ServerConfig.layerTest(process.cwd(), { prefix: "t3-cross-provider-agent-test-" }),
    ),
  );
  return CrossProviderAgentLive.pipe(
    Layer.provideMerge(orchestration),
    Layer.provideMerge(providerRegistry),
    Layer.provideMerge(
      ServerSettingsService.layerTest({
        enableCrossProviderAgentAccess: true,
        ...input?.settings,
      }),
    ),
    Layer.provideMerge(NodeServices.layer),
  );
}

const seedRoot = (input?: {
  readonly threadId?: ThreadId;
  readonly instanceId?: ProviderInstanceId;
  readonly model?: string;
}) =>
  Effect.gen(function* () {
    const engine = yield* OrchestrationEngineService;
    const threadId = input?.threadId ?? ROOT;
    yield* engine
      .dispatch({
        type: "project.create",
        commandId: CommandId.make(`cmd-project-${threadId}`),
        projectId: PROJECT_ID,
        title: "XP project",
        workspaceRoot: "/tmp/xp-project",
        createdAt: NOW,
      })
      .pipe(Effect.ignore);
    yield* engine.dispatch({
      type: "thread.create",
      commandId: CommandId.make(`cmd-thread-${threadId}`),
      threadId,
      projectId: PROJECT_ID,
      title: "Root orchestrator",
      modelSelection: {
        instanceId: input?.instanceId ?? CLAUDE,
        model: input?.model ?? "claude-fable-5-1",
      },
      interactionMode: DEFAULT_PROVIDER_INTERACTION_MODE,
      runtimeMode: "full-access",
      branch: "feat/xp",
      worktreePath: "/tmp/xp-project/.worktrees/xp",
      createdAt: NOW,
    });
    return threadId;
  });

/**
 * Drive a child the way the provider reactor and ingestion would. Stamps
 * come from the ambient clock so they order correctly against the
 * service's own `DateTime.now` under both the TestClock and the live clock.
 */
const settleChild = (input: {
  readonly childId: ThreadId;
  readonly turnId: string;
  readonly text: string;
  readonly status?: "ready" | "error" | "interrupted";
}) =>
  Effect.gen(function* () {
    const engine = yield* OrchestrationEngineService;
    const turnId = TurnId.make(input.turnId);
    const at = DateTime.formatIso(yield* DateTime.now);
    const session = {
      threadId: input.childId,
      providerName: "codex",
      runtimeMode: "full-access" as const,
      lastError: null,
      updatedAt: at,
    };
    yield* engine.dispatch({
      type: "thread.session.set",
      commandId: CommandId.make(`cmd-session-running-${input.childId}-${input.turnId}`),
      threadId: input.childId,
      session: { ...session, status: "running", activeTurnId: turnId },
      createdAt: at,
    });
    const messageId = MessageId.make(`assistant-${input.childId}-${input.turnId}`);
    yield* engine.dispatch({
      type: "thread.message.assistant.delta",
      commandId: CommandId.make(`cmd-delta-${input.childId}-${input.turnId}`),
      threadId: input.childId,
      messageId,
      delta: input.text,
      turnId,
      createdAt: at,
    });
    yield* engine.dispatch({
      type: "thread.message.assistant.complete",
      commandId: CommandId.make(`cmd-complete-${input.childId}-${input.turnId}`),
      threadId: input.childId,
      messageId,
      turnId,
      createdAt: at,
    });
    const finalStatus = input.status ?? "ready";
    yield* engine.dispatch({
      type: "thread.session.set",
      commandId: CommandId.make(`cmd-session-settled-${input.childId}-${input.turnId}`),
      threadId: input.childId,
      session: {
        ...session,
        status: finalStatus,
        activeTurnId: null,
        ...(finalStatus === "error" ? { lastError: "provider exploded" } : {}),
        updatedAt: at,
      },
      createdAt: at,
    });
  });

/** Under the TestClock, move time so the next stamp strictly follows the last. */
const tick = TestClock.adjust("1 minute");

const readShell = (threadId: ThreadId) =>
  Effect.gen(function* () {
    const projection = yield* ProjectionSnapshotQuery;
    const shell = yield* projection.getThreadShellById(threadId);
    return Option.getOrThrow(shell);
  });

const readActivities = (threadId: ThreadId) =>
  Effect.gen(function* () {
    const projection = yield* ProjectionSnapshotQuery;
    const thread = yield* projection.getThreadDetailById(threadId);
    return Option.getOrThrow(thread).activities;
  });

const readThreadEvents = (threadId: ThreadId) =>
  Effect.gen(function* () {
    const engine = yield* OrchestrationEngineService;
    const head = yield* engine.latestSequence;
    return Array.from(
      yield* Stream.runCollect(
        engine.readThreadEvents({ threadId, fromSequenceExclusive: 0, toSequenceInclusive: head }),
      ),
    ) as OrchestrationEvent[];
  });

const spawnSol = (caller: ThreadId, overrides?: Record<string, unknown>) =>
  Effect.gen(function* () {
    const service = yield* CrossProviderAgentService;
    const result = yield* service.spawn(caller, {
      providerInstanceId: CODEX,
      model: "gpt-5.6-sol",
      prompt: "Review the diff and report risks.",
      ...overrides,
    });
    if (isCrossProviderAgentErrorOutput(result)) {
      return yield* Effect.die(`unexpected spawn error ${result.error.code}`);
    }
    return result;
  });

const expectError = (
  value: unknown,
  code: CrossProviderAgentErrorOutput["error"]["code"],
): CrossProviderAgentErrorOutput => {
  assert.isTrue(
    isCrossProviderAgentErrorOutput(value),
    `expected ${code}, got ${(value as { error?: { code?: string } }).error?.code ?? "a success value"}`,
  );
  const output = value as CrossProviderAgentErrorOutput;
  assert.equal(output.error.code, code);
  assert.notMatch(output.error.message, /token|secret|CODEX_HOME|api[_-]?key/iu);
  return output;
};

describe("evaluateCrossProviderChild", () => {
  const shell = (
    overrides: Partial<
      Pick<OrchestrationThreadShell, "latestTurn" | "latestUserMessageAt" | "session">
    >,
  ) => ({ latestTurn: null, latestUserMessageAt: null, session: null, ...overrides });
  const turn = (state: "running" | "completed" | "error" | "interrupted", requestedAt: string) => ({
    turnId: TurnId.make("turn-1"),
    state,
    requestedAt,
    startedAt: requestedAt,
    completedAt: null,
    assistantMessageId: null,
  });

  it("treats a requested-but-unstarted turn as running", () => {
    assert.deepEqual(evaluateCrossProviderChild(shell({ latestUserMessageAt: NOW })), {
      settled: false,
      state: "running",
      turnId: null,
    });
    assert.isFalse(
      evaluateCrossProviderChild(
        shell({
          latestTurn: turn("completed", "2026-09-07T00:01:00.000Z"),
          latestUserMessageAt: "2026-09-07T00:02:00.000Z",
        }),
      ).settled,
    );
  });

  it("settles on the turn state once the turn started after the last user message", () => {
    const status = evaluateCrossProviderChild(
      shell({
        latestTurn: turn("interrupted", "2026-09-07T00:02:00.000Z"),
        latestUserMessageAt: "2026-09-07T00:01:00.000Z",
      }),
    );
    assert.deepEqual(status, {
      settled: true,
      state: "interrupted",
      turnId: TurnId.make("turn-1"),
    });
  });

  it("settles as error when the session failed before any turn ran", () => {
    const status = evaluateCrossProviderChild(
      shell({
        latestUserMessageAt: "2026-09-07T00:01:00.000Z",
        session: {
          threadId: ROOT,
          status: "error",
          providerName: "codex",
          runtimeMode: "full-access",
          activeTurnId: null,
          lastError: "boom",
          updatedAt: "2026-09-07T00:01:30.000Z",
        },
      }),
    );
    assert.deepEqual(status, { settled: true, state: "error", turnId: null });
  });
});

describe("applyOutputCap", () => {
  it("returns short output whole and splits long output into a flagged head and tail", () => {
    assert.deepEqual(applyOutputCap("short", 500, "child"), {
      output: "short",
      truncated: false,
      totalChars: 5,
    });
    const long = "a".repeat(300) + "b".repeat(300);
    const capped = applyOutputCap(long, 500, "child-1");
    assert.isTrue(capped.truncated);
    assert.equal(capped.totalChars, 600);
    assert.isTrue(capped.output.startsWith("a".repeat(250)));
    assert.isTrue(capped.output.endsWith("b".repeat(250)));
    assert.include(capped.output, "100 characters omitted");
    assert.include(capped.output, 'childId "child-1", offset 250, limit 100');
  });
});

describe("CrossProviderAgentService", () => {
  it.effect("catalog lists eligible routes exactly and flags the caller's own instance", () =>
    Effect.gen(function* () {
      yield* seedRoot();
      const service = yield* CrossProviderAgentService;
      const output = yield* service.catalog(ROOT);
      if (isCrossProviderAgentErrorOutput(output)) return yield* Effect.die(output.error.code);
      assert.deepEqual(
        output.routes.map((route) => [route.providerInstanceId, route.isCallerInstance]),
        [
          [CLAUDE, true],
          [CODEX, false],
        ],
      );
      assert.deepEqual(
        output.routes[1]?.models.map((model) => model.slug),
        ["gpt-5.6-sol", "gpt-5.6-luna"],
      );
      assert.equal(output.routes[1]?.driver, "codex");
      assert.equal(output.maxDepth, 2);
      assert.equal(output.callerDepth, 0);
      assert.isFalse(output.routes.some((route) => route.providerInstanceId === CODEX_WORK));
    }).pipe(Effect.provide(makeLayer())),
  );

  it.effect("catalog honours explicit routes and model allowlists", () =>
    Effect.gen(function* () {
      yield* seedRoot();
      const service = yield* CrossProviderAgentService;
      const output = yield* service.catalog(ROOT);
      if (isCrossProviderAgentErrorOutput(output)) return yield* Effect.die(output.error.code);
      assert.deepEqual(
        output.routes.map((route) => [route.providerInstanceId, route.models.map((m) => m.slug)]),
        [[CODEX, ["gpt-5.6-luna"]]],
      );
    }).pipe(
      Effect.provide(
        makeLayer({
          settings: {
            crossProviderAgentRoutes: {
              [CODEX]: { enabled: true, models: ["gpt-5.6-luna"] },
              [CLAUDE]: { enabled: false, models: [] },
            },
          },
        }),
      ),
    ),
  );

  it.effect("spawn creates an owned child thread, the parent row, and the first turn", () =>
    Effect.gen(function* () {
      yield* seedRoot();
      const handle = yield* spawnSol(ROOT, { title: "Risk review" });
      assert.equal(handle.providerInstanceId, CODEX);
      assert.equal(handle.model, "gpt-5.6-sol");
      assert.equal(handle.status, "running");

      const childId = ThreadId.make(handle.childId);
      const child = yield* readShell(childId);
      assert.deepEqual(child.spawn, {
        parentThreadId: ROOT,
        allowOrchestration: false,
        depth: 1,
        taskId: handle.taskId,
      });
      assert.equal(child.title, "Risk review");
      assert.equal(child.projectId, PROJECT_ID);
      assert.equal(child.branch, "feat/xp");
      assert.equal(child.worktreePath, "/tmp/xp-project/.worktrees/xp");
      assert.equal(child.runtimeMode, "full-access");
      assert.deepEqual(child.modelSelection, { instanceId: CODEX, model: "gpt-5.6-sol" });

      const childEvents = yield* readThreadEvents(childId);
      assert.deepEqual(
        childEvents.map((event) => event.type),
        ["thread.created", "thread.message-sent", "thread.turn-start-requested"],
      );
      const created = childEvents[0];
      assert.equal(created?.type, "thread.created");
      assert.deepEqual((created!.payload as { spawn?: unknown }).spawn, child.spawn);
      assert.match(created?.commandId ?? "", /^server:xp-agent:thread-create:thread-root:/u);

      const activities = yield* readActivities(ROOT);
      assert.equal(activities.length, 1);
      const started = activities[0]!;
      assert.equal(started.kind, "task.started");
      assert.deepInclude(started.payload as Record<string, unknown>, {
        taskId: handle.taskId,
        taskType: "cross_provider_agent",
        agentKind: "agent",
        title: "Risk review",
        role: "cross-provider",
        model: "gpt-5.6-sol",
        timelineBypass: true,
        childThreadId: childId,
        providerInstanceId: CODEX,
        status: "running",
      });

      const createdCommandId = created?.commandId;
      assert.isOk(createdCommandId);
      const receipts = yield* OrchestrationCommandReceiptRepository;
      const receipt = yield* receipts.getByCommandId({ commandId: createdCommandId! });
      assert.isTrue(Option.isSome(receipt));
    }).pipe(Effect.provide(makeLayer())),
  );

  it.effect("the same thread.create command id yields exactly one child thread", () =>
    Effect.gen(function* () {
      yield* seedRoot();
      const engine = yield* OrchestrationEngineService;
      const command = {
        type: "thread.create" as const,
        commandId: CommandId.make("server:xp-agent:thread-create:thread-root:fixed"),
        threadId: ThreadId.make("thread-child-fixed"),
        projectId: PROJECT_ID,
        title: "Fixed child",
        modelSelection: { instanceId: CODEX, model: "gpt-5.6-sol" },
        interactionMode: DEFAULT_PROVIDER_INTERACTION_MODE,
        runtimeMode: "full-access" as const,
        branch: null,
        worktreePath: null,
        createdAt: NOW,
        spawn: {
          parentThreadId: ROOT,
          allowOrchestration: false,
          depth: 1,
          taskId: RuntimeTaskId.make("xp-agent:thread-child-fixed"),
        },
      };
      const first = yield* engine.dispatch(command);
      const replay = yield* engine.dispatch(command);
      assert.equal(replay.sequence, first.sequence);
      const events = yield* readThreadEvents(ThreadId.make("thread-child-fixed"));
      assert.equal(events.filter((event) => event.type === "thread.created").length, 1);
    }).pipe(Effect.provide(makeLayer())),
  );

  it.effect("spawn fails closed on every route rule before creating anything", () =>
    Effect.gen(function* () {
      yield* seedRoot();
      const service = yield* CrossProviderAgentService;
      const base = { model: "gpt-5.6-sol", prompt: "x" };
      expectError(
        yield* service.spawn(ROOT, {
          ...base,
          providerInstanceId: CLAUDE,
          model: "claude-haiku-4-5",
        }),
        "same_instance",
      );
      expectError(
        yield* service.spawn(ROOT, { ...base, providerInstanceId: CODEX_WORK }),
        "instance_unauthenticated",
      );
      expectError(
        yield* service.spawn(ROOT, { ...base, providerInstanceId: CURSOR, model: "composer-1" }),
        "route_not_eligible",
      );
      expectError(
        yield* service.spawn(ROOT, { ...base, providerInstanceId: "Codex Display Name" }),
        "route_not_eligible",
      );
      const modelError = expectError(
        yield* service.spawn(ROOT, {
          ...base,
          providerInstanceId: CODEX,
          model: "gpt-5.6-sol-ultra",
        }),
        "model_not_offered",
      );
      assert.equal(modelError.error.providerInstanceId, CODEX);
      assert.equal(modelError.error.model, "gpt-5.6-sol-ultra");
      expectError(
        yield* service.spawn(ROOT, {
          ...base,
          providerInstanceId: CODEX,
          allowOrchestration: true,
        }),
        "depth_exceeded",
      );
      const rootEvents = yield* readThreadEvents(ROOT);
      assert.deepEqual(
        rootEvents.map((event) => event.type),
        ["thread.created"],
      );
    }).pipe(Effect.provide(makeLayer({ settings: { crossProviderAgentMaxDepth: 1 } }))),
  );

  it.effect("spawn rejects a disabled instance and is refused entirely when access is off", () =>
    Effect.gen(function* () {
      yield* seedRoot();
      const service = yield* CrossProviderAgentService;
      expectError(
        yield* service.spawn(ROOT, {
          providerInstanceId: CODEX,
          model: "gpt-5.6-sol",
          prompt: "x",
        }),
        "instance_disabled",
      );
      const settings = yield* ServerSettingsService;
      yield* settings.updateSettings({ enableCrossProviderAgentAccess: false });
      expectError(yield* service.catalog(ROOT), "access_disabled");
      expectError(
        yield* service.spawn(ROOT, {
          providerInstanceId: CODEX,
          model: "gpt-5.6-sol",
          prompt: "x",
        }),
        "access_disabled",
      );
    }).pipe(
      Effect.provide(
        makeLayer({
          providers: [
            provider(CLAUDE, "claudeAgent", ["claude-fable-5-1"]),
            provider(CODEX, "codex", ["gpt-5.6-sol"], { enabled: false }),
          ],
        }),
      ),
    ),
  );

  it.effect("unsupported callers are refused and never granted tools", () =>
    Effect.gen(function* () {
      const cursorRoot = yield* seedRoot({
        threadId: ThreadId.make("thread-cursor"),
        instanceId: CURSOR,
        model: "composer-1",
      });
      const service = yield* CrossProviderAgentService;
      expectError(yield* service.catalog(cursorRoot), "unsupported_caller");
      assert.isTrue(Option.isNone(yield* service.toolsForThread(cursorRoot)));
    }).pipe(Effect.provide(makeLayer())),
  );

  // Live clock: the timeout under test is the feature itself, and the
  // TestClock would freeze it.
  it.live("wait returns unsettled on timeout, then the capped output once the child settles", () =>
    Effect.gen(function* () {
      yield* seedRoot();
      const service = yield* CrossProviderAgentService;
      const handle = yield* spawnSol(ROOT);
      const childId = ThreadId.make(handle.childId);

      const pending = yield* service.wait(ROOT, { childIds: [childId], timeoutSeconds: 0.05 });
      if (isCrossProviderAgentErrorOutput(pending)) return yield* Effect.die(pending.error.code);
      assert.deepEqual(pending.children, [{ childId, settled: false, state: "running" }]);

      const text = "A".repeat(400) + "Z".repeat(400);
      const waiting = yield* service.wait(ROOT, { childIds: [childId] }).pipe(Effect.forkScoped);
      yield* settleChild({ childId, turnId: "turn-1", text });
      const settled = yield* Fiber.join(waiting);
      if (isCrossProviderAgentErrorOutput(settled)) return yield* Effect.die(settled.error.code);
      assert.equal(settled.children.length, 1);
      const entry = settled.children[0]!;
      assert.equal(entry.settled, true);
      assert.equal(entry.state, "completed");
      assert.equal(entry.truncated, true);
      assert.equal(entry.totalChars, 800);
      assert.isTrue(entry.output?.startsWith("A".repeat(250)));
      assert.isTrue(entry.output?.endsWith("Z".repeat(250)));

      // Settlement is mirrored onto the parent's row exactly once, even
      // though both the watcher and the wait observed it.
      const activities = yield* readActivities(ROOT);
      const completed = activities.filter((activity) => activity.kind === "task.completed");
      assert.equal(completed.length, 1);
      assert.deepInclude(completed[0]!.payload as Record<string, unknown>, {
        taskId: handle.taskId,
        status: "completed",
        agentKind: "agent",
        childThreadId: childId,
      });
      assert.equal((completed[0]!.payload as { summary?: string }).summary?.length, 200);

      // A later wait backfills from the projection without any new event.
      const again = yield* service.wait(ROOT, { childIds: [childId], timeoutSeconds: 1 });
      if (isCrossProviderAgentErrorOutput(again)) return yield* Effect.die(again.error.code);
      assert.equal(again.children[0]?.settled, true);
    }).pipe(
      Effect.scoped,
      Effect.provide(makeLayer({ settings: { crossProviderAgentOutputCapChars: 500 } })),
    ),
  );

  it.effect("result returns the full stored output by range and refuses running children", () =>
    Effect.gen(function* () {
      yield* seedRoot();
      const service = yield* CrossProviderAgentService;
      const handle = yield* spawnSol(ROOT);
      const childId = ThreadId.make(handle.childId);
      expectError(yield* service.result(ROOT, { childId }), "child_not_settled");

      const text = "0123456789".repeat(100);
      yield* tick;
      yield* settleChild({ childId, turnId: "turn-1", text });
      const range = yield* service.result(ROOT, { childId, offset: 995, limit: 10 });
      if (isCrossProviderAgentErrorOutput(range)) return yield* Effect.die(range.error.code);
      assert.deepEqual(range, {
        childId,
        state: "completed",
        output: "56789",
        truncated: true,
        totalChars: 1000,
        offset: 995,
        length: 5,
      });
      const whole = yield* service.result(ROOT, { childId });
      if (isCrossProviderAgentErrorOutput(whole)) return yield* Effect.die(whole.error.code);
      assert.equal(whole.output, text);
      assert.equal(whole.truncated, false);
      assert.equal(whole.length, 1000);
    }).pipe(Effect.provide(makeLayer())),
  );

  it.effect(
    "follow-up and interrupt respect the child's lifecycle and use the engine commands",
    () =>
      Effect.gen(function* () {
        yield* seedRoot();
        const service = yield* CrossProviderAgentService;
        const handle = yield* spawnSol(ROOT);
        const childId = ThreadId.make(handle.childId);

        expectError(
          yield* service.followUp(ROOT, { childId, prompt: "more" }),
          "child_not_settled",
        );
        yield* tick;
        yield* settleChild({ childId, turnId: "turn-1", text: "first answer" });
        expectError(yield* service.interrupt(ROOT, { childId }), "child_not_running");

        yield* tick;
        const followed = yield* service.followUp(ROOT, { childId, prompt: "Now fix them." });
        if (isCrossProviderAgentErrorOutput(followed))
          return yield* Effect.die(followed.error.code);
        assert.deepEqual(followed, { childId, status: "running" });
        const afterFollowUp = yield* readThreadEvents(childId);
        assert.deepEqual(
          afterFollowUp.slice(-2).map((event) => event.type),
          ["thread.message-sent", "thread.turn-start-requested"],
        );
        const running = (yield* readActivities(ROOT)).filter(
          (activity) => activity.kind === "task.updated",
        );
        assert.equal(running.length, 1);
        assert.deepInclude(running[0]!.payload as Record<string, unknown>, {
          taskId: handle.taskId,
          status: "running",
        });
        expectError(yield* service.result(ROOT, { childId }), "child_not_settled");

        const interrupting = yield* service.interrupt(ROOT, { childId });
        if (isCrossProviderAgentErrorOutput(interrupting)) {
          return yield* Effect.die(interrupting.error.code);
        }
        assert.deepEqual(interrupting, { childId, status: "interrupting" });
        const afterInterrupt = yield* readThreadEvents(childId);
        assert.equal(afterInterrupt.at(-1)?.type, "thread.turn-interrupt-requested");

        yield* tick;
        yield* settleChild({ childId, turnId: "turn-2", text: "partial", status: "interrupted" });
        const settled = yield* service.wait(ROOT, { childIds: [childId], timeoutSeconds: 1 });
        if (isCrossProviderAgentErrorOutput(settled)) return yield* Effect.die(settled.error.code);
        assert.equal(settled.children[0]?.state, "interrupted");
        assert.equal(settled.children[0]?.output, "partial");
        const completed = (yield* readActivities(ROOT)).filter(
          (activity) => activity.kind === "task.completed",
        );
        assert.deepEqual(
          completed.map((activity) => (activity.payload as { status: string }).status),
          ["completed", "stopped"],
        );
      }).pipe(Effect.provide(makeLayer())),
  );

  it.effect(
    "ownership follows the spawn chain: ancestors may act, siblings and strangers may not",
    () =>
      Effect.gen(function* () {
        yield* seedRoot();
        const stranger = yield* seedRoot({ threadId: ThreadId.make("thread-stranger") });
        const service = yield* CrossProviderAgentService;
        const sub = yield* spawnSol(ROOT, { allowOrchestration: true });
        const subId = ThreadId.make(sub.childId);
        const sibling = yield* spawnSol(ROOT);
        const siblingId = ThreadId.make(sibling.childId);
        assert.isTrue(Option.isSome(yield* service.toolsForThread(subId)));
        assert.isTrue(Option.isNone(yield* service.toolsForThread(siblingId)));

        // The sub-orchestrator (depth 1, Codex) fans out to Claude at depth 2.
        const grandchild = yield* service.spawn(subId, {
          providerInstanceId: CLAUDE,
          model: "claude-haiku-4-5",
          prompt: "leaf work",
        });
        if (isCrossProviderAgentErrorOutput(grandchild)) {
          return yield* Effect.die(grandchild.error.code);
        }
        const grandchildId = ThreadId.make(grandchild.childId);
        assert.equal((yield* readShell(grandchildId)).spawn?.depth, 2);
        expectError(
          yield* service.spawn(subId, {
            providerInstanceId: CLAUDE,
            model: "claude-haiku-4-5",
            prompt: "too deep",
            allowOrchestration: true,
          }),
          "depth_exceeded",
        );
        expectError(
          yield* service.spawn(grandchildId, {
            providerInstanceId: CODEX,
            model: "gpt-5.6-sol",
            prompt: "beyond the cap",
          }),
          "depth_exceeded",
        );

        expectError(yield* service.interrupt(siblingId, { childId: grandchildId }), "not_owner");
        expectError(yield* service.interrupt(stranger, { childId: subId }), "not_owner");
        expectError(yield* service.interrupt(ROOT, { childId: ROOT }), "not_owner");
        expectError(yield* service.wait(ROOT, { childIds: ["nope"] }), "not_owner");
        const viaGrandparent = yield* service.interrupt(ROOT, { childId: grandchildId });
        assert.isFalse(isCrossProviderAgentErrorOutput(viaGrandparent));
        const grandchildRow = (yield* readActivities(subId)).find(
          (activity) => activity.kind === "task.started",
        );
        assert.isDefined(grandchildRow);
        assert.equal(
          (grandchildRow!.payload as { childThreadId?: string }).childThreadId,
          grandchildId,
        );
      }).pipe(Effect.provide(makeLayer())),
  );

  it.effect(
    "the tool host grants tools by policy and answers stale calls with structured errors",
    () =>
      Effect.gen(function* () {
        yield* seedRoot();
        const service = yield* CrossProviderAgentService;
        assert.strictEqual(readCrossProviderAgentToolHost(), service);
        const granted = yield* service.toolsForThread(ROOT);
        assert.isTrue(Option.isSome(granted));
        assert.deepEqual(
          Option.getOrThrow(granted).map((spec) => spec.name),
          [
            "agent_catalog",
            "agent_spawn",
            "agent_wait",
            "agent_result",
            "agent_follow_up",
            "agent_interrupt",
          ],
        );
        const spawnSpec = Option.getOrThrow(granted).find((spec) => spec.name === "agent_spawn")!;
        assert.equal(spawnSpec.inputSchema.type, "object");
        assert.deepEqual(spawnSpec.inputSchema.required, ["providerInstanceId", "model", "prompt"]);
        assert.include(spawnSpec.description, "DIFFERENT provider instance");

        const invalid = yield* service.call(ROOT, "agent_spawn", { model: 1 });
        assert.isTrue(invalid.isError);
        expectError(invalid.output, "invalid_input");
        const unknown = yield* service.call(ROOT, "agent_delete", {});
        expectError(unknown.output, "invalid_input");

        const catalog = yield* service.call(ROOT, "agent_catalog", {});
        assert.isFalse(catalog.isError);
        assert.equal((catalog.output as { callerDepth: number }).callerDepth, 0);

        const settings = yield* ServerSettingsService;
        yield* settings.updateSettings({ enableCrossProviderAgentAccess: false });
        assert.isTrue(Option.isNone(yield* service.toolsForThread(ROOT)));
        const stale = yield* service.call(ROOT, "agent_catalog", {});
        assert.isTrue(stale.isError);
        expectError(stale.output, "access_disabled");
      }).pipe(Effect.provide(makeLayer())),
  );
});
