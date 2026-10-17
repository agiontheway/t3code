import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, describe, it } from "@effect/vitest";
import {
  CommandId,
  type CrossProviderAgentCatalogOutput,
  type CrossProviderAgentErrorOutput,
  type CrossProviderAgentResultOutput,
  type CrossProviderAgentSpawnOutput,
  type CrossProviderAgentWaitOutput,
  EnvironmentId,
  MessageId,
  type ModelCapabilities,
  type ModelSelection,
  type OrchestrationV2ProviderCapabilities,
  type OrchestrationV2ProviderSession,
  type OrchestrationV2ProviderThread,
  type OrchestrationV2ThreadProjection,
  ProjectId,
  ProviderDriverKind,
  ProviderInstanceId,
  ProviderThreadId,
  ProviderTurnId,
  type ServerProvider,
  type ServerSettings,
  ThreadId,
  TurnItemId,
} from "@t3tools/contracts";
import * as ProviderAdapter from "@t3tools/provider-core/server/ProviderAdapter";
import * as ProviderContinuationRequests from "@t3tools/provider-core/server/ProviderContinuationRequests";
import { checkpointWorkspace } from "@t3tools/provider-testing/replayWorkspace";
import * as NodeSqliteClient from "@t3tools/shared/nodeSqliteClient";
import * as DateTime from "effect/DateTime";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as PubSub from "effect/PubSub";
import type * as Scope from "effect/Scope";
import * as Stream from "effect/Stream";
import { McpSchema, McpServer } from "effect/ai";

import { ClaudeProviderCapabilitiesV2 } from "../../../orchestration-v2/Adapters/ClaudeAdapterV2.ts";
import { CodexProviderCapabilitiesV2 } from "../../../orchestration-v2/Adapters/CodexAdapterV2.ts";
import * as CommandReceiptStore from "../../../orchestration-v2/CommandReceiptStore.ts";
import * as DelegationGrants from "../../../orchestration-v2/fork/DelegationGrants.ts";
import * as DelegationPolicy from "../../../orchestration-v2/fork/DelegationPolicy.ts";
import * as Orchestrator from "../../../orchestration-v2/Orchestrator.ts";
import * as ProviderAdapterRegistry from "../../../orchestration-v2/ProviderAdapterRegistry.ts";
import * as ProviderReplayHarness from "../../../orchestration-v2/testkit/ProviderReplayHarness.ts";
import * as ThreadLaunchService from "../../../orchestration-v2/ThreadLaunchService.ts";
import * as ThreadManagementService from "../../../orchestration-v2/ThreadManagementService.ts";
import DelegationGrantsMigration from "../../../persistence/ForkMigrations/004_DelegationGrants.ts";
import * as SqlitePersistence from "../../../persistence/Sqlite.ts";
import * as ProjectService from "../../../project/ProjectService.ts";
import * as ProviderRegistryMock from "../../../provider/testUtils/providerRegistryMock.ts";
import * as ScheduledTaskService from "../../../scheduledTasks/ScheduledTaskService.ts";
import * as SecretRequests from "../../../secrets/SecretRequests.ts";
import * as ServerSettingsService from "../../../serverSettings.ts";
import * as GitVcsDriver from "../../../vcs/GitVcsDriver.ts";
import * as McpHttpServer from "../../McpHttpServer.ts";
import * as McpInvocationContext from "../../McpInvocationContext.ts";

const CLAUDE = ProviderInstanceId.make("claudeAgent");
const CODEX = ProviderInstanceId.make("codex");
const CODEX_WORK = ProviderInstanceId.make("codex-work");
const CODEX_API_KEY = ProviderInstanceId.make("codex-openrouter");
const CURSOR = ProviderInstanceId.make("cursor");
const PROJECT_ID = ProjectId.make("project:xp-agent");
const ROOT = ThreadId.make("thread:xp-root");

/** Turns whose prompt starts with this stay running until interrupted. */
const KEEP_RUNNING = "Keep running:";

/** Effort choices the way each driver's catalog describes them. */
const EFFORT_CAPABILITIES: Record<string, ModelCapabilities> = {
  claudeAgent: {
    optionDescriptors: [
      {
        id: "effort",
        label: "Effort",
        type: "select",
        options: [
          { id: "low", label: "Low" },
          { id: "medium", label: "Medium" },
          { id: "high", label: "High", isDefault: true },
          { id: "max", label: "Max" },
        ],
      },
    ],
  },
  codex: {
    optionDescriptors: [
      {
        id: "reasoningEffort",
        label: "Reasoning",
        type: "select",
        options: [
          { id: "low", label: "Low" },
          { id: "medium", label: "Medium", isDefault: true },
          { id: "high", label: "High" },
          { id: "xhigh", label: "Extra high" },
        ],
        currentValue: "medium",
      },
    ],
  },
};

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
  checkedAt: "2026-09-07T00:00:00.000Z",
  models: models.map((slug) => ({
    slug,
    name: slug.toUpperCase(),
    isCustom: false,
    capabilities: EFFORT_CAPABILITIES[driver] ?? null,
  })),
  slashCommands: [],
  skills: [],
  ...overrides,
});

const DEFAULT_PROVIDERS: ReadonlyArray<ServerProvider> = [
  provider(CLAUDE, "claudeAgent", ["claude-haiku-4-5", "claude-fable-5-1"]),
  provider(CODEX, "codex", ["gpt-5.6-sol", "gpt-5.6-luna"]),
  provider(CODEX_WORK, "codex", ["gpt-5.6-sol"], { auth: { status: "unauthenticated" } }),
  // API-key backed instances report `unknown`, never `authenticated`.
  provider(CODEX_API_KEY, "codex", ["glm-5.3-flash"], {
    auth: { status: "unknown" },
    models: [{ slug: "glm-5.3-flash", name: "GLM", isCustom: false, capabilities: null }],
  }),
  provider(CURSOR, "cursor", ["composer-1"]),
];

/** What a scripted child turn answers, and an optional gate it waits on before answering. */
interface TurnScript {
  readonly responses: Map<string, string>;
  readonly gates: Map<string, Deferred.Deferred<void>>;
}

function unsupported(driver: ProviderDriverKind, detail: string) {
  return Effect.fail(new ProviderAdapter.ProviderAdapterProtocolError({ driver, detail }));
}

/**
 * A provider that answers each turn from the script: `Keep running:` turns stay active until
 * interrupted, gated turns answer once their gate opens, the rest answer at once.
 */
function scriptedAdapter(input: {
  readonly instanceId: ProviderInstanceId;
  readonly driver: ProviderDriverKind;
  readonly capabilities: OrchestrationV2ProviderCapabilities;
  readonly script: TurnScript;
}): ProviderAdapter.ProviderAdapterV2["Service"] {
  return {
    instanceId: input.instanceId,
    driver: input.driver,
    getCapabilities: () => Effect.succeed(input.capabilities),
    planSelectionTransition: () => Effect.succeed({ type: "apply_on_next_turn" }),
    openSession: (sessionInput) =>
      Effect.gen(function* () {
        const events = yield* PubSub.unbounded<ProviderAdapter.ProviderAdapterV2Event>();
        const now = yield* DateTime.now;
        const providerSession: OrchestrationV2ProviderSession = {
          id: sessionInput.providerSessionId,
          driver: input.driver,
          providerInstanceId: input.instanceId,
          status: "ready",
          cwd: sessionInput.runtimePolicy.cwd ?? process.cwd(),
          model: sessionInput.modelSelection.model,
          capabilities: input.capabilities,
          createdAt: now,
          updatedAt: now,
          lastError: null,
        };
        const publish = (providerEvents: ReadonlyArray<ProviderAdapter.ProviderAdapterV2Event>) =>
          Effect.forEach(providerEvents, (event) => PubSub.publish(events, event), {
            discard: true,
          });
        const turnInputs = new Map<ProviderTurnId, ProviderAdapter.ProviderAdapterV2TurnInput>();
        const providerTurn = (
          turnInput: ProviderAdapter.ProviderAdapterV2TurnInput,
          providerTurnId: ProviderTurnId,
          status: "running" | "completed" | "interrupted",
          at: DateTime.Utc,
        ): ProviderAdapter.ProviderAdapterV2Event => ({
          type: "provider_turn.updated",
          driver: input.driver,
          providerTurn: {
            id: providerTurnId,
            providerThreadId: turnInput.providerThread.id,
            nodeId: turnInput.rootNodeId,
            runAttemptId: turnInput.attemptId,
            nativeTurnRef: {
              driver: input.driver,
              nativeId: `native-turn:${turnInput.threadId}:${turnInput.runOrdinal}`,
              strength: "strong",
            },
            ordinal: turnInput.providerTurnOrdinal,
            status,
            startedAt: at,
            completedAt: status === "running" ? null : at,
          },
        });
        const complete = (
          turnInput: ProviderAdapter.ProviderAdapterV2TurnInput,
          providerTurnId: ProviderTurnId,
        ) =>
          Effect.gen(function* () {
            const at = yield* DateTime.now;
            const text = turnInput.message.text;
            yield* publish([
              providerTurn(turnInput, providerTurnId, "completed", at),
              {
                type: "turn_item.updated",
                driver: input.driver,
                turnItem: {
                  id: TurnItemId.make(
                    `turn-item:${turnInput.threadId}:${turnInput.runOrdinal}:assistant`,
                  ),
                  threadId: turnInput.threadId,
                  runId: turnInput.runId,
                  nodeId: turnInput.rootNodeId,
                  providerThreadId: turnInput.providerThread.id,
                  providerTurnId,
                  nativeItemRef: null,
                  parentItemId: null,
                  ordinal: turnInput.runOrdinal * 100 + 1,
                  status: "completed",
                  title: null,
                  startedAt: at,
                  completedAt: at,
                  updatedAt: at,
                  type: "assistant_message",
                  messageId: MessageId.make(
                    `message:${turnInput.threadId}:${turnInput.runOrdinal}:assistant`,
                  ),
                  text: input.script.responses.get(text) ?? `done: ${text}`,
                  streaming: false,
                },
              },
              {
                type: "turn.terminal",
                driver: input.driver,
                providerThreadId: turnInput.providerThread.id,
                providerTurnId,
                runOrdinal: turnInput.runOrdinal,
                status: "completed",
                failure: null,
                threadDisposition: "reusable",
              },
            ]);
          });

        return {
          instanceId: input.instanceId,
          driver: input.driver,
          providerSessionId: sessionInput.providerSessionId,
          providerSession,
          events: Stream.fromPubSub(events),
          ensureThread: (threadInput) =>
            Effect.gen(function* () {
              const createdAt = yield* DateTime.now;
              const nativeThreadId = `${input.driver}:${threadInput.threadId}`;
              return {
                id: ProviderThreadId.make(`provider-thread:${nativeThreadId}`),
                driver: input.driver,
                providerInstanceId: input.instanceId,
                providerSessionId: sessionInput.providerSessionId,
                appThreadId: threadInput.threadId,
                ownerNodeId: null,
                nativeThreadRef: {
                  driver: input.driver,
                  nativeId: nativeThreadId,
                  strength: "strong",
                },
                nativeConversationHeadRef: null,
                status: "idle",
                firstRunOrdinal: null,
                lastRunOrdinal: null,
                handoffIds: [],
                forkedFrom: null,
                createdAt,
                updatedAt: createdAt,
              } satisfies OrchestrationV2ProviderThread;
            }),
          resumeThread: ({ providerThread }) => Effect.succeed(providerThread),
          startTurn: (turnInput) =>
            Effect.gen(function* () {
              const providerTurnId = ProviderTurnId.make(
                `provider-turn:${input.instanceId}:${turnInput.threadId}:${turnInput.runOrdinal}`,
              );
              turnInputs.set(providerTurnId, turnInput);
              yield* publish([
                providerTurn(turnInput, providerTurnId, "running", yield* DateTime.now),
              ]);
              const text = turnInput.message.text;
              if (text.startsWith(KEEP_RUNNING)) return;
              const gate = input.script.gates.get(text);
              if (gate === undefined) return yield* complete(turnInput, providerTurnId);
              yield* Effect.forkDetach(
                Deferred.await(gate).pipe(Effect.andThen(complete(turnInput, providerTurnId))),
              );
            }),
          steerTurn: () => Effect.void,
          interruptTurn: ({ providerThread, providerTurnId }) =>
            Effect.gen(function* () {
              const turnInput = turnInputs.get(providerTurnId);
              const at = yield* DateTime.now;
              yield* publish([
                ...(turnInput === undefined
                  ? []
                  : [providerTurn(turnInput, providerTurnId, "interrupted", at)]),
                {
                  type: "turn.terminal",
                  driver: input.driver,
                  providerThreadId: providerThread.id,
                  providerTurnId,
                  runOrdinal: turnInput?.runOrdinal ?? 1,
                  status: "interrupted",
                  failure: null,
                  threadDisposition: "reusable",
                },
              ]);
            }),
          respondToRuntimeRequest: () => Effect.void,
          readThreadSnapshot: () => unsupported(input.driver, "unused"),
          rollbackThread: () => unsupported(input.driver, "unused"),
          forkThread: () => unsupported(input.driver, "unused"),
        };
      }),
  };
}

const mcpClient = McpSchema.McpServerClient.of({
  clientId: 1,
  protocolVersion: "2025-06-18",
  clientCapabilities: {},
  clientInfo: { name: "xp-agent-test", version: "1.0.0" },
  initializePayload: {
    protocolVersion: "2025-06-18",
    capabilities: {},
    clientInfo: { name: "xp-agent-test", version: "1.0.0" },
  },
  getClient: Effect.die("unused"),
});

/** The credential the session manager mints for a thread allowed to use the aliases. */
const threadScope = (
  threadId: ThreadId,
  providerInstanceId: ProviderInstanceId,
  capabilities: ReadonlyArray<McpInvocationContext.McpCapability> = [
    "orchestration",
    "delegation",
    "crossProviderAgents",
  ],
): McpInvocationContext.McpInvocationScope => ({
  environmentId: EnvironmentId.make("environment:xp-agent"),
  requestNamespace: `provider-session:${threadId}`,
  thread: { threadId, providerSessionId: `provider-session:${threadId}`, providerInstanceId },
  client: undefined,
  capabilities: new Set(capabilities),
  issuedAt: 1,
});

const makeLayer = (input: {
  readonly cwd: string;
  readonly script: TurnScript;
  readonly settings?: Partial<ServerSettings>;
  readonly providers?: ReadonlyArray<ServerProvider>;
}) => {
  const providers = input.providers ?? DEFAULT_PROVIDERS;
  const layerRegistry = ProviderAdapterRegistry.layerFromAdapters(
    providers
      .filter((candidate) => ["claudeAgent", "codex"].includes(candidate.driver))
      .filter((candidate) => candidate.instanceId !== CODEX_WORK)
      .map((candidate) =>
        scriptedAdapter({
          instanceId: candidate.instanceId,
          driver: candidate.driver,
          capabilities:
            candidate.driver === "codex"
              ? CodexProviderCapabilitiesV2
              : ClaudeProviderCapabilitiesV2,
          script: input.script,
        }),
      ),
  );
  const layerSettings = ServerSettingsService.layerTest({
    enableCrossProviderAgentAccess: true,
    // Custom instances are enabled once configured, as they are in a real environment.
    providerInstances: {
      [CODEX_WORK]: { driver: ProviderDriverKind.make("codex") },
      [CODEX_API_KEY]: { driver: ProviderDriverKind.make("codex") },
    },
    ...input.settings,
  });
  // The parent wake is upstream's; these tests only need its offer to land somewhere.
  const layerContinuations = Layer.succeed(
    ProviderContinuationRequests.ProviderContinuationRequests,
    { offer: () => Effect.void, take: Effect.never },
  );
  const layerOrchestrator = ProviderReplayHarness.layerWithRegistry(
    {
      name: "cross-provider-agent-toolkit",
      runtimePolicyOverride: {
        cwd: input.cwd,
        approvalPolicy: "never",
        sandboxPolicy: { type: "readOnly", access: { type: "fullAccess" }, networkAccess: false },
      },
    },
    layerRegistry,
  ).pipe(Layer.provide(layerContinuations));
  const layerOrchestration = Layer.merge(
    layerOrchestrator,
    ThreadManagementService.layer.pipe(
      Layer.provide(layerOrchestrator),
      Layer.provide(layerSettings),
      Layer.provide(CommandReceiptStore.layer.pipe(Layer.provide(SqlitePersistence.layerMemory))),
    ),
  );
  const layerGrants = Layer.mergeAll(
    DelegationGrants.layer,
    Layer.effectDiscard(DelegationGrantsMigration),
  ).pipe(Layer.provide(NodeSqliteClient.layer({ filename: ":memory:" })));
  return McpHttpServer.layerCrossProviderAgentToolkit.pipe(
    Layer.provideMerge(McpServer.McpServer.layer),
    Layer.provideMerge(layerOrchestration),
    Layer.provideMerge(DelegationPolicy.layer.pipe(Layer.provideMerge(layerGrants))),
    Layer.provide(layerRegistry),
    Layer.provide(ProviderRegistryMock.layer(providers)),
    Layer.provideMerge(layerSettings),
    Layer.provide(Layer.mock(ThreadLaunchService.ThreadLaunchService)({})),
    Layer.provide(Layer.mock(GitVcsDriver.GitVcsDriver)({})),
    Layer.provide(Layer.mock(ScheduledTaskService.ScheduledTaskService)({})),
    Layer.provide(Layer.mock(SecretRequests.SecretRequests)({})),
    Layer.provide(Layer.mock(ProjectService.ProjectService)({})),
    Layer.provide(NodeServices.layer),
  );
};

/** Runs `body` against a fresh environment; the live clock drives the provider turns. */
const withEnvironment = <A, E>(
  options: {
    readonly settings?: Partial<ServerSettings>;
    readonly providers?: ReadonlyArray<ServerProvider>;
  },
  body: (
    script: TurnScript,
    cwd: string,
  ) => Effect.Effect<A, E, Layer.Success<ReturnType<typeof makeLayer>> | Scope.Scope>,
) =>
  Effect.scoped(
    Effect.gen(function* () {
      const cwd = yield* checkpointWorkspace("cross-provider-agent-toolkit");
      const script: TurnScript = { responses: new Map(), gates: new Map() };
      return yield* body(script, cwd).pipe(Effect.provide(makeLayer({ cwd, script, ...options })));
    }),
  );

const callTool = (
  scope: McpInvocationContext.McpInvocationScope,
  name: string,
  args: Record<string, unknown>,
) =>
  Effect.gen(function* () {
    const server = yield* McpServer.McpServer;
    return yield* server
      .callTool({ name, arguments: args })
      .pipe(
        Effect.provideService(McpInvocationContext.McpInvocationContext, scope),
        Effect.provideService(McpSchema.McpServerClient, mcpClient),
      );
  });

const textOf = (result: McpSchema.CallToolResult) => {
  const content = result.content[0];
  return content?.type === "text" ? content.text : "";
};

/** A successful call's structured output. */
const succeeded = <A>(result: McpSchema.CallToolResult): A => {
  assert.notStrictEqual(result.isError, true, textOf(result));
  return result.structuredContent as A;
};

/** A fork error result with the expected code. */
const failedWith = (
  result: McpSchema.CallToolResult,
  code: CrossProviderAgentErrorOutput["error"]["code"],
): CrossProviderAgentErrorOutput => {
  assert.strictEqual(result.isError, true, textOf(result));
  const output = JSON.parse(textOf(result)) as CrossProviderAgentErrorOutput;
  assert.strictEqual(output.error?.code, code, textOf(result));
  return output;
};

function waitForProjection(
  threadId: ThreadId,
  predicate: (projection: OrchestrationV2ThreadProjection) => boolean,
) {
  return Effect.gen(function* () {
    const orchestrator = yield* Orchestrator.OrchestratorV2;
    for (let attempt = 0; attempt < 1_000; attempt += 1) {
      const projection = yield* orchestrator.getThreadProjection(threadId);
      if (predicate(projection)) return projection;
      yield* Effect.sleep("5 millis");
    }
    return yield* Effect.die(new Error(`Timed out waiting for thread ${threadId}.`));
  });
}

/** A top-level thread whose own turn stays running, so it may act as a caller. */
const seedRoot = (
  cwd: string,
  input: { readonly threadId?: ThreadId; readonly modelSelection?: ModelSelection } = {},
) =>
  Effect.gen(function* () {
    const threadId = input.threadId ?? ROOT;
    const modelSelection = input.modelSelection ?? {
      instanceId: CLAUDE,
      model: "claude-fable-5-1",
    };
    const orchestrator = yield* Orchestrator.OrchestratorV2;
    yield* orchestrator.dispatch({
      type: "thread.create",
      createdBy: "user",
      creationSource: "web",
      commandId: CommandId.make(`command:${threadId}:create`),
      threadId,
      projectId: PROJECT_ID,
      title: "Root",
      modelSelection,
      runtimeMode: "full-access",
      interactionMode: "default",
      branch: null,
      worktreePath: cwd,
    });
    yield* orchestrator.dispatch({
      type: "message.dispatch",
      createdBy: "user",
      creationSource: "web",
      commandId: CommandId.make(`command:${threadId}:start`),
      threadId,
      messageId: MessageId.make(`message:${threadId}:start`),
      text: `${KEEP_RUNNING} orchestrate`,
      attachments: [],
      modelSelection,
      dispatchMode: { type: "start_immediately" },
    });
    yield* waitUntilRunning(threadId);
    return threadScope(threadId, modelSelection.instanceId);
  });

const waitUntilRunning = (threadId: ThreadId) =>
  waitForProjection(threadId, (projection) =>
    projection.providerTurns.some((turn) => turn.status === "running"),
  );

const spawn = (scope: McpInvocationContext.McpInvocationScope, args: Record<string, unknown>) =>
  Effect.map(callTool(scope, "agent_spawn", args), succeeded<CrossProviderAgentSpawnOutput>);

const waitFor = (
  scope: McpInvocationContext.McpInvocationScope,
  args: { readonly childIds: ReadonlyArray<string>; readonly timeoutSeconds?: number },
) => Effect.map(callTool(scope, "agent_wait", args), succeeded<CrossProviderAgentWaitOutput>);

const childShell = (childId: string) =>
  Effect.gen(function* () {
    const orchestrator = yield* Orchestrator.OrchestratorV2;
    const shell = yield* orchestrator.getThreadShell(ThreadId.make(childId));
    assert.isNotNull(shell);
    return shell!;
  });

describe("cross-provider agent toolkit", () => {
  it.live("catalog lists eligible routes exactly and flags the caller's own instance", () =>
    withEnvironment({}, (_script, cwd) =>
      Effect.gen(function* () {
        const root = yield* seedRoot(cwd);
        const output = succeeded<CrossProviderAgentCatalogOutput>(
          yield* callTool(root, "agent_catalog", {}),
        );
        assert.deepStrictEqual(
          output.routes.map((route) => [route.providerInstanceId, route.isCallerInstance]),
          [
            [CLAUDE, true],
            [CODEX, false],
            [CODEX_API_KEY, false],
          ],
        );
        assert.deepStrictEqual(
          output.routes[1]?.models.map((model) => model.slug),
          ["gpt-5.6-sol", "gpt-5.6-luna"],
        );
        assert.strictEqual(output.routes[1]?.driver, "codex");
        assert.strictEqual(output.routes[1]?.displayName, "codex display");
        assert.strictEqual(output.maxDepth, 2);
        assert.strictEqual(output.callerDepth, 0);
        // An API-key instance is spawnable; only an explicit sign-out excludes one.
        yield* spawn(root, {
          providerInstanceId: CODEX_API_KEY,
          model: "glm-5.3-flash",
          prompt: "use the key-backed instance",
        });
      }),
    ),
  );

  it.live("catalog honours explicit routes and model allowlists", () =>
    withEnvironment(
      {
        settings: {
          crossProviderAgentRoutes: {
            [CODEX]: { enabled: true, models: ["gpt-5.6-luna"] },
            [CLAUDE]: { enabled: false, models: [] },
          },
        },
      },
      (_script, cwd) =>
        Effect.gen(function* () {
          const root = yield* seedRoot(cwd);
          const output = succeeded<CrossProviderAgentCatalogOutput>(
            yield* callTool(root, "agent_catalog", {}),
          );
          assert.deepStrictEqual(
            output.routes.map((route) => [
              route.providerInstanceId,
              route.models.map((model) => model.slug),
            ]),
            [[CODEX, ["gpt-5.6-luna"]]],
          );
          failedWith(
            yield* callTool(root, "agent_spawn", {
              providerInstanceId: CODEX,
              model: "gpt-5.6-sol",
              prompt: "not on the allowlist",
            }),
            "model_not_offered",
          );
        }),
    ),
  );

  it.live(
    "spawn creates an owned native child with the fork shape, grant and inherited effort",
    () =>
      withEnvironment({}, (script, cwd) =>
        Effect.gen(function* () {
          const root = yield* seedRoot(cwd);
          script.responses.set("Review the risks.", "risk report");
          const handle = yield* spawn(root, {
            providerInstanceId: CODEX,
            model: "gpt-5.6-sol",
            prompt: "Review the risks.",
            title: "Risk review",
          });
          assert.deepStrictEqual(Object.keys(handle).sort(), [
            "childId",
            "model",
            "providerInstanceId",
            "status",
            "taskId",
          ]);
          assert.strictEqual(handle.providerInstanceId, CODEX);
          assert.strictEqual(handle.model, "gpt-5.6-sol");
          assert.strictEqual(handle.status, "running");

          const child = yield* childShell(handle.childId);
          assert.deepStrictEqual(child.lineage, {
            parentThreadId: ROOT,
            relationshipToParent: "subagent",
            rootThreadId: ROOT,
          });
          assert.strictEqual(child.title, "Risk review");
          assert.strictEqual(child.projectId, PROJECT_ID);
          assert.strictEqual(child.runtimeMode, "full-access");
          // No explicit effort: the Claude parent's default (high) is offered by the Codex
          // target, so the child inherits it under Codex's option id.
          assert.deepStrictEqual(child.modelSelection, {
            instanceId: CODEX,
            model: "gpt-5.6-sol",
            options: [{ id: "reasoningEffort", value: "high" }],
          });
          const grants = yield* DelegationGrants.DelegationGrants;
          const grant = yield* grants.read(ThreadId.make(handle.childId));
          assert.isTrue(Option.isSome(grant));
          assert.deepInclude(Option.getOrThrow(grant), {
            parentThreadId: ROOT,
            depth: 1,
            allowOrchestration: false,
          });

          const settled = yield* waitFor(root, { childIds: [handle.childId] });
          assert.deepStrictEqual(settled.children, [
            {
              childId: handle.childId,
              settled: true,
              state: "completed",
              output: "risk report",
              truncated: false,
              totalChars: 11,
            },
          ]);
          // Without a title the child is named after the prompt's start.
          const untitled = yield* spawn(root, {
            providerInstanceId: CODEX,
            model: "gpt-5.6-sol",
            prompt:
              "  Summarize   the\nfindings for the release notes and flag anything that needs a migration.",
          });
          assert.strictEqual(
            (yield* childShell(untitled.childId)).title,
            "Summarize the findings for the release notes and flag anythi",
          );
        }),
      ),
  );

  it.live("wait bounds its inputs and accepts the 60-minute maximum", () =>
    withEnvironment({}, (_script, cwd) =>
      Effect.gen(function* () {
        const root = yield* seedRoot(cwd);
        const tooMany = yield* Effect.flip(
          callTool(root, "agent_wait", { childIds: Array.from({ length: 33 }, () => "x") }),
        );
        assert.strictEqual(tooMany._tag, "InvalidParams");
        const tooLong = yield* Effect.flip(
          callTool(root, "agent_wait", { childIds: [], timeoutSeconds: 3601 }),
        );
        assert.strictEqual(tooLong._tag, "InvalidParams");
        assert.deepStrictEqual(
          (yield* waitFor(root, { childIds: [], timeoutSeconds: 3600 })).children,
          [],
        );
      }),
    ),
  );

  it.live("spawn fails closed on every route rule before creating anything", () =>
    withEnvironment({ settings: { crossProviderAgentMaxDepth: 1 } }, (_script, cwd) =>
      Effect.gen(function* () {
        const root = yield* seedRoot(cwd);
        const base = { model: "gpt-5.6-sol", prompt: "x" };
        failedWith(
          yield* callTool(root, "agent_spawn", {
            ...base,
            providerInstanceId: CLAUDE,
            model: "claude-haiku-4-5",
          }),
          "same_instance",
        );
        failedWith(
          yield* callTool(root, "agent_spawn", { ...base, providerInstanceId: CODEX_WORK }),
          "instance_unauthenticated",
        );
        failedWith(
          yield* callTool(root, "agent_spawn", {
            ...base,
            providerInstanceId: CURSOR,
            model: "composer-1",
          }),
          "route_not_eligible",
        );
        failedWith(
          yield* callTool(root, "agent_spawn", {
            ...base,
            providerInstanceId: "Codex Display Name",
          }),
          "route_not_eligible",
        );
        const modelError = failedWith(
          yield* callTool(root, "agent_spawn", {
            ...base,
            providerInstanceId: CODEX,
            model: "gpt-5.6-sol-ultra",
          }),
          "model_not_offered",
        );
        assert.strictEqual(modelError.error.providerInstanceId, CODEX);
        assert.strictEqual(modelError.error.model, "gpt-5.6-sol-ultra");
        const depthError = failedWith(
          yield* callTool(root, "agent_spawn", {
            ...base,
            providerInstanceId: CODEX,
            allowOrchestration: true,
          }),
          "depth_exceeded",
        );
        assert.strictEqual(depthError.error.providerInstanceId, CODEX);
        const orchestrator = yield* Orchestrator.OrchestratorV2;
        assert.deepStrictEqual((yield* orchestrator.getThreadProjection(ROOT)).subagents, []);
      }),
    ),
  );

  it.live("spawn rejects a disabled instance and every tool is refused when access is off", () =>
    withEnvironment(
      {
        providers: [
          provider(CLAUDE, "claudeAgent", ["claude-fable-5-1"]),
          provider(CODEX, "codex", ["gpt-5.6-sol"], { enabled: false }),
          provider(CURSOR, "cursor", ["composer-1"]),
        ],
      },
      (_script, cwd) =>
        Effect.gen(function* () {
          const root = yield* seedRoot(cwd);
          const spawnArgs = { providerInstanceId: CODEX, model: "gpt-5.6-sol", prompt: "x" };
          failedWith(yield* callTool(root, "agent_spawn", spawnArgs), "instance_disabled");

          // A credential minted without the bit (a child never granted orchestration).
          const ungranted = threadScope(ROOT, CLAUDE, ["orchestration"]);
          failedWith(yield* callTool(ungranted, "agent_catalog", {}), "access_disabled");

          // Only Claude and Codex threads, and only threads, may call.
          const cursorThread = ThreadId.make("thread:xp-cursor");
          const orchestrator = yield* Orchestrator.OrchestratorV2;
          yield* orchestrator.dispatch({
            type: "thread.create",
            createdBy: "user",
            creationSource: "web",
            commandId: CommandId.make("command:xp-cursor:create"),
            threadId: cursorThread,
            projectId: PROJECT_ID,
            title: "Cursor",
            modelSelection: { instanceId: CURSOR, model: "composer-1" },
            runtimeMode: "full-access",
            interactionMode: "default",
            branch: null,
            worktreePath: cwd,
          });
          failedWith(
            yield* callTool(threadScope(cursorThread, CURSOR), "agent_catalog", {}),
            "unsupported_caller",
          );
          failedWith(
            yield* callTool({ ...root, thread: undefined }, "agent_catalog", {}),
            "unsupported_caller",
          );

          const settings = yield* ServerSettingsService.ServerSettingsService;
          yield* settings.updateSettings({ enableCrossProviderAgentAccess: false });
          failedWith(yield* callTool(root, "agent_catalog", {}), "access_disabled");
          failedWith(yield* callTool(root, "agent_spawn", spawnArgs), "access_disabled");
          failedWith(yield* callTool(root, "agent_wait", { childIds: [] }), "access_disabled");
        }),
    ),
  );

  it.live("wait returns unsettled on timeout, then the capped output once the child settles", () =>
    withEnvironment({ settings: { crossProviderAgentOutputCapChars: 500 } }, (script, cwd) =>
      Effect.gen(function* () {
        const root = yield* seedRoot(cwd);
        const prompt = "Write the long report.";
        const gate = yield* Deferred.make<void>();
        script.gates.set(prompt, gate);
        script.responses.set(prompt, "A".repeat(400) + "Z".repeat(400));
        const handle = yield* spawn(root, {
          providerInstanceId: CODEX,
          model: "gpt-5.6-sol",
          prompt,
        });

        const pending = yield* waitFor(root, { childIds: [handle.childId], timeoutSeconds: 0.05 });
        assert.deepStrictEqual(pending.children, [
          { childId: handle.childId, settled: false, state: "running" },
        ]);

        const waiting = yield* Effect.forkScoped(waitFor(root, { childIds: [handle.childId] }));
        yield* Deferred.succeed(gate, undefined);
        const settled = yield* Fiber.join(waiting);
        const entry = settled.children[0]!;
        assert.strictEqual(entry.settled, true);
        assert.strictEqual(entry.state, "completed");
        assert.strictEqual(entry.truncated, true);
        assert.strictEqual(entry.totalChars, 800);
        assert.isAtMost(entry.output!.length, 500);
        assert.match(entry.output!, /^A+\n\[…/u);
        assert.match(entry.output!, /…\]\nZ+$/u);
        assert.include(entry.output!, `call agent_result with childId "${handle.childId}"`);

        // A later wait answers from the stored result without any new event.
        const again = yield* waitFor(root, { childIds: [handle.childId], timeoutSeconds: 1 });
        assert.strictEqual(again.children[0]?.settled, true);
      }),
    ),
  );

  it.live("result returns the full stored output by range and refuses running children", () =>
    withEnvironment({}, (script, cwd) =>
      Effect.gen(function* () {
        const root = yield* seedRoot(cwd);
        const prompt = "Count to ten a hundred times.";
        const gate = yield* Deferred.make<void>();
        const text = "0123456789".repeat(100);
        script.gates.set(prompt, gate);
        script.responses.set(prompt, text);
        const handle = yield* spawn(root, {
          providerInstanceId: CODEX,
          model: "gpt-5.6-sol",
          prompt,
        });
        const childId = handle.childId;
        failedWith(yield* callTool(root, "agent_result", { childId }), "child_not_settled");

        yield* Deferred.succeed(gate, undefined);
        yield* waitFor(root, { childIds: [childId] });
        const range = succeeded<CrossProviderAgentResultOutput>(
          yield* callTool(root, "agent_result", { childId, offset: 995, limit: 10 }),
        );
        assert.deepStrictEqual(range, {
          childId,
          state: "completed",
          output: "56789",
          truncated: true,
          totalChars: 1000,
          offset: 995,
          length: 5,
        });
        const whole = succeeded<CrossProviderAgentResultOutput>(
          yield* callTool(root, "agent_result", { childId }),
        );
        assert.strictEqual(whole.output, text);
        assert.strictEqual(whole.truncated, false);
        assert.strictEqual(whole.length, 1000);
      }),
    ),
  );

  it.live("follow-up and interrupt respect the child's lifecycle", () =>
    withEnvironment({}, (script, cwd) =>
      Effect.gen(function* () {
        const root = yield* seedRoot(cwd);
        const prompt = "Find the bugs.";
        const gate = yield* Deferred.make<void>();
        script.gates.set(prompt, gate);
        script.responses.set(prompt, "first answer");
        const handle = yield* spawn(root, {
          providerInstanceId: CODEX,
          model: "gpt-5.6-sol",
          prompt,
        });
        const childId = handle.childId;

        failedWith(
          yield* callTool(root, "agent_follow_up", { childId, prompt: "more" }),
          "child_not_settled",
        );
        yield* Deferred.succeed(gate, undefined);
        const first = yield* waitFor(root, { childIds: [childId] });
        assert.strictEqual(first.children[0]?.output, "first answer");
        failedWith(yield* callTool(root, "agent_interrupt", { childId }), "child_not_running");

        const followed = succeeded(
          yield* callTool(root, "agent_follow_up", {
            childId,
            prompt: `${KEEP_RUNNING} now fix them`,
          }),
        );
        assert.deepStrictEqual(followed, { childId, status: "running" });
        failedWith(yield* callTool(root, "agent_result", { childId }), "child_not_settled");
        yield* waitUntilRunning(ThreadId.make(childId));

        const interrupting = succeeded(yield* callTool(root, "agent_interrupt", { childId }));
        assert.deepStrictEqual(interrupting, { childId, status: "interrupting" });
        const settled = yield* waitFor(root, { childIds: [childId] });
        assert.strictEqual(settled.children[0]?.state, "interrupted");
        assert.strictEqual(settled.children[0]?.settled, true);

        // Interrupted children may be followed up again.
        script.responses.set("Try again.", "second answer");
        succeeded(yield* callTool(root, "agent_follow_up", { childId, prompt: "Try again." }));
        const again = yield* waitFor(root, { childIds: [childId] });
        assert.strictEqual(again.children[0]?.output, "second answer");
      }),
    ),
  );

  it.live(
    "spawn resolves effort like a native spawn: explicit, inherited, default, or rejected",
    () =>
      withEnvironment({}, (_script, cwd) =>
        Effect.gen(function* () {
          const root = yield* seedRoot(cwd);
          const sol = { providerInstanceId: CODEX, model: "gpt-5.6-sol" };

          const explicit = yield* spawn(root, { ...sol, prompt: "explicit", effort: "low" });
          assert.deepStrictEqual((yield* childShell(explicit.childId)).modelSelection.options, [
            { id: "reasoningEffort", value: "low" },
          ]);

          // A parent effort the target does not offer falls back to the target's default.
          const maxParent = yield* seedRoot(cwd, {
            threadId: ThreadId.make("thread:xp-max"),
            modelSelection: {
              instanceId: CLAUDE,
              model: "claude-fable-5-1",
              options: [{ id: "effort", value: "max" }],
            },
          });
          const fromMax = yield* spawn(maxParent, { ...sol, prompt: "from max" });
          assert.deepStrictEqual((yield* childShell(fromMax.childId)).modelSelection.options, [
            { id: "reasoningEffort", value: "medium" },
          ]);

          // A Codex parent's explicit effort is inherited by a Claude child that offers it.
          const codexParent = yield* seedRoot(cwd, {
            threadId: ThreadId.make("thread:xp-codex-parent"),
            modelSelection: {
              instanceId: CODEX,
              model: "gpt-5.6-sol",
              options: [{ id: "reasoningEffort", value: "low" }],
            },
          });
          const inherited = yield* spawn(codexParent, {
            providerInstanceId: CLAUDE,
            model: "claude-haiku-4-5",
            prompt: "leaf",
          });
          assert.deepStrictEqual((yield* childShell(inherited.childId)).modelSelection.options, [
            { id: "effort", value: "low" },
          ]);

          const rejected = failedWith(
            yield* callTool(root, "agent_spawn", { ...sol, prompt: "x", effort: "ultra" }),
            "invalid_input",
          );
          assert.strictEqual(rejected.error.model, "gpt-5.6-sol");
          assert.strictEqual(rejected.error.providerInstanceId, CODEX);
          assert.include(rejected.error.message, "low, medium, high, xhigh");

          // A target model without an effort setting gets none and rejects an explicit one.
          const plain = yield* spawn(root, {
            providerInstanceId: CODEX_API_KEY,
            model: "glm-5.3-flash",
            prompt: "no effort here",
          });
          assert.isUndefined((yield* childShell(plain.childId)).modelSelection.options);
          const noEffort = failedWith(
            yield* callTool(root, "agent_spawn", {
              providerInstanceId: CODEX_API_KEY,
              model: "glm-5.3-flash",
              prompt: "x",
              effort: "high",
            }),
            "invalid_input",
          );
          assert.strictEqual(noEffort.error.model, "glm-5.3-flash");
        }),
      ),
  );

  it.live(
    "ownership follows the spawn chain: ancestors may act, siblings and strangers may not",
    () =>
      withEnvironment({}, (_script, cwd) =>
        Effect.gen(function* () {
          const root = yield* seedRoot(cwd);
          const stranger = yield* seedRoot(cwd, { threadId: ThreadId.make("thread:xp-stranger") });
          const sub = yield* spawn(root, {
            providerInstanceId: CODEX,
            model: "gpt-5.6-sol",
            prompt: `${KEEP_RUNNING} sub-orchestrate`,
            allowOrchestration: true,
          });
          const sibling = yield* spawn(root, {
            providerInstanceId: CODEX,
            model: "gpt-5.6-sol",
            prompt: `${KEEP_RUNNING} sibling`,
            allowOrchestration: true,
          });
          const subId = ThreadId.make(sub.childId);
          const siblingId = ThreadId.make(sibling.childId);
          yield* waitUntilRunning(subId);
          yield* waitUntilRunning(siblingId);
          const subScope = threadScope(subId, CODEX);

          // The sub-orchestrator (depth 1, Codex) fans out to Claude at depth 2.
          const catalog = succeeded<CrossProviderAgentCatalogOutput>(
            yield* callTool(subScope, "agent_catalog", {}),
          );
          assert.strictEqual(catalog.callerDepth, 1);
          const grandchild = yield* spawn(subScope, {
            providerInstanceId: CLAUDE,
            model: "claude-haiku-4-5",
            prompt: `${KEEP_RUNNING} leaf work`,
          });
          const grandchildId = ThreadId.make(grandchild.childId);
          const grants = yield* DelegationGrants.DelegationGrants;
          assert.strictEqual(yield* grants.depthOf(grandchildId), 2);
          failedWith(
            yield* callTool(subScope, "agent_spawn", {
              providerInstanceId: CLAUDE,
              model: "claude-haiku-4-5",
              prompt: "too deep",
              allowOrchestration: true,
            }),
            "depth_exceeded",
          );
          // A leaf never gets the bit; even a stale credential cannot exceed the cap.
          yield* waitUntilRunning(grandchildId);
          failedWith(
            yield* callTool(threadScope(grandchildId, CLAUDE), "agent_spawn", {
              providerInstanceId: CODEX,
              model: "gpt-5.6-sol",
              prompt: "beyond the cap",
            }),
            "depth_exceeded",
          );

          failedWith(
            yield* callTool(threadScope(siblingId, CODEX), "agent_interrupt", {
              childId: grandchildId,
            }),
            "not_owner",
          );
          failedWith(yield* callTool(stranger, "agent_interrupt", { childId: subId }), "not_owner");
          failedWith(yield* callTool(root, "agent_interrupt", { childId: ROOT }), "not_owner");
          failedWith(yield* callTool(root, "agent_wait", { childIds: ["nope"] }), "not_owner");
          const viaGrandparent = succeeded(
            yield* callTool(root, "agent_interrupt", { childId: grandchildId }),
          );
          assert.deepStrictEqual(viaGrandparent, { childId: grandchildId, status: "interrupting" });
          const settled = yield* waitFor(root, { childIds: [grandchildId] });
          assert.strictEqual(settled.children[0]?.state, "interrupted");
        }),
      ),
  );
});
