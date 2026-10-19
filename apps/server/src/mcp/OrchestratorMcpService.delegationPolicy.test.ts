import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, describe, it } from "@effect/vitest";
import {
  CommandId,
  EnvironmentId,
  NodeId,
  OrchestratorMcpFailure,
  type OrchestrationV2ThreadProjection,
  ProjectId,
  ProviderDriverKind,
  ProviderInstanceId,
  RunId,
  type ServerProvider,
  ThreadId,
} from "@t3tools/contracts";
import * as IdAllocator from "@t3tools/provider-core/server/IdAllocator";
import type * as ProviderAdapter from "@t3tools/provider-core/server/ProviderAdapter";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Ref from "effect/Ref";

import * as DelegationPolicy from "../orchestration-v2/fork/DelegationPolicy.ts";
import * as ProviderAdapterRegistry from "../orchestration-v2/ProviderAdapterRegistry.ts";
import * as ThreadLaunchService from "../orchestration-v2/ThreadLaunchService.ts";
import * as ThreadManagementService from "../orchestration-v2/ThreadManagementService.ts";
import * as ProjectService from "../project/ProjectService.ts";
import * as ProviderRegistry from "../provider/ProviderRegistry.ts";
import * as ScheduledTaskService from "../scheduledTasks/ScheduledTaskService.ts";
import * as SecretRequests from "../secrets/SecretRequests.ts";
import * as ServerSettings from "../serverSettings.ts";
import * as GitVcsDriver from "../vcs/GitVcsDriver.ts";
import type { McpInvocationScope } from "./McpInvocationContext.ts";
import * as OrchestratorMcpService from "./OrchestratorMcpService.ts";

const parentThreadId = ThreadId.make("thread:fork-policy-parent");
const parentRunId = RunId.make("run:fork-policy-parent");
const parentNodeId = NodeId.make("node:fork-policy-root");
const taskId = NodeId.make("node:fork-policy-task");
const projectId = ProjectId.make("project:fork-policy");
const codexInstanceId = ProviderInstanceId.make("codex");
const modelSelection = { instanceId: codexInstanceId, model: "gpt-5.4" };

const scope: McpInvocationScope = {
  environmentId: EnvironmentId.make("environment:fork-policy"),
  requestNamespace: "provider-session:fork-policy",
  thread: {
    threadId: parentThreadId,
    providerSessionId: "provider-session:fork-policy",
    providerInstanceId: codexInstanceId,
  },
  client: undefined,
  capabilities: new Set(["orchestration", "delegation"]),
  issuedAt: 1,
};

const codexProvider: ServerProvider = {
  instanceId: codexInstanceId,
  driver: ProviderDriverKind.make("codex"),
  enabled: true,
  installed: true,
  version: "test",
  status: "ready",
  auth: { status: "authenticated" },
  checkedAt: "2026-09-13T00:00:00.000Z",
  models: [{ slug: "gpt-5.4", name: "gpt-5.4", isCustom: false, capabilities: null }],
  slashCommands: [],
  skills: [],
};

const parentProjection = (subagents: ReadonlyArray<unknown>) =>
  ({
    thread: {
      id: parentThreadId,
      projectId,
      title: "Fork policy parent",
      createdBy: "user",
      creationSource: "web",
      modelSelection,
      runtimeMode: "full-access",
      interactionMode: "default",
    },
    runs: [
      {
        id: parentRunId,
        ordinal: 1,
        status: "running",
        rootNodeId: parentNodeId,
        providerInstanceId: codexInstanceId,
        modelSelection,
      },
    ],
    contextTransfers: [],
    subagents,
  }) as unknown as OrchestrationV2ThreadProjection;

type Admission = Parameters<DelegationPolicy.DelegationPolicy["Service"]["admit"]>[0];

/** Runs one async delegate_task against a recording policy that admits or refuses it. */
const delegateWithPolicy = (input: {
  readonly refuse?: OrchestratorMcpFailure;
  readonly fork?: { readonly allowOrchestration: boolean };
}) =>
  Effect.gen(function* () {
    const admissions = yield* Ref.make<ReadonlyArray<Admission>>([]);
    const dispatched = yield* Ref.make<ReadonlyArray<{ readonly commandId: CommandId }>>([]);
    let delegated = false;
    const childThreadId = ThreadId.make("thread:fork-policy-child");
    const task = {
      id: taskId,
      threadId: parentThreadId,
      runId: parentRunId,
      parentNodeId,
      origin: "app_owned",
      createdBy: "agent",
      driver: ProviderDriverKind.make("codex"),
      providerInstanceId: codexInstanceId,
      providerThreadId: null,
      childThreadId,
      nativeTaskRef: null,
      prompt: "Summarize the diff.",
      title: null,
      model: "gpt-5.4",
      status: "running",
      result: null,
      startedAt: null,
      completedAt: null,
    };
    const childProjection = {
      thread: { id: childThreadId },
      runs: [],
      contextTransfers: [],
      messages: [],
      subagents: [],
      providerThreads: [],
      runtimeRequests: [],
      turnItems: [],
    } as unknown as OrchestrationV2ThreadProjection;
    const layerDependencies = Layer.mergeAll(
      NodeServices.layer,
      Layer.mock(ThreadManagementService.ThreadManagementService)({
        hasReceipt: () => Effect.succeed(false),
        assertProviderInstanceEnabled: () => Effect.void,
        getThreadRecords: (threadId) =>
          Effect.succeed(
            threadId === parentThreadId
              ? parentProjection(delegated ? [task] : [])
              : childProjection,
          ),
        dispatch: (command) =>
          Ref.update(dispatched, (commands) => [
            ...commands,
            { commandId: command.commandId as CommandId },
          ]).pipe(
            Effect.andThen(
              Effect.sync(() => {
                delegated = true;
              }),
            ),
            Effect.as({
              sequence: 1,
              storedEvents: [
                {
                  sequence: 1,
                  commandId: null,
                  event: { type: "subagent.updated", payload: task },
                },
              ],
            } as never),
          ),
      }),
      ServerSettings.layerTest(),
      Layer.mock(ProviderRegistry.ProviderRegistry)({
        getProviders: Effect.succeed([codexProvider]),
        refreshInstance: () => Effect.succeed([codexProvider]),
      }),
      Layer.succeed(
        ProviderAdapterRegistry.ProviderAdapterRegistryV2,
        ProviderAdapterRegistry.ProviderAdapterRegistryV2.of({
          list: () => Effect.succeed([codexInstanceId]),
          get: (instanceId) =>
            Effect.succeed({
              instanceId,
            } as unknown as ProviderAdapter.ProviderAdapterV2["Service"]),
        }),
      ),
      Layer.mock(ProjectService.ProjectService)({}),
      Layer.mock(SecretRequests.SecretRequests)({}),
      Layer.mock(ScheduledTaskService.ScheduledTaskService)({}),
      Layer.mock(ThreadLaunchService.ThreadLaunchService)({}),
      Layer.mock(GitVcsDriver.GitVcsDriver)({}),
      Layer.succeed(
        DelegationPolicy.DelegationPolicy,
        DelegationPolicy.DelegationPolicy.of({
          admit: (admission) =>
            Ref.update(admissions, (current) => [...current, admission]).pipe(
              Effect.andThen(input.refuse === undefined ? Effect.void : Effect.fail(input.refuse)),
            ),
        }),
      ),
    );
    const result = yield* Effect.gen(function* () {
      const service = yield* OrchestratorMcpService.OrchestratorMcpService;
      return yield* service
        .delegateTask(
          scope,
          {
            task: "Summarize the diff.",
            target: { providerInstanceId: codexInstanceId, model: "gpt-5.4" },
            mode: "async",
            clientRequestId: "fork-policy-1",
          },
          input.fork,
        )
        .pipe(Effect.result);
    }).pipe(Effect.provide(OrchestratorMcpService.layer.pipe(Layer.provide(layerDependencies))));
    return {
      result,
      admissions: yield* Ref.get(admissions),
      dispatched: yield* Ref.get(dispatched),
    };
  });

describe("OrchestratorMcpService fork delegation policy", () => {
  it.effect("admits a native delegate_task under the child id the orchestrator derives", () =>
    Effect.gen(function* () {
      const { result, admissions, dispatched } = yield* delegateWithPolicy({});
      assert.strictEqual(result._tag, "Success");
      assert.strictEqual(admissions.length, 1);
      assert.strictEqual(dispatched.length, 1);
      const admission = admissions[0]!;
      assert.strictEqual(admission.parentThreadId, parentThreadId);
      assert.strictEqual(admission.allowOrchestration, false);
      assert.strictEqual(admission.replay, false);
      assert.strictEqual(
        admission.childThreadId,
        IdAllocator.derive.delegatedTaskThread({ commandId: dispatched[0]!.commandId }),
      );
    }),
  );

  it.effect("passes an alias's orchestration grant through to the policy", () =>
    Effect.gen(function* () {
      const { admissions } = yield* delegateWithPolicy({ fork: { allowOrchestration: true } });
      assert.strictEqual(admissions[0]?.allowOrchestration, true);
    }),
  );

  it.effect("dispatches nothing when the policy refuses", () =>
    Effect.gen(function* () {
      const { result, dispatched } = yield* delegateWithPolicy({
        refuse: new OrchestratorMcpFailure({ code: "depth_exceeded", message: "too deep" }),
      });
      assert.strictEqual(result._tag, "Failure");
      if (result._tag === "Failure") assert.strictEqual(result.failure.code, "depth_exceeded");
      assert.strictEqual(dispatched.length, 0);
    }),
  );
});
