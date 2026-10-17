// @effect-diagnostics-next-line nodeBuiltinImport:off -- Effect's Crypto has no sync sha256.
import * as NodeCrypto from "node:crypto";
import {
  EnvironmentId,
  OrchestrationV2ThreadShell,
  type OrchestrationV2ServerCommand,
  ServerProvider,
  SignalTrigger,
  ThreadId,
} from "@t3tools/contracts";
import * as NodeServices from "@effect/platform-node/NodeServices";
import { expect, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import { ServerConfig } from "../config.ts";
import { ServerEnvironment } from "../environment/ServerEnvironment.ts";
import {
  OrchestratorDispatchError,
  OrchestratorThreadBusyError,
  type OrchestratorV2Error,
} from "../orchestration-v2/Orchestrator.ts";
import { ThreadManagementService } from "../orchestration-v2/ThreadManagementService.ts";
import { ProjectService } from "../project/ProjectService.ts";
import { ProviderRegistry } from "../provider/ProviderRegistry.ts";
import * as SignalIntegration from "./SignalIntegration.ts";

const NOW = "2026-10-13T00:00:00.000Z";
const thread = Schema.decodeSync(Schema.toCodecJson(OrchestrationV2ThreadShell))({
  id: "thread",
  projectId: "project",
  title: "Review",
  providerInstanceId: "codex",
  modelSelection: { instanceId: "codex", model: "gpt-6" },
  runtimeMode: "approval-required",
  interactionMode: "default",
  branch: null,
  worktreePath: null,
  lineage: { rootThreadId: "thread", parentThreadId: null, relationshipToParent: null },
  forkedFrom: null,
  activeProviderThreadId: null,
  latestRunId: null,
  activeRunId: null,
  status: "idle",
  pendingRuntimeRequest: null,
  latestVisibleMessage: null,
  latestUserMessageAt: null,
  hasActionableProposedPlan: false,
  itemCount: 0,
  visibleItemCount: 0,
  createdBy: "user",
  creationSource: "web",
  createdAt: NOW,
  updatedAt: NOW,
  archivedAt: null,
  settledOverride: null,
  settledAt: null,
  deletedAt: null,
});
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

const requestKey = (connectionId: string, requestId: string) =>
  NodeCrypto.createHash("sha256").update(`${connectionId}:${requestId}`).digest("hex");

type MessageDispatch = Extract<OrchestrationV2ServerCommand, { readonly type: "message.dispatch" }>;

const makeDependencies = (
  stateDir: string,
  dispatch: (command: MessageDispatch) => Effect.Effect<void, OrchestratorV2Error>,
) =>
  Layer.mergeAll(
    ServerConfig.layerTest(stateDir, stateDir),
    Layer.mock(ServerEnvironment)({
      getEnvironmentId: Effect.succeed(EnvironmentId.make("environment")),
    }),
    Layer.mock(ThreadManagementService)({
      getThreadShell: (id) => Effect.succeed({ ...thread, id }),
      dispatch: (command) =>
        dispatch(command as MessageDispatch).pipe(Effect.as({ sequence: 1 } as never)),
    }),
    Layer.mock(ProjectService)({
      getById: (id) => Effect.succeedSome({ id, workspaceRoot: "/tmp/project" } as never),
    }),
    Layer.mock(ProviderRegistry)({ refreshWorkspaceSnapshot: () => Effect.succeed([provider]) }),
  );

it.effect("binds server targets, rejects credentials, persists replay payload and revokes", () =>
  Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    const stateDir = yield* fs.makeTempDirectoryScoped();
    const dispatched: MessageDispatch[] = [];
    const dependencies = makeDependencies(stateDir, (command) =>
      Effect.sync(() => {
        dispatched.push(command);
      }),
    );
    yield* Effect.gen(function* () {
      const signal = yield* SignalIntegration.SignalIntegration;
      expect(yield* signal.get).toBeNull();
      const configured = yield* signal.configure({
        commands: [
          { command: "/reviews", threadId: ThreadId.make("thread"), skill: "review-risk" },
          { command: "/audit", threadId: ThreadId.make("other-thread"), skill: "review-risk" },
        ],
      });
      const input = {
        connectionId: configured.binding.connectionId,
        environmentId: "environment",
        requestId: "a".repeat(64),
        command: "/reviews" as const,
      };
      expect((yield* signal.trigger(input, "bad").pipe(Effect.flip)).reason).toBe(
        "credential_or_binding_invalid",
      );
      expect(
        (yield* signal
          .trigger({ ...input, environmentId: "other" }, configured.token)
          .pipe(Effect.flip)).reason,
      ).toBe("credential_or_binding_invalid");
      const decoded = yield* Schema.decodeUnknownEffect(SignalTrigger)({
        ...input,
        threadId: "attacker-target",
        prompt: "arbitrary",
      });
      expect("threadId" in decoded).toBe(false);
      const first = yield* signal.trigger(decoded, configured.token);
      const second = yield* signal.trigger(input, configured.token);
      expect(second).toEqual(first);
      expect(dispatched[0]).toEqual(dispatched[1]);
      expect(dispatched[0]).toMatchObject({
        type: "message.dispatch",
        threadId: "thread",
        creationSource: "server",
        dispatchMode: { type: "start_if_idle" },
        text: expect.stringContaining("$review-risk"),
      });
      expect(
        (yield* signal
          .trigger({ ...input, command: "/unknown" }, configured.token)
          .pipe(Effect.flip)).reason,
      ).toBe("command_unavailable");
      expect(
        (yield* signal.trigger({ ...input, command: "/audit" }, configured.token).pipe(Effect.flip))
          .reason,
      ).toBe("request_id_conflict");
      yield* signal.trigger(
        { ...input, command: "/audit", requestId: "b".repeat(64) },
        configured.token,
      );
      expect(dispatched[2]).toMatchObject({ threadId: "other-thread" });
      const persisted = yield* fs.readFileString(`${stateDir}/userdata/signal/binding.json`);
      expect(persisted).not.toContain(configured.token);
      yield* signal.revoke;
      expect((yield* signal.trigger(input, configured.token).pipe(Effect.flip)).reason).toBe(
        "credential_or_binding_invalid",
      );
      expect(dispatched).toHaveLength(3);
    }).pipe(Effect.provide(SignalIntegration.layer.pipe(Layer.provide(dependencies))));
  }).pipe(Effect.provide(NodeServices.layer)),
);

it.effect("returns thread_busy without recording, then the same request starts once idle", () =>
  Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    const stateDir = yield* fs.makeTempDirectoryScoped();
    const dispatched: MessageDispatch[] = [];
    let busy = true;
    const dependencies = makeDependencies(stateDir, (command) =>
      Effect.suspend(() => {
        dispatched.push(command);
        return busy
          ? Effect.fail(
              new OrchestratorThreadBusyError({
                commandId: command.commandId,
                threadId: command.threadId,
              }),
            )
          : Effect.void;
      }),
    );
    yield* Effect.gen(function* () {
      const signal = yield* SignalIntegration.SignalIntegration;
      const configured = yield* signal.configure({
        commands: [
          {
            command: "/reviews",
            threadId: ThreadId.make("thread"),
            skill: "review-risk",
            skillArguments: "owner/repo",
          },
        ],
      });
      const requestId = "c".repeat(64);
      const input = {
        connectionId: configured.binding.connectionId,
        environmentId: "environment",
        requestId,
        command: "/reviews" as const,
      };
      expect((yield* signal.trigger(input, configured.token).pipe(Effect.flip)).reason).toBe(
        "thread_busy",
      );
      busy = false;
      const result = yield* signal.trigger(input, configured.token);
      expect(dispatched).toHaveLength(2);
      expect(dispatched[1]).toEqual(dispatched[0]);
      const key = requestKey(configured.binding.connectionId, requestId);
      expect(result).toEqual({ threadId: "thread", messageId: `signal-${key}` });
      expect(dispatched[1]).toMatchObject({
        commandId: `signal-${key}`,
        messageId: `signal-${key}`,
        text: `Requested from the linked Signal group using /reviews. Request: cccccccc.\n\n$review-risk owner/repo`,
      });
      const stored = JSON.parse(
        yield* fs.readFileString(`${stateDir}/userdata/signal/${key}.json`),
      ) as { inputCommand: string; command: unknown };
      expect(stored.inputCommand).toBe("/reviews");
      expect(stored.command).toEqual(dispatched[0]);
    }).pipe(Effect.provide(SignalIntegration.layer.pipe(Layer.provide(dependencies))));
  }).pipe(Effect.provide(NodeServices.layer)),
);

it.effect("replays a request file written by the v1 server without rewriting it", () =>
  Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    const stateDir = yield* fs.makeTempDirectoryScoped();
    const dispatched: MessageDispatch[] = [];
    const dependencies = makeDependencies(stateDir, (command) =>
      Effect.sync(() => {
        dispatched.push(command);
      }),
    );
    yield* Effect.gen(function* () {
      const signal = yield* SignalIntegration.SignalIntegration;
      const configured = yield* signal.configure({
        commands: [
          { command: "/reviews", threadId: ThreadId.make("thread"), skill: "review-risk" },
        ],
      });
      const requestId = "d".repeat(64);
      const key = requestKey(configured.binding.connectionId, requestId);
      const text =
        "Requested from the linked Signal group using /reviews. Request: dddddddd.\n\n$review-risk";
      const legacy = JSON.stringify({
        inputCommand: "/reviews",
        command: {
          type: "thread.turn.start",
          requireIdle: true,
          commandId: `signal-${key}`,
          threadId: "thread",
          message: {
            messageId: `signal-${key}`,
            role: "user",
            attachments: [],
            text,
          },
          runtimeMode: "approval-required",
          interactionMode: "default",
          createdAt: NOW,
        },
      });
      const path = `${stateDir}/userdata/signal/${key}.json`;
      yield* fs.writeFileString(path, legacy);
      const result = yield* signal.trigger(
        {
          connectionId: configured.binding.connectionId,
          environmentId: "environment",
          requestId,
          command: "/reviews",
        },
        configured.token,
      );
      expect(result).toEqual({ threadId: "thread", messageId: `signal-${key}` });
      expect(dispatched).toEqual([
        {
          type: "message.dispatch",
          commandId: `signal-${key}`,
          threadId: "thread",
          messageId: `signal-${key}`,
          text,
          attachments: [],
          createdBy: "user",
          creationSource: "server",
          dispatchMode: { type: "start_if_idle" },
        },
      ]);
      expect(yield* fs.readFileString(path)).toBe(legacy);
    }).pipe(Effect.provide(SignalIntegration.layer.pipe(Layer.provide(dependencies))));
  }).pipe(Effect.provide(NodeServices.layer)),
);

it.effect("maps every other dispatch failure to dispatch_failed", () =>
  Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    const stateDir = yield* fs.makeTempDirectoryScoped();
    const dependencies = makeDependencies(stateDir, (command) =>
      Effect.fail(
        new OrchestratorDispatchError({ commandId: command.commandId, commandType: command.type }),
      ),
    );
    yield* Effect.gen(function* () {
      const signal = yield* SignalIntegration.SignalIntegration;
      const configured = yield* signal.configure({
        commands: [
          { command: "/reviews", threadId: ThreadId.make("thread"), skill: "review-risk" },
        ],
      });
      expect(
        (yield* signal
          .trigger(
            {
              connectionId: configured.binding.connectionId,
              environmentId: "environment",
              requestId: "e".repeat(64),
              command: "/reviews",
            },
            configured.token,
          )
          .pipe(Effect.flip)).reason,
      ).toBe("dispatch_failed");
    }).pipe(Effect.provide(SignalIntegration.layer.pipe(Layer.provide(dependencies))));
  }).pipe(Effect.provide(NodeServices.layer)),
);
