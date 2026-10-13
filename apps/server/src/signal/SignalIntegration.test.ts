import {
  EnvironmentId,
  SignalTrigger,
  OrchestrationThread,
  OrchestrationProject,
  ServerProvider,
  ThreadId,
  type OrchestrationCommand,
} from "@t3tools/contracts";
import * as NodeServices from "@effect/platform-node/NodeServices";
import { expect, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import { ServerConfig } from "../config.ts";
import { ServerEnvironment } from "../environment/ServerEnvironment.ts";
import { ProjectionSnapshotQuery } from "../orchestration/Services/ProjectionSnapshotQuery.ts";
import { OrchestrationEngineService } from "../orchestration/Services/OrchestrationEngine.ts";
import { ProviderRegistry } from "../provider/Services/ProviderRegistry.ts";
import * as SignalIntegration from "./SignalIntegration.ts";

const NOW = "2026-10-13T00:00:00.000Z";
const thread = Schema.decodeUnknownSync(OrchestrationThread)({
  id: "thread",
  projectId: "project",
  title: "Review",
  modelSelection: { instanceId: "codex", model: "gpt-6" },
  runtimeMode: "approval-required",
  interactionMode: "default",
  branch: null,
  worktreePath: null,
  latestTurn: null,
  createdAt: NOW,
  updatedAt: NOW,
  archivedAt: null,
  deletedAt: null,
  messages: [],
  proposedPlans: [],
  activities: [],
  checkpoints: [],
  session: null,
});
const project = Schema.decodeUnknownSync(OrchestrationProject)({
  id: "project",
  title: "Project",
  workspaceRoot: "/tmp/project",
  defaultModelSelection: null,
  scripts: [],
  createdAt: NOW,
  updatedAt: NOW,
  deletedAt: null,
});
const provider = Schema.decodeUnknownSync(ServerProvider)({
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

it.effect("binds server targets, rejects credentials, persists replay payload and revokes", () =>
  Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    const stateDir = yield* fs.makeTempDirectoryScoped();
    const dispatched: OrchestrationCommand[] = [];
    const dependencies = Layer.mergeAll(
      ServerConfig.layerTest(stateDir, stateDir),
      Layer.mock(ServerEnvironment)({
        getEnvironmentId: Effect.succeed(EnvironmentId.make("environment")),
      }),
      Layer.mock(ProjectionSnapshotQuery)({
        getThreadDetailSnapshot: (id) =>
          Effect.succeedSome({ snapshotSequence: 0, thread: { ...thread, id } }),
        getCommandReadModel: () =>
          Effect.succeed({
            snapshotSequence: 0,
            threads: [thread],
            projects: [project],
            updatedAt: NOW,
          }),
      }),
      Layer.mock(ProviderRegistry)({ refreshWorkspaceSnapshot: () => Effect.succeed([provider]) }),
      Layer.mock(OrchestrationEngineService)({
        dispatch: (command) =>
          Effect.sync(() => {
            dispatched.push(command);
            return { sequence: dispatched.length };
          }),
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
      yield* signal.trigger(decoded, configured.token);
      yield* signal.trigger(input, configured.token);
      expect(dispatched[0]).toEqual(dispatched[1]);
      expect(dispatched[0]).toMatchObject({
        threadId: "thread",
        requireIdle: true,
        message: { text: expect.stringContaining("$review-risk") },
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
