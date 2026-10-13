import * as NodeCrypto from "node:crypto";
import {
  CommandId,
  MessageId,
  SignalIntegrationError,
  type SignalBinding,
  type SignalConfigure,
  type SignalTrigger,
  type SignalTriggerResult,
  ThreadTurnStartCommand,
  type ThreadId,
} from "@t3tools/contracts";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as FileSystem from "effect/FileSystem";
import * as Path from "effect/Path";
import * as DateTime from "effect/DateTime";
import * as Schema from "effect/Schema";
import * as Option from "effect/Option";
import * as Semaphore from "effect/Semaphore";
import { ServerConfig } from "../config.ts";
import { ServerEnvironment } from "../environment/ServerEnvironment.ts";
import { ProjectionSnapshotQuery } from "../orchestration/Services/ProjectionSnapshotQuery.ts";
import { OrchestrationEngineService } from "../orchestration/Services/OrchestrationEngine.ts";
import { ProviderRegistry } from "../provider/Services/ProviderRegistry.ts";

interface StoredBinding {
  binding: SignalBinding;
  tokenHash: string;
}
export class SignalIntegration extends Context.Service<
  SignalIntegration,
  {
    readonly get: Effect.Effect<SignalBinding | null, SignalIntegrationError>;
    readonly choices: (
      threadId: ThreadId,
    ) => Effect.Effect<readonly string[], SignalIntegrationError>;
    readonly configure: (
      input: SignalConfigure,
    ) => Effect.Effect<{ binding: SignalBinding; token: string }, SignalIntegrationError>;
    readonly revoke: Effect.Effect<void, SignalIntegrationError>;
    readonly trigger: (
      input: SignalTrigger,
      token: string,
    ) => Effect.Effect<SignalTriggerResult, SignalIntegrationError>;
  }
>()("t3/signal/SignalIntegration") {}

const fail = (reason: string) => new SignalIntegrationError({ reason });
const hash = (value: string) => NodeCrypto.createHash("sha256").update(value).digest("hex");

const encodeJson = Schema.encodeEffect(Schema.fromJsonString(Schema.Unknown));
const decodeJson = Schema.decodeUnknownEffect(Schema.fromJsonString(Schema.Unknown));
const decodeCommand = Schema.decodeEffect(ThreadTurnStartCommand);

const make = Effect.gen(function* () {
  const config = yield* ServerConfig;
  const fs = yield* FileSystem.FileSystem;
  const pathService = yield* Path.Path;
  const join = pathService.join;
  const environment = yield* ServerEnvironment;
  const query = yield* ProjectionSnapshotQuery;
  const engine = yield* OrchestrationEngineService;
  const providers = yield* ProviderRegistry;
  const lock = yield* Semaphore.make(1);
  const directory = join(config.stateDir, "signal");
  const bindingPath = join(directory, "binding.json");
  const write = (path: string, value: unknown) =>
    Effect.gen(function* () {
      yield* fs.makeDirectory(directory, { recursive: true, mode: 0o700 });
      const tmp = `${path}.${NodeCrypto.randomUUID()}.tmp`;
      yield* fs.writeFileString(tmp, yield* encodeJson(value), { mode: 0o600 });
      yield* fs.rename(tmp, path);
    }).pipe(Effect.mapError(() => fail("storage_unavailable")));
  const read = <A>(path: string) =>
    Effect.gen(function* () {
      if (!(yield* fs.exists(path))) return null;
      const contents = yield* fs.readFileString(path);
      return (yield* decodeJson(contents)) as A;
    }).pipe(Effect.mapError(() => fail("storage_unavailable")));
  const target = (threadId: ThreadId) =>
    Effect.gen(function* () {
      const snapshot = yield* query
        .getThreadDetailSnapshot(threadId)
        .pipe(Effect.mapError(() => fail("target_unavailable")));
      if (Option.isNone(snapshot) || snapshot.value.thread.archivedAt !== null)
        return yield* fail("target_unavailable");
      return snapshot.value.thread;
    });
  const choices = (threadId: ThreadId) =>
    Effect.gen(function* () {
      const thread = yield* target(threadId);
      const snapshot = yield* query
        .getCommandReadModel()
        .pipe(Effect.mapError(() => fail("target_unavailable")));
      const project = snapshot.projects.find((p) => p.id === thread.projectId);
      if (!project) return yield* fail("target_unavailable");
      const cwd = thread.worktreePath ?? project.workspaceRoot;
      const list = yield* providers.refreshWorkspaceSnapshot({
        instanceId: thread.modelSelection.instanceId,
        cwd,
        fresh: true,
      });
      const provider = list.find((p) => p.instanceId === thread.modelSelection.instanceId);
      // These adapters implement the existing composer $skill invocation path.
      if (!provider || !["codex", "claudeAgent", "cursor"].includes(provider.driver))
        return yield* fail("unsupported_skill_provider");
      const skills =
        provider.workspaceSnapshots?.find((s) => s.cwd === cwd)?.skills ?? provider.skills;
      return skills
        .filter((s) => s.enabled && /^[a-zA-Z][a-zA-Z0-9:_-]{0,100}$/.test(s.name))
        .map((s) => s.name);
    });
  const get = read<StoredBinding>(bindingPath).pipe(Effect.map((s) => s?.binding ?? null));
  const configure = (input: SignalConfigure) =>
    lock.withPermit(
      Effect.gen(function* () {
        const names = new Set<string>();
        for (const route of input.commands) {
          if (
            names.has(route.command) ||
            ["/ping", "/help", "/commands", "/status", "/pair"].includes(route.command)
          )
            return yield* fail("command_reserved_or_duplicate");
          names.add(route.command);
          if (!(yield* choices(route.threadId)).includes(route.skill))
            return yield* fail("skill_unavailable");
        }
        const token = NodeCrypto.randomBytes(32).toString("base64url");
        const binding: SignalBinding = {
          ...input,
          environmentId: yield* environment.getEnvironmentId,
          connectionId: NodeCrypto.randomUUID(),
          enabled: true,
        };
        yield* write(bindingPath, { binding, tokenHash: hash(token) });
        return { binding, token };
      }),
    );
  const revoke = lock.withPermit(
    Effect.gen(function* () {
      const stored = yield* read<StoredBinding>(bindingPath);
      if (stored)
        yield* write(bindingPath, {
          ...stored,
          tokenHash: "",
          binding: { ...stored.binding, enabled: false },
        });
    }),
  );
  const trigger = (input: SignalTrigger, token: string) =>
    lock.withPermit(
      Effect.gen(function* () {
        const stored = yield* read<StoredBinding>(bindingPath);
        const actualHash = hash(token);
        if (
          !stored?.binding.enabled ||
          stored.tokenHash.length !== actualHash.length ||
          !NodeCrypto.timingSafeEqual(Buffer.from(stored.tokenHash), Buffer.from(actualHash)) ||
          stored.binding.connectionId !== input.connectionId ||
          stored.binding.environmentId !== input.environmentId ||
          input.environmentId !== (yield* environment.getEnvironmentId)
        )
          return yield* fail("credential_or_binding_invalid");
        const binding = stored.binding;
        const route = binding.commands.find((entry) => entry.command === input.command);
        if (!route) return yield* fail("command_unavailable");
        const thread = yield* target(route.threadId);
        const key = hash(`${binding.connectionId}:${input.requestId}`);
        const path = join(directory, `${key}.json`);
        const storedRequest = yield* read<{
          inputCommand: string;
          command: typeof ThreadTurnStartCommand.Type;
        }>(path);
        if (storedRequest && storedRequest.inputCommand !== input.command)
          return yield* fail("request_id_conflict");
        let command = storedRequest?.command ?? null;
        if (command === null) {
          if (!(yield* choices(route.threadId)).includes(route.skill))
            return yield* fail("skill_unavailable");
          command = {
            type: "thread.turn.start",
            requireIdle: true,
            commandId: CommandId.make(`signal-${key}`),
            threadId: route.threadId,
            message: {
              messageId: MessageId.make(`signal-${key}`),
              role: "user",
              attachments: [],
              text: `Requested from the linked Signal group using ${route.command}. Request: ${input.requestId.slice(0, 8)}.\n\n$${route.skill}${route.skillArguments ? ` ${route.skillArguments}` : ""}`,
            },
            runtimeMode: thread.runtimeMode,
            interactionMode: thread.interactionMode,
            createdAt: DateTime.formatIso(yield* DateTime.now),
          };
          yield* write(path, { inputCommand: input.command, command });
        }
        const decoded = yield* decodeCommand(command).pipe(
          Effect.mapError(() => fail("storage_invalid")),
        );
        yield* engine
          .dispatch(decoded)
          .pipe(
            Effect.mapError((error) =>
              fail(
                error._tag === "OrchestrationThreadBusyError" ? "thread_busy" : "dispatch_failed",
              ),
            ),
          );
        return { threadId: route.threadId, messageId: command.message.messageId };
      }),
    );
  return SignalIntegration.of({ get, choices, configure, revoke, trigger });
});
export const layer = Layer.effect(SignalIntegration, make);
