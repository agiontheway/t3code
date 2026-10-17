// @effect-diagnostics-next-line nodeBuiltinImport:off -- Effect's Crypto has no timingSafeEqual.
import * as NodeCrypto from "node:crypto";
import {
  CommandId,
  MessageId,
  type OrchestrationV2ServerCommand,
  SignalIntegrationError,
  type SignalBinding,
  type SignalConfigure,
  type SignalTrigger,
  type SignalTriggerResult,
  ThreadId,
} from "@t3tools/contracts";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as FileSystem from "effect/FileSystem";
import * as Path from "effect/Path";
import * as Schema from "effect/Schema";
import * as Option from "effect/Option";
import * as Semaphore from "effect/Semaphore";
import { ServerConfig } from "../config.ts";
import { ServerEnvironment } from "../environment/ServerEnvironment.ts";
import { ThreadManagementService } from "../orchestration-v2/ThreadManagementService.ts";
import { ProjectService } from "../project/ProjectService.ts";
import { ProviderRegistry } from "../provider/ProviderRegistry.ts";

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

// A request file holds the v2 dispatch written by this server, or the v1
// `thread.turn.start` written before the upgrade. Both replay as the same
// v2 dispatch, keyed by the stored command and message ids; old files are
// never rewritten.
const StoredDispatch = Schema.Union([
  Schema.Struct({
    type: Schema.Literal("message.dispatch"),
    commandId: CommandId,
    threadId: ThreadId,
    messageId: MessageId,
    text: Schema.String,
  }),
  Schema.Struct({
    type: Schema.Literal("thread.turn.start"),
    commandId: CommandId,
    threadId: ThreadId,
    message: Schema.Struct({ messageId: MessageId, text: Schema.String }),
  }),
]);
const decodeCommand = Schema.decodeUnknownEffect(StoredDispatch);

const toDispatch = (stored: typeof StoredDispatch.Type) => {
  const message =
    stored.type === "message.dispatch"
      ? { messageId: stored.messageId, text: stored.text }
      : stored.message;
  return {
    type: "message.dispatch",
    commandId: stored.commandId,
    threadId: stored.threadId,
    messageId: message.messageId,
    text: message.text,
    attachments: [],
    createdBy: "user",
    creationSource: "server",
    // Busy threads refuse instead of queueing, and the refusal is not
    // recorded, so the worker's retry under the same command id can start.
    dispatchMode: { type: "start_if_idle" },
  } satisfies OrchestrationV2ServerCommand;
};

const make = Effect.gen(function* () {
  const config = yield* ServerConfig;
  const fs = yield* FileSystem.FileSystem;
  const pathService = yield* Path.Path;
  const join = pathService.join;
  const environment = yield* ServerEnvironment;
  const threads = yield* ThreadManagementService;
  const projects = yield* ProjectService;
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
      const thread = yield* threads
        .getThreadShell(threadId)
        .pipe(Effect.mapError(() => fail("target_unavailable")));
      if (thread === null || thread.archivedAt !== null || thread.deletedAt !== null)
        return yield* fail("target_unavailable");
      return thread;
    });
  const choices = (threadId: ThreadId) =>
    Effect.gen(function* () {
      const thread = yield* target(threadId);
      const project = yield* projects
        .getById(thread.projectId)
        .pipe(Effect.mapError(() => fail("target_unavailable")));
      if (Option.isNone(project)) return yield* fail("target_unavailable");
      const cwd = thread.worktreePath ?? project.value.workspaceRoot;
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
        yield* target(route.threadId);
        const key = hash(`${binding.connectionId}:${input.requestId}`);
        const path = join(directory, `${key}.json`);
        const storedRequest = yield* read<{ inputCommand: string; command: unknown }>(path);
        if (storedRequest && storedRequest.inputCommand !== input.command)
          return yield* fail("request_id_conflict");
        let command = storedRequest?.command ?? null;
        if (command === null) {
          if (!(yield* choices(route.threadId)).includes(route.skill))
            return yield* fail("skill_unavailable");
          command = toDispatch({
            type: "message.dispatch",
            commandId: CommandId.make(`signal-${key}`),
            threadId: route.threadId,
            messageId: MessageId.make(`signal-${key}`),
            text: `Requested from the linked Signal group using ${route.command}. Request: ${input.requestId.slice(0, 8)}.\n\n$${route.skill}${route.skillArguments ? ` ${route.skillArguments}` : ""}`,
          });
          // Persisted before dispatch so a retry replays the same command.
          yield* write(path, { inputCommand: input.command, command });
        }
        const dispatch = toDispatch(
          yield* decodeCommand(command).pipe(Effect.mapError(() => fail("storage_invalid"))),
        );
        yield* threads
          .dispatch(dispatch)
          .pipe(
            Effect.mapError((error) =>
              fail(
                error._tag === "OrchestratorThreadBusyError" ? "thread_busy" : "dispatch_failed",
              ),
            ),
          );
        return { threadId: route.threadId, messageId: dispatch.messageId };
      }),
    );
  return SignalIntegration.of({ get, choices, configure, revoke, trigger });
});
export const layer = Layer.effect(SignalIntegration, make);
