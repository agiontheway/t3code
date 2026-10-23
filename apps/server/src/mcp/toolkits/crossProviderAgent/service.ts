/**
 * fork: the cross-provider agent tools (`agent_*`) as aliases over the native delegated-task
 * substrate. Children are native app-owned delegated tasks; this layer keeps the 0.0.45 inputs,
 * outputs and error codes and adds what native lacks: the route allowlist,
 * the Claude/Codex driver filter, effort inheritance, the head + tail output cap,
 * multi-child waits (clamped and rate limited, see `waitGuard.ts`) and ownership by any
 * ancestor. Depth and the orchestration grant are applied by `DelegationPolicy` inside
 * `OrchestratorMcpService.delegateTask`.
 */
import {
  CROSS_PROVIDER_AGENT_SUPPORTED_DRIVERS,
  type CrossProviderAgentCatalogOutput,
  type CrossProviderAgentCatalogRoute,
  type CrossProviderAgentError,
  type CrossProviderAgentErrorOutput,
  type CrossProviderAgentFollowUpInput,
  type CrossProviderAgentFollowUpOutput,
  type CrossProviderAgentInterruptInput,
  type CrossProviderAgentInterruptOutput,
  type CrossProviderAgentResultInput,
  type CrossProviderAgentResultOutput,
  type CrossProviderAgentSpawnInput,
  type CrossProviderAgentSpawnOutput,
  type CrossProviderAgentWaitChild,
  type CrossProviderAgentWaitInput,
  type CrossProviderAgentWaitOutput,
  type NodeId,
  type OrchestratorMcpFailure,
  type OrchestrationV2ThreadShell,
  ProviderInstanceId,
  type ServerProvider,
  type ServerSettings,
  ThreadId,
} from "@t3tools/contracts";
import { resolveCrossProviderAgentRoutes } from "@t3tools/shared/crossProviderAgentRoutes";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Hash from "effect/Hash";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";

import * as DelegationGrants from "../../../orchestration-v2/fork/DelegationGrants.ts";
import * as ThreadManagementService from "../../../orchestration-v2/ThreadManagementService.ts";
import * as ProviderRegistry from "../../../provider/ProviderRegistry.ts";
import * as ServerSettingsService from "../../../serverSettings.ts";
import type { McpInvocationScope, McpThreadInvocationScope } from "../../McpInvocationContext.ts";
import * as OrchestratorMcpService from "../../OrchestratorMcpService.ts";
import {
  applyOutputCap,
  childStateFromTaskStatus,
  resolveCrossProviderEffort,
  threadTitle,
} from "./policy.ts";
import { makeWaitRateLimiter, WAIT_RATE_LIMITED_MESSAGE, waitTimeoutMs } from "./waitGuard.ts";

const MAX_OWNERSHIP_HOPS = 32;

type Failed = CrossProviderAgentErrorOutput;

export class CrossProviderAgentMcpService extends Context.Service<
  CrossProviderAgentMcpService,
  {
    readonly catalog: (
      scope: McpInvocationScope,
    ) => Effect.Effect<CrossProviderAgentCatalogOutput, Failed>;
    readonly spawn: (
      scope: McpInvocationScope,
      input: CrossProviderAgentSpawnInput,
    ) => Effect.Effect<CrossProviderAgentSpawnOutput, Failed>;
    readonly wait: (
      scope: McpInvocationScope,
      input: CrossProviderAgentWaitInput,
    ) => Effect.Effect<CrossProviderAgentWaitOutput, Failed>;
    readonly result: (
      scope: McpInvocationScope,
      input: CrossProviderAgentResultInput,
    ) => Effect.Effect<CrossProviderAgentResultOutput, Failed>;
    readonly followUp: (
      scope: McpInvocationScope,
      input: CrossProviderAgentFollowUpInput,
    ) => Effect.Effect<CrossProviderAgentFollowUpOutput, Failed>;
    readonly interrupt: (
      scope: McpInvocationScope,
      input: CrossProviderAgentInterruptInput,
    ) => Effect.Effect<CrossProviderAgentInterruptOutput, Failed>;
  }
>()("t3/mcp/toolkits/crossProviderAgent/service/CrossProviderAgentMcpService") {}

const fail = (error: CrossProviderAgentError) => Effect.fail({ error } satisfies Failed);
const isThreadId = Schema.is(ThreadId);
const isProviderInstanceId = Schema.is(ProviderInstanceId);

/** Internal failures become a structured result; the cause is logged, never surfaced. */
const orInternal =
  (operation: string) =>
  <A, E, R>(effect: Effect.Effect<A, E, R>): Effect.Effect<A, Failed, R> =>
    effect.pipe(
      Effect.catch((cause) =>
        Effect.logError("cross-provider agent operation failed", { operation, cause }).pipe(
          Effect.andThen(
            fail({
              code: "internal",
              message: `${operation} failed inside T3; see the server log.`,
            }),
          ),
        ),
      ),
    );

/** A native refusal in the fork's error vocabulary. */
const fromNative =
  (detail: Pick<CrossProviderAgentError, "providerInstanceId" | "model" | "childId">) =>
  (failure: OrchestratorMcpFailure) => {
    const code: CrossProviderAgentError["code"] =
      failure.code === "depth_exceeded"
        ? "depth_exceeded"
        : failure.code === "provider_unavailable"
          ? "route_not_eligible"
          : failure.code === "model_unavailable"
            ? "model_not_offered"
            : failure.code === "invalid_request"
              ? "invalid_input"
              : failure.code === "capability_denied"
                ? "access_disabled"
                : "internal";
    return fail({ code, message: failure.message, ...detail });
  };

interface CallerContext {
  readonly scope: McpThreadInvocationScope;
  readonly settings: ServerSettings;
  readonly shell: OrchestrationV2ThreadShell;
  readonly providers: ReadonlyArray<ServerProvider>;
  readonly callerProvider: ServerProvider;
  readonly callerDepth: number;
}

/** An owned child and the task its latest alias turn runs under. */
interface OwnedChild {
  readonly childThreadId: ThreadId;
  readonly parentThreadId: ThreadId;
  readonly taskId: NodeId;
}

const make = Effect.gen(function* () {
  const orchestrator = yield* OrchestratorMcpService.OrchestratorMcpService;
  const threads = yield* ThreadManagementService.ThreadManagementService;
  const providerRegistry = yield* ProviderRegistry.ProviderRegistry;
  const serverSettings = yield* ServerSettingsService.ServerSettingsService;
  // Optional like DelegationPolicy, so hand-assembled MCP layers keep working; without it every
  // thread reports depth 0. The production composition always provides it.
  const grants = yield* Effect.serviceOption(DelegationGrants.DelegationGrants);
  const waitRateLimiter = makeWaitRateLimiter();

  const readShell = (threadId: ThreadId) =>
    threads.getThreadShell(threadId).pipe(
      Effect.map((shell) => (shell === null || shell.deletedAt !== null ? null : shell)),
      orInternal("readThread"),
    );

  const callerContext = (scope: McpInvocationScope) =>
    Effect.gen(function* () {
      const settings = yield* serverSettings.getSettings.pipe(orInternal("readSettings"));
      if (!settings.enableCrossProviderAgentAccess) {
        return yield* fail({
          code: "access_disabled",
          message: "Cross-provider agent access is disabled in Settings → Integrations → Agents.",
        });
      }
      if (scope.thread === undefined) {
        return yield* fail({
          code: "unsupported_caller",
          message: "Only an agent running inside a T3 thread can use cross-provider agent tools.",
        });
      }
      const threadScope: McpThreadInvocationScope = { ...scope, thread: scope.thread };
      const shell = yield* readShell(scope.thread.threadId);
      if (shell === null) {
        return yield* fail({
          code: "unsupported_caller",
          message: "The calling thread does not exist.",
        });
      }
      const providers = yield* providerRegistry.getProviders;
      const callerInstanceId = shell.modelSelection.instanceId;
      const callerProvider = providers.find((provider) => provider.instanceId === callerInstanceId);
      if (
        callerProvider === undefined ||
        !CROSS_PROVIDER_AGENT_SUPPORTED_DRIVERS.includes(callerProvider.driver)
      ) {
        return yield* fail({
          code: "unsupported_caller",
          message: "Only Claude and Codex threads can use cross-provider agent tools.",
          providerInstanceId: callerInstanceId,
        });
      }
      // Minted with the credential: a session started while access was off, or a child without
      // an orchestration grant, has no bit.
      if (!scope.capabilities.has("crossProviderAgents")) {
        return yield* fail({
          code: "access_disabled",
          message: "Cross-provider agent tools are not granted to this thread.",
        });
      }
      const callerDepth = Option.isSome(grants)
        ? yield* grants.value.depthOf(shell.id).pipe(orInternal("readDepth"))
        : 0;
      return {
        scope: threadScope,
        settings,
        shell,
        providers,
        callerProvider,
        callerDepth,
      } satisfies CallerContext;
    });

  /** The caller owns a child when it is the parent or any ancestor in the subagent chain. */
  const resolveOwnedChild = (context: CallerContext, rawChildId: string) =>
    Effect.gen(function* () {
      const notOwner = fail({
        code: "not_owner",
        message: "No child with that id is owned by this thread.",
        childId: rawChildId,
      });
      if (!isThreadId(rawChildId)) return yield* notOwner;
      const childThreadId = ThreadId.make(rawChildId);
      const child = yield* readShell(childThreadId);
      const parentThreadId = child?.lineage.parentThreadId ?? null;
      if (
        child === null ||
        parentThreadId === null ||
        child.lineage.relationshipToParent !== "subagent"
      ) {
        return yield* notOwner;
      }
      let cursor: OrchestrationV2ThreadShell = child;
      let owned = false;
      for (let hops = 0; hops < MAX_OWNERSHIP_HOPS; hops += 1) {
        const ancestorId = cursor.lineage.parentThreadId;
        if (cursor.lineage.relationshipToParent !== "subagent" || ancestorId === null) break;
        if (ancestorId === context.scope.thread.threadId) {
          owned = true;
          break;
        }
        const ancestor = yield* readShell(ancestorId);
        if (ancestor === null) break;
        cursor = ancestor;
      }
      if (!owned) return yield* notOwner;

      const { subagents } = yield* threads
        .getThreadRecords(parentThreadId, ["subagents"])
        .pipe(orInternal("readChild"));
      const tasks = subagents.filter(
        (task) =>
          task.origin === "app_owned" &&
          task.threadId === parentThreadId &&
          task.childThreadId === childThreadId,
      );
      if (tasks.length === 0) return yield* notOwner;
      // A follow-up is a fresh task in the same child thread; the newest run names the latest.
      const taskIds = new Set(tasks.map((task) => task.id));
      const { runs } = yield* threads
        .getThreadRecords(childThreadId, ["runs"])
        .pipe(orInternal("readChild"));
      const latestRun = runs
        .filter((run) => run.delegatedTaskId !== undefined && taskIds.has(run.delegatedTaskId))
        .toSorted((left, right) => right.ordinal - left.ordinal)[0];
      const task =
        tasks.find((candidate) => candidate.id === latestRun?.delegatedTaskId) ?? tasks[0]!;
      return { childThreadId, parentThreadId, taskId: task.id } satisfies OwnedChild;
    });

  const readChildTask = (context: CallerContext, child: OwnedChild) =>
    orchestrator
      .readTaskAsParent(context.scope, child.parentThreadId, child.taskId)
      .pipe(Effect.catch(fromNative({ childId: child.childThreadId })));

  return CrossProviderAgentMcpService.of({
    catalog: (scope) =>
      Effect.gen(function* () {
        const context = yield* callerContext(scope);
        // Never offer a route native delegation would refuse.
        const capabilities = yield* orchestrator
          .capabilities(context.scope)
          .pipe(Effect.catch(fromNative({})));
        const runnable = new Set(
          capabilities.providers
            .filter((provider) => provider.canRunChildTask)
            .map((provider) => provider.providerInstanceId),
        );
        const routes: Array<CrossProviderAgentCatalogRoute> = [];
        for (const route of resolveCrossProviderAgentRoutes(
          context.settings.crossProviderAgentRoutes,
          context.providers,
        )) {
          if (!runnable.has(route.providerInstanceId)) continue;
          const enabledForProject = yield* threads.isProviderInstanceEnabledForProject({
            projectId: context.shell.projectId,
            instanceId: route.providerInstanceId,
          });
          if (!enabledForProject) continue;
          routes.push({
            providerInstanceId: route.providerInstanceId,
            displayName: route.provider.displayName ?? route.providerInstanceId,
            driver: route.provider.driver,
            models: route.provider.models
              .filter((model) => route.models.length === 0 || route.models.includes(model.slug))
              .map((model) => ({ slug: model.slug, name: model.name })),
            isCallerInstance: route.providerInstanceId === context.callerProvider.instanceId,
          });
        }
        return {
          routes,
          maxDepth: context.settings.crossProviderAgentMaxDepth,
          callerDepth: context.callerDepth,
        };
      }),

    spawn: (scope, input) =>
      Effect.gen(function* () {
        const context = yield* callerContext(scope);
        const { settings, shell: parent, providers, callerProvider } = context;
        if (!isProviderInstanceId(input.providerInstanceId)) {
          return yield* fail({
            code: "route_not_eligible",
            message: "Unknown provider instance; use an id from agent_catalog.",
            providerInstanceId: input.providerInstanceId,
          });
        }
        const targetInstanceId = ProviderInstanceId.make(input.providerInstanceId);
        const target = providers.find((provider) => provider.instanceId === targetInstanceId);
        const supportedTarget =
          target !== undefined && CROSS_PROVIDER_AGENT_SUPPORTED_DRIVERS.includes(target.driver);
        const enabledForProject =
          !supportedTarget ||
          (target.enabled &&
            (yield* threads.isProviderInstanceEnabledForProject({
              projectId: parent.projectId,
              instanceId: targetInstanceId,
            })));
        if (supportedTarget && !enabledForProject) {
          return yield* fail({
            code: "instance_disabled",
            message: "That provider instance is disabled.",
            providerInstanceId: targetInstanceId,
          });
        }
        if (supportedTarget && target.auth.status === "unauthenticated") {
          return yield* fail({
            code: "instance_unauthenticated",
            message: "That provider instance is signed out.",
            providerInstanceId: targetInstanceId,
          });
        }
        const route = resolveCrossProviderAgentRoutes(
          settings.crossProviderAgentRoutes,
          providers,
        ).find((candidate) => candidate.providerInstanceId === targetInstanceId);
        if (route === undefined) {
          return yield* fail({
            code: "route_not_eligible",
            message: "That provider instance is not an eligible cross-provider route.",
            providerInstanceId: targetInstanceId,
          });
        }
        const targetModel = route.provider.models.find((model) => model.slug === input.model);
        if (
          targetModel === undefined ||
          (route.models.length > 0 && !route.models.includes(input.model))
        ) {
          return yield* fail({
            code: "model_not_offered",
            message: "That exact model is not offered on that provider instance.",
            providerInstanceId: targetInstanceId,
            model: input.model,
          });
        }
        const effort = resolveCrossProviderEffort({
          explicit: input.effort,
          parent: {
            driver: callerProvider.driver,
            model: callerProvider.models.find(
              (model) => model.slug === parent.modelSelection.model,
            ),
            modelSelection: parent.modelSelection,
          },
          target: { driver: route.provider.driver, model: targetModel },
        });
        if ("unsupported" in effort) {
          return yield* fail({
            code: "invalid_input",
            message:
              effort.unsupported.length === 0
                ? `Model ${input.model} on that instance has no effort setting; omit effort.`
                : `Model ${input.model} on that instance does not offer effort "${input.effort}"; offered: ${effort.unsupported.join(", ")}.`,
            providerInstanceId: targetInstanceId,
            model: input.model,
          });
        }
        // Native delegation creates the child. DelegationPolicy refuses a child beyond the depth
        // cap and records its grant; no clientRequestId, since HTTP MCP carries no stable call id.
        const delegated = yield* orchestrator
          .delegateTask(
            context.scope,
            {
              task: input.prompt,
              target: {
                providerInstanceId: targetInstanceId,
                model: input.model,
                ...(effort.selection === undefined ? {} : { options: [effort.selection] }),
              },
              title: threadTitle(input.title, input.prompt),
              mode: "async",
            },
            { allowOrchestration: input.allowOrchestration === true, aliasOwned: true },
          )
          .pipe(
            Effect.catch(fromNative({ providerInstanceId: targetInstanceId, model: input.model })),
          );
        yield* Effect.logInfo("cross-provider agent spawned", {
          parentThreadId: parent.id,
          childThreadId: delegated.childThreadId,
          providerInstanceId: targetInstanceId,
          model: input.model,
          effort: effort.selection?.value ?? null,
          depth: context.callerDepth + 1,
          allowOrchestration: input.allowOrchestration === true,
        });
        return {
          childId: delegated.childThreadId,
          taskId: delegated.taskId,
          providerInstanceId: targetInstanceId,
          model: input.model,
          status: "running" as const,
        };
      }),

    wait: (scope, input) =>
      Effect.gen(function* () {
        const context = yield* callerContext(scope);
        if (!(yield* waitRateLimiter.admit(context.scope.thread.threadId))) {
          return yield* fail({ code: "rate_limited", message: WAIT_RATE_LIMITED_MESSAGE });
        }
        const children: Array<OwnedChild> = [];
        for (const rawChildId of input.childIds) {
          children.push(yield* resolveOwnedChild(context, rawChildId));
        }
        // Every child waits concurrently under the same clamped deadline: a wait retrieves
        // results, waiting for time belongs to the heartbeat tools.
        const timeoutMs = waitTimeoutMs(input.timeoutSeconds);
        yield* Effect.forEach(
          children,
          (child) =>
            orchestrator
              .waitForTaskAsParent(context.scope, child.parentThreadId, child.taskId, timeoutMs)
              .pipe(Effect.catch(fromNative({ childId: child.childThreadId }))),
          { concurrency: "unbounded", discard: true },
        );
        const cap = context.settings.crossProviderAgentOutputCapChars;
        const entries: Array<CrossProviderAgentWaitChild> = [];
        for (const child of children) {
          const task = yield* readChildTask(context, child);
          const { settled, state } = childStateFromTaskStatus(task.status);
          entries.push(
            settled
              ? {
                  childId: child.childThreadId,
                  settled,
                  state,
                  ...applyOutputCap(task.summary ?? "", cap, child.childThreadId),
                }
              : { childId: child.childThreadId, settled, state },
          );
        }
        return { children: entries };
      }),

    result: (scope, input) =>
      Effect.gen(function* () {
        const context = yield* callerContext(scope);
        const child = yield* resolveOwnedChild(context, input.childId);
        const task = yield* readChildTask(context, child);
        const { settled, state } = childStateFromTaskStatus(task.status);
        if (!settled) {
          return yield* fail({
            code: "child_not_settled",
            message: "The child is still running; wait for it first.",
            childId: child.childThreadId,
          });
        }
        const text = task.summary ?? "";
        if (input.offset === undefined && input.limit === undefined) {
          const capped = applyOutputCap(
            text,
            context.settings.crossProviderAgentOutputCapChars,
            child.childThreadId,
          );
          return {
            childId: child.childThreadId,
            state,
            ...capped,
            offset: 0,
            length: capped.output.length,
          };
        }
        const offset = Math.min(input.offset ?? 0, text.length);
        const slice = text.slice(
          offset,
          input.limit === undefined ? undefined : offset + input.limit,
        );
        return {
          childId: child.childThreadId,
          state,
          output: slice,
          truncated: slice.length < text.length,
          totalChars: text.length,
          offset,
          length: slice.length,
        };
      }),

    followUp: (scope, input) =>
      Effect.gen(function* () {
        const context = yield* callerContext(scope);
        const child = yield* resolveOwnedChild(context, input.childId);
        const task = yield* readChildTask(context, child);
        if (!childStateFromTaskStatus(task.status).settled) {
          return yield* fail({
            code: "child_not_settled",
            message: "The child is still running; wait for it before following up.",
            childId: child.childThreadId,
          });
        }
        // The follow-up's completion wake carries its result inline, like a spawned child's.
        if (Option.isSome(grants)) {
          yield* grants.value
            .markAliasOwned(child.childThreadId)
            .pipe(orInternal("markAliasOwned"));
        }
        // A send to a settled app-owned child from its direct parent starts a fresh task whose
        // completion wakes that parent, so an ancestor sends on the parent's behalf. The key
        // collapses concurrent duplicates of the same follow-up to one turn.
        yield* orchestrator
          .sendToThread(
            context.scope,
            {
              threadId: child.childThreadId,
              message: input.prompt,
              mode: "queue",
              clientRequestId: `agent-follow-up:${child.taskId}:${Hash.string(input.prompt)}`,
            },
            { senderThreadId: child.parentThreadId },
          )
          .pipe(Effect.catch(fromNative({ childId: child.childThreadId })));
        return { childId: child.childThreadId, status: "running" as const };
      }),

    interrupt: (scope, input) =>
      Effect.gen(function* () {
        const context = yield* callerContext(scope);
        const child = yield* resolveOwnedChild(context, input.childId);
        const task = yield* readChildTask(context, child);
        const notRunning = fail({
          code: "child_not_running",
          message: "The child is not running.",
          childId: child.childThreadId,
        });
        if (childStateFromTaskStatus(task.status).settled) return yield* notRunning;
        // A turn interrupt, not task_cancel: the child stays usable for a later follow-up.
        const interrupted = yield* orchestrator
          .interruptThread(context.scope, { threadId: child.childThreadId })
          .pipe(Effect.catch(fromNative({ childId: child.childThreadId })));
        if (interrupted.status !== "interrupt_requested") return yield* notRunning;
        return { childId: child.childThreadId, status: "interrupting" as const };
      }),
  });
});

export const layer: Layer.Layer<
  CrossProviderAgentMcpService,
  never,
  | OrchestratorMcpService.OrchestratorMcpService
  | ThreadManagementService.ThreadManagementService
  | ProviderRegistry.ProviderRegistry
  | ServerSettingsService.ServerSettingsService
> = Layer.effect(CrossProviderAgentMcpService, make);
