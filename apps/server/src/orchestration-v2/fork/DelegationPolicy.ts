import { OrchestratorMcpFailure, type ThreadId } from "@t3tools/contracts";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";

import type { McpInvocationScope } from "../../mcp/McpInvocationContext.ts";
import * as ServerSettings from "../../serverSettings.ts";
import { DelegationGrants } from "./DelegationGrants.ts";

/**
 * Fork admission for every delegated child, native `delegate_task` and `agent_spawn` alike.
 *
 * Runs inside `OrchestratorMcpService.delegateTask` before the child is dispatched: the caller's
 * credential must carry `delegation`, the child must fit under `crossProviderAgentMaxDepth`, and
 * the child's grant row is written under its deterministic thread id so its own credential is
 * minted with the right bits. Native callers never grant orchestration.
 */
export class DelegationPolicy extends Context.Service<
  DelegationPolicy,
  {
    readonly admit: (input: {
      readonly scope: McpInvocationScope;
      readonly parentThreadId: ThreadId;
      readonly childThreadId: ThreadId;
      readonly allowOrchestration: boolean;
      /** Spawned through `agent_spawn`: its parent wakes carry the capped result inline. */
      readonly aliasOwned?: boolean;
      /** A retry of an accepted delegation only replays it, so its depth was already admitted. */
      readonly replay: boolean;
    }) => Effect.Effect<void, OrchestratorMcpFailure>;
  }
>()("t3/orchestration-v2/fork/DelegationPolicy") {}

const failure = (code: OrchestratorMcpFailure["code"], message: string) =>
  new OrchestratorMcpFailure({ code, message });

const make = Effect.gen(function* () {
  const grants = yield* DelegationGrants;
  const serverSettings = yield* ServerSettings.ServerSettingsService;

  return DelegationPolicy.of({
    admit: (input) =>
      Effect.gen(function* () {
        if (!input.scope.capabilities.has("delegation")) {
          return yield* failure(
            "capability_denied",
            "This thread was not granted orchestration, so it cannot delegate tasks.",
          );
        }
        if (input.replay) return;
        const settings = yield* serverSettings.getSettings.pipe(
          Effect.mapError((error) =>
            failure("orchestration_error", `Unable to read settings: ${error.message}`),
          ),
        );
        const childDepth =
          (yield* grants
            .depthOf(input.parentThreadId)
            .pipe(
              Effect.mapError((error) =>
                failure(
                  "orchestration_error",
                  `Unable to read the delegation depth: ${error.message}`,
                ),
              ),
            )) + 1;
        const maxDepth = settings.crossProviderAgentMaxDepth;
        if (childDepth > maxDepth || (input.allowOrchestration && childDepth >= maxDepth)) {
          return yield* failure(
            "depth_exceeded",
            input.allowOrchestration
              ? `A sub-orchestrator at depth ${childDepth} could never spawn within the depth cap of ${maxDepth}.`
              : `Spawning at depth ${childDepth} exceeds the depth cap of ${maxDepth}.`,
          );
        }
        yield* grants
          .record({
            childThreadId: input.childThreadId,
            parentThreadId: input.parentThreadId,
            depth: childDepth,
            allowOrchestration: input.allowOrchestration,
          })
          .pipe(
            Effect.andThen(
              input.aliasOwned === true ? grants.markAliasOwned(input.childThreadId) : Effect.void,
            ),
            Effect.mapError((error) =>
              failure(
                "orchestration_error",
                `Unable to record the delegation grant: ${error.message}`,
              ),
            ),
          );
      }),
  });
});

export const layer = Layer.effect(DelegationPolicy, make);
