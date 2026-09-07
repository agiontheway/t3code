/**
 * CrossProviderAgentService - provider-neutral cross-provider agent
 * operations (catalog, spawn, wait, result, follow-up, interrupt).
 *
 * Every operation is scoped to a calling thread and returns a structured
 * value: the tool output on success, `{ error }` on any rejection. Nothing
 * here fails in the error channel, so provider bindings can serialise the
 * result verbatim. See the build spec §3.2.
 *
 * @module CrossProviderAgentService
 */
import type {
  CrossProviderAgentCatalogOutput,
  CrossProviderAgentErrorOutput,
  CrossProviderAgentFollowUpInput,
  CrossProviderAgentFollowUpOutput,
  CrossProviderAgentInterruptInput,
  CrossProviderAgentInterruptOutput,
  CrossProviderAgentResultInput,
  CrossProviderAgentResultOutput,
  CrossProviderAgentSpawnInput,
  CrossProviderAgentSpawnOutput,
  CrossProviderAgentWaitInput,
  CrossProviderAgentWaitOutput,
  ThreadId,
} from "@t3tools/contracts";
import * as Context from "effect/Context";
import type * as Effect from "effect/Effect";

import type { CrossProviderAgentToolHost } from "../../provider/CrossProviderAgentToolHost.ts";

export type CrossProviderAgentResult<Output> = Output | CrossProviderAgentErrorOutput;

/** Provider-supplied identity of the invocation; see `CrossProviderAgentToolHost.call`. */
export interface CrossProviderAgentCallOptions {
  readonly callId?: string | undefined;
}

export interface CrossProviderAgentServiceShape extends CrossProviderAgentToolHost {
  readonly catalog: (
    callerThreadId: ThreadId,
  ) => Effect.Effect<CrossProviderAgentResult<CrossProviderAgentCatalogOutput>>;
  readonly spawn: (
    callerThreadId: ThreadId,
    input: CrossProviderAgentSpawnInput,
    options?: CrossProviderAgentCallOptions,
  ) => Effect.Effect<CrossProviderAgentResult<CrossProviderAgentSpawnOutput>>;
  readonly wait: (
    callerThreadId: ThreadId,
    input: CrossProviderAgentWaitInput,
  ) => Effect.Effect<CrossProviderAgentResult<CrossProviderAgentWaitOutput>>;
  readonly result: (
    callerThreadId: ThreadId,
    input: CrossProviderAgentResultInput,
  ) => Effect.Effect<CrossProviderAgentResult<CrossProviderAgentResultOutput>>;
  readonly followUp: (
    callerThreadId: ThreadId,
    input: CrossProviderAgentFollowUpInput,
    options?: CrossProviderAgentCallOptions,
  ) => Effect.Effect<CrossProviderAgentResult<CrossProviderAgentFollowUpOutput>>;
  readonly interrupt: (
    callerThreadId: ThreadId,
    input: CrossProviderAgentInterruptInput,
    options?: CrossProviderAgentCallOptions,
  ) => Effect.Effect<CrossProviderAgentResult<CrossProviderAgentInterruptOutput>>;
}

export class CrossProviderAgentService extends Context.Service<
  CrossProviderAgentService,
  CrossProviderAgentServiceShape
>()("t3/orchestration/Services/CrossProviderAgent/CrossProviderAgentService") {}
