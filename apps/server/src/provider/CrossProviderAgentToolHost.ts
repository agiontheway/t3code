import type { ThreadId } from "@t3tools/contracts";
import type * as Effect from "effect/Effect";
import type * as Option from "effect/Option";
import * as Schema from "effect/Schema";

/**
 * One logical cross-provider agent tool. `inputSchema` is a JSON Schema
 * object; each provider binding derives its native shape from it (Zod for the
 * Claude SDK, verbatim for Codex dynamic tools) so there is one source of
 * truth for the tool contract.
 */
export interface CrossProviderToolSpec {
  readonly name: string;
  readonly description: string;
  readonly inputSchema: Readonly<Record<string, unknown>>;
}

/**
 * Structured tool outcome. `output` is the JSON value the model sees, both on
 * success and on failure (`{ error: { code, ... } }`); bindings serialise it
 * verbatim and only map `isError` onto their transport's success flag.
 * Errors never cross this seam as failures.
 */
export interface CrossProviderToolResult {
  readonly output: unknown;
  readonly isError: boolean;
}

/**
 * Provider-neutral seam between the adapters and the cross-provider agent
 * service. Orchestration depends on the provider layer, never the reverse,
 * so the service registers itself here at startup and adapters only consult
 * the registry while building a session.
 */
export interface CrossProviderAgentToolHost {
  /** `None` when the thread is not granted the tools (access off, unsupported caller, leaf child). */
  readonly toolsForThread: (
    threadId: ThreadId,
  ) => Effect.Effect<Option.Option<ReadonlyArray<CrossProviderToolSpec>>>;
  readonly call: (
    threadId: ThreadId,
    tool: string,
    args: unknown,
  ) => Effect.Effect<CrossProviderToolResult>;
}

let registeredHost: CrossProviderAgentToolHost | undefined;

export function setCrossProviderAgentToolHost(host: CrossProviderAgentToolHost): void {
  registeredHost = host;
}

export function readCrossProviderAgentToolHost(): CrossProviderAgentToolHost | undefined {
  return registeredHost;
}

export function clearCrossProviderAgentToolHost(): void {
  registeredHost = undefined;
}

/** Tool output as the text payload both bindings hand back to the model. */
export const encodeCrossProviderToolOutput = Schema.encodeSync(
  Schema.fromJsonString(Schema.Unknown),
);
