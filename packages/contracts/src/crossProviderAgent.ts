import * as Schema from "effect/Schema";

import { NonNegativeInt, TrimmedNonEmptyString } from "./baseSchemas.ts";
import { ProviderDriverKind, ProviderInstanceId } from "./providerInstance.ts";

/**
 * Provider-neutral contract for the cross-provider agent tools. Both provider
 * bindings derive their native tool schema from these Effect schemas, so the
 * logical tool surface has exactly one definition. See
 * docs/internals/native-cross-provider-agent-tools-build-spec.md §3.4.
 */

export const CROSS_PROVIDER_AGENT_TOOL_NAMES = [
  "agent_catalog",
  "agent_spawn",
  "agent_wait",
  "agent_result",
  "agent_follow_up",
  "agent_interrupt",
] as const;
export type CrossProviderAgentToolName = (typeof CROSS_PROVIDER_AGENT_TOOL_NAMES)[number];

/** Drivers that may act as cross-provider callers and targets. */
export const CROSS_PROVIDER_AGENT_SUPPORTED_DRIVER_KINDS = ["claudeAgent", "codex"] as const;
export type CrossProviderAgentSupportedDriverKind =
  (typeof CROSS_PROVIDER_AGENT_SUPPORTED_DRIVER_KINDS)[number];
export const CROSS_PROVIDER_AGENT_SUPPORTED_DRIVERS: ReadonlyArray<ProviderDriverKind> =
  CROSS_PROVIDER_AGENT_SUPPORTED_DRIVER_KINDS.map((kind) => ProviderDriverKind.make(kind));

export function isCrossProviderAgentSupportedDriverKind(
  driver: string,
): driver is CrossProviderAgentSupportedDriverKind {
  return (CROSS_PROVIDER_AGENT_SUPPORTED_DRIVER_KINDS as ReadonlyArray<string>).includes(driver);
}

/**
 * The `modelSelection.options` id each supported driver reads its reasoning
 * effort from (Claude: `effort`, Codex: `reasoningEffort`). A child's
 * resolved effort is stored under the target driver's id so the adapter
 * applies it exactly as it would a user-selected one. Keyed by the supported
 * driver union so adding a driver forces a decision here.
 */
export const CROSS_PROVIDER_AGENT_EFFORT_OPTION_IDS: Readonly<
  Record<CrossProviderAgentSupportedDriverKind, string>
> = {
  claudeAgent: "effort",
  codex: "reasoningEffort",
};

export function crossProviderAgentEffortOptionId(driver: ProviderDriverKind): string | undefined {
  const kind: string = driver;
  return isCrossProviderAgentSupportedDriverKind(kind)
    ? CROSS_PROVIDER_AGENT_EFFORT_OPTION_IDS[kind]
    : undefined;
}

const SAME_INSTANCE_NOTE =
  "Use this only to delegate to a DIFFERENT provider instance than your own; for your own provider use your native spawn/subagent tool. providerInstanceId is authoritative and must come from agent_catalog (never a display name, driver, or model name). childId values are durable T3 thread ids that survive session restarts.";

export const CROSS_PROVIDER_AGENT_TOOL_DESCRIPTIONS: Record<CrossProviderAgentToolName, string> = {
  agent_catalog:
    "List the provider instances and exact model ids you may spawn cross-provider children on, plus your orchestration depth. Entries flagged isCallerInstance are your own instance and cannot be targeted. " +
    SAME_INSTANCE_NOTE,
  agent_spawn:
    "Create and start ONE child thread on an exact different provider instance with the given prompt. The child inherits your project, worktree, branch, and permission mode. Returns a durable childId; use agent_wait to collect its result. Set allowOrchestration only when the child must itself delegate further (costs one depth level). Omit effort to inherit your own reasoning effort when the target model offers it (otherwise the target's default); an effort the target model does not offer is rejected. " +
    SAME_INSTANCE_NOTE,
  agent_wait:
    "Block until the given owned children settle (completed, error, or interrupted) or timeoutSeconds elapses, then return each child's state and final output. Waits on T3 events; call again with the unsettled ids to keep waiting. Long outputs are returned as head + tail with truncated:true; fetch the rest with agent_result.",
  agent_result:
    "Read the stored final output of an owned settled child, optionally a character range (offset, limit). Use after agent_wait reported truncated:true, or after a restart.",
  agent_follow_up:
    "Start another turn on an owned settled child with a new prompt (same thread, same provider instance and model). Returns immediately; use agent_wait to collect the result.",
  agent_interrupt:
    "Interrupt an owned child that is currently running. The child settles as interrupted; you may follow it up later.",
};

const ProviderInstanceIdInput = Schema.String.annotate({
  description: "Exact providerInstanceId from agent_catalog.",
});
const ChildIdInput = Schema.String.annotate({
  description: "childId returned by agent_spawn.",
});

export const CrossProviderAgentCatalogInput = Schema.Struct({});
export type CrossProviderAgentCatalogInput = typeof CrossProviderAgentCatalogInput.Type;

export const CrossProviderAgentSpawnInput = Schema.Struct({
  providerInstanceId: ProviderInstanceIdInput,
  model: Schema.String.annotate({
    description: "Exact model slug offered by that instance in agent_catalog.",
  }),
  prompt: Schema.String.annotate({ description: "The child's task, self-contained." }),
  title: Schema.optionalKey(
    Schema.String.annotate({ description: "Short thread title; defaults to the prompt's start." }),
  ),
  allowOrchestration: Schema.optionalKey(
    Schema.Boolean.annotate({
      description: "Grant the child these same tools so it can delegate further. Default false.",
    }),
  ),
  effort: Schema.optionalKey(
    Schema.String.annotate({
      description:
        "Reasoning effort for the child, one of the levels the target model offers (for example low, medium, high). Defaults to your own effort when the target offers it, else the target's default.",
    }),
  ),
});
export type CrossProviderAgentSpawnInput = typeof CrossProviderAgentSpawnInput.Type;

/** Bounds on one wait call; larger inputs fail with `invalid_input`. */
export const CROSS_PROVIDER_AGENT_WAIT_MAX_CHILDREN = 32;
export const CROSS_PROVIDER_AGENT_WAIT_MAX_TIMEOUT_SECONDS = 600;

export const CrossProviderAgentWaitInput = Schema.Struct({
  childIds: Schema.Array(ChildIdInput)
    .check(Schema.isMaxLength(CROSS_PROVIDER_AGENT_WAIT_MAX_CHILDREN))
    .annotate({
      description: `Owned children to wait on (at most ${CROSS_PROVIDER_AGENT_WAIT_MAX_CHILDREN}).`,
    }),
  timeoutSeconds: Schema.optionalKey(
    Schema.Finite.check(
      Schema.isGreaterThan(0),
      Schema.isLessThanOrEqualTo(CROSS_PROVIDER_AGENT_WAIT_MAX_TIMEOUT_SECONDS),
    ).annotate({
      description: `Return unsettled entries after this many seconds (max ${CROSS_PROVIDER_AGENT_WAIT_MAX_TIMEOUT_SECONDS}) instead of blocking.`,
    }),
  ),
});
export type CrossProviderAgentWaitInput = typeof CrossProviderAgentWaitInput.Type;

export const CrossProviderAgentResultInput = Schema.Struct({
  childId: ChildIdInput,
  offset: Schema.optionalKey(
    Schema.Int.check(Schema.isGreaterThanOrEqualTo(0)).annotate({
      description: "Character offset to start from.",
    }),
  ),
  limit: Schema.optionalKey(
    Schema.Int.check(Schema.isGreaterThanOrEqualTo(1)).annotate({
      description: "Maximum characters to return from offset.",
    }),
  ),
});
export type CrossProviderAgentResultInput = typeof CrossProviderAgentResultInput.Type;

export const CrossProviderAgentFollowUpInput = Schema.Struct({
  childId: ChildIdInput,
  prompt: Schema.String.annotate({ description: "The follow-up instruction." }),
});
export type CrossProviderAgentFollowUpInput = typeof CrossProviderAgentFollowUpInput.Type;

export const CrossProviderAgentInterruptInput = Schema.Struct({
  childId: ChildIdInput,
});
export type CrossProviderAgentInterruptInput = typeof CrossProviderAgentInterruptInput.Type;

export const CROSS_PROVIDER_AGENT_TOOL_INPUTS = {
  agent_catalog: CrossProviderAgentCatalogInput,
  agent_spawn: CrossProviderAgentSpawnInput,
  agent_wait: CrossProviderAgentWaitInput,
  agent_result: CrossProviderAgentResultInput,
  agent_follow_up: CrossProviderAgentFollowUpInput,
  agent_interrupt: CrossProviderAgentInterruptInput,
} as const;

/** Lifecycle state of a child as seen through its own thread projection. */
export const CrossProviderAgentChildState = Schema.Literals([
  "running",
  "completed",
  "error",
  "interrupted",
]);
export type CrossProviderAgentChildState = typeof CrossProviderAgentChildState.Type;

export const CrossProviderAgentCatalogRoute = Schema.Struct({
  providerInstanceId: ProviderInstanceId,
  displayName: TrimmedNonEmptyString,
  driver: ProviderDriverKind,
  models: Schema.Array(Schema.Struct({ slug: TrimmedNonEmptyString, name: TrimmedNonEmptyString })),
  isCallerInstance: Schema.Boolean,
});
export type CrossProviderAgentCatalogRoute = typeof CrossProviderAgentCatalogRoute.Type;

export const CrossProviderAgentCatalogOutput = Schema.Struct({
  routes: Schema.Array(CrossProviderAgentCatalogRoute),
  maxDepth: NonNegativeInt,
  callerDepth: NonNegativeInt,
});
export type CrossProviderAgentCatalogOutput = typeof CrossProviderAgentCatalogOutput.Type;

export const CrossProviderAgentSpawnOutput = Schema.Struct({
  childId: Schema.String,
  taskId: Schema.String,
  providerInstanceId: ProviderInstanceId,
  model: Schema.String,
  status: Schema.Literal("running"),
});
export type CrossProviderAgentSpawnOutput = typeof CrossProviderAgentSpawnOutput.Type;

export const CrossProviderAgentWaitChild = Schema.Struct({
  childId: Schema.String,
  settled: Schema.Boolean,
  state: CrossProviderAgentChildState,
  output: Schema.optionalKey(Schema.String),
  truncated: Schema.optionalKey(Schema.Boolean),
  totalChars: Schema.optionalKey(NonNegativeInt),
});
export type CrossProviderAgentWaitChild = typeof CrossProviderAgentWaitChild.Type;

export const CrossProviderAgentWaitOutput = Schema.Struct({
  children: Schema.Array(CrossProviderAgentWaitChild),
});
export type CrossProviderAgentWaitOutput = typeof CrossProviderAgentWaitOutput.Type;

export const CrossProviderAgentResultOutput = Schema.Struct({
  childId: Schema.String,
  state: CrossProviderAgentChildState,
  output: Schema.String,
  truncated: Schema.Boolean,
  totalChars: NonNegativeInt,
  offset: NonNegativeInt,
  length: NonNegativeInt,
});
export type CrossProviderAgentResultOutput = typeof CrossProviderAgentResultOutput.Type;

export const CrossProviderAgentFollowUpOutput = Schema.Struct({
  childId: Schema.String,
  status: Schema.Literal("running"),
});
export type CrossProviderAgentFollowUpOutput = typeof CrossProviderAgentFollowUpOutput.Type;

export const CrossProviderAgentInterruptOutput = Schema.Struct({
  childId: Schema.String,
  status: Schema.Literal("interrupting"),
});
export type CrossProviderAgentInterruptOutput = typeof CrossProviderAgentInterruptOutput.Type;

/**
 * Closed error union. Every entry names only the rejected route or child;
 * never credentials, homes, prompts, or outputs.
 */
export const CrossProviderAgentErrorCode = Schema.Literals([
  "access_disabled",
  "route_not_eligible",
  "instance_disabled",
  "instance_unauthenticated",
  "model_not_offered",
  "same_instance",
  "depth_exceeded",
  "not_owner",
  "child_not_settled",
  "child_not_running",
  "unsupported_caller",
  "invalid_input",
  "internal",
]);
export type CrossProviderAgentErrorCode = typeof CrossProviderAgentErrorCode.Type;

export const CrossProviderAgentError = Schema.Struct({
  code: CrossProviderAgentErrorCode,
  message: Schema.String,
  providerInstanceId: Schema.optionalKey(Schema.String),
  model: Schema.optionalKey(Schema.String),
  childId: Schema.optionalKey(Schema.String),
});
export type CrossProviderAgentError = typeof CrossProviderAgentError.Type;

export const CrossProviderAgentErrorOutput = Schema.Struct({ error: CrossProviderAgentError });
export type CrossProviderAgentErrorOutput = typeof CrossProviderAgentErrorOutput.Type;

export const isCrossProviderAgentErrorOutput = (
  value: unknown,
): value is CrossProviderAgentErrorOutput =>
  typeof value === "object" && value !== null && "error" in value;
