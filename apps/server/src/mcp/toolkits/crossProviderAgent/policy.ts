/**
 * fork: pure helpers for the cross-provider agent tools (`agent_*`). Ported
 * unchanged from the 0.0.45 fork service so output capping, titles and effort
 * resolution behave exactly as before; only the child state mapping is new,
 * because children are now native delegated tasks.
 */
import {
  crossProviderAgentEffortOptionId,
  type CrossProviderAgentChildState,
  type ModelSelection,
  type OrchestratorMcpDelegatedTaskStatus,
  type ProviderDriverKind,
  type ServerProviderModel,
} from "@t3tools/contracts";
import {
  getModelSelectionStringOptionValue,
  getProviderOptionCurrentValue,
  getProviderOptionDescriptors,
} from "@t3tools/shared/model";

const MAX_TITLE_CHARS = 60;

/**
 * The only permitted reduction of child output: above the cap, return a
 * deterministic head + tail and say so. Never a silent cut.
 */
export function applyOutputCap(
  text: string,
  cap: number,
  childId: string,
): { readonly output: string; readonly truncated: boolean; readonly totalChars: number } {
  const totalChars = text.length;
  if (totalChars <= cap) {
    return { output: text, truncated: false, totalChars };
  }
  const marker = (head: number, omitted: number) =>
    `\n[… ${omitted} characters omitted; call agent_result with childId "${childId}", offset ${head}, limit ${omitted} to read them …]\n`;
  // The marker is part of the returned string, so it comes out of the cap.
  // Its length depends on the digits it prints; two passes settle that.
  let budget = cap - marker(0, totalChars).length;
  for (let pass = 0; pass < 2; pass += 1) {
    const head = Math.max(0, Math.floor(budget / 2));
    budget = cap - marker(head, totalChars - Math.max(0, budget)).length;
  }
  budget = Math.max(0, budget);
  const head = Math.floor(budget / 2);
  const tail = budget - head;
  const omitted = totalChars - head - tail;
  return {
    output: `${text.slice(0, head)}${marker(head, omitted)}${text.slice(totalChars - tail)}`,
    truncated: true,
    totalChars,
  };
}

/** Child thread title: the explicit one, else the prompt's start. */
export function threadTitle(title: string | undefined, prompt: string): string {
  const explicit = title?.trim();
  if (explicit) return explicit;
  const fromPrompt = prompt.trim().replace(/\s+/g, " ").slice(0, MAX_TITLE_CHARS).trim();
  return fromPrompt.length > 0 ? fromPrompt : "Cross-provider agent";
}

/**
 * A native delegated-task status in the fork's four-state vocabulary.
 * `settled` is true exactly for terminal native statuses.
 */
export function childStateFromTaskStatus(status: OrchestratorMcpDelegatedTaskStatus): {
  readonly settled: boolean;
  readonly state: CrossProviderAgentChildState;
} {
  switch (status) {
    case "queued":
    case "running":
    case "waiting":
      return { settled: false, state: "running" };
    case "completed":
      return { settled: true, state: "completed" };
    case "failed":
      return { settled: true, state: "error" };
    case "cancelled":
    case "interrupted":
      return { settled: true, state: "interrupted" };
  }
}

function effortDescriptor(model: ServerProviderModel | undefined, optionId: string | undefined) {
  if (model?.capabilities === null || model?.capabilities === undefined || optionId === undefined) {
    return undefined;
  }
  const descriptor = getProviderOptionDescriptors({ caps: model.capabilities }).find(
    (candidate) => candidate.id === optionId,
  );
  return descriptor?.type === "select" ? descriptor : undefined;
}

/** A thread's effective effort: its explicit selection resolved against its model's choices, else the model default. */
export function effectiveThreadEffort(input: {
  readonly driver: ProviderDriverKind;
  readonly model: ServerProviderModel | undefined;
  readonly modelSelection: ModelSelection;
}): string | undefined {
  const optionId = crossProviderAgentEffortOptionId(input.driver);
  if (optionId === undefined) return undefined;
  const raw = getModelSelectionStringOptionValue(input.modelSelection, optionId);
  if (input.model?.capabilities === null || input.model?.capabilities === undefined) return raw;
  const descriptor = getProviderOptionDescriptors({
    caps: input.model.capabilities,
    ...(raw !== undefined ? { selections: [{ id: optionId, value: raw }] } : {}),
  }).find((candidate) => candidate.id === optionId);
  const value = getProviderOptionCurrentValue(descriptor);
  return typeof value === "string" ? value : undefined;
}

/**
 * The effort a child runs at, the way a native spawn resolves it: an
 * explicit request wins (and must be one the target model offers), else the
 * parent's effective effort when the target offers it, else the target's
 * default. A target without an effort option gets none.
 */
export function resolveCrossProviderEffort(input: {
  readonly explicit: string | undefined;
  readonly parent: {
    readonly driver: ProviderDriverKind;
    readonly model: ServerProviderModel | undefined;
    readonly modelSelection: ModelSelection;
  };
  readonly target: { readonly driver: ProviderDriverKind; readonly model: ServerProviderModel };
}):
  | { readonly selection: { readonly id: string; readonly value: string } | undefined }
  | { readonly unsupported: ReadonlyArray<string> } {
  const optionId = crossProviderAgentEffortOptionId(input.target.driver);
  const descriptor = effortDescriptor(input.target.model, optionId);
  const choices = descriptor?.options.map((choice) => choice.id) ?? [];
  const explicit = input.explicit?.trim();
  if (explicit !== undefined && explicit.length > 0) {
    return optionId !== undefined && choices.includes(explicit)
      ? { selection: { id: optionId, value: explicit } }
      : { unsupported: choices };
  }
  if (optionId === undefined || descriptor === undefined) return { selection: undefined };
  const inherited = effectiveThreadEffort(input.parent);
  if (inherited !== undefined && choices.includes(inherited)) {
    return { selection: { id: optionId, value: inherited } };
  }
  const fallback = getProviderOptionCurrentValue(descriptor);
  return {
    selection: typeof fallback === "string" ? { id: optionId, value: fallback } : undefined,
  };
}
