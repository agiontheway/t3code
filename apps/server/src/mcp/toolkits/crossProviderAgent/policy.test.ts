import {
  ProviderDriverKind,
  ProviderInstanceId,
  type ModelCapabilities,
  type ServerProviderModel,
} from "@t3tools/contracts";
import { createModelSelection } from "@t3tools/shared/model";
import { assert, describe, it } from "@effect/vitest";

import {
  applyOutputCap,
  childStateFromTaskStatus,
  effectiveThreadEffort,
  resolveCrossProviderEffort,
  threadTitle,
} from "./policy.ts";

const CLAUDE_DRIVER = ProviderDriverKind.make("claudeAgent");
const CODEX_DRIVER = ProviderDriverKind.make("codex");
const CLAUDE = ProviderInstanceId.make("claudeAgent");
const CODEX = ProviderInstanceId.make("codex");

/** Effort choices the way each driver's catalog describes them. */
const EFFORT_CAPABILITIES = {
  claudeAgent: {
    optionDescriptors: [
      {
        id: "effort",
        label: "Effort",
        type: "select",
        options: [
          { id: "low", label: "Low" },
          { id: "medium", label: "Medium" },
          { id: "high", label: "High", isDefault: true },
          { id: "max", label: "Max" },
        ],
      },
    ],
  },
  codex: {
    optionDescriptors: [
      {
        id: "reasoningEffort",
        label: "Reasoning",
        type: "select",
        options: [
          { id: "low", label: "Low" },
          { id: "medium", label: "Medium", isDefault: true },
          { id: "high", label: "High" },
          { id: "xhigh", label: "Extra high" },
        ],
        currentValue: "medium",
      },
    ],
  },
} satisfies Record<string, ModelCapabilities>;

const model = (slug: string, capabilities: ModelCapabilities | null): ServerProviderModel => ({
  slug,
  name: slug.toUpperCase(),
  isCustom: false,
  capabilities,
});

const claudeModel = model("claude-haiku-4-5", EFFORT_CAPABILITIES.claudeAgent);
const codexModel = model("gpt-5.6-sol", EFFORT_CAPABILITIES.codex);
const plainCodexModel = model("glm-5.3-flash", null);

const claudeParent = (effort?: string) => ({
  driver: CLAUDE_DRIVER,
  model: claudeModel,
  modelSelection: createModelSelection(
    CLAUDE,
    claudeModel.slug,
    effort === undefined ? undefined : [{ id: "effort", value: effort }],
  ),
});

describe("applyOutputCap", () => {
  it("returns short output whole and splits long output into a flagged head and tail", () => {
    assert.deepEqual(applyOutputCap("short", 500, "child"), {
      output: "short",
      truncated: false,
      totalChars: 5,
    });
    const long = "a".repeat(300) + "b".repeat(300);
    const capped = applyOutputCap(long, 500, "child-1");
    assert.isTrue(capped.truncated);
    assert.equal(capped.totalChars, 600);
    // The marker is charged to the cap: the whole returned string fits.
    assert.isAtMost(capped.output.length, 500);
    assert.isAbove(capped.output.length, 450);
    const [head, tail] = capped.output.split(/\n\[….*…\]\n/u);
    assert.match(head!, /^a+$/u);
    assert.match(tail!, /^b+$/u);
    assert.equal(
      head!.length + tail!.length,
      600 - Number(/(\d+) characters omitted/u.exec(capped.output)![1]),
    );
    assert.include(
      capped.output,
      `offset ${head!.length}, limit ${600 - head!.length - tail!.length}`,
    );
    // Any cap that can hold the marker is honoured exactly.
    for (const cap of [120, 200, 1000]) {
      const sized = applyOutputCap(long + long, cap, "child-1");
      assert.isTrue(sized.truncated);
      assert.isAtMost(sized.output.length, cap);
    }
    // A cap smaller than the marker returns only the marker, never a negative slice.
    const tiny = applyOutputCap(long, 20, "child-1");
    assert.isTrue(tiny.truncated);
    assert.match(tiny.output, /^\n\[….*…\]\n$/u);
    assert.include(tiny.output, "600 characters omitted");
  });
});

describe("threadTitle", () => {
  it("prefers an explicit title, else the first 60 prompt characters", () => {
    assert.equal(threadTitle("  Review  ", "ignored"), "Review");
    assert.equal(threadTitle(undefined, `  fix\n\nthe   ${"x".repeat(80)}`).length, 60);
    assert.isTrue(threadTitle(undefined, "fix\n\nthe   tests").startsWith("fix the tests"));
    assert.equal(threadTitle("   ", "   "), "Cross-provider agent");
  });
});

describe("childStateFromTaskStatus", () => {
  it("folds native task statuses into running, completed, error, and interrupted", () => {
    assert.deepEqual(
      (
        ["queued", "running", "waiting", "completed", "failed", "cancelled", "interrupted"] as const
      ).map(childStateFromTaskStatus),
      [
        { settled: false, state: "running" },
        { settled: false, state: "running" },
        { settled: false, state: "running" },
        { settled: true, state: "completed" },
        { settled: true, state: "error" },
        { settled: true, state: "interrupted" },
        { settled: true, state: "interrupted" },
      ],
    );
  });
});

describe("resolveCrossProviderEffort", () => {
  const codexTarget = { driver: CODEX_DRIVER, model: codexModel };

  it("stores an explicit effort under the target driver's option id", () => {
    assert.deepEqual(
      resolveCrossProviderEffort({ explicit: "low", parent: claudeParent(), target: codexTarget }),
      { selection: { id: "reasoningEffort", value: "low" } },
    );
  });

  it("inherits the parent's effort across providers when the target offers it", () => {
    assert.deepEqual(
      resolveCrossProviderEffort({
        explicit: undefined,
        parent: {
          driver: CODEX_DRIVER,
          model: codexModel,
          modelSelection: createModelSelection(CODEX, codexModel.slug, [
            { id: "reasoningEffort", value: "low" },
          ]),
        },
        target: { driver: CLAUDE_DRIVER, model: claudeModel },
      }),
      { selection: { id: "effort", value: "low" } },
    );
  });

  it("falls back to the target default when the parent's effort is not offered", () => {
    assert.deepEqual(
      resolveCrossProviderEffort({
        explicit: undefined,
        parent: claudeParent("max"),
        target: codexTarget,
      }),
      { selection: { id: "reasoningEffort", value: "medium" } },
    );
  });

  it("inherits the parent's model default when the parent made no explicit choice", () => {
    assert.equal(effectiveThreadEffort(claudeParent()), "high");
    assert.deepEqual(
      resolveCrossProviderEffort({
        explicit: undefined,
        parent: claudeParent(),
        target: codexTarget,
      }),
      { selection: { id: "reasoningEffort", value: "high" } },
    );
  });

  it("rejects an effort the target model does not offer, listing its choices", () => {
    assert.deepEqual(
      resolveCrossProviderEffort({
        explicit: "ultra",
        parent: claudeParent(),
        target: codexTarget,
      }),
      { unsupported: ["low", "medium", "high", "xhigh"] },
    );
  });

  it("gives a target model without an effort setting none and rejects an explicit one", () => {
    const plainTarget = { driver: CODEX_DRIVER, model: plainCodexModel };
    assert.deepEqual(
      resolveCrossProviderEffort({
        explicit: undefined,
        parent: claudeParent("low"),
        target: plainTarget,
      }),
      { selection: undefined },
    );
    assert.deepEqual(
      resolveCrossProviderEffort({ explicit: "high", parent: claudeParent(), target: plainTarget }),
      { unsupported: [] },
    );
  });
});
