// fork: exercise the real sidebar row, tooltip and shell-to-row usage wiring.
import { act, cloneElement, type ComponentProps, type ReactElement, type ReactNode } from "react";
import { create, type ReactTestInstance, type ReactTestRenderer } from "react-test-renderer";
import { afterEach, expect, it, vi } from "vite-plus/test";
import { ProviderDriverKind } from "@t3tools/contracts";
import { deriveProviderInstanceEntries } from "../providerInstances";
import { makeThreadFixture } from "../test-fixtures";

const state = vi.hoisted(() => ({ showTooltips: false }));

// Same visibility seam as ThreadRelationshipsControl.agents.test.tsx: the real
// popup content mounts only when tooltips are enabled, without a browser portal.
vi.mock("./ui/tooltip", () => ({
  Tooltip: ({ children }: { children: ReactNode }) => children,
  TooltipTrigger: ({ render, children }: { render: ReactElement; children: ReactNode }) =>
    cloneElement(render, {}, children),
  TooltipPopup: ({ children }: { children: ReactNode }) =>
    state.showTooltips ? <aside role="tooltip">{children}</aside> : null,
}));
vi.mock("../state/session", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../state/session")>()),
  useEnvironmentScope: () => false,
}));
vi.mock("../state/query", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../state/query")>()),
  useEnvironmentQuery: () => ({ data: undefined }),
}));
vi.mock("../state/terminalSessions", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../state/terminalSessions")>()),
  useThreadRunningTerminalIds: () => [],
}));
vi.mock("../hooks/useSupportsMultiplePullRequests", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../hooks/useSupportsMultiplePullRequests")>()),
  useSupportsMultiplePullRequests: () => false,
}));
vi.mock("../lib/openPullRequestLink", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../lib/openPullRequestLink")>()),
  useOpenPrLink: () => vi.fn(),
}));
vi.mock("../composerDraftStore", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../composerDraftStore")>()),
  useThreadHasUnsentDraft: () => false,
}));
vi.mock("./Sidebar.logic", async (importOriginal) => ({
  ...(await importOriginal<typeof import("./Sidebar.logic")>()),
  useSidebarRowSubscriptionLease: () => ({ leaseLiveStatus: false, rowRef: null }),
}));
vi.mock("./ThreadStatusIndicators", async (importOriginal) => ({
  ...(await importOriginal<typeof import("./ThreadStatusIndicators")>()),
  useLinkedThreadPullRequest: () => null,
  ThreadTerminalStatusButton: () => null,
}));

import { SidebarThreadRow } from "./Sidebar";

let renderer: ReactTestRenderer;
afterEach(async () => {
  await act(async () => renderer?.unmount());
  state.showTooltips = false;
  vi.unstubAllGlobals();
});

function text(node: ReactTestInstance | string): string {
  return typeof node === "string" ? node : node.children.map(text).join("");
}

const thread = makeThreadFixture({ title: "Usage thread" });
const providerEntries = deriveProviderInstanceEntries([
  {
    instanceId: thread.modelSelection.instanceId,
    driver: ProviderDriverKind.make("codex"),
    enabled: true,
    installed: true,
    version: null,
    status: "ready",
    auth: { status: "authenticated" },
    checkedAt: "2026-01-01T00:00:00.000Z",
    models: [],
    slashCommands: [],
    skills: [],
  },
]);
const rowProps: ComponentProps<typeof SidebarThreadRow> = {
  thread,
  variant: "slim",
  variantAction: "settle",
  settlementSupported: false,
  snoozeSupported: false,
  pinningSupported: false,
  isPinned: false,
  dropVerb: null,
  dragOverPinned: false,
  sweepAction: null,
  snoozeWakeLabelText: null,
  wokeAt: null,
  isActive: false,
  openPullRequestsInRightPanel: false,
  jumpLabel: null,
  currentEnvironmentId: thread.environmentId,
  environmentLabel: "Environment",
  environmentMachine: "desktop",
  scratchMachineLabel: null,
  project: null,
  projectDisplayName: "Project",
  providerEntryByInstanceId: new Map(providerEntries.map((entry) => [entry.instanceId, entry])),
  timestampFormat: "locale",
  isRenaming: false,
  renamingTitle: "",
  changeRequestSnapshot: null,
  onThreadClick: vi.fn(),
  onThreadActivate: vi.fn(),
  onStartRename: vi.fn(),
  onRenameTitleChange: vi.fn(),
  onCommitRename: vi.fn(),
  onCancelRename: vi.fn(),
  onContextMenu: vi.fn(),
  onSettle: vi.fn(),
  onActionSweepStart: vi.fn(),
  onUnsettle: vi.fn(),
  onSnooze: vi.fn(),
  onUnsnooze: vi.fn(),
  onUnpin: vi.fn(),
  onAcknowledgeWoke: vi.fn(),
  onChangeRequestSnapshot: vi.fn(),
};

it.each(["slim", "card"] as const)(
  "shows usage in the %s sidebar hover card and omits it for a fresh thread",
  async (variant) => {
    vi.stubGlobal("IS_REACT_ACT_ENVIRONMENT", true);
    const withUsage = {
      ...thread,
      usage: {
        contextUsedTokens: 312_000,
        contextMaxTokens: 1_000_000,
        totalTokens: 1_400_000,
        toolUses: 87,
      },
    };
    await act(async () => {
      renderer = create(<SidebarThreadRow {...rowProps} variant={variant} thread={withUsage} />);
    });
    expect(renderer.root.findAllByProps({ role: "tooltip" })).toHaveLength(0);

    state.showTooltips = true;
    await act(async () =>
      renderer.update(
        <SidebarThreadRow {...rowProps} variant={variant} thread={{ ...withUsage }} />,
      ),
    );
    const card = renderer.root.findByProps({ role: "tooltip" });
    expect(text(card)).toContain("Usage thread");
    expect(text(card)).toContain("Project");
    expect(text(card)).toContain(thread.modelSelection.model);
    expect(text(card).indexOf(thread.modelSelection.model)).toBeLessThan(
      text(card).indexOf("312k/1m ctx"),
    );
    expect(
      card.findAllByType("div").some((node) => text(node) === "312k/1m ctx · 1.4M tok · 87 tools"),
    ).toBe(true);

    await act(async () =>
      renderer.update(<SidebarThreadRow {...rowProps} variant={variant} thread={{ ...thread }} />),
    );
    const freshCard = renderer.root.findByProps({ role: "tooltip" });
    expect(text(freshCard)).toContain("Usage thread");
    expect(text(freshCard)).not.toContain(" ctx · ");
    expect(text(freshCard)).not.toContain(" tok · ");
  },
);
