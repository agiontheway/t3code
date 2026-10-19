import { act } from "react";
import { create, type ReactTestRenderer } from "react-test-renderer";
import { afterEach, expect, it, vi } from "vite-plus/test";
import { SidebarThreadUsage } from "./SidebarThreadUsage";

let renderer: ReactTestRenderer;
afterEach(async () => {
  await act(async () => renderer?.unmount());
  vi.unstubAllGlobals();
});

it("renders the sidebar usage line and removes it for a fresh thread", async () => {
  vi.stubGlobal("IS_REACT_ACT_ENVIRONMENT", true);
  await act(async () => {
    renderer = create(
      <SidebarThreadUsage
        usage={{
          contextUsedTokens: 312_000,
          contextMaxTokens: 1_000_000,
          totalTokens: 1_400_000,
          toolUses: 87,
        }}
      />,
    );
  });
  expect(
    renderer.root
      .findAllByType("div")
      .flatMap((node) => node.children)
      .filter((child) => typeof child === "string"),
  ).toEqual(["312k/1m ctx · 1.4M tok · 87 tools"]);
  await act(async () => renderer.update(<SidebarThreadUsage usage={undefined} />));
  expect(renderer.toJSON()).toBeNull();
});

it("distinguishes unknown limits and totals from reported zeroes", async () => {
  vi.stubGlobal("IS_REACT_ACT_ENVIRONMENT", true);
  await act(async () => {
    renderer = create(<SidebarThreadUsage usage={{ contextUsedTokens: 312_000, toolUses: 3 }} />);
  });
  expect(JSON.stringify(renderer.toJSON())).toContain("312k/? ctx · — tok · 3 tools");
  await act(async () =>
    renderer.update(
      <SidebarThreadUsage usage={{ contextUsedTokens: 0, totalTokens: 0, toolUses: 0 }} />,
    ),
  );
  expect(JSON.stringify(renderer.toJSON())).toContain("0/? ctx · 0 tok · 0 tools");
  await act(async () => renderer.update(<SidebarThreadUsage usage={{ toolUses: 1 }} />));
  expect(JSON.stringify(renderer.toJSON())).toContain("—/? ctx · — tok · 1 tools");
});
