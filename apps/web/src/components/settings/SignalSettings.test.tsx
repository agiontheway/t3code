import { act, type ReactNode } from "react";
import { create, type ReactTestRenderer } from "react-test-renderer";
import { afterEach, beforeEach, describe, expect, it, vi } from "vite-plus/test";
import * as Option from "effect/Option";

const state = vi.hoisted(() => ({
  scopes: ["access:read", "access:write"],
  prepared: { httpBaseUrl: "http://localhost" },
  run: vi.fn(),
}));
vi.mock("../../connection/runtime", () => ({ connectionAtomRuntime: {} }));
vi.mock("@t3tools/client-runtime/state/runtime", () => ({ createRuntimeCommand: () => ({}) }));
vi.mock("@t3tools/client-runtime/state/signal", () => ({ signalRequest: () => undefined }));
vi.mock("../../state/use-atom-command", () => ({ useAtomCommand: () => state.run }));
vi.mock("../../state/session", () => ({
  usePreparedConnection: () => Option.some(state.prepared),
  useEnvironmentSessionState: () => ({ data: { scopes: state.scopes } }),
}));
vi.mock("../../state/entities", () => ({ useThreadShells: () => [] }));
vi.mock("./SettingsScopeContext", () => ({
  useSettingsScope: () => ({ environments: [{ environmentId: "machine", label: "Machine" }] }),
}));
vi.mock("./settingsLayout", () => ({
  SettingsSection: ({ children }: { children: ReactNode }) => children,
}));
vi.mock("../ui/button", () => ({
  Button: (props: React.ComponentProps<"button">) => <button {...props} />,
}));
vi.mock("../ui/input", () => ({
  Input: (props: React.ComponentProps<"input">) => <input {...props} />,
}));
vi.mock("../ui/select", () => {
  const Passthrough = ({ children }: { children: ReactNode }) => children;
  return {
    Select: Passthrough,
    SelectTrigger: Passthrough,
    SelectValue: Passthrough,
    SelectPopup: Passthrough,
    SelectItem: Passthrough,
    SelectButton: Passthrough,
  };
});
vi.mock("../ui/combobox", () => {
  const Passthrough = ({ children }: { children: ReactNode }) => children;
  return {
    Combobox: Passthrough,
    ComboboxTrigger: Passthrough,
    ComboboxPopup: Passthrough,
    ComboboxSearchInput: () => null,
    ComboboxList: Passthrough,
    ComboboxItem: Passthrough,
    ComboboxEmpty: () => null,
  };
});

import { SignalSettings } from "./SignalSettings";
let renderer: ReactTestRenderer | undefined;
beforeEach(() => {
  vi.stubGlobal("IS_REACT_ACT_ENVIRONMENT", true);
  state.run.mockReset();
  state.scopes = ["access:read", "access:write"];
});
afterEach(async () => {
  await act(() => renderer?.unmount());
  vi.unstubAllGlobals();
});
const binding = {
  enabled: true,
  connectionId: "connection",
  environmentId: "machine",
  commands: [
    { command: "/review", threadId: "thread", skill: "review-risk", skillArguments: "saved args" },
  ],
};

describe("Signal settings loading", () => {
  it("does not expose a blank editor while loading or after denied access, then permits retry", async () => {
    state.scopes = [];
    let finish: ((result: unknown) => void) | undefined;
    state.run.mockImplementation(
      () =>
        new Promise((resolve) => {
          finish = resolve;
        }),
    );
    await act(() => {
      renderer = create(<SignalSettings />);
    });
    expect(renderer!.root.findAllByType("input")).toHaveLength(0);
    await act(() => finish?.({ _tag: "Failure" }));
    expect(renderer!.root.findAllByType("input")).toHaveLength(0);
    expect(JSON.stringify(renderer!.toJSON())).toContain("administrator access");
    state.scopes = ["access:read", "access:write"];
    state.run.mockImplementation(({ action }) =>
      Promise.resolve({
        _tag: "Success",
        value: action === "get" ? { binding } : { skills: ["review-risk"] },
      }),
    );
    const retry = renderer!.root
      .findAllByType("button")
      .find((button) => button.props.children === "Retry loading")!;
    await act(() => retry.props.onClick());
    expect(renderer!.root.findByProps({ "aria-label": "Signal skill arguments" }).props.value).toBe(
      "saved args",
    );
  });
  it("keeps unsaved values when the same environment connection refreshes", async () => {
    state.run.mockImplementation(({ action }) =>
      Promise.resolve({
        _tag: "Success",
        value: action === "get" ? { binding } : { skills: ["review-risk"] },
      }),
    );
    await act(() => {
      renderer = create(<SignalSettings />);
    });
    await act(() =>
      renderer!.root
        .findByProps({ "aria-label": "Signal skill arguments" })
        .props.onChange({ target: { value: "unsaved args" } }),
    );
    state.prepared = { httpBaseUrl: "http://localhost" };
    await act(() => renderer!.update(<SignalSettings />));
    expect(renderer!.root.findByProps({ "aria-label": "Signal skill arguments" }).props.value).toBe(
      "unsaved args",
    );
    expect(state.run.mock.calls.filter(([input]) => input.action === "get")).toHaveLength(1);
  });
});
