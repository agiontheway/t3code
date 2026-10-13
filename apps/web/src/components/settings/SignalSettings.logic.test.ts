import { describe, expect, it } from "vite-plus/test";
import { EnvironmentId } from "@t3tools/contracts";
import { signalThreadOptions } from "./SignalSettings.logic";

const environmentId = EnvironmentId.make("machine-a");
const base = {
  environmentId,
  archivedAt: null,
  settledAt: null,
  updatedAt: "2026-01-01T00:00:00Z",
};
const threads = [
  {
    ...base,
    id: "settled",
    title: "Review previous",
    settledAt: "2026-01-03T00:00:00Z",
    updatedAt: "2026-01-03T00:00:00Z",
  },
  { ...base, id: "active-old", title: "Review earlier" },
  { ...base, id: "active-new", title: "Review latest", updatedAt: "2026-01-02T00:00:00Z" },
  { ...base, id: "archived", title: "Review archived", archivedAt: "2026-01-03T00:00:00Z" },
  { ...base, id: "remote", title: "Review remote", environmentId: EnvironmentId.make("machine-b") },
];

describe("Signal thread picker", () => {
  it("puts active threads first, recent first, while retaining settled choices", () => {
    expect(signalThreadOptions(threads, environmentId, "").map((thread) => thread.id)).toEqual([
      "active-new",
      "active-old",
      "settled",
    ]);
    expect(threads[0]?.id).toBe("settled");
  });
  it("searches titles and IDs without matching archived or other machine threads", () => {
    expect(
      signalThreadOptions(threads, environmentId, " PREVIOUS ").map((thread) => thread.id),
    ).toEqual(["settled"]);
    expect(
      signalThreadOptions(threads, environmentId, "active-new").map((thread) => thread.id),
    ).toEqual(["active-new"]);
    expect(signalThreadOptions(threads, environmentId, "remote")).toEqual([]);
  });
});
