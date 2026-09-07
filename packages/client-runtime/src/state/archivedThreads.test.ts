import {
  EnvironmentId,
  ProjectId,
  ProviderInstanceId,
  RuntimeTaskId,
  ThreadId,
  type OrchestrationShellSnapshot,
  type OrchestrationThreadShell,
} from "@t3tools/contracts";
import * as Cause from "effect/Cause";
import { AsyncResult, Atom, AtomRegistry } from "effect/unstable/reactivity";
import { expect, it } from "vite-plus/test";

import {
  createArchivedThreadSnapshotsAtomFamily,
  makeArchivedThreadsEnvironmentKey,
  parseArchivedThreadsEnvironmentKey,
} from "./archivedThreads.ts";

it("round-trips environment keys in sorted order", () => {
  const envA = EnvironmentId.make("env-a");
  const envB = EnvironmentId.make("env-b");
  const key = makeArchivedThreadsEnvironmentKey([envB, envA]);

  expect(parseArchivedThreadsEnvironmentKey(key)).toEqual([envA, envB]);
});

it("does not expose an archived snapshot failure message", () => {
  const environmentId = EnvironmentId.make("env-sensitive");
  const snapshotsAtom = createArchivedThreadSnapshotsAtomFamily<Error>({
    getSnapshotAtom: () =>
      Atom.make(
        AsyncResult.failure<OrchestrationShellSnapshot, Error>(
          Cause.fail(new Error("credential=secret-value")),
        ),
      ),
    labelPrefix: "test:archived-thread-snapshots",
  });
  const registry = AtomRegistry.make();

  expect(registry.get(snapshotsAtom(makeArchivedThreadsEnvironmentKey([environmentId])))).toEqual({
    snapshots: [],
    error: "Failed to load archived threads.",
    isLoading: false,
  });

  registry.dispose();
});

it("drops spawned child threads from archived snapshots", () => {
  const environmentId = EnvironmentId.make("env-archived");
  const parentId = ThreadId.make("thread-parent");
  const shell = (
    id: ThreadId,
    spawn?: OrchestrationThreadShell["spawn"],
  ): OrchestrationThreadShell => ({
    id,
    projectId: ProjectId.make("project-1"),
    title: id,
    modelSelection: { instanceId: ProviderInstanceId.make("codex"), model: "gpt-5.6-sol" },
    runtimeMode: "full-access",
    interactionMode: "default",
    branch: null,
    worktreePath: null,
    createdAt: "2026-06-01T00:00:00.000Z",
    updatedAt: "2026-06-01T00:00:00.000Z",
    archivedAt: "2026-06-02T00:00:00.000Z",
    pinnedAt: null,
    pinOrderKey: null,
    activeOrderKey: null,
    snoozedUntil: null,
    settledOverride: null,
    settledAt: null,
    session: null,
    latestTurn: null,
    latestUserMessageAt: null,
    hasPendingApprovals: false,
    hasPendingUserInput: false,
    hasActionableProposedPlan: false,
    ...(spawn ? { spawn } : {}),
  });
  const snapshot: OrchestrationShellSnapshot = {
    snapshotSequence: 1,
    updatedAt: "2026-06-02T00:00:00.000Z",
    projects: [],
    threads: [
      shell(parentId),
      shell(ThreadId.make("thread-child"), {
        parentThreadId: parentId,
        allowOrchestration: false,
        depth: 1,
        taskId: RuntimeTaskId.make("xp-agent:thread-child"),
      }),
    ],
  };
  const snapshotsAtom = createArchivedThreadSnapshotsAtomFamily<Error>({
    getSnapshotAtom: () =>
      Atom.make(AsyncResult.success<OrchestrationShellSnapshot, Error>(snapshot)),
    labelPrefix: "test:archived-thread-snapshots",
  });
  const registry = AtomRegistry.make();

  const state = registry.get(snapshotsAtom(makeArchivedThreadsEnvironmentKey([environmentId])));
  expect(state.snapshots.map((entry) => entry.snapshot.threads.map((thread) => thread.id))).toEqual(
    [[parentId]],
  );

  registry.dispose();
});
