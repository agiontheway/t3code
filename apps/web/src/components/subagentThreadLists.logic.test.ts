import { ProjectId, ThreadId } from "@t3tools/contracts";
import { describe, expect, it } from "vite-plus/test";

import { makeThreadFixture } from "../test-fixtures";
import { buildThreadActionItems } from "./CommandPalette.logic";
import { withoutSubagentThreads } from "./subagentThreadLists.logic";

const PROJECT_ID = ProjectId.make("project-1");
const PARENT_ID = ThreadId.make("thread-parent");
const CHILD_ID = ThreadId.make("thread-child");
const FORK_ID = ThreadId.make("thread-fork");

const threads = (archivedAt: string | null) => [
  makeThreadFixture({ id: PARENT_ID, projectId: PROJECT_ID, title: "Parent", archivedAt }),
  makeThreadFixture({
    id: CHILD_ID,
    projectId: PROJECT_ID,
    title: "Spawned child",
    archivedAt,
    lineage: {
      rootThreadId: PARENT_ID,
      parentThreadId: PARENT_ID,
      relationshipToParent: "subagent",
    },
  }),
  makeThreadFixture({
    id: FORK_ID,
    projectId: PROJECT_ID,
    title: "Forked thread",
    archivedAt,
    lineage: { rootThreadId: PARENT_ID, parentThreadId: PARENT_ID, relationshipToParent: "fork" },
  }),
];

describe("withoutSubagentThreads", () => {
  it("keeps a spawned child out of the command palette's thread items", () => {
    const items = buildThreadActionItems({
      threads: withoutSubagentThreads(threads(null)),
      projectTitleById: new Map([[PROJECT_ID, "Project"]]),
      sortOrder: "updated_at",
      icon: null,
      runThread: async () => undefined,
    });
    expect(items.map((item) => item.value)).toEqual([`thread:${PARENT_ID}`, `thread:${FORK_ID}`]);
  });

  it("drops archived subagent threads while keeping their archived parent and forks", () => {
    expect(withoutSubagentThreads(threads("2026-06-02T00:00:00.000Z")).map((t) => t.id)).toEqual([
      PARENT_ID,
      FORK_ID,
    ]);
  });

  it("returns the same list when no thread is a subagent", () => {
    const list = threads(null).filter((thread) => thread.id !== CHILD_ID);
    expect(withoutSubagentThreads(list)).toBe(list);
  });
});
