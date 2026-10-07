import {
  CommandId,
  EventId,
  HeartbeatJobId,
  HeartbeatOccurrenceId,
  HeartbeatPrompt,
  MessageId,
  ProjectId,
  ProviderInstanceId,
  ThreadId,
  TurnId,
  type OrchestrationCommand,
  type OrchestrationReadModel,
  type OrchestrationThread,
} from "@t3tools/contracts";
import * as NodeServices from "@effect/platform-node/NodeServices";
import { expect, it } from "@effect/vitest";
import * as Effect from "effect/Effect";

import type { CrossProviderResultDeliveryState } from "./crossProviderResultDelivery.ts";
import { decideOrchestrationCommand } from "./decider.ts";

const NOW = "2026-09-17T10:00:00.000Z";
const THREAD_ID = ThreadId.make("heartbeat-decider-thread");

const heartbeat = {
  type: "thread.heartbeat.due",
  commandId: CommandId.make("server:heartbeat:decider-occurrence"),
  threadId: THREAD_ID,
  jobId: HeartbeatJobId.make("deadbeef"),
  occurrenceId: HeartbeatOccurrenceId.make("decider-occurrence"),
  jobKind: "cron",
  reservedAt: NOW,
  scheduledAfterSequence: 0,
  prompt: HeartbeatPrompt.make("continue"),
  dueAt: NOW,
  createdAt: NOW,
} satisfies OrchestrationCommand;

function readModel(
  input: {
    readonly activities?: OrchestrationThread["activities"];
    readonly messages?: OrchestrationThread["messages"];
    readonly session?: OrchestrationThread["session"];
  } = {},
): OrchestrationReadModel {
  return {
    snapshotSequence: 0,
    projects: [],
    threads: [
      {
        id: THREAD_ID,
        projectId: ProjectId.make("heartbeat-decider-project"),
        title: "Heartbeat",
        modelSelection: { instanceId: ProviderInstanceId.make("codex"), model: "gpt-5.6" },
        runtimeMode: "full-access",
        interactionMode: "default",
        branch: null,
        worktreePath: null,
        pullRequests: [],
        latestTurn: null,
        createdAt: NOW,
        updatedAt: NOW,
        archivedAt: null,
        settledOverride: null,
        settledAt: null,
        deletedAt: null,
        messages: input.messages ?? [],
        proposedPlans: [],
        activities: input.activities ?? [],
        checkpoints: [],
        session: input.session ?? null,
      },
    ],
    updatedAt: NOW,
  };
}

const decide = (
  model: OrchestrationReadModel,
  heartbeatResultDeliveries: ReadonlyArray<CrossProviderResultDeliveryState> = [],
) =>
  decideOrchestrationCommand({
    command: heartbeat,
    readModel: model,
    heartbeatResultDeliveries,
  });

it.layer(NodeServices.layer)("heartbeat admission priority", (it) => {
  it.effect("does not treat a failed child-result delivery as pending via activity fallback", () =>
    Effect.gen(function* () {
      const requested = {
        id: EventId.make("xp-agent:result-delivery:failed"),
        tone: "info" as const,
        kind: "provider.cross-provider-result.requested",
        summary: "child result",
        payload: {},
        turnId: null,
        createdAt: NOW,
      } as OrchestrationThread["activities"][number];
      const result = yield* decide(readModel({ activities: [requested] }), [
        {
          requestId: requested.id,
          createdAt: NOW,
          acceptedTurnId: null,
          turnState: null,
          failed: 1,
        },
      ]);
      expect(Array.isArray(result) ? result.map((event) => event.type) : []).toEqual([
        "thread.message-sent",
        "thread.turn-start-requested",
      ]);
    }),
  );

  it.effect("keeps an accepted future child-result turn ahead of heartbeat", () =>
    Effect.gen(function* () {
      const error = yield* decide(readModel(), [
        {
          requestId: "xp-agent:result-delivery:accepted",
          createdAt: NOW,
          acceptedTurnId: TurnId.make("future-child-result-turn"),
          turnState: null,
          failed: 0,
        },
      ]).pipe(Effect.flip);
      expect(error._tag).toBe("HeartbeatThreadBusyError");
    }),
  );

  it.effect("puts queued human/compaction input and unresolved native requests first", () =>
    Effect.gen(function* () {
      const queuedCompact = {
        id: MessageId.make("user:compact"),
        role: "user" as const,
        text: "/compact",
        turnId: null,
        streaming: false,
        createdAt: NOW,
        updatedAt: NOW,
      };
      const approval = {
        id: EventId.make("approval-requested"),
        tone: "approval" as const,
        kind: "approval.requested",
        summary: "approval",
        payload: { requestId: "approval-1" },
        turnId: null,
        createdAt: NOW,
      } as OrchestrationThread["activities"][number];
      const queued = yield* decide(readModel({ messages: [queuedCompact] })).pipe(Effect.flip);
      const blocked = yield* decide(readModel({ activities: [approval] })).pipe(Effect.flip);
      expect(queued._tag).toBe("HeartbeatThreadBusyError");
      expect(blocked._tag).toBe("HeartbeatThreadBusyError");
    }),
  );
});
