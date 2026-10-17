import { McpCapabilityUnavailableError } from "@t3tools/contracts";
import * as Effect from "effect/Effect";

import { heartbeatAccessAllowed } from "../../../heartbeat/HeartbeatAccess.ts";
import { HeartbeatService } from "../../../heartbeat/HeartbeatService.ts";
import { DelegationGrants } from "../../../orchestration-v2/fork/DelegationGrants.ts";
import * as ThreadManagementService from "../../../orchestration-v2/ThreadManagementService.ts";
import * as ServerSettings from "../../../serverSettings.ts";
import * as McpInvocationContext from "../../McpInvocationContext.ts";
import * as McpToolAccess from "../../McpToolAccess.ts";
import { HeartbeatToolkit } from "./tools.ts";

const make = Effect.gen(function* () {
  const heartbeat = yield* HeartbeatService;
  const threads = yield* ThreadManagementService.ThreadManagementService;
  const serverSettings = yield* ServerSettings.ServerSettingsService;
  const grants = yield* DelegationGrants;

  // The credential bit is minted at session start; settings and the child's
  // grant are rechecked on every call, as the credential may predate a change.
  const requireHeartbeatAccess = Effect.fn("HeartbeatToolkit.requireAccess")(function* () {
    const scope = yield* McpInvocationContext.requireThreadMcpCapability("heartbeat");
    const allowed = yield* Effect.gen(function* () {
      const settings = yield* serverSettings.getSettings;
      const thread = yield* threads.getThreadShell(scope.thread.threadId);
      if (thread === null || thread.deletedAt !== null) return false;
      return yield* heartbeatAccessAllowed(settings, thread);
    }).pipe(
      Effect.provideService(DelegationGrants, grants),
      Effect.orElseSucceed(() => false),
    );
    if (allowed) return scope.thread.threadId;
    return yield* new McpCapabilityUnavailableError({
      capability: "heartbeat",
      environmentId: scope.environmentId,
      threadId: scope.thread.threadId,
      providerSessionId: scope.thread.providerSessionId,
      providerInstanceId: scope.thread.providerInstanceId,
    });
  });

  return {
    t3_heartbeat_create: McpToolAccess.actsAsCaller((input) =>
      Effect.gen(function* () {
        const threadId = yield* requireHeartbeatAccess();
        return yield* heartbeat.create(threadId, {
          cron: input.cron,
          prompt: input.prompt,
          recurring: input.recurring,
          ...(input.timezone === undefined ? {} : { timezone: input.timezone }),
        });
      }),
    ),
    t3_heartbeat_list: McpToolAccess.readsAsCaller(() =>
      Effect.gen(function* () {
        const threadId = yield* requireHeartbeatAccess();
        return yield* heartbeat.list(threadId);
      }),
    ),
    t3_heartbeat_delete: McpToolAccess.actsAsCaller((input) =>
      Effect.gen(function* () {
        const threadId = yield* requireHeartbeatAccess();
        return yield* heartbeat.delete(threadId, input);
      }),
    ),
    t3_heartbeat_wakeup: McpToolAccess.actsAsCaller((input) =>
      Effect.gen(function* () {
        const threadId = yield* requireHeartbeatAccess();
        return yield* heartbeat.wakeup(
          threadId,
          input.stop === true
            ? { type: "stop" }
            : {
                type: "arm",
                delaySeconds: input.delaySeconds!,
                prompt: input.prompt!,
                noop: input.noop ?? false,
                ...(input.reason === undefined ? {} : { reason: input.reason }),
              },
        );
      }),
    ),
  } satisfies McpToolAccess.Handlers<typeof HeartbeatToolkit.tools>;
});

export const layer = McpToolAccess.toLayer(HeartbeatToolkit, make);
