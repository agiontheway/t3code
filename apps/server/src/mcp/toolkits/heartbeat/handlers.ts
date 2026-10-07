import { McpCapabilityUnavailableError } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";

import { heartbeatAccessAllowed } from "../../../heartbeat/HeartbeatAccess.ts";
import { HeartbeatService } from "../../../heartbeat/HeartbeatService.ts";
import * as ProjectionSnapshotQuery from "../../../orchestration/Services/ProjectionSnapshotQuery.ts";
import * as ServerSettings from "../../../serverSettings.ts";
import * as McpInvocationContext from "../../McpInvocationContext.ts";
import { HeartbeatToolkit } from "./tools.ts";

const make = Effect.gen(function* () {
  const heartbeat = yield* HeartbeatService;
  const snapshots = yield* ProjectionSnapshotQuery.ProjectionSnapshotQuery;
  const serverSettings = yield* ServerSettings.ServerSettingsService;

  const requireHeartbeatAccess = Effect.fn("HeartbeatToolkit.requireAccess")(function* () {
    const scope = yield* McpInvocationContext.requireMcpCapability("heartbeat");
    const allowed = yield* Effect.gen(function* () {
      const settings = yield* serverSettings.getSettings;
      const thread = yield* snapshots.getThreadShellById(scope.threadId);
      return Option.isSome(thread) && heartbeatAccessAllowed(settings, thread.value);
    }).pipe(Effect.orElseSucceed(() => false));
    if (allowed) return scope;
    return yield* new McpCapabilityUnavailableError({
      capability: "heartbeat",
      environmentId: scope.environmentId,
      threadId: scope.threadId,
      providerSessionId: scope.providerSessionId,
      providerInstanceId: scope.providerInstanceId,
    });
  });

  return HeartbeatToolkit.of({
    t3_heartbeat_create: (input) =>
      Effect.gen(function* () {
        const scope = yield* requireHeartbeatAccess();
        return yield* heartbeat.create(scope.threadId, {
          cron: input.cron,
          prompt: input.prompt,
          recurring: input.recurring,
          ...(input.timezone === undefined ? {} : { timezone: input.timezone }),
        });
      }),
    t3_heartbeat_list: () =>
      Effect.gen(function* () {
        const scope = yield* requireHeartbeatAccess();
        return yield* heartbeat.list(scope.threadId);
      }),
    t3_heartbeat_delete: (input) =>
      Effect.gen(function* () {
        const scope = yield* requireHeartbeatAccess();
        return yield* heartbeat.delete(scope.threadId, input);
      }),
    t3_heartbeat_wakeup: (input) =>
      Effect.gen(function* () {
        const scope = yield* requireHeartbeatAccess();
        return yield* heartbeat.wakeup(
          scope.threadId,
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
  });
});

export const HeartbeatToolkitHandlersLive = HeartbeatToolkit.toLayer(make);
