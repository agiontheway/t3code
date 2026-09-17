import {
  HeartbeatCreateResult,
  HeartbeatCronExpression,
  HeartbeatDelaySeconds,
  HeartbeatDeleteResult,
  HeartbeatJobId,
  HeartbeatListResult,
  HeartbeatPrompt,
  HeartbeatServiceError,
  HeartbeatTimeZone,
  HeartbeatWakeupResult,
  McpCapabilityUnavailableError,
  TrimmedNonEmptyString,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import * as McpSchema from "effect/unstable/ai/McpSchema";
import * as Tool from "effect/unstable/ai/Tool";
import * as Toolkit from "effect/unstable/ai/Toolkit";

import { HeartbeatService } from "../../../heartbeat/HeartbeatService.ts";
import * as ProjectionSnapshotQuery from "../../../orchestration/Services/ProjectionSnapshotQuery.ts";
import * as ServerSettings from "../../../serverSettings.ts";
import * as McpInvocationContext from "../../McpInvocationContext.ts";

const dependencies = [
  McpInvocationContext.McpInvocationContext,
  HeartbeatService,
  ProjectionSnapshotQuery.ProjectionSnapshotQuery,
  ServerSettings.ServerSettingsService,
];

export const heartbeatToolVisible = () =>
  McpInvocationContext.currentMcpInvocationHasCapability("heartbeat");

export const HeartbeatCreateToolInput = Schema.Struct({
  cron: HeartbeatCronExpression.annotate({
    description: "Five-field numeric cron expression: minute hour day-of-month month day-of-week.",
  }),
  prompt: HeartbeatPrompt,
  recurring: Schema.Boolean.pipe(Schema.withDecodingDefault(Effect.succeed(true))),
  timezone: Schema.optional(HeartbeatTimeZone),
  durable: Schema.optional(
    Schema.Boolean.annotate({
      description:
        "Compatibility field only. Cron jobs use the same restoration-on-resume policy regardless of this value.",
    }),
  ),
});
export type HeartbeatCreateToolInput = typeof HeartbeatCreateToolInput.Type;

export const HeartbeatWakeupToolInput = Schema.Struct({
  stop: Schema.optional(Schema.Boolean),
  delaySeconds: Schema.optional(HeartbeatDelaySeconds),
  prompt: Schema.optional(HeartbeatPrompt),
  reason: Schema.optional(TrimmedNonEmptyString),
  noop: Schema.optional(Schema.Boolean),
}).check(
  Schema.makeFilter((input) => {
    if (input.stop === true) {
      return (
        (input.delaySeconds === undefined &&
          input.prompt === undefined &&
          input.reason === undefined &&
          input.noop === undefined) ||
        "stop cannot be combined with wakeup arm fields"
      );
    }
    return (
      (input.delaySeconds !== undefined && input.prompt !== undefined) ||
      "pass either { stop: true } or both delaySeconds and prompt"
    );
  }),
);
export type HeartbeatWakeupToolInput = typeof HeartbeatWakeupToolInput.Type;

export const HeartbeatToolError = Schema.Union([
  McpCapabilityUnavailableError,
  ...HeartbeatServiceError.members,
]);
export type HeartbeatToolError = typeof HeartbeatToolError.Type;

const HeartbeatCreateTool = Tool.make("t3_heartbeat_create", {
  description:
    "Create a cron heartbeat for this thread. Cron jobs persist and are restored dormant after restart, then reactivate when this thread accepts a normal turn. Recurring jobs expire seven days after creation. Common examples: '*/5 * * * *' is every five minutes, '0 * * * *' is hourly, and '0 9 * * 1-5' is weekdays at 09:00. Other valid expressions are returned without a generated natural-language description.",
  parameters: HeartbeatCreateToolInput,
  success: HeartbeatCreateResult,
  failure: HeartbeatToolError,
  dependencies,
})
  .annotate(McpSchema.EnabledWhen, heartbeatToolVisible)
  .annotate(Tool.Title, "Create thread heartbeat")
  .annotate(Tool.Readonly, false)
  .annotate(Tool.Destructive, false)
  .annotate(Tool.Idempotent, false)
  .annotate(Tool.OpenWorld, false);

const HeartbeatListTool = Tool.make("t3_heartbeat_list", {
  description:
    "List this thread's cron heartbeats and pending process-local relative wakeup, including full prompts and status. An empty list is successful.",
  success: HeartbeatListResult,
  failure: HeartbeatToolError,
  dependencies,
})
  .annotate(McpSchema.EnabledWhen, heartbeatToolVisible)
  .annotate(Tool.Title, "List thread heartbeats")
  .annotate(Tool.Readonly, true)
  .annotate(Tool.Destructive, false)
  .annotate(Tool.Idempotent, true)
  .annotate(Tool.OpenWorld, false);

const HeartbeatDeleteTool = Tool.make("t3_heartbeat_delete", {
  description:
    "Delete this thread's heartbeat by id. Unknown ids and ids owned by another thread return deleted=false.",
  parameters: Schema.Struct({ id: HeartbeatJobId }),
  success: HeartbeatDeleteResult,
  failure: HeartbeatToolError,
  dependencies,
})
  .annotate(McpSchema.EnabledWhen, heartbeatToolVisible)
  .annotate(Tool.Title, "Delete thread heartbeat")
  .annotate(Tool.Readonly, false)
  .annotate(Tool.Destructive, true)
  .annotate(Tool.Idempotent, true)
  .annotate(Tool.OpenWorld, false);

const HeartbeatWakeupTool = Tool.make("t3_heartbeat_wakeup", {
  description:
    "Arm one process-local relative wakeup for this thread, replacing its prior relative wakeup, or pass {stop:true} to cancel only that relative wakeup. Child-completion delivery may reach this thread earlier. noop is reported metadata only and never suppresses delivery or other behavior.",
  parameters: HeartbeatWakeupToolInput,
  success: HeartbeatWakeupResult,
  failure: HeartbeatToolError,
  dependencies,
})
  .annotate(McpSchema.EnabledWhen, heartbeatToolVisible)
  .annotate(Tool.Title, "Arm or stop relative thread wakeup")
  .annotate(Tool.Readonly, false)
  .annotate(Tool.Destructive, false)
  .annotate(Tool.Idempotent, false)
  .annotate(Tool.OpenWorld, false);

export const HeartbeatToolkit = Toolkit.make(
  HeartbeatCreateTool,
  HeartbeatListTool,
  HeartbeatDeleteTool,
  HeartbeatWakeupTool,
);
