import * as DateTime from "effect/DateTime";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";

import { IsoDateTime, ThreadId, TrimmedNonEmptyString } from "./baseSchemas.ts";
import { PROVIDER_SEND_TURN_MAX_INPUT_CHARS } from "./orchestration.ts";

export const HeartbeatJobId = Schema.String.check(Schema.isPattern(/^[0-9a-f]{8}$/)).pipe(
  Schema.brand("HeartbeatJobId"),
);
export type HeartbeatJobId = typeof HeartbeatJobId.Type;

export const HeartbeatOccurrenceId = TrimmedNonEmptyString.pipe(
  Schema.brand("HeartbeatOccurrenceId"),
);
export type HeartbeatOccurrenceId = typeof HeartbeatOccurrenceId.Type;

export const HeartbeatPrompt = Schema.String.check(
  Schema.isMaxLength(PROVIDER_SEND_TURN_MAX_INPUT_CHARS),
  Schema.makeFilter((prompt) => prompt.trim().length > 0 || "prompt must not be blank"),
).pipe(Schema.brand("HeartbeatPrompt"));
export type HeartbeatPrompt = typeof HeartbeatPrompt.Type;

export const HeartbeatCronExpression = Schema.String.check(
  Schema.makeFilter((expression) => {
    const fields = expression.trim().split(/\s+/);
    return (
      (fields.length === 5 && fields.every((field) => /^[0-9*/,-]+$/.test(field))) ||
      "cron must contain five numeric fields using only *, /, -, and comma syntax"
    );
  }),
).pipe(Schema.brand("HeartbeatCronExpression"));
export type HeartbeatCronExpression = typeof HeartbeatCronExpression.Type;

export const HeartbeatTimeZone = Schema.String.check(
  Schema.makeFilter(
    (timeZone) =>
      Option.isSome(DateTime.zoneMakeNamed(timeZone)) ||
      "timezone must be a valid IANA time zone identifier",
  ),
).pipe(Schema.brand("HeartbeatTimeZone"));
export type HeartbeatTimeZone = typeof HeartbeatTimeZone.Type;

export const HeartbeatDelaySeconds = Schema.Number.check(
  Schema.isFinite(),
  Schema.isGreaterThan(0),
).pipe(Schema.brand("HeartbeatDelaySeconds"));
export type HeartbeatDelaySeconds = typeof HeartbeatDelaySeconds.Type;

export const HeartbeatStatus = Schema.Literals(["active", "dormant", "pending"]);
export type HeartbeatStatus = typeof HeartbeatStatus.Type;

export const HeartbeatJobKind = Schema.Literals(["cron", "relative"]);
export type HeartbeatJobKind = typeof HeartbeatJobKind.Type;

export const HeartbeatRestartPolicy = Schema.Literals(["restore-on-resume", "process-local"]);
export type HeartbeatRestartPolicy = typeof HeartbeatRestartPolicy.Type;

export const HeartbeatCronJob = Schema.Struct({
  id: HeartbeatJobId,
  kind: Schema.Literal("cron"),
  prompt: HeartbeatPrompt,
  status: HeartbeatStatus,
  cron: HeartbeatCronExpression,
  recurring: Schema.Boolean,
  timezone: HeartbeatTimeZone,
  scheduleDescription: Schema.String,
  nextNominalAt: IsoDateTime,
  nextFireAt: IsoDateTime,
  expiresAt: Schema.NullOr(IsoDateTime),
  restartPolicy: Schema.Literal("restore-on-resume"),
});
export type HeartbeatCronJob = typeof HeartbeatCronJob.Type;

export const HeartbeatRelativeJob = Schema.Struct({
  id: HeartbeatJobId,
  kind: Schema.Literal("relative"),
  prompt: HeartbeatPrompt,
  status: HeartbeatStatus,
  dueAt: IsoDateTime,
  reason: Schema.optional(TrimmedNonEmptyString),
  noop: Schema.Boolean,
  restartPolicy: Schema.Literal("process-local"),
});
export type HeartbeatRelativeJob = typeof HeartbeatRelativeJob.Type;

export const HeartbeatJob = Schema.Union([HeartbeatCronJob, HeartbeatRelativeJob]);
export type HeartbeatJob = typeof HeartbeatJob.Type;

export const HeartbeatCreateInput = Schema.Struct({
  cron: HeartbeatCronExpression,
  prompt: HeartbeatPrompt,
  recurring: Schema.Boolean,
  timezone: Schema.optional(HeartbeatTimeZone),
});
export type HeartbeatCreateInput = typeof HeartbeatCreateInput.Type;

export const HeartbeatCreateResult = HeartbeatCronJob;
export type HeartbeatCreateResult = typeof HeartbeatCreateResult.Type;

export const HeartbeatListResult = Schema.Struct({ jobs: Schema.Array(HeartbeatJob) });
export type HeartbeatListResult = typeof HeartbeatListResult.Type;

export const HeartbeatDeleteInput = Schema.Struct({ id: HeartbeatJobId });
export type HeartbeatDeleteInput = typeof HeartbeatDeleteInput.Type;

export const HeartbeatDeleteResult = Schema.Struct({
  id: HeartbeatJobId,
  deleted: Schema.Boolean,
});
export type HeartbeatDeleteResult = typeof HeartbeatDeleteResult.Type;

export const HeartbeatWakeupInput = Schema.Union([
  Schema.Struct({ type: Schema.Literal("stop") }),
  Schema.Struct({
    type: Schema.Literal("arm"),
    delaySeconds: HeartbeatDelaySeconds,
    prompt: HeartbeatPrompt,
    reason: Schema.optional(TrimmedNonEmptyString),
    noop: Schema.Boolean,
  }),
]);
export type HeartbeatWakeupInput = typeof HeartbeatWakeupInput.Type;

export const HeartbeatWakeupResult = Schema.Union([
  Schema.Struct({
    action: Schema.Literal("stopped"),
    stopped: Schema.Boolean,
    cronJobsUnaffected: Schema.Literal(true),
  }),
  Schema.Struct({
    action: Schema.Literal("armed"),
    job: HeartbeatRelativeJob,
    replaced: Schema.Boolean,
  }),
]);
export type HeartbeatWakeupResult = typeof HeartbeatWakeupResult.Type;

export const HeartbeatOccurrenceOutcome = Schema.Struct({
  status: Schema.Literals(["admitted", "sent", "failed", "canceled", "expired"]),
  providerAcceptedAt: Schema.optional(IsoDateTime),
  error: Schema.optional(Schema.String),
});
export type HeartbeatOccurrenceOutcome = typeof HeartbeatOccurrenceOutcome.Type;

export class HeartbeatInvalidCronError extends Schema.TaggedError<HeartbeatInvalidCronError>()(
  "HeartbeatInvalidCronError",
  { reason: Schema.String },
) {
  override get message(): string {
    return `Invalid heartbeat cron expression: ${this.reason}`;
  }
}

export class HeartbeatInvalidTimeZoneError extends Schema.TaggedError<HeartbeatInvalidTimeZoneError>()(
  "HeartbeatInvalidTimeZoneError",
  { timezone: Schema.String },
) {
  override get message(): string {
    return `Invalid heartbeat time zone: ${this.timezone}`;
  }
}

export class HeartbeatJobLimitExceededError extends Schema.TaggedError<HeartbeatJobLimitExceededError>()(
  "HeartbeatJobLimitExceededError",
  { limit: Schema.Int },
) {
  override get message(): string {
    return `This thread already has the maximum of ${this.limit} heartbeat cron jobs.`;
  }
}

export class HeartbeatThreadNotFoundError extends Schema.TaggedError<HeartbeatThreadNotFoundError>()(
  "HeartbeatThreadNotFoundError",
  { threadId: ThreadId },
) {
  override get message(): string {
    return `Thread ${this.threadId} was not found.`;
  }
}

export class HeartbeatOperationError extends Schema.TaggedError<HeartbeatOperationError>()(
  "HeartbeatOperationError",
  {
    operation: Schema.Literals([
      "create",
      "list",
      "delete",
      "wakeup",
      "activate",
      "cancel-relative",
      "cancel-thread",
      "set-occurrence-outcome",
    ]),
    cause: Schema.Defect(),
  },
) {
  override get message(): string {
    return `Heartbeat ${this.operation} failed.`;
  }
}

export const HeartbeatServiceError = Schema.Union([
  HeartbeatInvalidCronError,
  HeartbeatInvalidTimeZoneError,
  HeartbeatJobLimitExceededError,
  HeartbeatThreadNotFoundError,
  HeartbeatOperationError,
]);
export type HeartbeatServiceError = typeof HeartbeatServiceError.Type;
