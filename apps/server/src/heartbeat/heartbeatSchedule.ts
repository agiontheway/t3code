import * as Cron from "effect/Cron";
import * as DateTime from "effect/DateTime";
import * as Option from "effect/Option";
import * as Result from "effect/Result";
import * as Schema from "effect/Schema";

const RECURRING_JITTER_LIMIT_MS = 30 * 60 * 1_000;
const ONE_SHOT_EARLY_LIMIT_MS = 90 * 1_000;
const MINUTE_MS = 60 * 1_000;

export class HeartbeatScheduleError extends Schema.TaggedError<HeartbeatScheduleError>()(
  "HeartbeatScheduleError",
  { message: Schema.String },
) {}

export interface ParsedHeartbeatCron {
  readonly cron: Cron.Cron;
  readonly expression: string;
  readonly timezone: string;
}

/**
 * The public format deliberately stays below Effect Cron's full grammar. This
 * keeps seconds, aliases, and implementation-specific calendar operators out
 * of persisted schedules while delegating calendar validation to Effect.
 */
export const parseHeartbeatCron = (
  expression: string,
  timezone?: string | undefined,
): Result.Result<ParsedHeartbeatCron, HeartbeatScheduleError> => {
  const fields = expression.trim().split(/\s+/);
  if (fields.length !== 5 || fields.some((field) => !/^[0-9*,/-]+$/.test(field))) {
    return Result.fail(
      new HeartbeatScheduleError({
        message:
          "Cron must contain five numeric fields using only wildcards, steps, ranges, and lists",
      }),
    );
  }

  const zone =
    timezone === undefined
      ? DateTime.zoneMakeLocal()
      : Option.getOrUndefined(DateTime.zoneMakeNamed(timezone));
  if (zone === undefined) {
    return Result.fail(
      new HeartbeatScheduleError({ message: `Invalid IANA timezone: ${timezone}` }),
    );
  }

  const parsed = Cron.parse(fields.join(" "), zone);
  if (Result.isFailure(parsed)) {
    return Result.fail(new HeartbeatScheduleError({ message: parsed.failure.message }));
  }

  // Effect treats wildcard-step DOM/DOW combinations as an intersection.
  // Public heartbeat cron follows the usual DOM-or-DOW rule; Cron still owns
  // parsing, validation, timezone handling, and all occurrence arithmetic.
  const cron = Cron.make({
    ...parsed.success,
    tz: Option.getOrUndefined(parsed.success.tz),
    and: false,
  });

  return Result.succeed({
    cron,
    expression: fields.join(" "),
    timezone: DateTime.zoneToString(zone),
  });
};

export const nextNominalOccurrence = (cron: Cron.Cron, afterMs: number): number =>
  Cron.next(cron, afterMs).getTime();

export const relativeDueTime = (nowMs: number, delaySeconds: number): number =>
  Math.ceil((nowMs + delaySeconds * 1_000) / MINUTE_MS) * MINUTE_MS;

const stableIdFraction = (publicId: string): number => Number.parseInt(publicId, 16) / 0xffff_ffff;

export const recurringDueTime = (cron: Cron.Cron, publicId: string, nominalMs: number): number => {
  const followingNominalMs = nextNominalOccurrence(cron, nominalMs);
  const jitterLimitMs = Math.min(
    RECURRING_JITTER_LIMIT_MS,
    Math.floor((followingNominalMs - nominalMs) / 2),
  );
  return nominalMs + Math.floor(jitterLimitMs * stableIdFraction(publicId));
};

export const oneShotDueTime = (
  publicId: string,
  nominalMs: number,
  createdAtMs: number,
  hostZone: DateTime.TimeZone = DateTime.zoneMakeLocal(),
): number => {
  const minute = DateTime.toParts(
    DateTime.makeZonedUnsafe(nominalMs, { timeZone: hostZone }),
  ).minute;
  if (minute !== 0 && minute !== 30) return nominalMs;
  const earlyMs = Math.floor(ONE_SHOT_EARLY_LIMIT_MS * stableIdFraction(publicId));
  return Math.max(createdAtMs, nominalMs - earlyMs);
};

export const cronDueTime = (options: {
  readonly cron: Cron.Cron;
  readonly publicId: string;
  readonly nominalMs: number;
  readonly createdAtMs: number;
  readonly recurring: boolean;
  readonly hostZone?: DateTime.TimeZone | undefined;
}): number =>
  options.recurring
    ? recurringDueTime(options.cron, options.publicId, options.nominalMs)
    : oneShotDueTime(options.publicId, options.nominalMs, options.createdAtMs, options.hostZone);
