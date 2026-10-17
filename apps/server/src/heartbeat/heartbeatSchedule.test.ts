import { describe, expect, it } from "vite-plus/test";
import * as DateTime from "effect/DateTime";
import * as Result from "effect/Result";

import {
  nextNominalOccurrence,
  oneShotDueTime,
  parseHeartbeatCron,
  recurringDueTime,
  relativeDueTime,
} from "./heartbeatSchedule.ts";

const parsed = (expression: string, timezone = "UTC") => {
  const result = parseHeartbeatCron(expression, timezone);
  if (Result.isFailure(result)) throw result.failure;
  return result.success;
};

describe("heartbeatSchedule", () => {
  it("rejects seconds, names, unsupported operators, and invalid IANA zones", () => {
    expect(Result.isFailure(parseHeartbeatCron("0 0 9 * * *", "UTC"))).toBe(true);
    expect(Result.isFailure(parseHeartbeatCron("0 9 * JAN *", "UTC"))).toBe(true);
    expect(Result.isFailure(parseHeartbeatCron("0 9 L * *", "UTC"))).toBe(true);
    expect(Result.isFailure(parseHeartbeatCron("0 9 * * MON", "UTC"))).toBe(true);
    expect(Result.isFailure(parseHeartbeatCron("0 9 * * *", "Mars/Olympus_Mons"))).toBe(true);
  });

  it("uses Cron.next for Sunday 0/7 and timezone-aware DST transitions", () => {
    const sundayZero = parsed("0 9 * * 0");
    const sundaySeven = parsed("0 9 * * 7");
    const friday = Date.parse("2026-09-18T12:00:00.000Z");
    expect(nextNominalOccurrence(sundayZero.cron, friday)).toBe(
      Date.parse("2026-09-20T09:00:00.000Z"),
    );
    expect(nextNominalOccurrence(sundaySeven.cron, friday)).toBe(
      Date.parse("2026-09-20T09:00:00.000Z"),
    );

    const newYork = parsed("30 2 * * *", "America/New_York");
    expect(
      DateTime.formatIso(
        DateTime.makeUnsafe(
          nextNominalOccurrence(newYork.cron, Date.parse("2026-03-07T08:00:00.000Z")),
        ),
      ),
    ).toBe("2026-03-08T07:30:00.000Z");
    expect(
      DateTime.formatIso(
        DateTime.makeUnsafe(
          nextNominalOccurrence(newYork.cron, Date.parse("2026-11-01T04:00:00.000Z")),
        ),
      ),
    ).toBe("2026-11-01T07:30:00.000Z");
  });

  it("uses OR parity for restricted DOM and DOW fields", () => {
    const either = parsed("0 9 13 * 5", "UTC").cron;
    expect(nextNominalOccurrence(either, Date.parse("2026-09-17T10:00:00.000Z"))).toBe(
      Date.parse("2026-09-18T09:00:00.000Z"),
    );
    const oddOrMonday = parsed("0 9 */2 * 1", "UTC").cron;
    expect(nextNominalOccurrence(either, Date.parse("2026-09-17T00:00:00.000Z"))).toBe(
      Date.parse("2026-09-18T09:00:00.000Z"),
    );
    expect(nextNominalOccurrence(oddOrMonday, Date.parse("2026-09-17T10:00:00.000Z"))).toBe(
      Date.parse("2026-09-19T09:00:00.000Z"),
    );
  });

  it("keeps recurring jitter stable and within half an interval and thirty minutes", () => {
    const everyMinute = parsed("* * * * *").cron;
    const hourly = parsed("0 * * * *").cron;
    const minuteNominal = Date.parse("2026-09-17T10:01:00.000Z");
    const hourNominal = Date.parse("2026-09-17T11:00:00.000Z");

    const minuteDue = recurringDueTime(everyMinute, "deadbeef", minuteNominal);
    expect(minuteDue).toBe(recurringDueTime(everyMinute, "deadbeef", minuteNominal));
    expect(minuteDue).toBeGreaterThanOrEqual(minuteNominal);
    expect(minuteDue).toBeLessThanOrEqual(minuteNominal + 30_000);

    const hourDue = recurringDueTime(hourly, "deadbeef", hourNominal);
    expect(hourDue).toBeGreaterThanOrEqual(hourNominal);
    expect(hourDue).toBeLessThanOrEqual(hourNominal + 30 * 60_000);
  });

  it("rounds relative wakeups up to the next wall-clock minute", () => {
    expect(relativeDueTime(Date.parse("2026-09-17T10:00:01.000Z"), 60)).toBe(
      Date.parse("2026-09-17T10:02:00.000Z"),
    );
    expect(relativeDueTime(Date.parse("2026-09-17T10:00:00.000Z"), 60)).toBe(
      Date.parse("2026-09-17T10:01:00.000Z"),
    );
  });

  it("only moves half-hour one-shots earlier and never before creation", () => {
    const utc = DateTime.zoneMakeNamedUnsafe("UTC");
    const createdAt = Date.parse("2026-09-17T09:59:45.000Z");
    const onHalfHour = Date.parse("2026-09-17T10:00:00.000Z");
    const ordinary = Date.parse("2026-09-17T10:17:00.000Z");
    const early = oneShotDueTime("ffffffff", onHalfHour, createdAt, utc);

    expect(early).toBe(createdAt);
    expect(oneShotDueTime("ffffffff", ordinary, createdAt, utc)).toBe(ordinary);
  });
});
