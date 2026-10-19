import { describe, expect, it } from "vite-plus/test";
import * as Schema from "effect/Schema";

import { HeartbeatCreateInput, HeartbeatJobId, HeartbeatWakeupInput } from "./heartbeat.ts";

const decodeCreateInput = Schema.decodeUnknownSync(HeartbeatCreateInput);
const decodeWakeupInput = Schema.decodeUnknownSync(HeartbeatWakeupInput);
const decodeJobId = Schema.decodeUnknownSync(HeartbeatJobId);

describe("heartbeat contracts", () => {
  it("accepts a five-field cron with a named time zone", () => {
    expect(
      decodeCreateInput({
        cron: "*/15 9-17 * * 1-5",
        prompt: "Check the build",
        recurring: true,
        timezone: "Europe/Berlin",
      }),
    ).toMatchObject({ cron: "*/15 9-17 * * 1-5", timezone: "Europe/Berlin" });
  });

  it.each(["* * * *", "@daily", "0 9 * * MON"])("rejects cron %s", (cron) => {
    expect(() => decodeCreateInput({ cron, prompt: "x", recurring: false })).toThrow();
  });

  it("rejects blank prompts and unknown time zones", () => {
    expect(() =>
      decodeCreateInput({ cron: "0 9 * * *", prompt: "  ", recurring: false }),
    ).toThrow();
    expect(() =>
      decodeCreateInput({
        cron: "0 9 * * *",
        prompt: "x",
        recurring: false,
        timezone: "Mars/Olympus",
      }),
    ).toThrow();
  });

  it("requires a positive wakeup delay", () => {
    expect(() =>
      decodeWakeupInput({ type: "arm", delaySeconds: 0, prompt: "x", noop: false }),
    ).toThrow();
    expect(decodeWakeupInput({ type: "stop" })).toEqual({ type: "stop" });
  });

  it("only accepts eight lowercase hex job ids", () => {
    expect(decodeJobId("0a1b2c3d")).toBe("0a1b2c3d");
    expect(() => decodeJobId("0A1B2C3D")).toThrow();
  });
});
