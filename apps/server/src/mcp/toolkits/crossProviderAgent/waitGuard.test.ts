import { assert, describe, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import { TestClock } from "effect/testing";

import { makeWaitRateLimiter, waitTimeoutMs } from "./waitGuard.ts";

/** Admits `count` waits for `threadId` and returns how many the limiter accepted. */
const admitMany = (
  limiter: ReturnType<typeof makeWaitRateLimiter>,
  threadId: string,
  count: number,
) =>
  Effect.gen(function* () {
    let accepted = 0;
    for (let call = 0; call < count; call += 1) {
      if (yield* limiter.admit(threadId)) accepted += 1;
    }
    return accepted;
  });

describe("agent_wait guard", () => {
  it("clamps every wait to at most 5 seconds", () => {
    assert.strictEqual(waitTimeoutMs(60), 5_000);
    assert.strictEqual(waitTimeoutMs(3600), 5_000);
    assert.strictEqual(waitTimeoutMs(undefined), 5_000);
    assert.strictEqual(waitTimeoutMs(5), 5_000);
    assert.strictEqual(waitTimeoutMs(2), 2_000);
    assert.strictEqual(waitTimeoutMs(0.05), 50);
  });

  it.effect("allows 10 waits per thread in a minute and refuses the 11th", () =>
    Effect.gen(function* () {
      const limiter = makeWaitRateLimiter();
      assert.strictEqual(yield* admitMany(limiter, "thread:a", 10), 10);
      assert.isFalse(yield* limiter.admit("thread:a"));
      yield* TestClock.adjust("59 seconds");
      assert.isFalse(yield* limiter.admit("thread:a"));
      // Refused calls do not count: once the first ten age out, the thread may wait again.
      yield* TestClock.adjust("1 second");
      assert.strictEqual(yield* admitMany(limiter, "thread:a", 11), 10);
    }),
  );

  it.effect("slides its window over the last 60 seconds", () =>
    Effect.gen(function* () {
      const limiter = makeWaitRateLimiter();
      assert.strictEqual(yield* admitMany(limiter, "thread:a", 5), 5);
      yield* TestClock.adjust("30 seconds");
      assert.strictEqual(yield* admitMany(limiter, "thread:a", 6), 5);
      // At 60 s only the five calls from 0 s left the window.
      yield* TestClock.adjust("30 seconds");
      assert.strictEqual(yield* admitMany(limiter, "thread:a", 6), 5);
      // At 90 s the five from 30 s left too; the five from 60 s remain.
      yield* TestClock.adjust("30 seconds");
      assert.strictEqual(yield* admitMany(limiter, "thread:a", 6), 5);
    }),
  );

  it.effect("limits each calling thread on its own", () =>
    Effect.gen(function* () {
      const limiter = makeWaitRateLimiter();
      assert.strictEqual(yield* admitMany(limiter, "thread:a", 11), 10);
      assert.strictEqual(yield* admitMany(limiter, "thread:b", 10), 10);
      assert.isFalse(yield* limiter.admit("thread:b"));
      assert.isFalse(yield* limiter.admit("thread:a"));
    }),
  );
});
