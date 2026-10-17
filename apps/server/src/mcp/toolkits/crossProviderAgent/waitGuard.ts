/**
 * fork: the `agent_wait` harness guard. A wait only retrieves results from children, so it never
 * blocks longer than a few seconds, and a thread that calls it in a loop is pointed at the
 * heartbeat tools instead.
 */
import { CROSS_PROVIDER_AGENT_WAIT_CLAMP_SECONDS } from "@t3tools/contracts";
import * as Clock from "effect/Clock";
import * as Effect from "effect/Effect";

const WAIT_RATE_LIMIT_CALLS = 10;
const WAIT_RATE_LIMIT_WINDOW_MS = 60_000;

export const WAIT_RATE_LIMITED_MESSAGE =
  "Remember that agent_wait is only to retrieve results from a subagent, use the heartbeat tool to wait for a specified period.";

/** The bound one wait call blocks for: the requested timeout, clamped to the guard's maximum. */
export const waitTimeoutMs = (timeoutSeconds: number | undefined) =>
  Math.min(
    timeoutSeconds ?? CROSS_PROVIDER_AGENT_WAIT_CLAMP_SECONDS,
    CROSS_PROVIDER_AGENT_WAIT_CLAMP_SECONDS,
  ) * 1_000;

/**
 * Admits at most 10 waits per calling thread in any rolling 60 second window. State is in
 * memory, so it resets with the server process; refused calls do not count against the window.
 */
export const makeWaitRateLimiter = () => {
  const admitted = new Map<string, Array<number>>();
  return {
    admit: (threadId: string) =>
      Clock.clockWith((clock) =>
        Effect.sync(() => {
          const now = clock.currentTimeMillisUnsafe();
          const cutoff = now - WAIT_RATE_LIMIT_WINDOW_MS;
          // Forget threads whose window emptied, so the map only holds recent callers.
          for (const [id, times] of admitted) {
            if (times.at(-1)! <= cutoff) admitted.delete(id);
          }
          const recent = (admitted.get(threadId) ?? []).filter((time) => time > cutoff);
          if (recent.length >= WAIT_RATE_LIMIT_CALLS) {
            admitted.set(threadId, recent);
            return false;
          }
          recent.push(now);
          admitted.set(threadId, recent);
          return true;
        }),
      ),
  };
};
