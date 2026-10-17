import type { MessageId, OrchestrationV2ThreadProjection } from "@t3tools/contracts";

/**
 * Message id prefixes only the server mints: heartbeat deliveries
 * (`heartbeat:<occurrenceId>`) and Signal messages (`signal-<hash>`). Readers
 * use the prefix as provenance, so clients and MCP callers may not claim it.
 */
export const SERVER_RESERVED_MESSAGE_ID_PREFIXES = ["heartbeat:", "signal-"] as const;

export function isServerReservedMessageId(messageId: MessageId): boolean {
  return SERVER_RESERVED_MESSAGE_ID_PREFIXES.some((prefix) => messageId.startsWith(prefix));
}

/**
 * Whether a `start_if_idle` dispatch must wait: a run is active (the
 * Orchestrator's blocking statuses, including post-turn `waiting`) or queued,
 * a runtime request is pending, or the thread is archived. Read from the
 * projection taken under the thread dispatch lock.
 */
export function isThreadBusyForStartIfIdle(
  projection: Pick<OrchestrationV2ThreadProjection, "thread" | "runs" | "runtimeRequests">,
): boolean {
  return (
    projection.thread.archivedAt !== null ||
    projection.runs.some(
      (run) =>
        run.status === "preparing" ||
        run.status === "starting" ||
        run.status === "running" ||
        run.status === "waiting" ||
        run.status === "queued",
    ) ||
    projection.runtimeRequests.some((request) => request.status === "pending")
  );
}

/** Command types whose accepted receipts trip a `start_if_idle` fence kind. */
export function startIfIdleFenceCommandTypes(
  kinds: ReadonlyArray<"stop" | "interrupt">,
): ReadonlyArray<string> {
  return kinds.map((kind) => (kind === "stop" ? "thread.stop" : "run.interrupt"));
}
