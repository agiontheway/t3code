import { ThreadId, TurnId, type OrchestrationThreadActivity } from "@t3tools/contracts";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";

export const CROSS_PROVIDER_RESULT_REQUESTED = "provider.cross-provider-result.requested";
export const CROSS_PROVIDER_INPUT_ACCEPTED = "provider.turn.input.accepted";
export const CROSS_PROVIDER_INPUT_FAILED = "provider.turn.start.failed";

const ResultDeliveryPayload = Schema.Struct({
  timelineBypass: Schema.Literal(true),
  childThreadId: ThreadId,
  childRequestId: Schema.NullOr(Schema.String),
  childTurnId: Schema.NullOr(TurnId),
  input: Schema.String,
});
const decodePayload = Schema.decodeUnknownOption(ResultDeliveryPayload);

/** Only the server-created, deterministic delivery activity is executable input. */
export function readCrossProviderResultDelivery(activity: OrchestrationThreadActivity) {
  if (
    activity.kind !== CROSS_PROVIDER_RESULT_REQUESTED ||
    !activity.id.startsWith("xp-agent:result-delivery:")
  )
    return Option.none();
  return decodePayload(activity.payload);
}

/** Lightweight durable input state; full result text stays on its request activity. */
export const CrossProviderResultDeliveryState = Schema.Struct({
  requestId: Schema.String,
  createdAt: Schema.String,
  acceptedTurnId: Schema.NullOr(TurnId),
  turnState: Schema.NullOr(Schema.String),
  failed: Schema.Number,
});
export type CrossProviderResultDeliveryState = typeof CrossProviderResultDeliveryState.Type;

export const isResultDeliveryPending = (request: CrossProviderResultDeliveryState) =>
  request.failed === 0 &&
  (request.acceptedTurnId === null ||
    request.turnState === null ||
    request.turnState === "running");
