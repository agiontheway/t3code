import { EnvironmentHttpApi, SignalIntegrationError } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as HttpApiBuilder from "effect/http-api/HttpApiBuilder";

/**
 * fork: placeholder for the Signal HTTP group so `EnvironmentHttpApi` can
 * carry the Signal contract before the integration service lands. Every
 * endpoint refuses; the Signal integration replaces this layer wholesale.
 */
const unavailable = () =>
  Effect.fail(new SignalIntegrationError({ reason: "Signal integration is not available." }));

export const layer = HttpApiBuilder.group(EnvironmentHttpApi, "signal", (handlers) =>
  handlers
    .handle("choices", unavailable)
    .handle("get", unavailable)
    .handle("configure", unavailable)
    .handle("revoke", unavailable)
    .handle("trigger", unavailable),
);
