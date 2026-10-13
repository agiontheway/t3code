import { AuthAccessReadScope, AuthAccessWriteScope, EnvironmentHttpApi } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as HttpEffect from "effect/unstable/http/HttpEffect";
import * as HttpServerResponse from "effect/unstable/http/HttpServerResponse";
import * as HttpApiBuilder from "effect/unstable/httpapi/HttpApiBuilder";
import { requireEnvironmentScope } from "../auth/http.ts";
import * as SignalIntegration from "./SignalIntegration.ts";

export const signalHttpApiLayer = HttpApiBuilder.group(
  EnvironmentHttpApi,
  "signal",
  Effect.fnUntraced(function* (handlers) {
    const signal = yield* SignalIntegration.SignalIntegration;
    return handlers
      .handle("get", () =>
        requireEnvironmentScope(AuthAccessReadScope).pipe(Effect.andThen(signal.get)),
      )
      .handle("choices", (args) =>
        requireEnvironmentScope(AuthAccessReadScope).pipe(
          Effect.andThen(signal.choices(args.params.threadId)),
        ),
      )
      .handle("configure", (args) =>
        requireEnvironmentScope(AuthAccessWriteScope).pipe(
          Effect.andThen(
            HttpEffect.appendPreResponseHandler((_request, response) =>
              Effect.succeed(HttpServerResponse.setHeader(response, "cache-control", "no-store")),
            ),
          ),
          Effect.andThen(signal.configure(args.payload)),
        ),
      )
      .handle("revoke", () =>
        requireEnvironmentScope(AuthAccessWriteScope).pipe(Effect.andThen(signal.revoke)),
      )
      .handle("trigger", (args) =>
        signal.trigger(
          args.payload,
          args.headers.authorization?.startsWith("Bearer ")
            ? args.headers.authorization.slice(7)
            : "",
        ),
      );
  }),
);
