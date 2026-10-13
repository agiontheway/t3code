import type { SignalBinding, SignalConfigure, ThreadId } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import { RemoteEnvironmentAuthorization } from "../authorization/service.ts";
import type { PreparedConnection } from "../connection/model.ts";
import { ManagedRelayDpopSigner } from "../relay/managedRelay.ts";
import { executeAuthenticatedEnvironmentHttpRequest } from "./environmentHttpAuth.ts";

export const signalRequest = Effect.fn("signalRequest")(function* (input: {
  prepared: PreparedConnection;
  action: "get" | "configure" | "revoke" | "choices";
  threadId?: ThreadId;
  configure?: SignalConfigure;
}) {
  const signer = yield* Effect.serviceOption(ManagedRelayDpopSigner);
  const remoteAuthorization = yield* Effect.serviceOption(RemoteEnvironmentAuthorization);
  const suffix = input.action === "choices" ? `/skills/${encodeURIComponent(input.threadId!)}` : "";
  return yield* executeAuthenticatedEnvironmentHttpRequest({
    prepared: input.prepared,
    signer,
    remoteAuthorization,
    group: "signal",
    method: input.action === "configure" ? "POST" : input.action === "revoke" ? "DELETE" : "GET",
    url: (base) => `${base.replace(/\/$/, "")}/api/integrations/signal${suffix}`,
    timeoutMs: 30_000,
    request: ({
      client,
      headers,
    }): Effect.Effect<
      { binding?: SignalBinding | null; token?: string; skills?: readonly string[] },
      unknown
    > => {
      switch (input.action) {
        case "configure":
          return client.configure({ headers, payload: input.configure! });
        case "revoke":
          return client.revoke({ headers }).pipe(Effect.as({ binding: null }));
        case "choices":
          return client
            .choices({ headers, params: { threadId: input.threadId! } })
            .pipe(Effect.map((skills) => ({ skills })));
        default:
          return client.get({ headers }).pipe(Effect.map((binding) => ({ binding })));
      }
    },
  });
});
