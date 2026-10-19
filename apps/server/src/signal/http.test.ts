import {
  AuthSessionId,
  type AuthEnvironmentScope,
  EnvironmentAuthenticatedAuth,
  EnvironmentAuthenticatedPrincipal,
  EnvironmentHttpApi,
  SignalIntegrationError,
  ThreadId,
} from "@t3tools/contracts";
import * as NodeServices from "@effect/platform-node/NodeServices";
import { expect, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Etag from "effect/http/Etag";
import * as HttpPlatform from "effect/http/HttpPlatform";
import * as HttpRouter from "effect/http/HttpRouter";
import * as HttpApi from "effect/http-api/HttpApi";
import * as HttpApiBuilder from "effect/http-api/HttpApiBuilder";
import * as SignalHttp from "./http.ts";
import { SignalIntegration } from "./SignalIntegration.ts";

class SignalTestApi extends HttpApi.make("environment").add(EnvironmentHttpApi.groups.signal) {}

const binding = {
  connectionId: "connection",
  environmentId: "environment",
  commands: [{ command: "/reviews", threadId: ThreadId.make("thread"), skill: "review-risk" }],
  enabled: true,
};

it.effect("serves the frozen Signal HTTP contract", () =>
  Effect.gen(function* () {
    let scopes: ReadonlyArray<AuthEnvironmentScope> = [];
    let authCalls = 0;
    const tokens: string[] = [];
    const routes = HttpApiBuilder.layer(SignalTestApi).pipe(
      Layer.provide(SignalHttp.layerGroup),
      Layer.provide(
        Layer.mock(SignalIntegration)({
          get: Effect.succeed(binding),
          choices: () => Effect.succeed(["review-risk"]),
          configure: () => Effect.succeed({ binding, token: "secret" }),
          revoke: Effect.void,
          trigger: (input, token) =>
            Effect.suspend(() => {
              tokens.push(token);
              return input.requestId === "b".repeat(64)
                ? Effect.fail(new SignalIntegrationError({ reason: "thread_busy" }))
                : Effect.succeed({ threadId: ThreadId.make("thread"), messageId: "signal-key" });
            }),
        }),
      ),
      Layer.provide(
        Layer.succeed(EnvironmentAuthenticatedAuth, (effect) =>
          Effect.suspend(() => {
            authCalls += 1;
            return effect.pipe(
              Effect.provideService(EnvironmentAuthenticatedPrincipal, {
                sessionId: AuthSessionId.make("test-session"),
                subject: "test",
                method: "bearer-access-token",
                scopes: new Set(scopes),
              }),
            );
          }),
        ),
      ),
      Layer.provide(HttpPlatform.layer),
      Layer.provide(Etag.layerWeak),
      Layer.provide(NodeServices.layer),
    );
    const { handler, dispose } = HttpRouter.toWebHandler(routes, { disableLogger: true });
    yield* Effect.addFinalizer(() => Effect.promise(dispose));
    const send = (method: string, path: string, body?: unknown, token = "") =>
      Effect.promise(() =>
        handler(
          new Request(`http://127.0.0.1${path}`, {
            method,
            headers: {
              ...(token ? { authorization: `Bearer ${token}` } : {}),
              ...(body === undefined ? {} : { "content-type": "application/json" }),
            },
            ...(body === undefined ? {} : { body: JSON.stringify(body) }),
          }),
        ),
      );
    const trigger = (requestId: string) =>
      send(
        "POST",
        "/api/integrations/signal/trigger",
        {
          connectionId: "connection",
          environmentId: "environment",
          requestId,
          command: "/reviews",
        },
        "worker-token",
      );

    const accepted = yield* trigger("a".repeat(64));
    expect(accepted.status).toBe(200);
    expect(yield* Effect.promise(() => accepted.json())).toEqual({
      threadId: "thread",
      messageId: "signal-key",
    });
    const busy = yield* trigger("b".repeat(64));
    expect(busy.status).toBe(400);
    expect(busy.headers.get("content-type")).toContain("application/json");
    expect(yield* Effect.promise(() => busy.json())).toMatchObject({ reason: "thread_busy" });
    expect(tokens).toEqual(["worker-token", "worker-token"]);
    expect(authCalls).toBe(0);

    expect((yield* send("GET", "/api/integrations/signal")).status).toBe(403);
    expect((yield* send("GET", "/api/integrations/signal/skills/thread")).status).toBe(403);
    scopes = ["access:read"];
    const read = yield* send("GET", "/api/integrations/signal");
    expect(read.status).toBe(200);
    expect(yield* Effect.promise(() => read.json())).toEqual(binding);
    const skills = yield* send("GET", "/api/integrations/signal/skills/thread");
    expect(yield* Effect.promise(() => skills.json())).toEqual(["review-risk"]);
    const configureBody = { commands: binding.commands };
    expect((yield* send("POST", "/api/integrations/signal", configureBody)).status).toBe(403);
    expect((yield* send("DELETE", "/api/integrations/signal")).status).toBe(403);

    scopes = ["access:read", "access:write"];
    const configured = yield* send("POST", "/api/integrations/signal", configureBody);
    expect(configured.status).toBe(200);
    expect(configured.headers.get("cache-control")).toBe("no-store");
    expect(yield* Effect.promise(() => configured.json())).toEqual({ binding, token: "secret" });
    expect((yield* send("DELETE", "/api/integrations/signal")).status).toBeLessThan(300);
  }),
);
