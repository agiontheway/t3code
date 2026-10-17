import { describe, expect, it } from "vite-plus/test";

import { EnvironmentHttpApi } from "./environmentHttp.ts";

// fork: the Signal connector calls these routes; their paths and auth must not drift.
describe("Signal HTTP contract", () => {
  const signal = EnvironmentHttpApi.groups.signal;

  it("is part of the environment API with the connector's routes", () => {
    expect(
      Object.values(signal.endpoints).map((endpoint) => [
        endpoint.identifier,
        endpoint.method,
        endpoint.path,
      ]),
    ).toEqual([
      ["choices", "GET", "/api/integrations/signal/skills/:threadId"],
      ["get", "GET", "/api/integrations/signal"],
      ["configure", "POST", "/api/integrations/signal"],
      ["revoke", "DELETE", "/api/integrations/signal"],
      ["trigger", "POST", "/api/integrations/signal/trigger"],
    ]);
  });

  it("leaves only trigger outside environment auth", () => {
    expect(
      Object.values(signal.endpoints)
        .filter((endpoint) => endpoint.middlewares.size === 0)
        .map((endpoint) => endpoint.identifier),
    ).toEqual(["trigger"]);
  });
});
