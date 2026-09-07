import { ThreadId } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import { assert, describe, it } from "vite-plus/test";

import { toJsonSchemaObject } from "../../textGeneration/TextGenerationUtils.ts";
import type { CrossProviderAgentToolHost } from "../CrossProviderAgentToolHost.ts";
import {
  buildClaudeInProcessToolServer,
  flattenJsonSchemaAllOf,
  zodShapeFromJsonSchema,
} from "./claudeInProcessTools.ts";

const asObjectSchema = (schema: Schema.Top) =>
  toJsonSchemaObject(schema) as Readonly<Record<string, unknown>>;

describe("zodShapeFromJsonSchema", () => {
  it("maps the primitive and array subset derived from Effect schemas", () => {
    const shape = zodShapeFromJsonSchema(
      asObjectSchema(
        Schema.Struct({
          childIds: Schema.Array(Schema.String),
          timeoutSeconds: Schema.optionalKey(Schema.Finite.check(Schema.isGreaterThan(0))),
          allowOrchestration: Schema.optionalKey(Schema.Boolean),
          offset: Schema.optionalKey(Schema.Int.check(Schema.isGreaterThanOrEqualTo(0))),
          prompt: Schema.String.annotate({ description: "What the child should do." }),
        }),
      ),
    );

    assert.deepEqual(Object.keys(shape).sort(), [
      "allowOrchestration",
      "childIds",
      "offset",
      "prompt",
      "timeoutSeconds",
    ]);
    assert.isTrue(shape.childIds!.safeParse(["a", "b"]).success);
    assert.isFalse(shape.childIds!.safeParse("a").success);
    assert.isFalse(shape.childIds!.safeParse(undefined).success);
    assert.isTrue(shape.timeoutSeconds!.safeParse(undefined).success);
    assert.isFalse(shape.timeoutSeconds!.safeParse(0).success);
    assert.isTrue(shape.offset!.safeParse(3).success);
    assert.isFalse(shape.offset!.safeParse(1.5).success);
    assert.isTrue(shape.allowOrchestration!.safeParse(true).success);
    assert.equal(shape.prompt!.description, "What the child should do.");
  });

  it("folds Effect's allOf refinement checks onto the node", () => {
    assert.deepEqual(
      flattenJsonSchemaAllOf(
        asObjectSchema(
          Schema.Struct({ depth: Schema.Int.check(Schema.isGreaterThanOrEqualTo(1)) }),
        ),
      ),
      {
        type: "object",
        properties: { depth: { type: "integer", minimum: 1 } },
        required: ["depth"],
        additionalProperties: false,
      },
    );
  });

  it("refuses schemas outside the supported subset instead of widening them", () => {
    assert.throws(
      () =>
        zodShapeFromJsonSchema(
          asObjectSchema(Schema.Struct({ nested: Schema.Struct({ a: Schema.String }) })),
        ),
      /no Zod mapping/u,
    );
    assert.throws(() => zodShapeFromJsonSchema({ type: "string" }), /object schemas/u);
  });
});

describe("buildClaudeInProcessToolServer", () => {
  it("registers one SDK server named t3 with a tool per granted spec", () => {
    const host: CrossProviderAgentToolHost = {
      toolsForThread: () => Effect.succeedNone,
      call: () => Effect.succeed({ output: {}, isError: false }),
    };
    const server = buildClaudeInProcessToolServer({
      threadId: ThreadId.make("thread-1"),
      specs: [
        {
          name: "agent_catalog",
          description: "List routes.",
          inputSchema: { type: "object", properties: {}, additionalProperties: false },
        },
      ],
      host,
      // Construction never invokes a handler; the round trip is proven in
      // ClaudeAdapter.test.ts over an in-memory MCP transport.
      runPromise: () => Promise.reject(new Error("not invoked")),
    });

    assert.equal(server.type, "sdk");
    assert.equal(server.name, "t3");
    assert.isDefined(server.instance);
  });
});
