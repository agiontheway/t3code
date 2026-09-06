import { createSdkMcpServer, tool } from "@anthropic-ai/claude-agent-sdk";
import type { ThreadId } from "@t3tools/contracts";
import * as Predicate from "effect/Predicate";
import { z } from "zod";

import {
  encodeCrossProviderToolOutput,
  type CrossProviderAgentToolHost,
  type CrossProviderToolResult,
  type CrossProviderToolSpec,
} from "../CrossProviderAgentToolHost.ts";

/** Name of the in-process SDK server; Claude sees tools as `mcp__t3__<name>`. */
export const CLAUDE_IN_PROCESS_SERVER_NAME = "t3";

type ZodField = z.ZodTypeAny;

/**
 * Effect emits refinement checks as `allOf: [{ minimum: 1 }]` siblings of
 * `type`; fold them onto the node so both bindings see one flat schema.
 */
export function flattenJsonSchemaAllOf(schema: unknown): unknown {
  if (!Predicate.isObject(schema)) return schema;
  const { allOf, ...rest } = schema;
  const merged: Record<string, unknown> = { ...rest };
  if (Array.isArray(allOf)) {
    for (const entry of allOf) {
      if (Predicate.isObject(entry)) Object.assign(merged, flattenJsonSchemaAllOf(entry));
    }
  }
  if (Predicate.isObject(merged.properties)) {
    merged.properties = Object.fromEntries(
      Object.entries(merged.properties).map(([key, value]) => [key, flattenJsonSchemaAllOf(value)]),
    );
  }
  if (merged.items !== undefined) merged.items = flattenJsonSchemaAllOf(merged.items);
  return merged;
}

function zodFieldFromJsonSchema(path: string, rawSchema: unknown): ZodField {
  const schema = flattenJsonSchemaAllOf(rawSchema);
  if (!Predicate.isObject(schema)) {
    throw new Error(`Unsupported JSON Schema at ${path}: expected an object schema.`);
  }
  const description = Predicate.isString(schema.description) ? schema.description : undefined;
  const withDescription = (field: ZodField): ZodField =>
    description ? field.describe(description) : field;
  switch (schema.type) {
    case "string": {
      let field = z.string();
      if (Predicate.isNumber(schema.minLength)) field = field.min(schema.minLength);
      if (Predicate.isNumber(schema.maxLength)) field = field.max(schema.maxLength);
      return withDescription(field);
    }
    case "integer":
    case "number": {
      let field = schema.type === "integer" ? z.number().int() : z.number();
      if (Predicate.isNumber(schema.minimum)) field = field.min(schema.minimum);
      if (Predicate.isNumber(schema.exclusiveMinimum)) field = field.gt(schema.exclusiveMinimum);
      if (Predicate.isNumber(schema.maximum)) field = field.max(schema.maximum);
      if (Predicate.isNumber(schema.exclusiveMaximum)) field = field.lt(schema.exclusiveMaximum);
      return withDescription(field);
    }
    case "boolean":
      return withDescription(z.boolean());
    case "array":
      return withDescription(z.array(zodFieldFromJsonSchema(`${path}[]`, schema.items)));
    default:
      throw new Error(
        `Unsupported JSON Schema at ${path}: type ${String(schema.type)} has no Zod mapping.`,
      );
  }
}

/**
 * Derive the Zod raw shape the SDK's `tool()` needs from a flat object JSON
 * Schema. Only the primitive/array subset the cross-provider tool contract
 * uses is mapped; anything else is a programming error surfaced at session
 * start rather than a silently widened schema.
 */
export function zodShapeFromJsonSchema(
  inputSchema: Readonly<Record<string, unknown>>,
): Record<string, ZodField> {
  if (inputSchema.type !== "object") {
    throw new Error("Cross-provider tool input schemas must be object schemas.");
  }
  const properties = Predicate.isObject(inputSchema.properties) ? inputSchema.properties : {};
  const required = new Set(
    Array.isArray(inputSchema.required) ? inputSchema.required.filter(Predicate.isString) : [],
  );
  const shape: Record<string, ZodField> = {};
  for (const [key, property] of Object.entries(properties)) {
    const field = zodFieldFromJsonSchema(key, property);
    shape[key] = required.has(key) ? field : field.optional();
  }
  return shape;
}

function toCallToolResult(result: CrossProviderToolResult) {
  return {
    content: [{ type: "text" as const, text: encodeCrossProviderToolOutput(result.output) }],
    ...(result.isError ? { isError: true } : {}),
  };
}

/**
 * Build the in-process MCP server that exposes the granted cross-provider
 * tools to one Claude session. Construction is cheap and side-effect free so
 * it can run on every start and resume.
 */
export function buildClaudeInProcessToolServer(input: {
  readonly threadId: ThreadId;
  readonly specs: ReadonlyArray<CrossProviderToolSpec>;
  readonly host: CrossProviderAgentToolHost;
  readonly runPromise: <A>(effect: import("effect/Effect").Effect<A>) => Promise<A>;
}) {
  return createSdkMcpServer({
    name: CLAUDE_IN_PROCESS_SERVER_NAME,
    tools: input.specs.map((spec) =>
      tool(spec.name, spec.description, zodShapeFromJsonSchema(spec.inputSchema), (args) =>
        input.runPromise(input.host.call(input.threadId, spec.name, args)).then(toCallToolResult),
      ),
    ),
  });
}
