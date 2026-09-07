import {
  CROSS_PROVIDER_AGENT_TOOL_DESCRIPTIONS,
  CROSS_PROVIDER_AGENT_TOOL_INPUTS,
  CROSS_PROVIDER_AGENT_TOOL_NAMES,
} from "@t3tools/contracts";
import * as Predicate from "effect/Predicate";
import * as Schema from "effect/Schema";

import type { CrossProviderToolSpec } from "./CrossProviderAgentToolHost.ts";

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

const EMPTY_OBJECT_SCHEMA: Readonly<Record<string, unknown>> = {
  type: "object",
  properties: {},
  additionalProperties: false,
};

/** Flat JSON Schema object for one tool input, derived from its Effect schema. */
export function toolInputJsonSchema(schema: Schema.Top): Readonly<Record<string, unknown>> {
  const document = Schema.toJsonSchemaDocument(schema);
  const flattened = flattenJsonSchemaAllOf(document.schema);
  // `Schema.Struct({})` describes "object or array"; a tool with no
  // parameters wants the plain empty object schema.
  if (!Predicate.isObject(flattened) || flattened.type !== "object") {
    return EMPTY_OBJECT_SCHEMA;
  }
  return flattened as Readonly<Record<string, unknown>>;
}

/** The complete cross-provider tool surface, built once per process. */
export const CROSS_PROVIDER_TOOL_SPECS: ReadonlyArray<CrossProviderToolSpec> =
  CROSS_PROVIDER_AGENT_TOOL_NAMES.map((name) => ({
    name,
    description: CROSS_PROVIDER_AGENT_TOOL_DESCRIPTIONS[name],
    inputSchema: toolInputJsonSchema(CROSS_PROVIDER_AGENT_TOOL_INPUTS[name]),
  }));
