import { ThreadId } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";

import * as McpInvocationContext from "../../McpInvocationContext.ts";
import * as McpToolAccess from "../../McpToolAccess.ts";
import * as CrossProviderAgentMcpService from "./service.ts";
import { CrossProviderAgentToolkit } from "./tools.ts";

const isThreadId = Schema.is(ThreadId);

/** The child a write targets, for the declaration's mode check; the service checks ownership. */
const childThread = <P extends { readonly childId: string }>(input: P) =>
  isThreadId(input.childId) ? [input.childId] : [];

const withService = <A, E, R>(
  use: (
    service: CrossProviderAgentMcpService.CrossProviderAgentMcpService["Service"],
    scope: McpInvocationContext.McpInvocationScope,
  ) => Effect.Effect<A, E, R>,
) =>
  Effect.gen(function* () {
    const scope = yield* McpInvocationContext.McpInvocationContext;
    const service = yield* CrossProviderAgentMcpService.CrossProviderAgentMcpService;
    return yield* use(service, scope);
  });

// fork: catalog, wait and result change nothing (they never acknowledge a task's delivery);
// spawn acts as the calling thread; follow-up and interrupt write to the child thread.
const handlers = {
  agent_catalog: McpToolAccess.reads(() => withService((service, scope) => service.catalog(scope))),
  agent_spawn: McpToolAccess.actsAsCaller((input) =>
    withService((service, scope) => service.spawn(scope, input)),
  ),
  agent_wait: McpToolAccess.reads((input) =>
    withService((service, scope) => service.wait(scope, input)),
  ),
  agent_result: McpToolAccess.reads((input) =>
    withService((service, scope) => service.result(scope, input)),
  ),
  agent_follow_up: McpToolAccess.writesThreads(childThread, (input) =>
    withService((service, scope) => service.followUp(scope, input)),
  ),
  agent_interrupt: McpToolAccess.writesThreads(childThread, (input) =>
    withService((service, scope) => service.interrupt(scope, input)),
  ),
} satisfies McpToolAccess.Handlers<typeof CrossProviderAgentToolkit.tools>;

export const layer = McpToolAccess.toLayer(CrossProviderAgentToolkit, handlers);
