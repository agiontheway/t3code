# ADR: Native provider tools for cross-provider agent orchestration

- **Status:** Proposed
- **Date:** 2026-09-06
- **Decision owners:** T3 Code fork maintainers
- **Scope:** Claude and Codex provider adapters; T3 orchestration; agent activity projection

## Context

T3 Code already runs Claude and Codex agents through provider instances authenticated with the
user's native subscriptions. Each provider's native agent events are normalized into T3
orchestration events and displayed in **Agents → Direct Spawns**. That path works well when a
provider's own orchestration tools create children within the same provider.

The missing capability is deterministic cross-provider delegation. A typical topology is a Claude
Fable 5.1 parent using the user's Claude subscription, delegating work to a GPT-5.6 Sol child using
the user's OpenAI subscription. The GPT child may itself be an authorized sub-orchestrator that
fans out further. A GLM 5.3 Flash parent reached through a separately configured OpenRouter-backed
Codex instance is also supported, but it must never cause Claude or OpenAI children to be billed
through OpenRouter.

Two earlier experiments proved that T3 can create an ordinary child thread against an explicit
provider instance and have it appear in Direct Spawns. They also showed that adding a parallel
dispatcher or a general HTTP MCP boundary creates avoidable lifecycle, routing, and context costs.
The durable architectural question is therefore where cross-provider control belongs, not whether
T3's orchestration engine can run the child.

## Decision drivers

- Preserve native Claude and OpenAI subscription authentication and billing.
- Preserve T3's existing child-thread lifecycle and Direct Spawns projection.
- Keep same-provider dispatch behavior unchanged.
- Make provider selection explicit and fail closed; never fall back to a provider with a matching
  model name.
- Support a Claude top-level orchestrator and a Codex sub-orchestrator with nested fan-out.
- Avoid a supervisor process, shell dispatcher, repository prompt file, or fork of Claude Code or
  Codex.
- Keep provider-specific complexity at the adapter boundary.
- Minimize persistent tool-schema and tool-result context.
- Remain practical to rebase on frequent upstream T3 Code releases.

## Decision

Implement cross-provider agent operations as a provider-neutral T3 service with thin,
provider-native tool bindings in the Claude and Codex adapters.

### Shared orchestration service

Add one internal `CrossProviderAgentService` (final name may follow an established local naming
pattern). It owns:

- the catalog of provider instances and models eligible for cross-provider use;
- exact provider-instance and model validation;
- creation of an ordinary T3 child thread with durable parent/child ownership;
- starting, observing, following up, and interrupting the child through existing orchestration
  commands and receipts;
- canonical agent/task activities consumed by the existing Direct Spawns projection; and
- structured, provider-neutral success and error values.

The service must use existing orchestration commands and event-sourced state. It is not a second
agent runtime or status machine.

### Claude binding

Expose the service to authorized Claude sessions using the Claude Agent SDK's in-process custom
tool facility (`createSdkMcpServer` and `tool`, or their supported equivalents in the pinned SDK).
This is MCP at the Claude SDK boundary, because the subscription-backed Claude SDK does not expose
an equivalent non-MCP dynamic-tool callback. It is deliberately not the T3 HTTP Effect MCP server:
there is no loopback network server, bearer token, discovery endpoint, or separately managed MCP
session for agent dispatch.

### Codex binding

Expose the same service to authorized Codex sessions through Codex app-server dynamic tools. Pass
the tool specifications when a thread is started or resumed, handle the app-server
`item/tool/call` server request, and return the corresponding `DynamicToolCallResponse`. The
generated app-server schemas already describe these protocol messages; the adapter must implement
the missing request/response plumbing without changing the generated contract.

GLM 5.3 Flash configured as a custom model on a Codex provider instance uses this same binding.
Only the parent inference uses that instance's OpenRouter configuration. A child request routes to
the exact native Claude or Codex provider instance selected by T3.

### Tool contract

Use a compact lifecycle-oriented surface:

- `agent_catalog`: return only eligible provider instances and models, with stable exact IDs.
- `agent_spawn`: validate the route, create and start one child, and return its durable handle.
- `agent_wait`: wait on T3 events for one or more owned children and return terminal results.
- `agent_follow_up`: add a turn to an owned settled child.
- `agent_interrupt`: interrupt an owned active child.

The names may be provider-namespaced when required to avoid collisions, but their schemas and
semantics must be shared. `agent_wait` is a model-invoked synchronization operation at the parent
boundary; it must subscribe to durable T3 events and must not poll. It does not mean that the child
uses MCP on each turn.

This contract is chosen over a single blocking `dispatch_agent` call because separate spawn and
wait operations preserve parallel fan-out, cancellation, follow-up, durable handles, and normal
Direct Spawns visibility. Tool results should contain the minimum state needed by the parent. Any
proposal to truncate or summarize child output requires approval because it changes semantics.

### Eligibility and recursive orchestration

Add a cross-provider agent-access setting that gates tool injection. When disabled, no custom
cross-provider tools are supplied. When enabled, eligible orchestration threads receive the tools
at session start and resume. This includes a GPT child explicitly authorized to act as a
sub-orchestrator, allowing it to fan out through the same service.

Tool possession does not grant access to arbitrary credentials. Every operation is limited to the
current environment, allowed provider instances, project/worktree context, and children owned by
the calling thread. Same-provider native spawn tools remain available and unchanged.

### Exact routing and failure behavior

`providerInstanceId` is authoritative. Friendly labels and driver names are display data, not
routing substitutes. The service validates that the exact instance is enabled, authenticated,
eligible, and offers the requested model before persisting a spawn. Ambiguous or stale routes fail
with a structured error.

There is no model-name fallback, alias guessing, or OpenRouter fallback. In particular, requesting
a GPT or Claude child can never select an OpenRouter-backed instance merely because it advertises
that model.

Mutating calls have exactly-once semantics at the T3 command boundary. Transport errors must not
silently replay a spawn, follow-up, or interrupt. Rate-limit retry behavior, including HTTP 429
handling for parent inference, is outside this decision and must not be smuggled into tool
handling.

## Why not the hand-rolled approach (first branch, abandoned)

The first experiment used a separate dispatch path built around scripts or CLI invocation. It
could invoke models, but it duplicated T3's provider selection, authentication, session ownership,
and lifecycle. Children were not inherently ordinary T3 threads, so Direct Spawns, resumption,
permissions, usage, and cancellation required parallel integration. It also encouraged a
supervisor-shaped runtime and made accidental OpenRouter routing harder to rule out.

That approach conflicts with the product goal: cross-provider dispatch should be a capability of a
normal T3 orchestrator session, not a second harness beside it.

## Why not the HTTP MCP approach (second branch, abandoned)

The second experiment exposed `agent_catalog`, `agent_spawn`, `agent_wait`, `agent_follow_up`, and
`agent_interrupt` through T3's Effect HTTP MCP server. It successfully proved explicit native
provider routing and Direct Spawns integration, but placed agent orchestration inside infrastructure
originally used for collaborative preview/browser tools.

Its costs were:

- legacy HTTP MCP discovery, authentication, session, and liveness machinery for an in-process T3
  capability;
- agent schemas and potentially large results retained in parent context alongside the existing
  preview toolkit;
- a second public boundary whose lifecycle could diverge from the provider session;
- added alias, endpoint-scope, and authorization failure modes; and
- greater risk of confusing an MCP tool lifecycle with the canonical T3 agent lifecycle.

The intermittent OpenRouter 429s observed after a child completed were not caused by MCP and must
not be represented as such. They occurred on subsequent GLM parent inference through the custom
provider transport. Removing HTTP MCP narrows the architecture, but it does not remove the need for
a parent-model continuation or guarantee that an upstream provider will not rate-limit it.

## Why not upgrade the shared MCP server

The existing Effect MCP implementation is pinned to the 2025-06-18 protocol and serves a different
product purpose. A later MCP protocol can reduce transport/session overhead, but upgrading it does
not turn the shared server into the correct ownership boundary, remove model-visible tool schemas,
or fix provider HTTP 429s. It would also couple this feature to a wider protocol migration. The
Claude SDK's in-process custom-tool facility is the narrow approved MCP exception.

## Consequences

### Positive

- Claude and OpenAI child calls continue through the user's native T3 provider instances.
- Children are ordinary T3 threads and appear through the existing Direct Spawns pipeline.
- Fable can orchestrate directly, while an authorized GPT child can recursively orchestrate.
- Provider-neutral lifecycle behavior has one implementation and focused tests.
- Provider protocol details remain in adapters, matching T3's house style.
- The original Effect MCP preview/browser server is left untouched.
- No Claude Code or Codex fork, proxy, supervisor, or external agent service is required.
- The fork's delta stays narrow enough to rebase regularly on upstream T3 Code.

### Negative

- Claude and Codex require distinct binding implementations and SDK compatibility tests.
- Claude custom tools remain MCP-shaped inside the SDK even though no HTTP MCP boundary is used.
- Tool definitions and results still consume model context; native injection cannot make them free.
- Start, resume, interruption, and reconnect paths must consistently install and remove the tools.
- The parent still needs an inference after receiving a child result, so parent-provider failures
  remain possible.

### Risks

- Provider protocol updates may change dynamic-tool or SDK-tool contracts.
- A tool response race could leave UI and parent state inconsistent if it bypasses orchestration
  receipts.
- Nested orchestration can create ownership or cycle problems unless parent-child authorization is
  explicit.
- Tool-name collisions or duplicate injection can cause the model to choose the wrong mechanism.
- Large child outputs may inflate parent context.

## Invariants

1. A child is created exactly once as an ordinary T3 thread before it is exposed as spawned.
2. The persisted child records the exact provider instance and model selected after validation.
3. Claude and OpenAI subscription children never route through OpenRouter implicitly.
4. Existing same-provider native dispatch is unchanged.
5. Direct Spawns is derived from canonical task activities; there is no parallel UI data source.
6. Only an owning parent or authorized ancestor may wait, follow up, or interrupt a child.
7. Disabling cross-provider access removes the custom tools from new and resumed sessions.
8. Tool transport failures never trigger an unobservable replay of a mutating command.
9. No secret, token, provider home, or credential material is returned in catalogs, tool results,
   logs, or activities.

## Compatibility and rollout

Implement behind the new cross-provider agent-access setting. Initially support only Claude
and Codex adapter sessions as callers and only explicitly eligible Claude and Codex instances as
targets. Unsupported adapters fail closed and do not receive the tools.

Ship adapter-contract tests first, followed by one integrated topology test for Fable → GPT-5.6 Sol
and one for GLM 5.3 Flash → GPT-5.6 Luna. Add a nested Fable → GPT-5.6 Sol → Claude Haiku test before
claiming sub-orchestrator support. All tests must assert canonical child-thread and task activity
creation, not merely tool-return JSON.

The two abandoned implementations are reference material only. Do not merge or transplant their
parallel transport layers. Reuse only behavior that can be expressed through the shared service and
current upstream orchestration contracts.

## Long-term advantages

This design turns cross-provider orchestration into a capability that provider adapters can opt
into, rather than a special provider or external service. Future adapter support requires a thin
binding to a stable T3-owned service, while routing, ownership, event projection, and policy remain
centralized. That separation should survive frequent upstream changes: adapter protocol churn is
localized, and the core continues to use the same commands, events, receipts, and UI projection as
native dispatch.

It also creates a clean place to add future policy—such as explicit orchestration depth, per-instance
allowlists, or result references—without teaching every provider about every other provider. Those
features are not part of this decision and require separate approval.
