# Build spec: Native cross-provider agent tools

- **Status:** Approved for implementation (2026-09-07)
- **Companions:** [ADR](./native-cross-provider-agent-tools-adr.md), [PRD](./native-cross-provider-agent-tools-prd.md)
- **Branch:** `feat/native-cross-provider-tools` off `design/native-cross-provider-tools-v3` (upstream `ea646c083`)

This document resolves the ADR/PRD open decisions and records the code-grounded findings the
implementation must honour. Where this spec and the PRD differ, this spec wins; it was written
after reading the code.

## 1. Resolved decisions

| Decision                   | Resolution                                                                                                                                                                                                                                                                                                                                                                                                                                                                  |
| -------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Sub-orchestrator authority | Explicit per-spawn `allowOrchestration: boolean` (default `false`). Only children spawned with it receive the tools.                                                                                                                                                                                                                                                                                                                                                        |
| Depth cap                  | Setting `crossProviderAgentMaxDepth`, default **2**. Root orchestrator is depth 0; a child's depth is parent depth + 1. Any spawn is rejected when `childDepth > cap`. A spawn with `allowOrchestration: true` is additionally rejected when `childDepth >= cap`, because that child could never spawn within the cap. With cap 2: root(0) → sub-orchestrator(1) → leaves(2). Cap 1 forbids sub-orchestrators. Depth is stored on the child's `spawn` metadata at creation. |
| Output cap                 | Setting `crossProviderAgentOutputCapChars`, default **5000**. `agent_wait`/`agent_result` return the final assistant text in full when `length <= cap`. Above cap: deterministic head `floor(cap/2)` + marker + tail `cap - floor(cap/2)`, with `truncated: true`, `totalChars`, and `childId` so the caller can fetch the rest. Never silently truncate.                                                                                                                   |
| Full retrieval             | Sixth read-only tool `agent_result(childId, offset?, limit?)` returns the stored final output of an owned settled child, by character range when `offset`/`limit` are given, otherwise subject to the same cap rule.                                                                                                                                                                                                                                                        |
| Policy timing              | Every tool call is validated against current settings at execution time. The tool list held by a running session refreshes only on next start/resume.                                                                                                                                                                                                                                                                                                                       |
| Tool names                 | Bare logical names `agent_catalog`, `agent_spawn`, `agent_wait`, `agent_result`, `agent_follow_up`, `agent_interrupt`. Claude in-process server is named `t3`, so Claude sees `mcp__t3__agent_spawn`. Codex sees bare names (function specs, no namespace).                                                                                                                                                                                                                 |
| Spawn inputs               | Child inherits `projectId`, `worktreePath`, `branch`, `runtimeMode`, `interactionMode` from the parent. Caller sets `prompt`, `title`, `providerInstanceId`, `model`, `allowOrchestration`. No per-spawn permission or worktree override.                                                                                                                                                                                                                                   |
| `agent_wait` timeout       | Optional `timeoutSeconds` (no default). On timeout, return each child's current state with `settled: false`; the caller may wait again. Never poll.                                                                                                                                                                                                                                                                                                                         |
| Settings UI                | Settings → Integrations, new **Agents** section above Browser, matching the abandoned prototype's placement (see §6).                                                                                                                                                                                                                                                                                                                                                       |
| Codex resume gap           | Keep tool-granted Codex sessions alive (exempt from idle reaper). On an unavoidable resume, post a visible activity in the thread stating cross-provider tools are unavailable until a new thread is started. See §4.3.                                                                                                                                                                                                                                                     |
| Same-provider calls        | `agent_spawn` targeting the caller's own provider **instance** is rejected with a structured error directing the model to its native spawn tool. Same driver on a _different_ instance is allowed.                                                                                                                                                                                                                                                                          |

## 2. Code-grounded findings the build depends on

All paths relative to repo root.

### 2.1 Claude adapter

- Pinned `@anthropic-ai/claude-agent-sdk` 0.3.260 exports `createSdkMcpServer` and `tool` (`sdk.d.ts:511`, `:8518`); `McpSdkServerConfigWithInstance` is assignable into `mcpServers`.
- One options site covers start and resume: `apps/server/src/provider/Layers/ClaudeAdapter.ts:4618` (`queryOptions`). `mcpServers` is currently a whole-object spread at `:4650-4670` keyed `"t3-code"` (the upstream preview/browser HTTP MCP). Merge the in-process `t3` server into that same object; do not add a second `mcpServers` key.
- Per-thread gating precedent: `McpProviderSession.readMcpProviderSession(threadId)` at `:4608`, registry `apps/server/src/mcp/McpProviderSession.ts`, populated by `ProviderService.prepareMcpSession` (`ProviderService.ts:746-764`). Copy the _shape_ (per-thread lookup consulted at options-build time), not the file.
- Identity at options-build time: `threadId = input.threadId` (`:4142`), `boundInstanceId` (`:1903`), `apiModelId` (`:4560-4572`).
- Interrupt tears down the query (`interruptTurn` `:4966-4975` → `stopSessionInternal`) and the next turn resumes through `startSession`. Tool server construction must be cheap and idempotent.
- Adapter options seam: `ClaudeAdapterLiveOptions` (`:343-355`), built in `ClaudeDriver.ts:150-157`.
- Test harness: `ClaudeAdapter.test.ts:163-219` (`makeHarness`, `getLastCreateQueryInput()`); no test currently asserts `options.mcpServers`.

### 2.2 Codex adapter

- Runtime: `apps/server/src/provider/Layers/CodexSessionRuntime.ts`; RPC client `packages/effect-codex-app-server/src/{protocol,client}.ts`.
- `item/tool/call` is already in `SERVER_REQUEST_METHODS` (`_generated/meta.gen.ts:104-115`) with typed params `DynamicToolCallParams` (`schema.gen.ts:34673`) and response `DynamicToolCallResponse` (`:34690`). **No handler is registered**; `dispatchRequest` (`client.ts:167-179`) takes the typed branch and fails on the missing handler today. Register it beside the four approval handlers (`CodexSessionRuntime.ts:1919`, `:1975`, `:2033`, `:2098`), using the same Deferred-correlation pattern.
- `DynamicToolSpec` exists in the generated schema (`schema.gen.ts:33941`) but **no request type carries `dynamicTools`**, because Codex marks the field `#[experimental("thread/start.dynamicTools")]` and the schema/TS generator strips experimental fields. Verified in Codex source at tag `rust-v0.153.2` (the installed binary) and on `main`: `ThreadStartParams.dynamic_tools` exists; `ThreadResumeParams` and `ThreadForkParams` have no such field.
- T3 already sends `capabilities.experimentalApi: true` on `initialize` (`CodexProvider.ts:341`), which is the documented opt-in.
- Therefore: send `dynamicTools` on `thread/start` through the raw request path (`client.raw.request`, already used for `turn/start` at `CodexSessionRuntime.ts:2330`) or by widening the typed call with a local, non-generated param type. **Do not hand-edit `_generated/*`.** Do not regenerate the schema; regeneration would not add the field.
- `openCodexThread` (`:691-726`) is the single call site for start and resume, and a recoverable resume failure falls back to `thread/start` with the same params, so `dynamicTools` must be included on the start params and is harmlessly ignored on resume (verify the binary ignores unknown resume fields; if it rejects them, strip `dynamicTools` from the resume request).
- `DynamicToolCallParams.threadId` is the **Codex** thread id, not the T3 `ThreadId`; map back through the session as the collab child routing does.
- Provider instance isolation: each Codex instance has its own `CODEX_HOME` (`CodexSessionRuntime.ts:1177-1183`). The OpenRouter-backed GLM instance is configured through its own home's `config.toml`; T3 sends no `modelProvider`. A child on a different instance therefore runs in a different home with different credentials by construction. Assert this in the integrated test by checking the child session's `providerInstanceId`.
- Idle reaper: `ProviderSessionReaper.ts`, 30-minute inactivity threshold (`:17`), 5-minute sweep.
- Tests: `CodexAdapter.test.ts` (fake runtime via `makeRuntime`), `CodexSessionRuntime.test.ts` (pure helpers), `CodexCollabRuntime.integration.test.ts` + `testFixtures/codexCollabMockPeer.mjs` (scripted server requests; `completeTurnOnServerResponse` is directly reusable for an `item/tool/call` round trip), `packages/effect-codex-app-server/test/fixtures/codex-app-server-mock-peer.ts`.

### 2.3 Orchestration engine

- Commands: `thread.create` (`packages/contracts/src/orchestration.ts:795-810`), `thread.turn.start` (`:970-993`), `thread.turn.interrupt` (`:1020-1026`), `thread.activity.append` (internal only, `:1178-1184`). Follow-up is another `thread.turn.start`.
- Engine: `OrchestrationEngineService` (`apps/server/src/orchestration/Services/OrchestrationEngine.ts:110-113`); `dispatch` returns `{ sequence }`.
- **Exactly-once exists**: durable command receipts keyed by `commandId` (`Layers/OrchestrationEngine.ts:144-172`). Replay returns the original sequence; rejected ids are sticky; aggregate is bound to the id. Mint one `commandId` per logical mutation (`server:xp-agent:<op>:<parentThreadId>:<uuid>` style, cf. `ThreadSettlementReactor.ts:73`) and reuse it on any retry.
- Subscription: `subscribeDomainEvents` (`:88-92`) — acquire **before** dispatching, then filter. `readThreadEvents` for bounded backfill after reconnect.
- Turn settlement signal: `thread.session-set` where session status leaves `running` (`ProviderRuntimeIngestion.ts:604-625`; projector `projector.ts:637-672`). `ready` ⇒ completed, `error` ⇒ error, `interrupted`/`stopped` ⇒ interrupted. `thread.turn-diff-completed` follows and carries `assistantMessageId`.
- Final assistant text: `ProjectionThreadMessages` via `ProjectionSnapshotQuery.getThreadDetailSnapshot` (`ProjectionSnapshotQuery.ts:234-256`). Note `MAX_THREAD_MESSAGES` cap in `projector.ts:604`.
- **No parent link on threads today.** `ThreadCreateCommand`/`ThreadCreatedPayload`/`OrchestrationThread` have no parent field. Direct Spawns is an _activity_ relation on the parent thread.
- Direct Spawns: client fold `packages/client-runtime/src/state/subagentRuntime.ts` (`foldSubagentActivities` `:463`, `deriveAgentPanelModel` `:732`, `directAgents` `:851`). Driven by `task.started` / `task.progress` / `task.updated` / `task.completed` activities on the parent thread. Row appears only when `payload.agentKind === "agent"`, stamped server-side at ingestion (`ProviderRuntimeIngestion.ts:309-345`, `classifyTaskAgentKind` `providerRuntime.ts:613-626`; denylisted `taskType`s: `monitor`, `monitor_mcp`, `local_bash`, `shell`, `plan`, `dream`). Linkage bundle `taskAgentLinkageFields` (`providerRuntime.ts:630-668`) must be repeated on every row. Codex collab rows set `timelineBypass: true`.
- Dependency direction: orchestration depends on provider adapters (`ProviderCommandReactor.ts:39-42`), never the reverse. Composition root `apps/server/src/server.ts:276-285`, `:420-426`; `OrchestrationLayerLive` in `orchestration/runtimeLayer.ts:33-36`.
- Service conventions: `Context.Service` tag `"t3/<area>/Services/<File>/<Name>"`, `Layer.effect` named `<Name>Live`. Logging: `Effect.logX(message, { structured })`, `Cause.pretty`, span annotations with dotted keys.
- Test conventions: `OrchestrationEngine.test.ts:62-80` (`makeOrchestrationLayer` over in-memory SQLite, real engine); receipt assertions by literal command ids; `ProviderRuntimeIngestion.activity.test.ts` for emitted activities; `subagentRuntime.test.ts` for the Direct Spawns fold.

### 2.4 Settings and UI

- Server settings: `packages/contracts/src/settings.ts` (`ServerSettings` `:847`, `DEFAULT_SERVER_SETTINGS` `:991`, `ServerSettingsPatch` `:1141`), `withDecodingDefault` on every field. Patch application `packages/shared/src/serverSettings.ts`. Store `apps/server/src/serverSettings.ts` (`ServerSettingsService` `:188`, `redactServerSettingsForClient` `:163`). Client hooks `apps/web/src/hooks/useSettings.ts:394`, `:556`.
- Reference boolean: `enableAgentBrowserAccess` (`settings.ts:870`, consumed `ProviderService.ts:729`, UI `ProjectDefaultsSettings.tsx:345-380`).
- Integrations page: `apps/web/src/routes/settings.integrations.tsx` → `apps/web/src/components/settings/IntegrationsSettings.tsx` (`IntegrationsSettingsPanel` `:1168`; single `SettingsSection id="browser"` at `:1185`). Vocabulary: `SettingsSection`, `SettingsRow` (`serverScoped`, `resetAction`, `status`), `SettingResetButton`, `AlertDialog` (lossy confirm, `:1090-1137`), `Collapsible`/`CollapsiblePanel`/`CollapsibleTrigger` in `apps/web/src/components/ui/collapsible`, `searchableSetting(id)` in `settingsSearch.ts`, bulk reset in `SettingsPanels.tsx:593-760`.
- Provider registry: `packages/contracts/src/providerInstance.ts` (`ProviderInstanceId` `:82` is the routing key; `ProviderDriverKind` `:70`; `ProviderInstanceConfig` `:124`). Enabled resolution `settings.ts:1028`. Wire snapshot `packages/contracts/src/server.ts` (`ServerProviderState` `:51`, `ServerProviderAuth` `:61`, `ServerProviderModel` `:69`, availability `~:140`). Registry `apps/server/src/provider/Layers/ProviderInstanceRegistryLive.ts`, `ProviderRegistry.ts`.
- Secrets never reach clients: per-instance env vars are redacted (`serverSettings.ts:149-171`). The catalog must expose only `providerInstanceId`, `displayName`, `driver`, and model slugs/names.
- Prototype UI to reuse for **placement and layout only**: branch `feat/cross-provider-child-agents`, commit `8ede0469b`, `apps/web/src/components/settings/IntegrationsSettings.tsx` lines ~599-740 (`CrossProviderAgentAccessSetting`) and the `SettingsSection id="agents" title="Agents"` mount above Browser. Do **not** reuse its alias model, its render-time `useEffect` that writes settings, or anything under `apps/server/src/mcp/toolkits/agents/`.

## 3. Component design

### 3.1 Contracts (`packages/contracts`)

Add to `ServerSettings` (+ defaults, + patch keys, + `applyServerSettingsPatch` pass-through):

```ts
enableCrossProviderAgentAccess: boolean            // default false
crossProviderAgentMaxDepth: number                 // default 2, integer >= 1
crossProviderAgentOutputCapChars: number           // default 5000, integer >= 500
crossProviderAgentRoutes: {                        // explicit eligibility; empty = generated defaults
  [providerInstanceId: string]: {
    enabled: boolean
    models: readonly string[]                      // eligible model slugs; empty = all catalog models
  }
}
```

Add to thread contracts, all optional and additive:

```ts
// ThreadCreateCommand, ThreadCreatedPayload, OrchestrationThread
spawn?: {
  parentThreadId: ThreadId
  allowOrchestration: boolean
  depth: number            // parent depth + 1, computed server-side
  taskId: RuntimeTaskId    // the Direct Spawns row id on the parent
}
```

Decider/projector carry `spawn` through unchanged. This is the durable ownership record required by ADR invariants 1, 2, and 6.

Add a provider-neutral module `packages/contracts/src/crossProviderAgent.ts` with the tool input/output schemas and error union (§3.4). Both adapters derive their provider-native schema (Zod for Claude, JSON Schema for Codex) from these Effect schemas; do not hand-write two copies.

### 3.2 `CrossProviderAgentService` (`apps/server/src/orchestration/Services|Layers/CrossProviderAgent*.ts`)

Provider-neutral. Depends on `OrchestrationEngineService`, `ProjectionSnapshotQuery`, `ProviderRegistry`/instance registry, `ServerSettingsService`. Imports nothing from the Claude SDK or Codex protocol packages.

Operations (all take `callerThreadId`):

- `catalog()` → eligible routes from current settings ∩ enabled ∩ authenticated ∩ supported drivers (`claudeAgent`, `codex`). Excludes the caller's own instance? **No** — include it but mark `isCallerInstance: true`; `spawn` rejects it.
- `spawn({ providerInstanceId, model, prompt, title?, allowOrchestration })`:
  1. Validate access enabled, route eligible, model offered by that exact instance, caller depth + 1 ≤ cap (and `< cap` if `allowOrchestration`), target ≠ caller instance.
  2. Read the parent `OrchestrationThread`; copy `projectId`, `worktreePath`, `branch`, `runtimeMode`, `interactionMode`.
  3. Mint `childThreadId`, `taskId`, and a deterministic `commandId` for `thread.create` and one for `thread.turn.start`. Subscribe to domain events **before** dispatching.
  4. Dispatch `thread.create` with `spawn` metadata, then `thread.turn.start` with the prompt. Emit `task.started` on the parent via the internal activity command with the linkage bundle (`title`, `role: "cross-provider"`, `model`, `taskType: "cross_provider_agent"`, `timelineBypass: true`, `childThreadId` in payload) so the row appears in Direct Spawns immediately.
  5. Return `{ childId: childThreadId, taskId, providerInstanceId, model, status: "running" }`.
- `wait({ childIds, timeoutSeconds? })` → subscribe, ownership-check each child (caller is parent or ancestor via `spawn` chain), backfill current state from the projection, then await `thread.session-set` transitions out of `running` for each; return per-child `{ childId, settled, state, output?, truncated?, totalChars?, usage? }`. Apply the output cap rule. On timeout return `settled: false` entries. Mirror settlement into `task.updated`/`task.completed` on the parent row.
- `result({ childId, offset?, limit? })` → ownership check; read final assistant text from projection; range or cap rule.
- `followUp({ childId, prompt })` → ownership check; child must be settled; deterministic `commandId`; `thread.turn.start`; emit `task.updated { status: "running" }` on the parent row; return handle.
- `interrupt({ childId })` → ownership check; child must be running; deterministic `commandId`; `thread.turn.interrupt`; emit `task.updated { status: "interrupted" }` once the session-set confirms.

Errors are a closed structured union: `access_disabled`, `route_not_eligible`, `instance_disabled`, `instance_unauthenticated`, `model_not_offered`, `same_instance`, `depth_exceeded`, `not_owner`, `child_not_settled`, `child_not_running`, `unsupported_caller`. Each names the rejected `providerInstanceId`/`model`/`childId` and nothing else.

Ownership: caller owns a child if `child.spawn.parentThreadId === caller` or the caller appears in the child's ancestor chain. Depth = `thread.spawn?.depth ?? 0`.

### 3.3 Tool host seam (breaking the dependency cycle)

Adapters cannot import the service. Define a tiny provider-neutral interface in `apps/server/src/provider/CrossProviderAgentToolHost.ts`:

```ts
interface CrossProviderAgentToolHost {
  readonly toolsForThread: (threadId: ThreadId) => Option<ReadonlyArray<CrossProviderToolSpec>>; // None when not granted
  readonly call: (
    threadId: ThreadId,
    tool: string,
    args: unknown,
  ) => Effect<CrossProviderToolResult, never>;
}
```

with a module-level registry (`setCrossProviderAgentToolHost` / `readCrossProviderAgentToolHost`) mirroring `McpProviderSession.ts`. The service's Live layer registers itself at startup in `server.ts` (it sits above both `ProviderLayerLive` and `OrchestrationLayerLive`). `toolsForThread` returns Some when: access is enabled **and** the thread is a root thread on a supported driver, **or** the thread has `spawn.allowOrchestration === true`. Errors are returned as structured tool results, never thrown across the seam.

### 3.4 Tool contract

| Tool              | Input                                                                | Output                                                                                                                         |
| ----------------- | -------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------ |
| `agent_catalog`   | `{}`                                                                 | `{ routes: [{ providerInstanceId, displayName, driver, models: [{ slug, name }], isCallerInstance }], maxDepth, callerDepth }` |
| `agent_spawn`     | `{ providerInstanceId, model, prompt, title?, allowOrchestration? }` | `{ childId, taskId, providerInstanceId, model, status }`                                                                       |
| `agent_wait`      | `{ childIds: string[], timeoutSeconds? }`                            | `{ children: [{ childId, settled, state, output?, truncated?, totalChars?, usage? }] }`                                        |
| `agent_result`    | `{ childId, offset?, limit? }`                                       | `{ childId, state, output, truncated, totalChars, offset, length }`                                                            |
| `agent_follow_up` | `{ childId, prompt }`                                                | `{ childId, status }`                                                                                                          |
| `agent_interrupt` | `{ childId }`                                                        | `{ childId, status }`                                                                                                          |

Every error output is `{ error: { code, message, providerInstanceId?, model?, childId? } }`. Descriptions must say: use only for a _different_ provider instance; for the same provider use the native spawn tool; `childId` is durable across sessions.

### 3.5 Claude binding

In `ClaudeAdapter.startSession`, next to the MCP session lookup: `readCrossProviderAgentToolHost()?.toolsForThread(threadId)`. When Some, build `createSdkMcpServer({ name: "t3", tools: specs.map(spec => tool(spec.name, spec.description, zodShapeFrom(spec), args => host.call(threadId, spec.name, args)) })` and merge into `mcpServers.t3`. Tool handler returns `{ content: [{ type: "text", text: JSON.stringify(result) }] }`. Because `canUseTool` gates every tool, ensure `mcp__t3__*` calls are auto-allowed (they are T3-owned, already policy-checked server-side); do not prompt the user for them.

### 3.6 Codex binding

- On `thread/start`, include `dynamicTools: specs.map(spec => ({ type: "function", name, description, inputSchema }))` via the raw request path when the host grants tools for `options.threadId`.
- Register `client.handleServerRequest("item/tool/call", ...)`: map `params.threadId` (Codex id) → T3 thread; call `host.call`; respond `{ contentItems: [{ type: "inputText", text: JSON.stringify(result) }], success: !result.error }`. No Deferred needed because the host call itself is the awaited effect, but keep a bounded concurrency and the same `Effect.ensuring` cleanup style as the approval handlers.
- Reaper exemption: mark the session as tool-granted (`CodexSessionRuntimeOptions` flag or a small registry) and make `ProviderSessionReaper` skip those sessions. Keep the change to a predicate.
- Resume notice: in `openCodexThread` when the resume path is taken for a thread the host grants tools to, emit a provider runtime event that ingestion turns into a neutral `thread.activity-appended` (kind e.g. `provider.notice`, summary "Cross-provider agent tools are not available on a resumed Codex session; start a new thread to use them."). If no suitable existing activity kind exists, add one minimal kind rather than a UI-only synthetic.

### 3.7 Settings UI (`apps/web`)

New `SettingsSection id="agents" title="Agents"` above Browser in `IntegrationsSettings.tsx`:

1. **Cross-provider agent access** toggle (`serverScoped`, searchable, reset button). Description: "Allow agents to start child threads on other configured providers. This may consume those providers' paid quota."
2. **Max orchestration depth** numeric row (1–5), reset button.
3. **Child output cap** numeric row (characters), reset button.
4. **Routes** collapsed `Collapsible`: per eligible instance a switch and a model multi-select drawn from that instance's current catalog. Empty stored routes render the generated defaults. **Restore defaults** button with `AlertDialog` confirm ("Replace your route edits with the generated defaults?"). Generated defaults are computed on the server (`crossProviderAgentRoutes` empty ⇒ derive) so remote clients never compute policy locally.

Mobile: the toggle only, if provider administration exists there; otherwise nothing. State which applied.

## 4. Behaviours and edge cases

### 4.1 Nested ownership

Grandparent may `wait`/`result`/`follow_up`/`interrupt` a grandchild (ancestor rule). A sibling may not. A child never gets tools unless spawned with `allowOrchestration: true`, and even then only while `enableCrossProviderAgentAccess` is on at its next session start.

### 4.2 Exactly-once

Spawn: two command ids minted once per call, retried verbatim on transport error. A lost tool response cannot create a second child because the receipt replays. Follow-up and interrupt: one id each. Tests must prove a repeated dispatch with the same id yields one `thread.created` event.

### 4.3 Codex resume

Tools are granted at `thread/start` only. Reaper skips tool-granted Codex sessions. Server restart or explicit stop leads to resume without tools plus the visible notice. Claude sessions always re-inject on resume.

### 4.4 Parent failure after child settles

The child's settled state lives in its own thread events; `agent_wait` after reconnect backfills from projection. Nothing about the child is rewritten on parent failure.

### 4.5 Access revoked

`toolsForThread` returns None on next start/resume; `call` returns `access_disabled` immediately for stale sessions.

## 5. Delivery sequence and commits

One PR, six commits in this order. Each commit must leave typecheck and the touched tests green. Do not run repo-wide checks; use targeted `vp test run <files>` and targeted typecheck/lint per AGENTS.md.

1. `docs(internals): add cross-provider agent tools ADR, PRD, and build spec` — the three docs.
2. `test(provider): prove Claude in-process tools and Codex dynamic tools round-trip` — contract spike only. Claude: a `ClaudeAdapter.test.ts` case asserting `options.mcpServers.t3` carries an in-process server whose tool handler is invoked by the fake query. Codex: an integration test on the mock peer that sends `item/tool/call` and asserts the client's response, plus a raw-request test that `thread/start` params include `dynamicTools`. **Escalate immediately if either proof fails.**
3. `feat(orchestration): add CrossProviderAgentService with exact routing and ownership` — contracts (`spawn` metadata, settings fields, tool schemas), service, tool host seam, engine tests (receipts, activities, settlement, ownership, depth, cap rule, exactly-once).
4. `feat(codex): bind cross-provider agent tools through app-server dynamic tools` — injection, `item/tool/call` handler, reaper exemption, resume notice, tests for start, resume, interrupt, revocation.
5. `feat(claude): bind cross-provider agent tools through SDK in-process tools` — injection on start/resume, auto-allow, tests.
6. `feat(settings): add cross-provider agent access, depth, output cap, and route editor` — contracts already landed; server defaults generation; Integrations UI; restore defaults; settings tests.

Then open the PR against `agiontheway/t3code` base `design/native-cross-provider-tools-v3` with title `feat(agents): native cross-provider agent tools`. Body per AGENTS.md. End the body with the model and harness.

## 6. Verification the builder owns

- Fake-adapter tests for the full PRD test matrix, asserting `thread.created` with `spawn`, `task.*` activities on the parent, session-set settlement, and receipts.
- A live smoke of Fable → GPT-5.6 Luna on the dev server is **not** the builder's job; a separate QA dispatch runs the live matrix after the PR lands.
- Report at the end: what was verified, exact test files and commands run, anything skipped and why.

## 7. Escalation

The PRD's 18 escalation rules stand. On any of them: stop, write the escalation report (failing contract or invariant, smallest repro, source locations, two bounded options with trade-offs), and end the turn. Do not implement either option. Known-resolved items that are **not** escalations: the missing `dynamicTools` field in generated schemas (use raw request), the missing parent link (add the optional `spawn` field), the adapter→orchestration cycle (use the tool host seam), the Codex resume gap (keep-alive plus notice).
