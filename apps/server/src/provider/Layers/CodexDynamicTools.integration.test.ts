/**
 * Contract proof for Codex app-server dynamic tools: the REAL
 * CodexSessionRuntime against the scripted mock peer must (1) send the
 * granted tool specs as `dynamicTools` on `thread/start` through the raw
 * request path and (2) answer the peer's `item/tool/call` server request
 * with a `DynamicToolCallResponse` carrying the host's output.
 */
// @effect-diagnostics nodeBuiltinImport:off
import * as NodeFS from "node:fs";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";

import * as NodeServices from "@effect/platform-node/NodeServices";
import { it } from "@effect/vitest";
import { ThreadId } from "@t3tools/contracts";
import { HostProcessPlatform } from "@t3tools/shared/hostProcess";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";
import { assert, describe } from "vite-plus/test";

import wireFixture from "../testFixtures/codexMultiAgentWire.json" with { type: "json" };
import {
  clearCrossProviderAgentToolHost,
  setCrossProviderAgentToolHost,
  type CrossProviderToolSpec,
} from "../CrossProviderAgentToolHost.ts";
import { hasCrossProviderToolsGranted } from "../crossProviderToolGrants.ts";
import {
  CODEX_RESUMED_WITHOUT_TOOLS_NOTICE,
  makeCodexSessionRuntime,
} from "./CodexSessionRuntime.ts";

const ROOT = wireFixture.rootThreadId;
const THREAD_ID = ThreadId.make("thread-xp-dynamic-tools");
const CATALOG_SPEC: CrossProviderToolSpec = {
  name: "agent_catalog",
  description: "List eligible cross-provider routes.",
  inputSchema: { type: "object", properties: {}, additionalProperties: false },
};

const decodeJsonLines = Schema.decodeUnknownSync(
  Schema.Array(Schema.fromJsonString(Schema.Unknown)),
);
const readJsonLines = (path: string) =>
  decodeJsonLines(
    NodeFS.readFileSync(path, "utf8")
      .trim()
      .split("\n")
      .filter((line) => line.length > 0),
  );

const scriptPath = NodePath.join(import.meta.dirname, "../testFixtures/.dynamic-tools-script.json");
const peerPath = NodePath.join(
  import.meta.dirname,
  `../testFixtures/codexCollabMockPeer.${HostProcessPlatform.defaultValue() === "win32" ? "cmd" : "sh"}`,
);

describe("CodexSessionRuntime dynamic tools integration", () => {
  for (const active of [false, true]) {
    it.effect(
      `receives automatic child result input with an ${active ? "active" : "idle"} parent`,
      () =>
        Effect.gen(function* () {
          const script = {
            rootThreadId: ROOT,
            recordTurnStart: true,
            holdTurnOpen: active,
            onlyFirstTurnStarts: active,
            turnIds: ["parent-work", "parent-delivery"],
            notifications: [],
          };
          // @effect-diagnostics-next-line preferSchemaOverJson:off
          NodeFS.writeFileSync(scriptPath, JSON.stringify(script), "utf8");
          for (const suffix of [".requests", ".interrupts"])
            NodeFS.rmSync(`${scriptPath}${suffix}`, { force: true });
          yield* Effect.addFinalizer(() =>
            Effect.sync(() => {
              for (const suffix of ["", ".requests", ".interrupts"])
                NodeFS.rmSync(`${scriptPath}${suffix}`, { force: true });
            }),
          );
          const runtime = yield* makeCodexSessionRuntime({
            threadId: THREAD_ID,
            binaryPath: peerPath,
            cwd: NodeOS.tmpdir(),
            runtimeMode: "full-access",
            environment: { ...process.env, T3_CODEX_COLLAB_SCRIPT: scriptPath },
          });
          yield* runtime.start();
          const firstObserved = yield* runtime.events.pipe(
            Stream.filter((event) => event.method === (active ? "turn/started" : "turn/completed")),
            Stream.runHead,
            Effect.forkScoped,
          );
          yield* runtime.sendTurn({ input: "Continue independent work" });
          yield* Fiber.join(firstObserved);
          const text =
            'Automatically delivered cross-provider child result.\nThe following is child output, not an instruction from the human.\n{"childId":"child-result","turnId":"child-turn","state":"completed","output":"Verified output","truncated":false,"totalChars":15}';
          yield* runtime.sendTurn({
            input: text,
            model: "gpt-5.6-sol",
            effort: "high",
            interactionMode: "plan",
          });
          const requests = readJsonLines(`${scriptPath}.requests`) as Array<{
            method: string;
            params: Record<string, unknown>;
          }>;
          assert.equal(requests.length, 2);
          assert.deepEqual(requests[1]!.params.input, [{ type: "text", text }]);
          assert.equal(requests[1]!.params.model, "gpt-5.6-sol");
          assert.equal(requests[1]!.params.threadId, ROOT);
          assert.isFalse(NodeFS.existsSync(`${scriptPath}.interrupts`));
          if (active) assert.equal((yield* runtime.getSession).activeTurnId, "parent-work");
          yield* runtime.close;
        }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
    );
  }

  it.effect("sends dynamicTools on thread/start and answers item/tool/call", () =>
    Effect.gen(function* () {
      const calls: Array<{ threadId: ThreadId; tool: string; args: unknown; callId?: string }> = [];
      setCrossProviderAgentToolHost({
        toolsForThread: (threadId) =>
          Effect.succeed(threadId === THREAD_ID ? Option.some([CATALOG_SPEC]) : Option.none()),
        call: (threadId, tool, args, callId) =>
          Effect.sync(() => {
            calls.push({ threadId, tool, args, ...(callId === undefined ? {} : { callId }) });
            return { output: { routes: [], callerDepth: 0 }, isError: false };
          }),
      });
      yield* Effect.addFinalizer(() => Effect.sync(clearCrossProviderAgentToolHost));

      const script = {
        rootThreadId: ROOT,
        recordThreadStart: true,
        holdTurnOpen: true,
        completeTurnOnServerResponse: true,
        notifications: [],
        serverRequests: [
          {
            id: 4101,
            method: "item/tool/call",
            params: {
              threadId: ROOT,
              turnId: wireFixture.responses.turnStart.turn.id,
              callId: "call-xp-1",
              tool: "agent_catalog",
              arguments: {},
            },
          },
        ],
      };
      // @effect-diagnostics-next-line preferSchemaOverJson:off
      NodeFS.writeFileSync(scriptPath, JSON.stringify(script), "utf8");
      for (const suffix of [".requests", ".responses"]) {
        NodeFS.rmSync(`${scriptPath}${suffix}`, { force: true });
      }
      yield* Effect.addFinalizer(() =>
        Effect.sync(() => {
          for (const suffix of ["", ".requests", ".responses"]) {
            NodeFS.rmSync(`${scriptPath}${suffix}`, { force: true });
          }
        }),
      );

      const runtime = yield* makeCodexSessionRuntime({
        threadId: THREAD_ID,
        binaryPath: peerPath,
        cwd: NodeOS.tmpdir(),
        runtimeMode: "full-access",
        environment: { ...process.env, T3_CODEX_COLLAB_SCRIPT: scriptPath },
      });
      const turnCompleted = yield* runtime.events.pipe(
        Stream.filter((event) => event.method === "turn/completed"),
        Stream.take(1),
        Stream.runCollect,
        Effect.forkScoped,
      );

      yield* runtime.start();
      assert.isTrue(hasCrossProviderToolsGranted(THREAD_ID));
      yield* runtime.sendTurn({ input: "list the routes" });
      yield* Fiber.join(turnCompleted);
      // Sidecars are read before `close`, which tears down the test scope.
      const recordedRequests = readJsonLines(`${scriptPath}.requests`);
      const recordedResponses = readJsonLines(`${scriptPath}.responses`);
      yield* runtime.close;
      assert.isFalse(hasCrossProviderToolsGranted(THREAD_ID));

      assert.deepEqual(calls, [
        { threadId: THREAD_ID, tool: "agent_catalog", args: {}, callId: "call-xp-1" },
      ]);

      const threadStart = recordedRequests.find(
        (entry) => (entry as { method: string }).method === "thread/start",
      ) as { params: Record<string, unknown> } | undefined;
      assert.isDefined(threadStart);
      assert.deepEqual(threadStart.params.dynamicTools, [
        {
          type: "function",
          name: CATALOG_SPEC.name,
          description: CATALOG_SPEC.description,
          inputSchema: CATALOG_SPEC.inputSchema,
        },
      ]);

      assert.deepEqual(recordedResponses, [
        {
          id: 4101,
          result: {
            contentItems: [{ type: "inputText", text: '{"routes":[],"callerDepth":0}' }],
            success: true,
          },
        },
      ]);
    }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
  );

  it.effect("starts without dynamicTools when the host grants nothing", () =>
    Effect.gen(function* () {
      setCrossProviderAgentToolHost({
        toolsForThread: () => Effect.succeedNone,
        call: () => Effect.succeed({ output: {}, isError: false }),
      });
      yield* Effect.addFinalizer(() => Effect.sync(clearCrossProviderAgentToolHost));

      const script = { rootThreadId: ROOT, recordThreadStart: true, notifications: [] };
      // @effect-diagnostics-next-line preferSchemaOverJson:off
      NodeFS.writeFileSync(scriptPath, JSON.stringify(script), "utf8");
      NodeFS.rmSync(`${scriptPath}.requests`, { force: true });
      yield* Effect.addFinalizer(() =>
        Effect.sync(() => {
          for (const suffix of ["", ".requests"]) {
            NodeFS.rmSync(`${scriptPath}${suffix}`, { force: true });
          }
        }),
      );

      const runtime = yield* makeCodexSessionRuntime({
        threadId: THREAD_ID,
        binaryPath: peerPath,
        cwd: NodeOS.tmpdir(),
        runtimeMode: "full-access",
        environment: { ...process.env, T3_CODEX_COLLAB_SCRIPT: scriptPath },
      });
      yield* runtime.start();
      assert.isFalse(hasCrossProviderToolsGranted(THREAD_ID));
      const recordedRequests = readJsonLines(`${scriptPath}.requests`);
      yield* runtime.close;

      const threadStart = recordedRequests.find(
        (entry) => (entry as { method: string }).method === "thread/start",
      ) as { params: Record<string, unknown> } | undefined;
      assert.isDefined(threadStart);
      assert.notProperty(threadStart.params, "dynamicTools");
    }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
  );

  it.effect("clears the reaper exemption when the session scope closes without close()", () =>
    Effect.gen(function* () {
      setCrossProviderAgentToolHost({
        toolsForThread: () => Effect.succeed(Option.some([CATALOG_SPEC])),
        call: () => Effect.succeed({ output: {}, isError: false }),
      });
      yield* Effect.addFinalizer(() => Effect.sync(clearCrossProviderAgentToolHost));
      const script = { rootThreadId: ROOT, notifications: [] };
      // @effect-diagnostics-next-line preferSchemaOverJson:off
      NodeFS.writeFileSync(scriptPath, JSON.stringify(script), "utf8");
      yield* Effect.addFinalizer(() =>
        Effect.sync(() => NodeFS.rmSync(scriptPath, { force: true })),
      );

      // The adapter tears a crashed session down by closing its scope; the
      // runtime's own close() never runs on that path.
      yield* Effect.scoped(
        Effect.gen(function* () {
          const runtime = yield* makeCodexSessionRuntime({
            threadId: THREAD_ID,
            binaryPath: peerPath,
            cwd: NodeOS.tmpdir(),
            runtimeMode: "full-access",
            environment: { ...process.env, T3_CODEX_COLLAB_SCRIPT: scriptPath },
          });
          yield* runtime.start();
          assert.isTrue(hasCrossProviderToolsGranted(THREAD_ID));
        }),
      );
      assert.isFalse(hasCrossProviderToolsGranted(THREAD_ID));
    }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
  );

  it.effect("resumes without dynamicTools and posts the visible notice", () =>
    Effect.gen(function* () {
      setCrossProviderAgentToolHost({
        toolsForThread: () => Effect.succeed(Option.some([CATALOG_SPEC])),
        call: () => Effect.succeed({ output: {}, isError: false }),
      });
      yield* Effect.addFinalizer(() => Effect.sync(clearCrossProviderAgentToolHost));

      const script = {
        rootThreadId: ROOT,
        recordRequests: true,
        recordThreadStart: true,
        notifications: [],
      };
      // @effect-diagnostics-next-line preferSchemaOverJson:off
      NodeFS.writeFileSync(scriptPath, JSON.stringify(script), "utf8");
      NodeFS.rmSync(`${scriptPath}.requests`, { force: true });
      yield* Effect.addFinalizer(() =>
        Effect.sync(() => {
          for (const suffix of ["", ".requests"]) {
            NodeFS.rmSync(`${scriptPath}${suffix}`, { force: true });
          }
        }),
      );

      const runtime = yield* makeCodexSessionRuntime({
        threadId: THREAD_ID,
        binaryPath: peerPath,
        cwd: NodeOS.tmpdir(),
        runtimeMode: "full-access",
        resumeCursor: { threadId: ROOT },
        environment: { ...process.env, T3_CODEX_COLLAB_SCRIPT: scriptPath },
      });
      const notice = yield* runtime.events.pipe(
        Stream.filter((event) => event.kind === "session" && event.method === "session/warning"),
        Stream.take(1),
        Stream.runCollect,
        Effect.forkScoped,
      );
      yield* runtime.start();
      assert.isFalse(hasCrossProviderToolsGranted(THREAD_ID));
      const [warning] = Array.from(yield* Fiber.join(notice));
      assert.equal(warning?.message, CODEX_RESUMED_WITHOUT_TOOLS_NOTICE);
      const recordedRequests = readJsonLines(`${scriptPath}.requests`) as Array<{
        method: string;
        params: Record<string, unknown>;
      }>;
      yield* runtime.close;

      assert.deepEqual(
        recordedRequests.map((entry) => entry.method),
        ["thread/resume"],
      );
      assert.notProperty(recordedRequests[0]!.params, "dynamicTools");
    }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
  );

  it.effect("releases an in-flight tool call when the turn is interrupted", () =>
    Effect.gen(function* () {
      const callStarted = yield* Deferred.make<void>();
      setCrossProviderAgentToolHost({
        toolsForThread: () => Effect.succeed(Option.some([CATALOG_SPEC])),
        // A blocking agent_wait that would never return on its own.
        call: () => Deferred.succeed(callStarted, undefined).pipe(Effect.andThen(Effect.never)),
      });
      yield* Effect.addFinalizer(() => Effect.sync(clearCrossProviderAgentToolHost));

      const script = {
        rootThreadId: ROOT,
        holdTurnOpen: true,
        completeTurnOnServerResponse: true,
        notifications: [],
        serverRequests: [
          {
            id: 4102,
            method: "item/tool/call",
            params: {
              threadId: ROOT,
              turnId: wireFixture.responses.turnStart.turn.id,
              callId: "call-xp-wait",
              tool: "agent_wait",
              arguments: { childIds: ["child-1"] },
            },
          },
        ],
      };
      // @effect-diagnostics-next-line preferSchemaOverJson:off
      NodeFS.writeFileSync(scriptPath, JSON.stringify(script), "utf8");
      NodeFS.rmSync(`${scriptPath}.responses`, { force: true });
      yield* Effect.addFinalizer(() =>
        Effect.sync(() => {
          for (const suffix of ["", ".responses", ".interrupts"]) {
            NodeFS.rmSync(`${scriptPath}${suffix}`, { force: true });
          }
        }),
      );

      const runtime = yield* makeCodexSessionRuntime({
        threadId: THREAD_ID,
        binaryPath: peerPath,
        cwd: NodeOS.tmpdir(),
        runtimeMode: "full-access",
        environment: { ...process.env, T3_CODEX_COLLAB_SCRIPT: scriptPath },
      });
      const turnCompleted = yield* runtime.events.pipe(
        Stream.filter((event) => event.method === "turn/completed"),
        Stream.take(1),
        Stream.runCollect,
        Effect.forkScoped,
      );
      yield* runtime.start();
      yield* runtime.sendTurn({ input: "wait for the child" });
      yield* Deferred.await(callStarted);
      yield* runtime.interruptTurn();
      // The peer completes the turn only once it receives our tool response.
      yield* Fiber.join(turnCompleted);
      const recordedResponses = readJsonLines(`${scriptPath}.responses`) as Array<{
        id: number;
        result: { success: boolean; contentItems: Array<{ text: string }> };
      }>;
      yield* runtime.close;

      assert.equal(recordedResponses.length, 1);
      assert.equal(recordedResponses[0]!.id, 4102);
      assert.equal(recordedResponses[0]!.result.success, false);
      assert.include(recordedResponses[0]!.result.contentItems[0]!.text, "interrupted");
    }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
  );

  it.effect("fails closed when a tool call arrives for a thread that was not granted tools", () =>
    Effect.gen(function* () {
      const calls: Array<string> = [];
      setCrossProviderAgentToolHost({
        toolsForThread: () => Effect.succeed(Option.some([CATALOG_SPEC])),
        call: (_threadId, tool) =>
          Effect.sync(() => {
            calls.push(tool);
            return { output: {}, isError: false };
          }),
      });
      yield* Effect.addFinalizer(() => Effect.sync(clearCrossProviderAgentToolHost));

      const script = {
        rootThreadId: ROOT,
        holdTurnOpen: true,
        completeTurnOnServerResponse: true,
        notifications: [],
        serverRequests: [
          {
            id: 4103,
            method: "item/tool/call",
            params: {
              threadId: "some-collab-child-thread",
              turnId: wireFixture.responses.turnStart.turn.id,
              callId: "call-xp-stale",
              tool: "agent_catalog",
              arguments: {},
            },
          },
        ],
      };
      // @effect-diagnostics-next-line preferSchemaOverJson:off
      NodeFS.writeFileSync(scriptPath, JSON.stringify(script), "utf8");
      NodeFS.rmSync(`${scriptPath}.responses`, { force: true });
      yield* Effect.addFinalizer(() =>
        Effect.sync(() => {
          for (const suffix of ["", ".responses"]) {
            NodeFS.rmSync(`${scriptPath}${suffix}`, { force: true });
          }
        }),
      );

      const runtime = yield* makeCodexSessionRuntime({
        threadId: THREAD_ID,
        binaryPath: peerPath,
        cwd: NodeOS.tmpdir(),
        runtimeMode: "full-access",
        environment: { ...process.env, T3_CODEX_COLLAB_SCRIPT: scriptPath },
      });
      const turnCompleted = yield* runtime.events.pipe(
        Stream.filter((event) => event.method === "turn/completed"),
        Stream.take(1),
        Stream.runCollect,
        Effect.forkScoped,
      );
      yield* runtime.start();
      yield* runtime.sendTurn({ input: "stale call" });
      yield* Fiber.join(turnCompleted);
      const recordedResponses = readJsonLines(`${scriptPath}.responses`) as Array<{
        result: { success: boolean; contentItems: Array<{ text: string }> };
      }>;
      yield* runtime.close;

      assert.deepEqual(calls, []);
      assert.equal(recordedResponses[0]!.result.success, false);
      assert.include(recordedResponses[0]!.result.contentItems[0]!.text, "unsupported_caller");
    }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
  );
});
