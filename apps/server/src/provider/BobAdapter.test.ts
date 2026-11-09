// @effect-diagnostics nodeBuiltinImport:off -- The fake ACP process records its wire requests.
import * as NodeFS from "node:fs";
import * as NodePath from "node:path";
import * as NodeCrypto from "@effect/platform-node/NodeCrypto";
import * as NodeServices from "@effect/platform-node/NodeServices";
import { expect, it } from "@effect/vitest";
import { BobSettings, ProviderInstanceId, ProviderSessionId, ThreadId } from "@t3tools/contracts";
import * as IdAllocator from "@t3tools/provider-core/server/IdAllocator";
import * as McpProviderSessions from "@t3tools/provider-core/server/McpProviderSessions";
import * as ProviderAdapter from "@t3tools/provider-core/server/ProviderAdapter";
import * as ProviderEventLoggers from "@t3tools/provider-core/server/ProviderEventLoggers";
import * as TestProviderHost from "@t3tools/provider-testing/TestProviderHost";
import { execScriptSource, writeFakeCli } from "@t3tools/provider-testing/fakeCli";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import { makeBobAdapterV2 } from "./BobAdapter.ts";

const decodeBobSettings = Schema.decodeSync(BobSettings);

const layer = TestProviderHost.layer({ runBackgroundWork: false }).pipe(
  Layer.provideMerge(Layer.mergeAll(NodeServices.layer, NodeCrypto.layer)),
  Layer.provideMerge(IdAllocator.layer),
  Layer.provideMerge(McpProviderSessions.layer),
  Layer.provideMerge(
    Layer.succeed(
      ProviderEventLoggers.ProviderEventLoggers,
      ProviderEventLoggers.NoOpProviderEventLoggers,
    ),
  ),
);

it.live("opens a supervised ACP session without overriding Bob's default model", () =>
  Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    const cwd = yield* fs.makeTempDirectoryScoped({ prefix: "t3-bob-adapter-" });
    const requestsPath = NodePath.join(cwd, "requests.ndjson");
    const binaryPath = writeFakeCli({
      directory: NodePath.join(cwd, "bin"),
      name: "bob",
      env: { T3_ACP_REQUEST_LOG_PATH: requestsPath },
      source: execScriptSource({
        scriptPath: NodePath.resolve(import.meta.dirname, "../../scripts/acp-mock-agent.ts"),
        expectedArgs: ["acp", "--trust"],
      }),
    });
    const instanceId = ProviderInstanceId.make("bob-test");
    const adapter = yield* makeBobAdapterV2({
      instanceId,
      settings: decodeBobSettings({ binaryPath }),
      environment: process.env,
    });
    const threadId = ThreadId.make("bob-thread");
    const modelSelection = { instanceId, model: "bob-build" };
    const runtimePolicy = ProviderAdapter.ProviderAdapterV2RuntimePolicy.make({
      runtimeMode: "approval-required",
      interactionMode: "default",
      cwd,
    });
    const session = yield* adapter.openSession({
      threadId,
      providerSessionId: ProviderSessionId.make("bob-session"),
      modelSelection,
      runtimePolicy,
    });
    yield* session.ensureThread({ threadId, modelSelection, runtimePolicy });
    const requests = NodeFS.readFileSync(requestsPath, "utf8")
      .trim()
      .split("\n")
      .map((line) => JSON.parse(line) as { method: string });
    expect(requests.some((request) => request.method === "initialize")).toBe(true);
    expect(requests.some((request) => request.method === "session/new")).toBe(true);
    expect(requests.some((request) => request.method === "session/set_model")).toBe(false);
  }).pipe(Effect.provide(layer), Effect.scoped),
);
