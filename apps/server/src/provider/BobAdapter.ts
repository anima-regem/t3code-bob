import type { BobSettings, ProviderInstanceId } from "@t3tools/contracts";
import { ProviderDriverKind } from "@t3tools/contracts";
import { AcpProviderCapabilitiesV2, makeAcpAdapterV2 } from "@t3tools/provider-acp/server/adapter";
import { makeAcpNativeLoggerFactory } from "@t3tools/provider-acp/server/nativeLogging";
import * as ProviderEventLoggers from "@t3tools/provider-core/server/ProviderEventLoggers";
import * as ProviderContinuationRequests from "@t3tools/provider-core/server/ProviderContinuationRequests";
import { resolveSelfInvocation } from "@t3tools/shared/nodeRuntime";
import * as Effect from "effect/Effect";

import {
  BOB_DEFAULT_MODEL_SLUG,
  makeBobAcpRuntime,
  resolveBobAcpBaseModelId,
} from "./acp/BobAcpSupport.ts";

export const makeBobAdapterV2 = Effect.fn("makeBobAdapterV2")(function* (options: {
  readonly instanceId: ProviderInstanceId;
  readonly settings: BobSettings;
  readonly environment: NodeJS.ProcessEnv;
}) {
  const loggers = yield* ProviderEventLoggers.ProviderEventLoggers;
  const continuationRequests = yield* ProviderContinuationRequests.ProviderContinuationRequests;
  const makeNativeLogger = yield* makeAcpNativeLoggerFactory();
  const driver = ProviderDriverKind.make("bob");
  return yield* makeAcpAdapterV2({
    instanceId: options.instanceId,
    selfInvocation: yield* resolveSelfInvocation(),
    continuationRequests,
    nativeLogging: (threadId) =>
      makeNativeLogger({ nativeEventLogger: loggers.native, provider: driver, threadId }),
    flavor: {
      driver,
      runtimeHarness: "Bob",
      capabilities: AcpProviderCapabilitiesV2,
      supportsCompaction: true,
      resolveModelId: (selection) => resolveBobAcpBaseModelId(selection.model),
      applyModelSelection: Effect.fn("BobAdapter.applyModelSelection")(function* ({
        runtime,
        startResult,
        modelSelection,
      }) {
        const current = startResult.sessionSetupResult.models?.currentModelId;
        const requested = resolveBobAcpBaseModelId(modelSelection.model);
        // The built-in slug keeps Bob's current session model.
        if (requested === BOB_DEFAULT_MODEL_SLUG || requested === current) return current;
        yield* runtime.setSessionModel(requested);
        return requested;
      }),
      makeRuntime: ({ runtimePolicy, ...input }) =>
        makeBobAcpRuntime({
          ...input,
          bobSettings: options.settings,
          environment: options.environment,
          runtimeMode:
            runtimePolicy.approvalPolicy === undefined && runtimePolicy.sandboxPolicy === undefined
              ? runtimePolicy.runtimeMode
              : "approval-required",
        }),
    },
  });
});
