import * as ProviderHost from "@t3tools/provider-core/server/ProviderHost";
import * as IdAllocator from "@t3tools/provider-core/server/IdAllocator";
import * as McpProviderSessions from "@t3tools/provider-core/server/McpProviderSessions";
import { bobUsageReader } from "./BobUsage.ts";
import { BobSettings, ProviderDriverKind } from "@t3tools/contracts";
import * as SecureRandom from "effect/crypto/SecureRandom";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Path from "effect/Path";
import * as Schema from "effect/Schema";
import { HttpClient } from "effect/http";
import { ChildProcessSpawner } from "effect/process";

import { makeBobTextGeneration } from "../../textGeneration/BobTextGeneration.ts";
import { ProviderDriverError } from "../Errors.ts";
import { makeBobAdapterV2 } from "../BobAdapter.ts";
import { buildInitialBobProviderSnapshot, checkBobProviderStatus } from "../BobProvider.ts";
import { discoverBobSkills } from "./BobSkills.ts";
import { ProviderEventLoggers } from "@t3tools/provider-core/server/ProviderEventLoggers";
import { makeManagedServerProvider } from "@t3tools/provider-core/server/managedProvider";
import {
  defaultProviderContinuationIdentity,
  type ProviderDriver,
  type ProviderInstance,
} from "@t3tools/provider-core/server/driver";
import { withInstanceIdentity } from "@t3tools/provider-core/server/instanceIdentity";
import { mergeProviderInstanceEnvironment } from "@t3tools/provider-core/server/instanceEnvironment";
import { makeManualOnlyProviderMaintenanceCapabilities } from "@t3tools/provider-core/server/maintenanceResolver";
import {
  haveProviderSnapshotSettingsChanged,
  makeProviderSnapshotSettingsSource,
  type ProviderSnapshotSettings,
} from "@t3tools/provider-core/server/snapshotSettings";

const decodeBobSettings = Schema.decodeSync(BobSettings);

const DRIVER_KIND = ProviderDriverKind.make("bob");
const MAINTENANCE_CAPABILITIES = makeManualOnlyProviderMaintenanceCapabilities({
  provider: DRIVER_KIND,
  packageName: null,
});

export type BobDriverEnv =
  | ChildProcessSpawner.ChildProcessSpawner
  | SecureRandom.SecureRandom
  | FileSystem.FileSystem
  | HttpClient.HttpClient
  | Path.Path
  | ProviderEventLoggers
  | ProviderHost.ProviderHost
  | IdAllocator.IdAllocatorV2
  | McpProviderSessions.McpProviderSessions;

export const BobDriver: ProviderDriver<
  BobSettings,
  BobDriverEnv,
  FileSystem.FileSystem | Path.Path
> = {
  driverKind: DRIVER_KIND,
  metadata: {
    displayName: "Bob",
    supportsMultipleInstances: true,
  },
  usage: bobUsageReader,
  configSchema: BobSettings,
  defaultConfig: (): BobSettings => decodeBobSettings({}),
  create: Effect.fn("BobDriver.create")(function* ({
    instanceId,
    displayName,
    accentColor,
    environment,
    enabled,
    config,
  }) {
    const random = yield* SecureRandom.SecureRandom;
    const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;
    const httpClient = yield* HttpClient.HttpClient;
    const fileSystem = yield* FileSystem.FileSystem;
    const path = yield* Path.Path;
    const processEnv = {
      ...(yield* mergeProviderInstanceEnvironment(environment)),
      ...(config.apiKey.trim() ? { BOBSHELL_API_KEY: config.apiKey.trim() } : {}),
    };
    const continuationIdentity = defaultProviderContinuationIdentity({
      driverKind: DRIVER_KIND,
      instanceId,
    });
    const stampIdentity = withInstanceIdentity({
      instanceId,
      driverKind: DRIVER_KIND,
      displayName,
      accentColor,
      continuationGroupKey: continuationIdentity.continuationKey,
    });
    const effectiveConfig = { ...config, enabled } satisfies BobSettings;

    const orchestrationAdapter = yield* makeBobAdapterV2({
      settings: effectiveConfig,
      environment: processEnv,
      instanceId,
    }).pipe(
      Effect.mapError(
        (cause) =>
          new ProviderDriverError({
            driver: DRIVER_KIND,
            instanceId,
            detail: "Failed to build Bob ACP adapter.",
            cause,
          }),
      ),
    );
    const textGeneration = yield* makeBobTextGeneration(effectiveConfig, processEnv);

    const checkProvider = checkBobProviderStatus(effectiveConfig, processEnv).pipe(
      Effect.map(stampIdentity),
      Effect.provideService(HttpClient.HttpClient, httpClient),
      Effect.provideService(FileSystem.FileSystem, fileSystem),
      Effect.provideService(Path.Path, path),
      Effect.provideService(SecureRandom.SecureRandom, random),
      Effect.provideService(ChildProcessSpawner.ChildProcessSpawner, spawner),
    );

    const snapshotSettings = yield* makeProviderSnapshotSettingsSource(effectiveConfig);
    const snapshot = yield* makeManagedServerProvider<ProviderSnapshotSettings<BobSettings>>({
      resolveMaintenance: () => Effect.succeed(MAINTENANCE_CAPABILITIES),
      getSettings: snapshotSettings.getSettings,
      streamSettings: snapshotSettings.streamSettings,
      haveSettingsChanged: haveProviderSnapshotSettingsChanged,
      initialSnapshot: (settings) =>
        buildInitialBobProviderSnapshot(settings.provider).pipe(Effect.map(stampIdentity)),
      checkProvider,
      enrichSnapshot: ({ snapshot: currentSnapshot, publishSnapshot }) =>
        publishSnapshot(currentSnapshot),
    }).pipe(
      Effect.mapError(
        (cause) =>
          new ProviderDriverError({
            driver: DRIVER_KIND,
            instanceId,
            detail: `Failed to build Bob snapshot: ${cause.message ?? String(cause)}`,
            cause,
          }),
      ),
    );

    const snapshotForCwd = (workspaceCwd: string) =>
      !effectiveConfig.enabled
        ? snapshot.getSnapshot
        : Effect.all([
            snapshot.getSnapshot,
            discoverBobSkills(effectiveConfig, workspaceCwd, processEnv).pipe(
              Effect.provideService(FileSystem.FileSystem, fileSystem),
              Effect.provideService(Path.Path, path),
              Effect.mapError(
                (cause) =>
                  new ProviderDriverError({
                    driver: DRIVER_KIND,
                    instanceId,
                    detail: `Failed to discover Bob skills for '${workspaceCwd}'`,
                    cause,
                  }),
              ),
            ),
          ]).pipe(Effect.map(([machineSnapshot, skills]) => ({ ...machineSnapshot, skills })));

    return {
      instanceId,
      driverKind: DRIVER_KIND,
      continuationIdentity,
      displayName,
      accentColor,
      enabled,
      snapshot,
      snapshotForCwd,
      orchestrationAdapter,
      textGeneration,
    } satisfies ProviderInstance;
  }),
};
