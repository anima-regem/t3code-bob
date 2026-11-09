import type { BobSettings } from "@t3tools/contracts";
import type { ProviderUsageReader, ProviderUsageScan } from "@t3tools/provider-core/server/usage";
import { expandHomePath } from "@t3tools/provider-core/server/pathExpansion";
import * as HostProcess from "@t3tools/shared/HostProcess";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Path from "effect/Path";

import { readBobDatabaseRecords } from "../../usage/usageBobDatabase.ts";

export const bobUsageReader: ProviderUsageReader<BobSettings, FileSystem.FileSystem | Path.Path> = {
  kind: "scan",
  provider: "bob",
  scan: Effect.fn("BobUsage.scan")(function* ({ instances, windowStartMs }) {
    const fs = yield* FileSystem.FileSystem;
    const path = yield* Path.Path;
    const hostHome = yield* HostProcess.HomeDirectory;
    const platform = yield* HostProcess.Platform;
    const seen = new Set<string>();
    const scans: ProviderUsageScan[] = [];
    for (const instance of instances) {
      const home =
        (platform === "win32" ? instance.environment.USERPROFILE : instance.environment.HOME) ||
        hostHome;
      const bobHome = expandHomePath(
        instance.config?.homePath.trim() ||
          instance.environment.BOB_HOME?.trim() ||
          path.join(home, ".bob"),
        home,
      );
      const file = path.join(bobHome, "db", "bob.db");
      const resolved = yield* fs.realPath(file).pipe(Effect.orElseSucceed(() => file));
      if (seen.has(resolved)) continue;
      seen.add(resolved);
      const exists = yield* fs.exists(resolved).pipe(Effect.orElseSucceed(() => false));
      const records = exists
        ? yield* Effect.sync(() =>
            readBobDatabaseRecords(resolved, windowStartMs, Number.MAX_SAFE_INTEGER),
          )
        : [];
      scans.push({
        dir: resolved,
        files: exists ? [{ path: resolved, records }] : null,
        ...(exists ? {} : { message: "No Bob database on this environment." }),
      });
    }
    return scans;
  }),
};
