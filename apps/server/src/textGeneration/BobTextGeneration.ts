import * as SecureRandom from "effect/crypto/SecureRandom";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as Ref from "effect/Ref";
import * as Schema from "effect/Schema";
import * as ChildProcessSpawner from "effect/process/ChildProcessSpawner";
import type * as EffectAcpErrors from "effect-acp/errors";

import { TextGenerationError } from "@t3tools/contracts";
import type { BobSettings } from "@t3tools/contracts";

import * as TextGenerationOperations from "@t3tools/provider-core/server/textGenerationOperations";
import { makeBobAcpRuntime } from "../provider/acp/BobAcpSupport.ts";

const BOB_TIMEOUT_MS = 180_000;

const isTextGenerationError = Schema.is(TextGenerationError);

export const makeBobTextGeneration = Effect.fn("makeBobTextGeneration")(function* (
  bobSettings: BobSettings,
  environment: NodeJS.ProcessEnv = process.env,
) {
  const random = yield* SecureRandom.SecureRandom;
  const commandSpawner = yield* ChildProcessSpawner.ChildProcessSpawner;

  const runBobJson: TextGenerationOperations.Runner = Effect.fn("BobTextGeneration.runBobJson")(
    function* (request) {
      const { operation, cwd, prompt } = request;

      const outputRef = yield* Ref.make("");
      const runtime = yield* makeBobAcpRuntime({
        bobSettings,
        environment,
        cwd,
        clientInfo: { name: "t3-code-git-text", version: "0.0.0" },
      }).pipe(
        Effect.provideService(SecureRandom.SecureRandom, random),
        Effect.provideService(ChildProcessSpawner.ChildProcessSpawner, commandSpawner),
      );

      yield* runtime.handleSessionUpdate((notification) => {
        const update = notification.update;
        if (update.sessionUpdate !== "agent_message_chunk") {
          return Effect.void;
        }
        const content = update.content;
        if (content.type !== "text") {
          return Effect.void;
        }
        return Ref.update(outputRef, (current) => current + content.text);
      });

      const promptResult = yield* Effect.gen(function* () {
        yield* runtime.start();
        return yield* runtime.prompt({
          prompt: [{ type: "text", text: prompt }],
        });
      }).pipe(
        Effect.timeoutOption(BOB_TIMEOUT_MS),
        Effect.flatMap(
          Option.match({
            onNone: () =>
              Effect.fail(
                new TextGenerationError({ operation, detail: "Bob ACP request timed out." }),
              ),
            onSome: (value) => Effect.succeed(value),
          }),
        ),
        Effect.mapError((cause: EffectAcpErrors.AcpError | TextGenerationError) =>
          isTextGenerationError(cause)
            ? cause
            : new TextGenerationError({
                operation,
                detail: "Bob ACP request failed.",
                cause,
              }),
        ),
      );

      const trimmed = (yield* Ref.get(outputRef)).trim();
      if (!trimmed) {
        return yield* new TextGenerationError({
          operation,
          detail:
            promptResult.stopReason === "cancelled"
              ? "Bob ACP request was cancelled."
              : "Bob Agent returned empty output.",
        });
      }

      return yield* TextGenerationOperations.decodeJsonReply(request, "Bob Agent", trimmed);
    },
    (effect, request) => {
      const { operation } = request;
      return effect.pipe(
        Effect.mapError((cause) =>
          isTextGenerationError(cause)
            ? cause
            : new TextGenerationError({
                operation,
                detail: "Bob ACP text generation failed.",
                cause,
              }),
        ),
        Effect.scoped,
      );
    },
  );

  return TextGenerationOperations.fromRunner(
    "BobTextGeneration",
    TextGenerationOperations.withoutInvestigation("Bob", runBobJson),
  );
});
