import {
  PiSettings,
  ProviderDriverKind,
  type ServerProvider,
  type ServerProviderModel,
  TextGenerationError,
} from "@t3tools/contracts";
import { Duration, Effect, FileSystem, Path, Schema, Stream } from "effect";
import { ChildProcessSpawner } from "effect/unstable/process";

import { ServerConfig } from "../../config.ts";
import { ProviderDriverError } from "../Errors.ts";
import { ProviderEventLoggers } from "../Layers/ProviderEventLoggers.ts";
import { makePiAdapter } from "../Layers/PiAdapter.ts";
import { makeManagedServerProvider } from "../makeManagedServerProvider.ts";
import type { ProviderDriver, ProviderInstance } from "../ProviderDriver.ts";
import type { ServerProviderDraft } from "../providerSnapshot.ts";
import { mergeProviderInstanceEnvironment } from "../ProviderInstanceEnvironment.ts";
import { defaultProviderContinuationIdentity } from "../ProviderDriver.ts";

const DRIVER_KIND = ProviderDriverKind.make("pi");
const SNAPSHOT_REFRESH_INTERVAL = Duration.minutes(5);

/**
 * Services the driver needs to materialize an instance.
 */
export type PiDriverEnv =
  | ChildProcessSpawner.ChildProcessSpawner
  | FileSystem.FileSystem
  | Path.Path
  | ProviderEventLoggers
  | ServerConfig;

const withInstanceIdentity =
  (input: {
    readonly instanceId: ProviderInstance["instanceId"];
    readonly displayName: string | undefined;
    readonly accentColor: string | undefined;
    readonly continuationGroupKey: string;
  }) =>
  (snapshot: ServerProviderDraft): ServerProvider => ({
    ...snapshot,
    instanceId: input.instanceId,
    driver: DRIVER_KIND,
    ...(input.displayName ? { displayName: input.displayName } : {}),
    ...(input.accentColor ? { accentColor: input.accentColor } : {}),
    continuation: { groupKey: input.continuationGroupKey },
  });

function makePendingPiProvider(settings: PiSettings): ServerProviderDraft {
  return {
    displayName: "Pi",
    enabled: settings.enabled,
    installed: true,
    version: null,
    status: "ready",
    auth: { status: "unknown" },
    checkedAt: new Date().toISOString() as any,
    models: [],
    slashCommands: [],
    skills: [],
  };
}

function checkPiProviderStatus(
  settings: PiSettings,
  _processEnv: NodeJS.ProcessEnv | undefined,
): Effect.Effect<ServerProviderDraft, never> {
  const now = new Date().toISOString() as any;
  const models: ServerProviderModel[] = [
    {
      slug: "anthropic/claude-sonnet-4",
      name: "Claude Sonnet 4",
      subProvider: "anthropic",
      isCustom: false,
      capabilities: null,
    },
    {
      slug: "anthropic/claude-haiku-4",
      name: "Claude Haiku 4",
      subProvider: "anthropic",
      isCustom: false,
      capabilities: null,
    },
    {
      slug: "anthropic/claude-opus-4",
      name: "Claude Opus 4",
      subProvider: "anthropic",
      isCustom: false,
      capabilities: null,
    },
    {
      slug: "openai/gpt-4o",
      name: "GPT-4o",
      subProvider: "openai",
      isCustom: false,
      capabilities: null,
    },
    {
      slug: "openai/o3-mini",
      name: "o3-mini",
      subProvider: "openai",
      isCustom: false,
      capabilities: null,
    },
  ];

  return Effect.succeed({
    displayName: "Pi",
    enabled: settings.enabled,
    installed: true,
    version: null,
    status: settings.enabled ? ("ready" as const) : ("disabled" as const),
    auth: { status: "unknown" as const },
    checkedAt: now,
    models,
    slashCommands: [],
    skills: [],
  });
}

export const PiDriver: ProviderDriver<PiSettings, PiDriverEnv> = {
  driverKind: DRIVER_KIND,
  metadata: {
    displayName: "Pi",
    supportsMultipleInstances: true,
  },
  configSchema: PiSettings,
  defaultConfig: (): PiSettings => Schema.decodeSync(PiSettings)({}),
  create: ({ instanceId, displayName, accentColor, environment, enabled, config }) =>
    Effect.gen(function* () {
      const _spawner = yield* ChildProcessSpawner.ChildProcessSpawner;
      const _eventLoggers = yield* ProviderEventLoggers;
      const processEnv = mergeProviderInstanceEnvironment(environment);
      const continuationIdentity = defaultProviderContinuationIdentity({
        driverKind: DRIVER_KIND,
        instanceId,
      });
      const stampIdentity = withInstanceIdentity({
        instanceId,
        displayName,
        accentColor,
        continuationGroupKey: continuationIdentity.continuationKey,
      });

      const effectiveConfig = { ...config, enabled } satisfies PiSettings;

      // Build the adapter — this is the core runtime that communicates with Pi
      const adapter = yield* makePiAdapter({
        instanceId,
        binaryPath: effectiveConfig.binaryPath,
        environment: processEnv,
        ...(effectiveConfig.model ? { model: effectiveConfig.model } : {}),
      });

      // Build a managed snapshot
      const checkProvider = checkPiProviderStatus(effectiveConfig, processEnv).pipe(
        Effect.map(stampIdentity),
      );
      const snapshot = yield* makeManagedServerProvider<PiSettings>({
        getSettings: Effect.succeed(effectiveConfig),
        streamSettings: Stream.never,
        haveSettingsChanged: () => false,
        initialSnapshot: (settings) => stampIdentity(makePendingPiProvider(settings)),
        checkProvider,
        refreshInterval: SNAPSHOT_REFRESH_INTERVAL,
      }).pipe(
        Effect.mapError(
          (cause) =>
            new ProviderDriverError({
              driver: DRIVER_KIND,
              instanceId,
              detail: `Failed to build Pi snapshot: ${cause.message ?? String(cause)}`,
              cause,
            }),
        ),
      );

      // Text generation is not supported for now — Pi manages its own models.
      const textGeneration = {
        generateCommitMessage: () =>
          Effect.fail(
            new TextGenerationError({
              operation: "generateCommitMessage",
              detail: "Pi does not support text generation.",
            }),
          ),
        generatePrContent: () =>
          Effect.fail(
            new TextGenerationError({
              operation: "generatePrContent",
              detail: "Pi does not support text generation.",
            }),
          ),
        generateBranchName: () =>
          Effect.fail(
            new TextGenerationError({
              operation: "generateBranchName",
              detail: "Pi does not support text generation.",
            }),
          ),
        generateThreadTitle: () =>
          Effect.fail(
            new TextGenerationError({
              operation: "generateThreadTitle",
              detail: "Pi does not support text generation.",
            }),
          ),
      };

      return {
        instanceId,
        driverKind: DRIVER_KIND,
        continuationIdentity,
        displayName,
        accentColor,
        enabled,
        snapshot,
        adapter,
        textGeneration,
      } satisfies ProviderInstance;
    }),
};
