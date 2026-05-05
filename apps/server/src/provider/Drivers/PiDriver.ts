/**
 * PiDriver — `ProviderDriver` for the Pi coding agent (RPC mode).
 *
 * Spawns `pi --mode rpc` per instance and translates Pi's JSON-RPC event
 * stream into t3code's `ProviderRuntimeEvent` model via `PiAdapter`.
 *
 * @module provider/Drivers/PiDriver
 */
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
import { makePiRpcClient } from "../Layers/PiRpcClient.ts";

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

// Fallback models used when Pi is not available to query
const STATIC_PI_MODELS: ServerProviderModel[] = [
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

/**
 * Probe Pi for its available models by spawning a temporary RPC process.
 */
const probePiProvider = (binaryPath: string, processEnv?: NodeJS.ProcessEnv) =>
  Effect.gen(function* () {
    yield* Effect.sleep(Duration.millis(500));

    const client = yield* makePiRpcClient({
      binaryPath,
      ...(processEnv ? { environment: processEnv } : {}),
    });

    yield* Effect.sleep(Duration.millis(500));

    let response: { success: boolean; data?: unknown } | null = null;
    try {
      response = yield* client.sendCommand({ type: "get_available_models" }).pipe(
        Effect.timeout(Duration.seconds(5)),
      );
    } catch {
      response = null;
    }

    yield* client.close.pipe(Effect.ignore);

    if (!response || !response.success || !response.data) {
      return [];
    }

    const data = response.data as
      | {
          models?: Array<{
            id: string;
            name: string;
            provider?: string;
          }>;
        }
      | undefined;
    if (!data?.models) return [];

    return data.models.map(
      (m): ServerProviderModel => ({
        slug: m.id,
        name: m.name,
        ...(m.provider ? { subProvider: m.provider } : {}),
        isCustom: false,
        capabilities: null,
      }),
    );
  });

function checkPiProviderStatus(
  settings: PiSettings,
  processEnv: NodeJS.ProcessEnv | undefined,
): Effect.Effect<ServerProviderDraft, never, ChildProcessSpawner.ChildProcessSpawner> {
  return Effect.gen(function* () {
    const now = new Date().toISOString() as any;

    let models: ServerProviderModel[] = [];
    if (settings.enabled) {
      try {
        models = yield* probePiProvider(settings.binaryPath, processEnv).pipe(
          Effect.scoped,
          Effect.timeout(Duration.seconds(8)),
        );
      } catch {
        models = [];
      }
    }

    return {
      displayName: "Pi",
      enabled: settings.enabled,
      installed: true,
      version: null,
      status: settings.enabled ? ("ready" as const) : ("disabled" as const),
      auth: { status: "unknown" as const },
      checkedAt: now,
      models: models.length > 0 ? models : STATIC_PI_MODELS,
      slashCommands: [],
      skills: [],
    };
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
      const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;
      const eventLoggers = yield* ProviderEventLoggers;
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

      // Build a managed snapshot. checkPiProviderStatus needs
      // ChildProcessSpawner; we satisfy it here so the managed layer sees R=never.
      const checkProvider = checkPiProviderStatus(effectiveConfig, processEnv).pipe(
        Effect.map(stampIdentity),
        Effect.provideService(ChildProcessSpawner.ChildProcessSpawner, spawner),
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
