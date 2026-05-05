import {
  PiSettings,
  ProviderDriverKind,
  type ServerProvider,
  type ServerProviderModel,
  TextGenerationError,
} from "@t3tools/contracts";
import { Duration, Effect, Exit, FileSystem, Path, Schema, Stream } from "effect";
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

const FALLBACK_PI_MODELS: ServerProviderModel[] = [
  {
    slug: "anthropic/claude-sonnet-4-20250514",
    name: "Claude Sonnet 4",
    subProvider: "anthropic",
    isCustom: false,
    capabilities: null,
  },
  {
    slug: "anthropic/claude-haiku-4-20250514",
    name: "Claude Haiku 4",
    subProvider: "anthropic",
    isCustom: false,
    capabilities: null,
  },
  {
    slug: "anthropic/claude-opus-4-20250514",
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
 * Spawn a temporary Pi RPC process and ask it for available models.
 */
const probePiModels = (binaryPath: string, environment?: NodeJS.ProcessEnv) =>
  Effect.gen(function* () {
    const client = yield* makePiRpcClient({
      binaryPath,
      ...(environment ? { environment } : {}),
    });

    // Pi needs time to boot (load config, extensions, etc.)
    yield* Effect.sleep(Duration.millis(1_500));

    const responseExit = yield* Effect.exit(
      client
        .sendCommand({ type: "get_available_models" })
        .pipe(Effect.timeout(Duration.seconds(6))),
    );

    yield* client.close.pipe(Effect.ignore);

    if (responseExit._tag !== "Success") {
      console.error("[PiDriver] get_available_models failed:", responseExit.cause);
      return [];
    }
    const response = responseExit.value;
    console.error("[PiDriver] get_available_models response:", JSON.stringify(response));
    if (!response.success || !response.data) {
      console.error("[PiDriver] get_available_models returned success=false or no data");
      return [];
    }

    const data = response.data as {
      models?: Array<{
        id: string;
        name: string;
        provider?: string;
      }>;
    };
    if (!data.models || data.models.length === 0) {
      console.error("[PiDriver] get_available_models returned empty model list");
      return [];
    }

    console.error(
      "[PiDriver] Discovered",
      data.models.length,
      "models from Pi:",
      data.models.map((m) => `${m.provider}/${m.id}`).join(", "),
    );

    return data.models.map(
      (m): ServerProviderModel => ({
        // Pi set_model needs provider AND modelId — encode both in slug
        slug: m.provider ? `${m.provider}/${m.id}` : m.id,
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

    const models = settings.enabled
      ? yield* Effect.gen(function* () {
          const exit = yield* Effect.exit(
            probePiModels(settings.binaryPath, processEnv).pipe(
              Effect.scoped,
              Effect.timeout(Duration.seconds(10)),
            ),
          );
          if (exit._tag === "Success") return exit.value;
          console.error("[PiDriver] probePiModels outer timeout:", exit.cause);
          return [];
        })
      : [];

    return {
      displayName: "Pi",
      enabled: settings.enabled,
      installed: true,
      version: null,
      status: settings.enabled ? ("ready" as const) : ("disabled" as const),
      auth: { status: "unknown" as const },
      checkedAt: now,
      models: models.length > 0 ? models : FALLBACK_PI_MODELS,
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

      // Build a managed snapshot. Provide ChildProcessSpawner so the
      // probe can spawn a temporary Pi process to fetch model list.
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
