import {
  DEFAULT_SERVER_SETTINGS,
  ProviderDriverKind,
  ProviderInstanceId,
  type ProviderInstanceConfig,
} from "@t3tools/contracts";
import { describe, expect, it } from "vitest";
import { buildProviderInstanceUpdatePatch } from "./SettingsPanels.logic";

describe("buildProviderInstanceUpdatePatch", () => {
  it("promotes an edited default provider into providerInstances and resets the legacy provider", () => {
    const instanceId = ProviderInstanceId.make("pi");
    const nextInstance = {
      driver: ProviderDriverKind.make("pi"),
      enabled: true,
      config: {
        binaryPath: "/usr/local/bin/pi",
      },
    } satisfies ProviderInstanceConfig;

    const patch = buildProviderInstanceUpdatePatch({
      settings: {
        ...DEFAULT_SERVER_SETTINGS,
        providers: {
          ...DEFAULT_SERVER_SETTINGS.providers,
          pi: {
            ...DEFAULT_SERVER_SETTINGS.providers.pi,
            binaryPath: "/legacy/pi",
          },
        },
      },
      instanceId,
      instance: nextInstance,
      driver: ProviderDriverKind.make("pi"),
      isDefault: true,
    });

    expect(patch.providerInstances?.[instanceId]).toEqual(nextInstance);
    expect(patch.providers?.pi).toEqual(DEFAULT_SERVER_SETTINGS.providers.pi);
  });

  it("updates custom instances without touching legacy provider settings", () => {
    const instanceId = ProviderInstanceId.make("pi_custom");
    const nextInstance = {
      driver: ProviderDriverKind.make("pi"),
      enabled: true,
      config: {
        binaryPath: "/usr/local/bin/pi",
        model: "anthropic/claude-sonnet-4-20250514",
      },
    } satisfies ProviderInstanceConfig;

    const patch = buildProviderInstanceUpdatePatch({
      settings: DEFAULT_SERVER_SETTINGS,
      instanceId,
      instance: nextInstance,
      driver: ProviderDriverKind.make("pi"),
      isDefault: false,
    });

    expect(patch.providerInstances?.[instanceId]).toEqual(nextInstance);
    expect(patch.providers).toBeUndefined();
  });
});
