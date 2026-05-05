import * as NodeServices from "@effect/platform-node/NodeServices";
import {
  DEFAULT_SERVER_SETTINGS,
  ProviderDriverKind,
  ProviderInstanceId,
  ServerSettings,
  ServerSettingsPatch,
} from "@t3tools/contracts";
import { assert, it } from "@effect/vitest";
import { Effect, Schema } from "effect";

it("decodes nested settings patches", () => {
  const decodePatch = Schema.decodeUnknownSync(ServerSettingsPatch);

  assert.deepEqual(decodePatch({ providers: { pi: { binaryPath: "/tmp/pi" } } }), {
    providers: { pi: { binaryPath: "/tmp/pi" } },
  });

  assert.deepEqual(
    decodePatch({
      textGenerationModelSelection: {
        options: [{ id: "fastMode", value: false }],
      },
    }),
    {
      textGenerationModelSelection: {
        options: [{ id: "fastMode", value: false }],
      },
    },
  );
});

it("decodes default server settings with pi provider", () => {
  const decoded = Schema.decodeSync(ServerSettings)({});
  assert.equal(decoded.providers.pi.enabled, true);
  assert.equal(decoded.providers.pi.binaryPath, "pi");
});

it("decodes provider instances", () => {
  const decoded = Schema.decodeSync(ServerSettings)({
    providerInstances: {
      pi_custom: {
        driver: "pi",
        config: { binaryPath: "/usr/local/bin/pi", model: "sonnet" },
      },
    },
  });
  const customId = ProviderInstanceId.make("pi_custom");
  assert.equal(decoded.providerInstances[customId]?.driver, "pi");
  assert.equal(
    (decoded.providerInstances[customId]?.config as any).binaryPath,
    "/usr/local/bin/pi",
  );
});
