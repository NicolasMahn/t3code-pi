/**
 * BUILT_IN_DRIVERS — the static set of `ProviderDriver`s this build ships
 * with.
 *
 * This is a Pi-focused fork. Only the PiDriver is registered.
 *
 * @module provider/builtInDrivers
 */
import { PiDriver, type PiDriverEnv } from "./Drivers/PiDriver.ts";
import type { AnyProviderDriver } from "./ProviderDriver.ts";

/**
 * Union of infrastructure services required to construct any built-in
 * driver. The registry layer declares `R = BuiltInDriversEnv`; the runtime
 * layer must provide every service in this union.
 */
export type BuiltInDriversEnv = PiDriverEnv;

/**
 * Ordered list of built-in drivers.
 */
export const BUILT_IN_DRIVERS: ReadonlyArray<AnyProviderDriver<BuiltInDriversEnv>> = [PiDriver];
