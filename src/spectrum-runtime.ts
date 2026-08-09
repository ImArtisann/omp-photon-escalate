import { createRequire } from "node:module";
import type * as IMessageRuntime from "spectrum-ts/providers/imessage";
import type * as SpectrumRuntime from "spectrum-ts";

const require = createRequire(import.meta.url);
let spectrum: typeof SpectrumRuntime | undefined;
let imessage: typeof IMessageRuntime | undefined;

export function loadSpectrum(): typeof SpectrumRuntime {
    return (spectrum ??= require("spectrum-ts") as typeof SpectrumRuntime);
}

export function loadIMessage(): typeof IMessageRuntime {
    return (imessage ??= require("spectrum-ts/providers/imessage") as typeof IMessageRuntime);
}
