import type * as IMessageRuntime from "spectrum-ts/providers/imessage";
import type * as SpectrumRuntime from "spectrum-ts";

export async function loadSpectrum(): Promise<typeof SpectrumRuntime> {
    return import("../vendor/spectrum-runtime/core/index.js");
}

export async function loadIMessage(): Promise<typeof IMessageRuntime> {
    return import("../vendor/spectrum-runtime/imessage/index.js");
}
