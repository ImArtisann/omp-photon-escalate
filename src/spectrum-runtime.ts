import type * as IMessageRuntime from "spectrum-ts/providers/imessage";
import type * as SpectrumRuntime from "spectrum-ts";

interface RuntimeBuild {
    onLoad(
        options: { filter: RegExp },
        callback: (args: { path: string }) => Promise<{ contents: string; loader: "js" }>,
    ): void;
}

interface BunRuntime {
    file(path: string): { text(): Promise<string> };
    plugin(plugin: { name: string; setup(build: RuntimeBuild): void }): void;
}

const CORE_IMPORT = 'import ogs from "open-graph-scraper";';
const PATCHED_CORE_IMPORT = `import { createRequire } from "node:module";
const ogs = createRequire(import.meta.url)("open-graph-scraper");`;

let installed = false;

function installSpectrumResolver(): void {
    if (installed) return;
    const bun = (globalThis as typeof globalThis & { Bun?: BunRuntime }).Bun;
    if (!bun) throw new Error("omp-photon-escalate requires the Bun runtime");

    bun.plugin({
        name: "omp-photon-escalate-spectrum-resolver",
        setup(build) {
            build.onLoad({ filter: /@spectrum-ts\/core\/dist\/index\.js$/ }, async ({ path }) => {
                const source = await bun.file(path).text();
                if (!source.includes(CORE_IMPORT)) {
                    throw new Error("Unsupported spectrum-ts build: cannot patch scraper import");
                }
                return {
                    contents: source.replace(CORE_IMPORT, PATCHED_CORE_IMPORT),
                    loader: "js",
                };
            });
        },
    });
    installed = true;
}

export async function loadSpectrum(): Promise<typeof SpectrumRuntime> {
    installSpectrumResolver();
    return import(import.meta.resolve("spectrum-ts"));
}

export async function loadIMessage(): Promise<typeof IMessageRuntime> {
    installSpectrumResolver();
    return import(import.meta.resolve("spectrum-ts/providers/imessage"));
}
