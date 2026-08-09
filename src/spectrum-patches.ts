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

const OUTBOUND_POLL =
    "const outboundPoll = (spaceId, poll, content) => outboundRecord(spaceId, poll.pollMessageGuid, content, /* @__PURE__ */ new Date());";
const PATCHED_OUTBOUND_POLL = `const outboundPollTitles = /* @__PURE__ */ new Map();
const outboundPoll = (spaceId, poll, content) => {
\tif (outboundPollTitles.size === 128) {
\t\tconst oldest = outboundPollTitles.keys().next().value;
\t\tif (oldest) outboundPollTitles.delete(oldest);
\t}
\toutboundPollTitles.set(poll.pollMessageGuid, content.title);
\treturn outboundRecord(spaceId, poll.pollMessageGuid, content, /* @__PURE__ */ new Date());
};`;

const CACHED_POLL_TITLE = "\t\ttitle: input.title,";
const PATCHED_CACHED_POLL_TITLE = '\t\ttitle: input.title.trim() || "iMessage poll",';

const CACHE_POLL_INFO = `const cachePollInfo = (cache, info) => {
\tconst cached = toCachedPoll(info);
\tcache.set(info.pollMessageGuid, cached);
\treturn cached;
};`;
const PATCHED_CACHE_POLL_INFO = `const resolvePollTitle = (cache, pollMessageGuid, title) => title.trim() || cache.get(pollMessageGuid)?.poll.title || outboundPollTitles.get(pollMessageGuid) || "iMessage poll";
const cachePollInfo = (cache, info) => {
\tconst cached = toCachedPoll({
\t\t...info,
\t\ttitle: resolvePollTitle(cache, info.pollMessageGuid, info.title)
\t});
\tcache.set(info.pollMessageGuid, cached);
\toutboundPollTitles.delete(info.pollMessageGuid);
\treturn cached;
};`;

const CACHE_POLL_EVENT = `\t\tconst cached = toCachedPoll({
\t\t\ttitle: event.delta.title,
\t\t\toptions: event.delta.options
\t\t});
\t\tcache.set(event.pollMessageGuid, cached);
\t\treturn cached;`;
const PATCHED_CACHE_POLL_EVENT = `\t\tconst cached = toCachedPoll({
\t\t\ttitle: resolvePollTitle(cache, event.pollMessageGuid, event.delta.title),
\t\t\toptions: event.delta.options
\t\t});
\t\tcache.set(event.pollMessageGuid, cached);
\t\toutboundPollTitles.delete(event.pollMessageGuid);
\t\treturn cached;`;

function replaceRequired(
    source: string,
    original: string,
    replacement: string,
    label: string,
): string {
    if (source.includes(replacement)) return source;
    if (!source.includes(original)) {
        throw new Error(`Unsupported spectrum-ts build: cannot apply ${label}`);
    }
    return source.replace(original, replacement);
}

function patchImessage(source: string): string {
    let patched = replaceRequired(
        source,
        OUTBOUND_POLL,
        PATCHED_OUTBOUND_POLL,
        "outbound poll-title patch",
    );
    patched = replaceRequired(
        patched,
        CACHED_POLL_TITLE,
        PATCHED_CACHED_POLL_TITLE,
        "empty poll-title patch",
    );
    patched = replaceRequired(
        patched,
        CACHE_POLL_INFO,
        PATCHED_CACHE_POLL_INFO,
        "poll-info correlation patch",
    );
    return replaceRequired(
        patched,
        CACHE_POLL_EVENT,
        PATCHED_CACHE_POLL_EVENT,
        "poll-event correlation patch",
    );
}

let installed = false;

export function installSpectrumRuntimePatches(): void {
    if (installed) return;
    const bun = (globalThis as typeof globalThis & { Bun?: BunRuntime }).Bun;
    if (!bun) throw new Error("omp-photon-escalate requires the Bun runtime");

    bun.plugin({
        name: "omp-photon-escalate-spectrum-patches",
        setup(build) {
            build.onLoad({ filter: /@spectrum-ts\/core\/dist\/index\.js$/ }, async ({ path }) => ({
                contents: replaceRequired(
                    await bun.file(path).text(),
                    CORE_IMPORT,
                    PATCHED_CORE_IMPORT,
                    "core ESM compatibility patch",
                ),
                loader: "js",
            }));
            build.onLoad(
                { filter: /@spectrum-ts\/imessage\/dist\/index\.js$/ },
                async ({ path }) => ({
                    contents: patchImessage(await bun.file(path).text()),
                    loader: "js",
                }),
            );
        },
    });
    installed = true;
}
