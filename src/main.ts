import type { AskToolDetails, AskToolInput, ExtensionAPI } from "@oh-my-pi/pi-coding-agent";
import { loadConfig } from "./config";
import { clearActivePhonePoll, notifyTerminalAnswer, runPhoneFlow } from "./escalate";
import { ASK_DESCRIPTION, buildAskSchema, buildResult } from "./native-ask";
import { setPhotonLogger, stopPhotonSession } from "./photon";

let awayMode = false;
let configWarningShown = false;
let askTail = Promise.resolve();

async function withAskLock<T>(run: () => Promise<T>): Promise<T> {
    const previous = askTail;
    const { promise: gate, resolve: release } = Promise.withResolvers<void>();
    askTail = previous.then(
        () => gate,
        () => gate,
    );
    await previous.catch(() => undefined);
    try {
        return await run();
    } finally {
        release();
    }
}

function errorMessage(error: unknown): string {
    return error instanceof Error ? error.message : String(error);
}

export default function photonEscalate(pi: ExtensionAPI): void {
    setPhotonLogger(pi.logger);
    pi.setLabel("Photon Ask Escalation");
    pi.logger.debug("photon-escalate: registering ask override", { entry: "src/main.ts" });

    pi.registerTool({
        name: "ask",
        label: "Ask",
        description: ASK_DESCRIPTION,
        parameters: buildAskSchema(pi.arktype),
        approval: "read",
        strict: true,
        loadMode: "discoverable",
        async execute(_toolCallId, params, signal, onUpdate, ctx) {
            return withAskLock(async () => {
                if (!ctx.invokeTool) {
                    return {
                        content: [
                            { type: "text" as const, text: "Ask tool requires interactive mode" },
                        ],
                        isError: true,
                        details: {},
                    };
                }

                const loaded = loadConfig(ctx.cwd, pi.pi.getAgentDir());
                if ("error" in loaded) {
                    if (!configWarningShown) {
                        ctx.ui.notify(loaded.error, "warning");
                        configWarningShown = true;
                    }
                    return ctx.invokeTool<AskToolDetails>(params, { signal, onUpdate });
                }
                if (!loaded.config.enabled) {
                    return ctx.invokeTool<AskToolDetails>(params, { signal, onUpdate });
                }

                const config = loaded.config;
                const terminalAbort = new AbortController();
                const phoneAbort = new AbortController();
                const abortBoth = () => {
                    terminalAbort.abort(signal?.reason);
                    phoneAbort.abort(signal?.reason);
                };
                signal?.addEventListener("abort", abortBoth, { once: true });
                if (signal?.aborted) abortBoth();

                // Keep escalation in the tool wrapper: legacy `tool_call` hooks run before
                // execution and cannot resolve or cancel the pending native ask.
                try {
                    const terminal = ctx
                        .invokeTool<AskToolDetails>(params, {
                            signal: terminalAbort.signal,
                            onUpdate,
                        })
                        .then(
                            (result) => ({ kind: "terminal" as const, result }),
                            (error) => ({ kind: "terminalError" as const, error }),
                        );
                    const delayMs = awayMode ? 0 : config.escalateAfterSeconds * 1000;
                    pi.logger.debug("photon-escalate: countdown started", { delayMs });
                    const phone = runPhoneFlow(
                        (params as AskToolInput).questions,
                        config,
                        ctx,
                        phoneAbort.signal,
                        delayMs,
                    ).then(
                        (results) => ({ kind: "phone" as const, results }),
                        (error) => ({ kind: "phoneError" as const, error }),
                    );

                    const winner = await Promise.race([terminal, phone]);
                    switch (winner.kind) {
                        case "terminal":
                            phoneAbort.abort();
                            awayMode = false;
                            await notifyTerminalAnswer().catch((error) => {
                                pi.logger.error(
                                    "photon-escalate: failed to send terminal-answer notice",
                                    {
                                        error: errorMessage(error),
                                    },
                                );
                            });
                            return winner.result;
                        case "terminalError":
                            phoneAbort.abort();
                            clearActivePhonePoll();
                            throw winner.error;
                        case "phone": {
                            terminalAbort.abort();
                            await terminal;
                            clearActivePhonePoll();
                            awayMode = config.stickyAwayMode;
                            return buildResult((params as AskToolInput).questions, winner.results);
                        }
                        case "phoneError": {
                            clearActivePhonePoll();
                            const message = errorMessage(winner.error);
                            pi.logger.error("photon-escalate: phone flow failed", {
                                error: message,
                            });
                            ctx.ui.notify(`photon-escalate: ${message}`, "warning");
                            const terminalResult = await terminal;
                            if (terminalResult.kind === "terminalError") throw terminalResult.error;
                            awayMode = false;
                            return terminalResult.result;
                        }
                    }
                } finally {
                    signal?.removeEventListener("abort", abortBoth);
                }
            });
        },
        renderCall: (args, options, theme) =>
            pi.pi.askToolRenderer.renderCall(args as never, options as never, theme as never),
        renderResult: (result, options, theme) =>
            pi.pi.askToolRenderer.renderResult(result as never, options as never, theme as never),
    });

    pi.on("session_start", async (_event, ctx) => {
        awayMode = false;
        configWarningShown = false;
        let timeout: number | undefined;
        try {
            timeout = pi.pi.settings.get("ask.timeout");
        } catch {
            timeout = undefined;
        }
        if (timeout !== undefined && timeout !== 0) {
            ctx.ui.notify(
                `photon-escalate: set ask.timeout to 0 (currently ${timeout}s) — the built-in timeout will auto-answer before iMessage escalation`,
                "warning",
            );
        }
    });

    pi.on("session_shutdown", async () => {
        await stopPhotonSession();
    });
}
