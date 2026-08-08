import type { ExtensionAPI } from "@oh-my-pi/pi-coding-agent";
import type { ContentInput, SpectrumInstance } from "spectrum-ts";
import type { IMessageMessage } from "spectrum-ts/providers/imessage";
import type { EscalateConfig } from "./config";

type InboundHandler = (message: IMessageMessage) => boolean;
type App = SpectrumInstance;
interface PollInterest {
    choiceCount: number;
    expectedChoices?: readonly string[];
}

export interface PhotonSession {
    send(...content: ContentInput[]): Promise<void>;
    waitForChoice(
        tagPrefix: string,
        choiceCount: number,
        signal: AbortSignal,
        expectedChoices?: readonly string[],
    ): Promise<{ index: number; selected: boolean }>;
    closeChoice(tagPrefix: string): void;
    waitForText(signal: AbortSignal): Promise<string>;
    stop(): Promise<void>;
}

let current: Promise<Session> | undefined;
let currentSession: Session | undefined;
let logger: ExtensionAPI["logger"] | undefined;

export function setPhotonLogger(value: ExtensionAPI["logger"]): void {
    logger = value;
}

function logRouterError(message: string, error: unknown): void {
    logger?.error(message, { error: error instanceof Error ? error.message : String(error) });
}
function parsePollChoice(
    message: IMessageMessage,
    tagPrefix: string,
    choiceCount: number,
    expectedChoices?: readonly string[],
): { index: number; selected: boolean } | undefined {
    const content = message.content;
    if (content.type !== "poll_option") return undefined;
    const titleMatches = content.poll.title.startsWith(tagPrefix);
    const titleUnavailable =
        content.poll.title === "iMessage poll" || content.poll.title.trim() === "";
    const optionsMatch =
        titleUnavailable &&
        expectedChoices !== undefined &&
        content.poll.options.length === expectedChoices.length &&
        content.poll.options.every((option, index) => option.title === expectedChoices[index]);
    if (!titleMatches && !optionsMatch) return undefined;
    const numbered = /^(\d+)\./.exec(content.option.title);
    let index = numbered ? Number(numbered[1]) - 1 : -1;
    if (index < 0 || index >= choiceCount) {
        index = content.poll.options.findIndex((option) => option.title === content.option.title);
    }
    return index >= 0 && index < choiceCount ? { index, selected: content.selected } : undefined;
}

function createAbortError(message: string): Error {
    const error = new Error(message);
    error.name = "AbortError";
    return error;
}

class Session implements PhotonSession {
    readonly #handlers = new Set<InboundHandler>();
    readonly #pollBacklog: IMessageMessage[] = [];
    readonly #pollInterests = new Map<string, PollInterest>();
    readonly #app: App;
    readonly #space: { id: string };

    constructor(app: App, space: { id: string }) {
        this.#app = app;
        this.#space = space;
        void this.#routeInbound().catch((error) => {
            logRouterError("photon-escalate: inbound router stopped", error);
        });
    }

    async send(...content: ContentInput[]): Promise<void> {
        for (const item of content) await this.#app.send(this.#space as never, item);
    }

    waitForChoice(
        tagPrefix: string,
        choiceCount: number,
        signal: AbortSignal,
        expectedChoices?: readonly string[],
    ): Promise<{ index: number; selected: boolean }> {
        if (signal.aborted)
            return Promise.reject(createAbortError("Photon choice wait was cancelled"));
        this.#pollInterests.set(tagPrefix, { choiceCount, expectedChoices });
        for (let index = 0; index < this.#pollBacklog.length; index += 1) {
            const message = this.#pollBacklog[index];
            if (!message) continue;
            const buffered = parsePollChoice(message, tagPrefix, choiceCount, expectedChoices);
            if (!buffered) continue;
            this.#pollBacklog.splice(index, 1);
            return Promise.resolve(buffered);
        }

        const { promise, resolve, reject } = Promise.withResolvers<{
            index: number;
            selected: boolean;
        }>();
        const onAbort = () => reject(createAbortError("Photon choice wait was cancelled"));
        const handler: InboundHandler = (message) => {
            const pollChoice = parsePollChoice(message, tagPrefix, choiceCount, expectedChoices);
            if (pollChoice) {
                resolve(pollChoice);
                return true;
            }
            if (message.content.type !== "text") return false;
            const body = message.content.text.trim();
            if (!/^\d+$/.test(body)) return false;
            const index = Number(body) - 1;
            if (index < 0 || index >= choiceCount) return false;
            resolve({ index, selected: true });
            return true;
        };

        this.#handlers.add(handler);
        signal.addEventListener("abort", onAbort, { once: true });
        return promise.finally(() => {
            signal.removeEventListener("abort", onAbort);
            this.#handlers.delete(handler);
        });
    }
    closeChoice(tagPrefix: string): void {
        const interest = this.#pollInterests.get(tagPrefix);
        this.#pollInterests.delete(tagPrefix);
        if (!interest) return;
        for (let index = this.#pollBacklog.length - 1; index >= 0; index -= 1) {
            const message = this.#pollBacklog[index];
            if (
                message &&
                parsePollChoice(message, tagPrefix, interest.choiceCount, interest.expectedChoices)
            ) {
                this.#pollBacklog.splice(index, 1);
            }
        }
    }

    waitForText(signal: AbortSignal): Promise<string> {
        if (signal.aborted)
            return Promise.reject(createAbortError("Photon text wait was cancelled"));
        const { promise, resolve, reject } = Promise.withResolvers<string>();
        const onAbort = () => reject(createAbortError("Photon text wait was cancelled"));
        const handler: InboundHandler = (message) => {
            if (message.content.type !== "text") return false;
            resolve(message.content.text);
            return true;
        };

        this.#handlers.add(handler);
        signal.addEventListener("abort", onAbort, { once: true });
        return promise.finally(() => {
            signal.removeEventListener("abort", onAbort);
            this.#handlers.delete(handler);
        });
    }

    async stop(): Promise<void> {
        if (currentSession === this) {
            current = undefined;
            currentSession = undefined;
        }
        await this.#app.stop();
    }

    async #routeInbound(): Promise<void> {
        for await (const [messageSpace, rawMessage] of this.#app.messages) {
            try {
                const message = rawMessage as unknown as IMessageMessage;
                if (message.direction === "outbound" || messageSpace.id !== this.#space.id)
                    continue;
                let handled = false;
                for (const handler of this.#handlers) {
                    if (!handler(message)) continue;
                    handled = true;
                    break;
                }
                if (!handled && message.content.type === "poll_option") {
                    for (const [tagPrefix, interest] of this.#pollInterests) {
                        if (
                            !parsePollChoice(
                                message,
                                tagPrefix,
                                interest.choiceCount,
                                interest.expectedChoices,
                            )
                        ) {
                            continue;
                        }
                        if (this.#pollBacklog.length === 64) this.#pollBacklog.shift();
                        this.#pollBacklog.push(message);
                        break;
                    }
                }
            } catch (error) {
                logRouterError("photon-escalate: failed to route inbound message", error);
            }
        }
    }
}

async function connect(cfg: EscalateConfig): Promise<Session> {
    // Keep the SDK optional at extension-load time; failed installs must preserve native ask.
    const { Spectrum } = await import("spectrum-ts");
    const { imessage } = await import("spectrum-ts/providers/imessage");
    const app = await Spectrum({
        projectId: cfg.projectId!,
        projectSecret: cfg.projectSecret!,
        providers: [imessage.config()],
    });
    const im = imessage(app);
    const user = await im.user(cfg.phone);
    const space = await im.space.create(user, cfg.line ? { phone: cfg.line } : undefined);
    const session = new Session(app, space);
    currentSession = session;
    return session;
}

export async function getPhotonSession(cfg: EscalateConfig): Promise<PhotonSession> {
    if (!current) {
        const pending = connect(cfg);
        current = pending;
        void pending.catch(() => {
            if (current === pending) current = undefined;
        });
    }
    return current;
}

export async function stopPhotonSession(): Promise<void> {
    const pending = current;
    current = undefined;
    currentSession = undefined;
    if (!pending) return;
    const session = await pending;
    await session.stop();
}
