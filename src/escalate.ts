import { randomUUID } from "node:crypto";
import type { AskToolInput, ExtensionContext, QuestionResult } from "@oh-my-pi/pi-coding-agent";
import type { ContentBuilder } from "spectrum-ts";
import type { EscalateConfig } from "./config";
import { getPhotonSession, type PhotonSession } from "./photon";
import { loadSpectrum } from "./spectrum-runtime";

const BASE32 = "ABCDEFGHIJKLMNOPQRSTUVWXYZ234567";
const SUBMIT_CHOICES = ["Submit", "Start over"] as const;
let activePollSession: PhotonSession | undefined;

function truncate(value: string, limit: number): string {
    return value.length <= limit ? value : `${value.slice(0, limit - 1)}…`;
}

function createAskId(): string {
    const bits = Number.parseInt(randomUUID().slice(0, 5), 16);
    return `${BASE32[(bits >> 15) & 31]}${BASE32[(bits >> 10) & 31]}${BASE32[(bits >> 5) & 31]}${BASE32[bits & 31]}`;
}
function throwIfAborted(signal: AbortSignal): void {
    if (!signal.aborted) return;
    const error = new Error("Phone escalation was cancelled");
    error.name = "AbortError";
    throw error;
}

async function sleep(ms: number, ctx: ExtensionContext, signal: AbortSignal): Promise<void> {
    throwIfAborted(signal);
    if (ms === 0) return;

    const { promise, resolve, reject } = Promise.withResolvers<void>();
    const timer = ctx.setTimeout(() => resolve(), ms);
    const onAbort = () => {
        const error = new Error("Phone escalation was cancelled");
        error.name = "AbortError";
        reject(error);
    };
    signal.addEventListener("abort", onAbort, { once: true });
    try {
        await promise;
        throwIfAborted(signal);
    } finally {
        signal.removeEventListener("abort", onAbort);
        ctx.clearTimer(timer);
    }
}

function startNudge(
    ctx: ExtensionContext,
    session: PhotonSession,
    intervalSeconds: number,
    message: string,
): { failure: Promise<never>; stop(): void } {
    const { promise: failure, reject } = Promise.withResolvers<never>();
    const timer =
        intervalSeconds === 0
            ? undefined
            : ctx.setInterval(() => {
                  void session.send(message).catch(reject);
              }, intervalSeconds * 1000);
    return {
        failure,
        stop() {
            if (timer) ctx.clearTimer(timer);
        },
    };
}

function answerText(result: QuestionResult): string {
    if (result.customInput !== undefined) return `"${result.customInput}"`;
    return result.multi
        ? result.selectedOptions.join(", ")
        : (result.selectedOptions[0] ?? "(none)");
}

function buildQuestionText(
    question: AskToolInput["questions"][number],
    index: number,
    count: number,
): string {
    const lines = question.options.map(
        (option, optionIndex) =>
            `${optionIndex + 1}. ${option.label}${option.description ? ` — ${option.description}` : ""}`,
    );
    const recommended = question.recommended;
    const recommendation =
        recommended !== undefined && recommended >= 0 && recommended < question.options.length
            ? `\nRecommended: ${recommended + 1}`
            : "";
    const header = question.header ? `[${question.header}]\n` : "";
    return `${header}[${index + 1}/${count}] ${question.question}\n\n${lines.join("\n")}\n${recommendation}\nReply with a number, or pick "Other…" to type your own.`;
}
interface ArmedWait<T> {
    pending: Promise<T>;
    cancel(): void;
}

function armWait<T>(
    signal: AbortSignal,
    wait: (childSignal: AbortSignal) => Promise<T>,
): ArmedWait<T> {
    const controller = new AbortController();
    const forwardAbort = () => controller.abort(signal.reason);
    signal.addEventListener("abort", forwardAbort, { once: true });
    if (signal.aborted) forwardAbort();
    const pending = wait(controller.signal).finally(() => {
        signal.removeEventListener("abort", forwardAbort);
    });
    return {
        pending,
        cancel() {
            controller.abort();
        },
    };
}

async function sendAfterArming<T>(
    armed: ArmedWait<T>,
    signal: AbortSignal,
    send: () => Promise<void>,
): Promise<void> {
    try {
        throwIfAborted(signal);
        await send();
        throwIfAborted(signal);
    } catch (error) {
        armed.cancel();
        try {
            await armed.pending;
        } catch {
            // Expected cancellation: drain the waiter before preserving the send or abort error.
        }
        throw error;
    }
}

async function awaitWithNudge<T>(armed: ArmedWait<T>, nudgeFailure: Promise<never>): Promise<T> {
    try {
        return await Promise.race([armed.pending, nudgeFailure]);
    } catch (error) {
        armed.cancel();
        try {
            await armed.pending;
        } catch {
            // The original wait or nudge error is rethrown below.
        }
        throw error;
    }
}

async function answerQuestion(
    question: AskToolInput["questions"][number],
    index: number,
    count: number,
    askId: string,
    cfg: EscalateConfig,
    ctx: ExtensionContext,
    session: PhotonSession,
    poll: (title: string, ...options: string[]) => ContentBuilder,
    signal: AbortSignal,
): Promise<QuestionResult> {
    const tag = `${cfg.labelPrefix}-${askId}.${index + 1}`;
    const optionChoices = question.options.map((option, optionIndex) =>
        truncate(`${optionIndex + 1}. ${option.label}`, 40),
    );
    const otherIndex = optionChoices.length;
    const doneIndex = question.multi ? otherIndex + 1 : -1;
    const choices = [...optionChoices, "Other…", ...(question.multi ? ["✓ Done"] : [])];

    throwIfAborted(signal);
    await session.send(buildQuestionText(question, index, count));
    throwIfAborted(signal);
    const firstChoice = armWait(signal, (childSignal) =>
        session.waitForChoice(tag, choices.length, childSignal, choices),
    );
    activePollSession = session;
    try {
        await sendAfterArming(firstChoice, signal, () =>
            session.send(poll(`${tag} ${truncate(question.question, 60)}`, ...choices)),
        );
    } catch (error) {
        if (activePollSession === session) activePollSession = undefined;
        session.closeChoice(tag);
        throw error;
    }

    const nudge = startNudge(
        ctx,
        session,
        cfg.nudgeIntervalSeconds,
        `Still waiting on ${index + 1}/${count}: ${truncate(question.question, 80)}`,
    );
    try {
        const selected = new Set<number>();
        let customInput: string | undefined;
        let pendingChoice: ArmedWait<{ index: number; selected: boolean }> | undefined =
            firstChoice;
        while (true) {
            const choiceWait =
                pendingChoice ??
                armWait(signal, (childSignal) =>
                    session.waitForChoice(tag, choices.length, childSignal, choices),
                );
            pendingChoice = undefined;
            const choice = await awaitWithNudge(choiceWait, nudge.failure);
            if (choice.index === otherIndex) {
                if (!choice.selected) continue;
                const textWait = armWait(signal, (childSignal) => session.waitForText(childSignal));
                await sendAfterArming(textWait, signal, () =>
                    session.send(`Reply with your answer for ${index + 1}/${count}.`),
                );
                customInput = await awaitWithNudge(textWait, nudge.failure);
                break;
            }
            if (question.multi) {
                if (choice.index === doneIndex) {
                    if (!choice.selected) continue;
                    if (selected.size === 0) {
                        throwIfAborted(signal);
                        await session.send("Pick at least one option first.");
                        throwIfAborted(signal);
                        continue;
                    }
                    break;
                }
                if (choice.index >= 0 && choice.index < question.options.length) {
                    if (choice.selected) selected.add(choice.index);
                    else selected.delete(choice.index);
                }
                continue;
            }
            if (!choice.selected || choice.index < 0 || choice.index >= question.options.length)
                continue;
            selected.add(choice.index);
            break;
        }

        return {
            id: question.id,
            question: question.question,
            options: question.options.map((option) => option.label),
            multi: question.multi ?? false,
            selectedOptions: [...selected].map(
                (selectedIndex) => question.options[selectedIndex]!.label,
            ),
            ...(customInput !== undefined ? { customInput } : {}),
        };
    } finally {
        nudge.stop();
        session.closeChoice(tag);
    }
}

export async function runPhoneFlow(
    questions: AskToolInput["questions"],
    cfg: EscalateConfig,
    ctx: ExtensionContext,
    signal: AbortSignal,
    delayMs: number,
): Promise<QuestionResult[]> {
    activePollSession = undefined;
    await sleep(delayMs, ctx, signal);
    const session = await getPhotonSession(cfg);
    throwIfAborted(signal);
    // Keep the SDK optional at extension-load time.
    const { poll } = loadSpectrum();
    throwIfAborted(signal);
    ctx.ui.setStatus("photon-escalate", "iMessage: waiting for answer");
    try {
        while (true) {
            const askId = createAskId();
            const results: QuestionResult[] = [];
            for (let index = 0; index < questions.length; index += 1) {
                const question = questions[index];
                if (!question)
                    throw new Error("Question index exceeded the requested question list");
                results.push(
                    await answerQuestion(
                        question,
                        index,
                        questions.length,
                        askId,
                        cfg,
                        ctx,
                        session,
                        poll,
                        signal,
                    ),
                );
            }

            const summary = results
                .map(
                    (result, index) =>
                        `${index + 1}. ${result.question}\n   Answer: ${answerText(result)}`,
                )
                .join("\n\n");
            throwIfAborted(signal);
            await session.send(`Review your answers:\n\n${summary}`);
            throwIfAborted(signal);
            const submitTag = `${cfg.labelPrefix}-${askId}.S`;
            const firstSubmitChoice = armWait(signal, (childSignal) =>
                session.waitForChoice(
                    submitTag,
                    SUBMIT_CHOICES.length,
                    childSignal,
                    SUBMIT_CHOICES,
                ),
            );
            activePollSession = session;
            try {
                await sendAfterArming(firstSubmitChoice, signal, () =>
                    session.send(poll(submitTag, ...SUBMIT_CHOICES)),
                );
            } catch (error) {
                if (activePollSession === session) activePollSession = undefined;
                session.closeChoice(submitTag);
                throw error;
            }

            const nudge = startNudge(
                ctx,
                session,
                cfg.nudgeIntervalSeconds,
                "Still waiting for Submit or Start over.",
            );
            try {
                let pendingChoice: ArmedWait<{ index: number; selected: boolean }> | undefined =
                    firstSubmitChoice;
                while (true) {
                    const choiceWait =
                        pendingChoice ??
                        armWait(signal, (childSignal) =>
                            session.waitForChoice(
                                submitTag,
                                SUBMIT_CHOICES.length,
                                childSignal,
                                SUBMIT_CHOICES,
                            ),
                        );
                    pendingChoice = undefined;
                    const choice = await awaitWithNudge(choiceWait, nudge.failure);
                    if (!choice.selected) continue;
                    if (choice.index === 0) return results;
                    if (choice.index === 1) break;
                }
            } finally {
                nudge.stop();
                session.closeChoice(submitTag);
            }
            activePollSession = undefined;
        }
    } finally {
        ctx.ui.setStatus("photon-escalate", undefined);
    }
}

export async function notifyTerminalAnswer(): Promise<void> {
    const session = activePollSession;
    activePollSession = undefined;
    if (session) await session.send("Answered in the terminal — ignore the poll above.");
}

export function clearActivePhonePoll(): void {
    activePollSession = undefined;
}
