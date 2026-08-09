import { C as replySchema, E as asRead, G as asGroup, H as asMarkdown, L as addMemberSchema, M as asPoll, N as asPollOption, O as readSchema, T as renameSchema, V as removeMemberSchema, W as markdownSchema, Y as asText, Z as textSchema, _ as asUnsend, a as createTokenRenewal, ct as buildPhotoAction, et as asCustom, f as envAwareConfig, ft as attachmentSchema, h as asVoice, i as stream, j as reactionSchema, k as asReaction, lt as photoActionSchema, m as fromEnv, mt as tracedFetch, nt as asContact, p as envFor, q as groupSchema, s as errorAttrs, st as avatarSchema, u as renderInlineTokens, ut as asAttachment, x as asReply, y as asRichlink, z as leaveSpaceSchema } from "./stream-t0CbOOUT.js";
import z from "zod";
import { createLogger, createLogger as createLogger$1, sanitizeEmail, sanitizeErrorMessage, sanitizePhone, sanitizeUrl, setLogLevel } from "@photon-ai/otel";
import { mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { join } from "node:path";
import { spawn } from "node:child_process";
import { tmpdir } from "node:os";
//#region src/content/effect.ts
const effectInnerSchema = z.discriminatedUnion("type", [
	textSchema,
	markdownSchema,
	attachmentSchema
]);
const messageEffectSchema = z.object({
	type: z.literal("effect"),
	content: effectInnerSchema,
	effect: z.string().nonempty()
});
//#endregion
//#region src/utils/audio.ts
const M4A_BRANDS = new Set([
	"M4A ",
	"M4B ",
	"M4P ",
	"mp42",
	"mp41",
	"isom",
	"iso2"
]);
const M4A_MIME_TYPES = new Set([
	"audio/mp4",
	"audio/mp4a-latm",
	"audio/x-m4a",
	"audio/aac",
	"audio/aacp"
]);
const FFMPEG_MISSING_MESSAGE = "voice content: input is not m4a/aac and ffmpeg is unavailable. Install `ffmpeg-static` or ensure `ffmpeg` is on PATH.";
const isM4a = (buffer) => {
	if (buffer.length < 12) return false;
	if (buffer.toString("ascii", 4, 8) !== "ftyp") return false;
	return M4A_BRANDS.has(buffer.toString("ascii", 8, 12));
};
const isM4aMimeType = (mimeType) => M4A_MIME_TYPES.has(mimeType.toLowerCase());
let cachedFfmpegPath;
const tryStaticBinary = async () => {
	try {
		return (await import("ffmpeg-static")).default ?? void 0;
	} catch {
		return;
	}
};
const resolveFfmpegPath = async () => {
	if (cachedFfmpegPath) return cachedFfmpegPath;
	cachedFfmpegPath = await tryStaticBinary() ?? "ffmpeg";
	return cachedFfmpegPath;
};
const collectStream = (stream) => {
	if (!stream) return Promise.resolve("");
	return new Promise((resolve, reject) => {
		const chunks = [];
		stream.on("data", (chunk) => {
			chunks.push(typeof chunk === "string" ? Buffer.from(chunk) : chunk);
		});
		stream.on("end", () => resolve(Buffer.concat(chunks).toString("utf8")));
		stream.on("error", reject);
	});
};
const isMissingBinaryError = (err) => err?.code === "ENOENT";
const runFfmpeg = (ffmpegPath, args) => {
	const proc = spawn(ffmpegPath, args, { stdio: [
		"ignore",
		"ignore",
		"pipe"
	] });
	const stderr = collectStream(proc.stderr);
	const exit = new Promise((resolve, reject) => {
		proc.on("error", (err) => reject(isMissingBinaryError(err) ? /* @__PURE__ */ new Error(FFMPEG_MISSING_MESSAGE) : err));
		proc.on("exit", (code) => resolve(code ?? -1));
	});
	return Promise.all([exit, stderr]).then(([code, text]) => ({
		code,
		stderr: text
	}));
};
const DURATION_PATTERN = /Duration:\s*(\d+):(\d{2}):(\d{2})(?:\.(\d{1,3}))?/;
const parseDuration = (stderr) => {
	const match = stderr.match(DURATION_PATTERN);
	if (!match) return;
	const [, hh, mm, ss, frac] = match;
	const seconds = Number(hh) * 3600 + Number(mm) * 60 + Number(ss) + Number(`0.${frac ?? 0}`);
	return Number.isFinite(seconds) ? seconds : void 0;
};
const transcodeToM4a = async (buffer) => {
	const ffmpeg = await resolveFfmpegPath();
	const dir = await mkdtemp(join(tmpdir(), "spectrum-voice-"));
	const inPath = join(dir, "in");
	const outPath = join(dir, "out.m4a");
	try {
		await writeFile(inPath, buffer);
		const { code, stderr } = await runFfmpeg(ffmpeg, [
			"-y",
			"-i",
			inPath,
			"-f",
			"ipod",
			"-c:a",
			"aac",
			outPath
		]);
		if (code !== 0) throw new Error(`ffmpeg conversion failed (exit ${code}): ${stderr}`);
		return {
			buffer: await readFile(outPath),
			duration: parseDuration(stderr)
		};
	} finally {
		await rm(dir, {
			recursive: true,
			force: true
		}).catch(() => {});
	}
};
const ensureM4a = async (buffer, mimeType) => {
	if (isM4aMimeType(mimeType) || isM4a(buffer)) return { buffer };
	return transcodeToM4a(buffer);
};
const log = createLogger$1("spectrum.stream");
var RetryableStreamError = class extends Error {
	constructor(message) {
		super(message);
		this.name = "RetryableStreamError";
	}
};
var LiveBufferOverflowError = class extends RetryableStreamError {
	constructor(limit) {
		super(`Live stream buffer exceeded ${limit} events during catch-up`);
		this.name = "LiveBufferOverflowError";
	}
};
var CursorRejectedError = class extends Error {
	constructor(cause) {
		super("Server rejected resume cursor", { cause });
		this.name = "CursorRejectedError";
	}
};
const closeIterable = async (iterable) => {
	if (!iterable) return;
	await iterable.close?.();
};
const ignoreCleanupError = () => void 0;
const RECOVER_TIMEOUT_MS = 3e4;
const runWithTimeout = async (work, timeoutMs, onTimeout) => {
	work.catch(ignoreCleanupError);
	let timer;
	try {
		await Promise.race([work, new Promise((_resolve, reject) => {
			timer = setTimeout(() => reject(onTimeout()), timeoutMs);
		})]);
	} finally {
		if (timer) clearTimeout(timer);
	}
};
const jitterDelay = (delayMs) => delayMs * (.5 + Math.random() * .5);
async function* throwOnCursorRejection(source, isCursorRejected) {
	try {
		yield* source;
	} catch (error) {
		throw isCursorRejected(error) ? new CursorRejectedError(error) : error;
	}
}
const numericCursor = (cursor) => {
	if (!cursor) return;
	const value = Number(cursor);
	return Number.isSafeInteger(value) && value >= 0 ? value : void 0;
};
const isCursorRegression = (next, current) => {
	const nextValue = numericCursor(next);
	const currentValue = numericCursor(current);
	return nextValue !== void 0 && currentValue !== void 0 && nextValue < currentValue;
};
/**
* Wraps a live event stream with cursor-based catch-up and reconnects forever
* with capped, jittered exponential backoff — the stream never ends with an
* error. The only terminal events are `close()` and consumer disconnect. When
* the server rejects the resume cursor, the cursor is dropped and consumption
* falls back to live, accepting (and logging) the event gap.
*/
const resumableOrderedStream = (options) => stream((emit, end) => {
	const catchUpPageSize = options.catchUpPageSize ?? 100;
	const bufferLimit = options.bufferLimit ?? 1e3;
	const initialRetryDelayMs = options.initialRetryDelayMs ?? 500;
	const maxRetryDelayMs = options.maxRetryDelayMs ?? 3e4;
	const jitter = options.jitter ?? jitterDelay;
	const recoverTimeoutMs = options.recoverTimeoutMs ?? RECOVER_TIMEOUT_MS;
	const label = options.label;
	let activeLive;
	let closed = false;
	let failedAttempts = 0;
	let lastRecoverAttempt = 0;
	let lastCursor;
	let retryDelayMs = initialRetryDelayMs;
	let sleepTimer;
	let wakeSleep;
	const deliveredSinceCursor = /* @__PURE__ */ new Set();
	const streamId = globalThis.crypto?.randomUUID?.() ?? `s_${Date.now().toString(36)}${Math.random().toString(36).slice(2, 8)}`;
	const baseStreamAttrs = (phase) => ({
		"spectrum.stream.id": streamId,
		"spectrum.stream.label": label,
		"spectrum.stream.phase": phase,
		"spectrum.stream.has_cursor": lastCursor !== void 0,
		"spectrum.stream.cursor": lastCursor
	});
	const noteRecovery = () => {
		retryDelayMs = initialRetryDelayMs;
		lastRecoverAttempt = 0;
		if (failedAttempts === 0) return;
		log.info("stream recovered", {
			"spectrum.stream.id": streamId,
			"spectrum.stream.label": label,
			"spectrum.stream.attempt": failedAttempts
		});
		failedAttempts = 0;
	};
	const advanceCursor = (cursor, clearDelivered) => {
		if (!cursor || cursor === lastCursor || isCursorRegression(cursor, lastCursor)) return;
		lastCursor = cursor;
		if (clearDelivered) deliveredSinceCursor.clear();
	};
	const deliverItem = async (item, resetRetry, clearOnCursorAdvance) => {
		if (!deliveredSinceCursor.has(item.id)) for (const value of item.values) await emit(value);
		advanceCursor(item.cursor, clearOnCursorAdvance);
		deliveredSinceCursor.add(item.id);
		if (resetRetry) noteRecovery();
	};
	const isCursorRejected = (error) => options.isCursorRejectedError?.(error) === true;
	const sleep = async (delayMs) => {
		if (delayMs <= 0 || closed) return;
		await new Promise((resolve) => {
			wakeSleep = resolve;
			sleepTimer = setTimeout(resolve, jitter(delayMs));
		});
		sleepTimer = void 0;
		wakeSleep = void 0;
	};
	const cancelSleep = () => {
		if (sleepTimer) {
			clearTimeout(sleepTimer);
			sleepTimer = void 0;
		}
		wakeSleep?.();
		wakeSleep = void 0;
	};
	const nextRetryDelay = () => {
		const delay = retryDelayMs;
		retryDelayMs = Math.min(retryDelayMs * 2, maxRetryDelayMs);
		return delay;
	};
	const failureKind = (error) => {
		if (error instanceof CursorRejectedError) return "cursor-rejected";
		if (failedAttempts >= 5) return "persistent";
		return "transient";
	};
	const handleFailure = (error, phase) => {
		failedAttempts += 1;
		const delayMs = nextRetryDelay();
		const attrs = {
			...baseStreamAttrs(phase),
			"spectrum.stream.attempt": failedAttempts,
			"spectrum.stream.delay_ms": delayMs,
			"spectrum.stream.failure_kind": failureKind(error),
			...errorAttrs(error instanceof CursorRejectedError ? error.cause : error)
		};
		if (error instanceof CursorRejectedError) {
			lastCursor = void 0;
			deliveredSinceCursor.clear();
			log.warn("resume cursor rejected; accepting event gap and resuming live", attrs, error);
			return delayMs;
		}
		if (failedAttempts >= 5) {
			log.error("stream persistently failing; still retrying", attrs, error);
			return delayMs;
		}
		log.warn("stream interrupted; reconnecting", attrs, error);
		return delayMs;
	};
	const maybeRecover = async (error) => {
		if (!options.recover || failedAttempts < 5 || failedAttempts - lastRecoverAttempt < 5) return;
		lastRecoverAttempt = failedAttempts;
		try {
			await runWithTimeout(Promise.resolve(options.recover(error, failedAttempts)), recoverTimeoutMs, () => /* @__PURE__ */ new Error(`recover hook did not settle within ${recoverTimeoutMs}ms`));
			log.info("stream recover hook ran", {
				"spectrum.stream.id": streamId,
				"spectrum.stream.label": label,
				"spectrum.stream.attempt": failedAttempts
			});
		} catch (recoverError) {
			log.warn("stream recover hook failed", {
				"spectrum.stream.id": streamId,
				"spectrum.stream.label": label,
				"spectrum.stream.attempt": failedAttempts,
				...errorAttrs(recoverError)
			}, recoverError);
		}
	};
	const consumeLive = async () => {
		const live = options.subscribeLive(lastCursor);
		activeLive = live;
		try {
			for await (const event of live) await deliverItem(await options.processLive(event), true, true);
			throw new RetryableStreamError("Live stream ended");
		} finally {
			if (activeLive === live) activeLive = void 0;
			await closeIterable(live);
		}
	};
	const throwLiveError = (liveError) => {
		if (liveError) throw liveError;
	};
	const bufferLiveEvent = (buffer, event) => {
		if (buffer.length >= bufferLimit) throw new LiveBufferOverflowError(bufferLimit);
		buffer.push(event);
	};
	const startLivePump = (live, isBuffering, liveBuffer) => {
		let liveError;
		return {
			getError: () => liveError,
			pump: (async () => {
				try {
					for await (const event of live) {
						if (isBuffering()) {
							bufferLiveEvent(liveBuffer, event);
							continue;
						}
						await deliverItem(await options.processLive(event), true, true);
					}
					throw new RetryableStreamError("Live stream ended");
				} catch (error) {
					liveError = error;
				}
			})()
		};
	};
	const replayMissed = async (cursor, getLiveError) => {
		const missed = throwOnCursorRejection(options.fetchMissed(cursor, { limit: catchUpPageSize }), isCursorRejected);
		for await (const event of missed) {
			throwLiveError(getLiveError());
			await deliverItem(await options.processMissed(event), false, false);
		}
		throwLiveError(getLiveError());
	};
	const flushLiveBuffer = async (liveBuffer, getLiveError, stopBuffering) => {
		let index = 0;
		let lastFlushedId;
		while (index < liveBuffer.length) {
			throwLiveError(getLiveError());
			const event = liveBuffer[index];
			if (event === void 0) throw new RetryableStreamError("Live stream buffer index missing");
			const item = await options.processLive(event);
			await deliverItem(item, true, false);
			lastFlushedId = item.id;
			index += 1;
		}
		liveBuffer.length = 0;
		throwLiveError(getLiveError());
		compactDeliveredIds(lastFlushedId);
		stopBuffering();
	};
	const compactDeliveredIds = (lastId) => {
		if (!lastId) return;
		deliveredSinceCursor.clear();
		deliveredSinceCursor.add(lastId);
	};
	const catchUpThenConsumeLive = async (cursor) => {
		const live = options.subscribeLive(cursor);
		activeLive = live;
		let buffering = true;
		const liveBuffer = [];
		const livePump = startLivePump(live, () => buffering, liveBuffer);
		try {
			await replayMissed(cursor, livePump.getError);
			await flushLiveBuffer(liveBuffer, livePump.getError, () => {
				buffering = false;
			});
			noteRecovery();
			await livePump.pump;
			throwLiveError(livePump.getError());
		} finally {
			buffering = false;
			if (activeLive === live) activeLive = void 0;
			await closeIterable(live);
			await livePump.pump.catch(ignoreCleanupError);
		}
	};
	const run = async () => {
		while (!closed) {
			const phase = lastCursor ? "catch-up" : "live";
			try {
				if (lastCursor) await catchUpThenConsumeLive(lastCursor);
				else await consumeLive();
			} catch (error) {
				await closeIterable(activeLive).catch(ignoreCleanupError);
				activeLive = void 0;
				if (closed) break;
				const delayMs = handleFailure(error, phase);
				await maybeRecover(error);
				await sleep(delayMs);
			}
		}
		end();
	};
	const pump = run().catch((error) => {
		log.error("resumable stream loop crashed", {
			"spectrum.stream.id": streamId,
			"spectrum.stream.label": label
		}, error);
		if (!closed) end(error);
	});
	return async () => {
		closed = true;
		cancelSleep();
		await closeIterable(activeLive);
		await pump.catch(ignoreCleanupError);
	};
});
//#endregion
//#region src/utils/stream-group.ts
const groupLog = createLogger$1("spectrum.stream");
/**
* A merged stream whose membership can change while it runs.
*
* Sibling of `mergeStreams`, with a deliberately different lifetime contract —
* pick by whether the source set is fixed:
*
*   - `mergeStreams` merges a *fixed* set. Its result represents all of them:
*     it ends when they have all ended (immediately, for an empty set) and one
*     member's error ends the whole merge.
*   - `createStreamGroup` owns a *mutable* set. It ends only via `close()` —
*     zero members parks the consumer rather than ending it, so a set that is
*     briefly empty mid-reconcile does not terminate the stream — and a member
*     that fails is logged and dropped while its siblings keep running.
*
* Dropping a faulted member (rather than restarting it here) keeps retry policy
* with the caller that knows the real inventory: `keys()` stops reporting it, so
* the caller's next reconcile re-adds it through `add`.
*/
function createStreamGroup(options = {}) {
	const label = options.label ?? "stream-group";
	const members = /* @__PURE__ */ new Map();
	let closed = false;
	let spawn;
	const memberAttrs = (key) => ({
		"spectrum.stream.group": label,
		"spectrum.stream.member": key
	});
	const reportMemberExit = (key, member, error) => {
		if (member.detached) return;
		if (error === void 0) {
			groupLog.error("stream group member ended unexpectedly", memberAttrs(key));
			return;
		}
		groupLog.error("stream group member failed", {
			...memberAttrs(key),
			...errorAttrs(error)
		}, error instanceof Error ? error : void 0);
	};
	const runMember = async (key, member, emit) => {
		try {
			const source = member.factory();
			member.source = source;
			for await (const value of source) await emit(value);
			reportMemberExit(key, member);
		} catch (error) {
			reportMemberExit(key, member, error ?? /* @__PURE__ */ new Error("unknown stream error"));
		} finally {
			if (members.get(key) === member) members.delete(key);
		}
	};
	const base = stream((emit) => {
		spawn = (key, member) => {
			member.worker = runMember(key, member, emit);
		};
		for (const [key, member] of members) spawn(key, member);
		return async () => {
			closed = true;
			spawn = void 0;
			const attached = Array.from(members.values());
			for (const member of attached) member.detached = true;
			await Promise.allSettled(attached.map((member) => member.source?.close()));
			await Promise.allSettled(attached.map((member) => member.worker));
		};
	});
	const closeBase = base.close.bind(base);
	return Object.assign(base, {
		add: (key, source) => {
			if (closed || members.has(key)) return false;
			const member = {
				detached: false,
				factory: source
			};
			members.set(key, member);
			spawn?.(key, member);
			return true;
		},
		close: async () => {
			closed = true;
			await closeBase();
		},
		has: (key) => members.has(key),
		keys: () => Array.from(members.keys()),
		remove: async (key) => {
			const member = members.get(key);
			if (!member) return false;
			member.detached = true;
			members.delete(key);
			await member.source?.close();
			return true;
		}
	});
}
//#endregion
export { addMemberSchema, asAttachment, asContact, asCustom, asGroup, asMarkdown, asPoll, asPollOption, asReaction, asRead, asReply, asRichlink, asText, asUnsend, asVoice, avatarSchema, buildPhotoAction, createLogger, createStreamGroup, createTokenRenewal, ensureM4a, envAwareConfig, envFor, errorAttrs, fromEnv, groupSchema, leaveSpaceSchema, messageEffectSchema, photoActionSchema, reactionSchema, readSchema, removeMemberSchema, renameSchema, renderInlineTokens, replySchema, resumableOrderedStream, sanitizeEmail, sanitizeErrorMessage, sanitizePhone, sanitizeUrl, setLogLevel, tracedFetch };
