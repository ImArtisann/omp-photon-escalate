import { ErrorCode, NotFoundError, ValidationError, createGrpcClient } from "@photon-ai/advanced-imessage/grpc";
import { sanitizePhone, withSpan } from "@photon-ai/otel";
import { UnsupportedError, appLayoutSchema, cloud, definePlatform, fromVCard, mergeStreams, read, text, toVCard } from "../core/index.js";
import { addMemberSchema, asAttachment, asContact, asCustom, asGroup, asPoll, asPollOption, asReply, asText, asVoice, avatarSchema, buildPhotoAction, createLogger, createStreamGroup, createTokenRenewal, ensureM4a, errorAttrs, groupSchema, leaveSpaceSchema, messageEffectSchema, photoActionSchema, reactionSchema, readSchema, removeMemberSchema, renameSchema, resumableOrderedStream, sanitizeErrorMessage } from "../core/authoring.js";
import z from "zod";
import { Marked } from "marked";
import { LRUCache } from "lru-cache";
//#region src/content/background.ts
/**
* iMessage-only chat background content. Lives entirely under the iMessage
* provider — never enters the universal `Content` discriminated union. The
* framework recognizes it via two generic content-level contracts:
*
* 1. `__platform: "imessage"` — `findUnsupportedPlatformContent` in
*    `platform/build.ts` reads this tag and warns-and-skips when a different
*    platform receives it.
* 2. `__fireAndForget: true` — `dispatchSend`'s fire-and-forget check
*    treats this as a side-effecting send that returns no message id, the
*    same way it treats `reaction` / `typing` / `edit`.
*
* iMessage's `send` handler narrows back to `Background` via the `isBackground`
* type guard before dispatching to `chats.setBackground` / `removeBackground`.
*/
const backgroundSchema = z.object({
	type: z.literal("background"),
	__platform: z.literal("imessage"),
	__fireAndForget: z.literal(true),
	action: photoActionSchema
});
const isBackground = (v) => backgroundSchema.safeParse(v).success;
function background(input, options) {
	const action = buildPhotoAction(input, options, "background");
	return { build: async () => backgroundSchema.parse({
		type: "background",
		__platform: "imessage",
		__fireAndForget: true,
		action
	}) };
}
//#endregion
//#region src/content/contact-card.ts
/**
* iMessage-only "share contact card" control signal. Pushes the *local
* account's native contact card* (the name + photo a recipient sees in their
* Messages app) to a chat via the SDK's `chats.shareContactInfo`.
*
* This is Apple's "Share Name and Photo" mechanism — distinct from the
* universal `contact(...)` content, which uploads an arbitrary person's vCard
* as a *file* attachment. There is no payload: the card shared is always the
* bot account's own.
*
* Like `background`, it lives entirely under the iMessage provider and never
* enters the universal `Content` discriminated union. The framework recognizes
* it via two generic content-level contracts:
*
* 1. `__platform: "imessage"` — `findUnsupportedPlatformContent` in
*    `platform/build.ts` reads this tag and warns-and-skips when a different
*    platform receives it.
* 2. `__fireAndForget: true` — `dispatchSend`'s fire-and-forget check treats
*    this as a side-effecting send that returns no message id, the same way it
*    treats `read` / `typing`.
*
* iMessage's `send` handler narrows back via the `isContactCard` type guard
* before dispatching to `chats.shareContactInfo`.
*/
const contactCardSchema = z.object({
	type: z.literal("contactCard"),
	__platform: z.literal("imessage"),
	__fireAndForget: z.literal(true)
});
const isContactCard = (v) => contactCardSchema.safeParse(v).success;
/**
* Share the bot account's native iMessage contact card (name + photo) with the
* chat. iMessage-only, remote-only.
*
* `space.send(nativeContactCard())` is the canonical form; `space.shareContactCard()`
* is sugar attached via `PlatformDef.space.actions` (only typed on
* `PlatformSpace<IMessageDef>`).
*
* This is an explicit, on-demand share and always fires — unlike the automatic
* best-effort share gated behind the `imessageSynced` project profile, which
* dedupes to once per chat per 24h (see `remote/contact-share.ts`). Works in
* both DMs and group chats; the recipient chooses whether to accept the card.
*
* `ContactCard` is intentionally not a member of the universal `Content`
* union — the `as unknown as Content` cast keeps the builder shape compatible
* with the framework's `ContentBuilder.build(): Promise<Content>` signature.
* The framework treats it as a fire-and-forget control signal at runtime.
*/
function nativeContactCard() {
	return { build: async () => contactCardSchema.parse({
		type: "contactCard",
		__platform: "imessage",
		__fireAndForget: true
	}) };
}
//#endregion
//#region src/content/customized-mini-app.ts
const layoutSchema = appLayoutSchema;
/**
* iMessage-only mini-app card content. Lives entirely under the iMessage
* provider — never enters the universal `Content` discriminated union. The
* framework recognizes it via the generic content-level platform contract:
*
* - `__platform: "imessage"` — `findUnsupportedPlatformContent` reads this tag
*   and warns-and-skips when a different platform receives it.
*
* Unlike `background` / `read`, this content is **not** `__fireAndForget`: it
* produces a real outbound message, so the iMessage `send` handler narrows
* back to `CustomizedMiniApp` via the `isCustomizedMiniApp` guard and returns
* the resulting `ProviderMessageRecord` (rather than `void`).
*/
const customizedMiniAppSchema = z.object({
	type: z.literal("customized-mini-app"),
	__platform: z.literal("imessage"),
	appName: z.string().nonempty(),
	appStoreId: z.number().int().positive().optional(),
	extensionBundleId: z.string().nonempty(),
	layout: layoutSchema,
	live: z.boolean().optional(),
	teamId: z.string(),
	url: z.url()
});
const isCustomizedMiniApp = (v) => customizedMiniAppSchema.safeParse(v).success;
const asCustomizedMiniApp = (input) => customizedMiniAppSchema.parse({
	type: "customized-mini-app",
	__platform: "imessage",
	...input
});
/**
* Construct a `customized-mini-app` content value. iMessage-only, remote-only.
*
* The layout is what recipients see in the bubble. `teamId` and
* `extensionBundleId` identify the iMessage extension that receives `url` when
* the recipient taps the card; the server constructs the matching
* `MSMessageExtensionBalloonPlugin` plugin id from these values. `appStoreId`
* is optional and only points recipients without the extension at its App
* Store entry. `live` is optional; when omitted, the remote server keeps the
* static layout preview visible.
*
* `space.send(customizedMiniApp(...))` is the canonical form.
*
* `CustomizedMiniApp` is intentionally not a member of the universal `Content`
* union — the `as unknown as Content` cast keeps the builder shape compatible
* with the framework's `ContentBuilder.build(): Promise<Content>` signature.
*/
function customizedMiniApp(input) {
	return { build: async () => asCustomizedMiniApp(input) };
}
//#endregion
//#region src/content/effect.ts
const messageEffects = {
	balloons: "com.apple.messages.effect.CKBalloonEffect",
	celebration: "com.apple.messages.effect.CKHappyBirthdayEffect",
	confetti: "com.apple.messages.effect.CKConfettiEffect",
	echo: "com.apple.messages.effect.CKEchoEffect",
	fireworks: "com.apple.messages.effect.CKFireworksEffect",
	gentle: "com.apple.MobileSMS.expressivesend.gentle",
	heart: "com.apple.messages.effect.CKHeartEffect",
	invisible: "com.apple.MobileSMS.expressivesend.invisibleink",
	lasers: "com.apple.messages.effect.CKLasersEffect",
	loud: "com.apple.MobileSMS.expressivesend.loud",
	slam: "com.apple.MobileSMS.expressivesend.impact",
	sparkles: "com.apple.messages.effect.CKSparklesEffect",
	spotlight: "com.apple.messages.effect.CKSpotlightEffect"
};
const SUPPORTED_EFFECTS = new Set(Object.values(messageEffects));
const resolveContent = (input) => typeof input === "string" ? text(input).build() : input.build();
function effect(input, messageEffect) {
	return { build: async () => {
		if (!SUPPORTED_EFFECTS.has(messageEffect)) throw new Error(`Unsupported iMessage message effect "${messageEffect}"`);
		const inner = await resolveContent(input);
		if (inner.type !== "text" && inner.type !== "markdown" && inner.type !== "attachment") throw new Error(`imessage effect() only supports text, markdown, and attachment content, got "${inner.type}"`);
		return messageEffectSchema.parse({
			type: "effect",
			content: inner,
			effect: messageEffect
		});
	} };
}
//#endregion
//#region src/lines.ts
const linesLog = createLogger("spectrum.imessage.lines");
const observers = /* @__PURE__ */ new WeakMap();
const lineIds = /* @__PURE__ */ new WeakMap();
let fallbackKeys = 0;
/** Pairs an entry with the cloud instance it was built from. */
const setLineId = (entry, instanceId) => {
	lineIds.set(entry, instanceId);
};
/**
* Stable per-entry key. Falls back to a generated id for explicitly-configured
* clients, which carry no instance id and may legitimately repeat a phone
* number — phone alone would collide.
*/
const lineKey = (entry) => {
	const existing = lineIds.get(entry);
	if (existing) return existing;
	fallbackKeys += 1;
	const generated = `line-${fallbackKeys}`;
	lineIds.set(entry, generated);
	return generated;
};
/**
* Registers `observer` and returns a disposer that removes just this one, so a
* closed message stream stops being called into. `clearLineObservers` remains
* the whole-array teardown used when the client itself is destroyed.
*/
const addLineObserver = (clients, observer) => {
	const existing = observers.get(clients);
	if (existing) existing.add(observer);
	else observers.set(clients, new Set([observer]));
	return () => {
		const current = observers.get(clients);
		if (!current?.delete(observer)) return;
		if (current.size === 0) observers.delete(clients);
	};
};
const clearLineObservers = (clients) => {
	observers.delete(clients);
};
/**
* Synchronous by contract: `reconcile` calls this immediately after pushing the
* entry, with no await in between, so an observer can never see a half-applied
* array. A throwing observer is contained — it must not be able to reject the
* token refresh, which would stall renewal for every line.
*/
const notifyLineAttached = (clients, entry) => {
	for (const observer of observers.get(clients) ?? []) try {
		observer.attach(entry);
	} catch (error) {
		linesLog.warn("imessage line observer failed to attach", errorAttrs(error), error instanceof Error ? error : void 0);
	}
};
/**
* Returns each observer's detach promise so the caller can settle them off the
* refresh path — a wedged stream close must not stall token renewal.
*/
const notifyLineDetached = (clients, entry) => {
	const pending = [];
	for (const observer of observers.get(clients) ?? []) try {
		pending.push(Promise.resolve(observer.detach(entry)));
	} catch (error) {
		linesLog.warn("imessage line observer failed to detach", errorAttrs(error), error instanceof Error ? error : void 0);
	}
	return pending;
};
//#endregion
//#region src/types.ts
/**
* Sentinel phone for shared-token mode. The single shared client serves an
* unknown set of numbers (the SDK exposes no recipient field on inbound and
* no `from` parameter on send), so all routing through it tags this sentinel.
*/
const SHARED_PHONE = "shared";
const clientEntry = z.object({
	address: z.string(),
	token: z.string(),
	phone: z.string()
});
const configSchema = z.strictObject({ clients: clientEntry.or(z.array(clientEntry)).optional() });
/**
* iMessage sender identity. `id` is the cross-provider key (the address);
* `address`/`country`/`service` mirror the SDK's `SingleServiceAddressInfo`,
* letting apps tell iMessage from SMS/RCS. All optional because actor-less
* events cannot always supply them.
*/
const SERVICE_VALUES = [
	"iMessage",
	"SMS",
	"RCS",
	"unknown"
];
const userSchema = z.object({
	address: z.string().optional(),
	country: z.string().optional(),
	service: z.enum(SERVICE_VALUES).optional()
});
const spaceSchema = z.object({
	id: z.string(),
	type: z.enum(["dm", "group"]),
	phone: z.string()
});
const spaceParamsSchema = z.object({ phone: z.string().optional() });
const miniAppCardSessionSchema = z.object({
	chatGuid: z.string(),
	messageGuid: z.string(),
	sessionId: z.string(),
	targetMessageGuid: z.string()
});
const textFormatSchema = z.object({
	effectName: z.string().optional(),
	length: z.number().int().nonnegative(),
	start: z.number().int().nonnegative(),
	type: z.string()
}).readonly();
const mentionSchema = z.object({
	address: z.string(),
	length: z.number().int().nonnegative(),
	start: z.number().int().nonnegative()
}).readonly();
const attachmentMetadataSchema = z.object({
	companionKind: z.enum(["live-photo-video", "unknown"]).optional(),
	fileName: z.string(),
	guid: z.string(),
	isHidden: z.boolean(),
	isSticker: z.boolean(),
	mimeType: z.string(),
	originalGuid: z.string().optional(),
	totalBytes: z.number().nonnegative(),
	transferState: z.enum([
		"pending",
		"transferring",
		"failed",
		"finished",
		"unknown"
	]),
	uti: z.string()
}).readonly();
const serviceAddressSchema = z.object({
	address: z.string(),
	country: z.string().optional(),
	service: z.enum(SERVICE_VALUES)
}).readonly();
const reactionSchema$1 = z.object({
	emoji: z.string().optional(),
	kind: z.enum([
		"love",
		"like",
		"dislike",
		"laugh",
		"emphasize",
		"question",
		"emoji",
		"sticker",
		"unknown"
	])
}).readonly();
const appliedReactionSchema = z.object({
	dateCreated: z.date(),
	isFromMe: z.boolean(),
	messageGuid: z.string(),
	reaction: reactionSchema$1,
	sender: serviceAddressSchema.optional(),
	targetPartIndex: z.number().int().nonnegative().optional()
}).readonly();
const stickerPlacementSchema = z.object({
	rotation: z.number().optional(),
	scale: z.number().optional(),
	width: z.number().nonnegative().optional(),
	x: z.number(),
	y: z.number()
}).readonly();
const placedStickerSchema = z.object({
	dateCreated: z.date(),
	isFromMe: z.boolean(),
	messageGuid: z.string(),
	placement: stickerPlacementSchema.optional(),
	sender: serviceAddressSchema.optional(),
	sticker: attachmentMetadataSchema.optional(),
	targetPartIndex: z.number().int().nonnegative().optional()
}).readonly();
const reactionRecordSchema = z.object({
	reaction: reactionSchema$1,
	selected: z.boolean().optional(),
	targetGuid: z.string(),
	targetPartIndex: z.number().int().nonnegative().optional()
}).readonly();
/**
* iMessage-specific per-message metadata surfaced on `IMessageMessage`.
* Native metadata is optional because synthetic event records do not carry a
* complete Advanced iMessage message.
*
* - `partIndex`: ordered part index within a multi-part message. Text and
*   attachment parts both consume an index (0 for bare or single-part
*   messages; 0..N-1 for a group's sub-items).
* - `parentId`: guid of the parent message for a group sub-item. Undefined
*   when the message itself is the parent.
* - `miniAppCardSession`: stable handle returned by mini-app card sends and
*   updates. It is required to update the card in place later.
*/
const messageSchema = z.object({
	dateDelivered: z.date().optional(),
	dateEdited: z.date().optional(),
	dateExpressiveSendPlayed: z.date().optional(),
	datePlayed: z.date().optional(),
	dateRead: z.date().optional(),
	dateRetracted: z.date().optional(),
	isSent: z.boolean(),
	isDelivered: z.boolean(),
	isDeliveredQuietly: z.boolean(),
	didNotifyRecipient: z.boolean(),
	isDelayed: z.boolean(),
	sendErrorCode: z.number().int(),
	nativeText: z.string().optional(),
	formatting: z.array(textFormatSchema).readonly(),
	mentions: z.array(mentionSchema).readonly(),
	subject: z.string().optional(),
	balloonBundleId: z.string().optional(),
	expressiveSendStyleId: z.string().optional(),
	attachmentMetadata: z.array(attachmentMetadataSchema).readonly(),
	appliedReactions: z.array(appliedReactionSchema).readonly(),
	placedStickers: z.array(placedStickerSchema).readonly(),
	reactionRecord: reactionRecordSchema.optional(),
	itemType: z.enum([
		"normal",
		"groupNameChange",
		"participantChange",
		"chatAction",
		"unknown"
	]),
	groupTitle: z.string().optional(),
	partCount: z.number().int().nonnegative().optional(),
	isAutoReply: z.boolean(),
	isCorrupt: z.boolean(),
	isExpirable: z.boolean(),
	isServiceMessage: z.boolean(),
	isSpam: z.boolean(),
	isSystemMessage: z.boolean()
}).partial().extend({
	miniAppCardSession: miniAppCardSessionSchema.optional(),
	partIndex: z.number().int().nonnegative().optional(),
	parentId: z.string().optional()
});
//#endregion
//#region src/auth.ts
const FORCE_REFRESH_MIN_INTERVAL_MS = 5e3;
const authLog = createLogger("spectrum.imessage.auth");
const cloudAuthState = /* @__PURE__ */ new WeakMap();
const instanceAttrs = (instanceId) => ({ "spectrum.imessage.instance": instanceId });
async function createCloudClients(projectId, projectSecret) {
	let tokenData = await cloud.issueImessageTokens(projectId, projectSecret);
	let lastRefreshAt = Date.now();
	const entries = [];
	const records = /* @__PURE__ */ new Map();
	const buildEntry = (instanceId, phone, initialToken) => ({
		phone,
		client: createGrpcClient({
			address: `${instanceId}.imsg.photon.codes:443`,
			autoIdempotency: true,
			retry: true,
			tls: true,
			token: async () => {
				await renewal.refreshIfNeeded();
				if (tokenData.type !== "dedicated") return initialToken;
				return tokenData.auth[instanceId] ?? initialToken;
			}
		})
	});
	const retire = async (entry) => {
		await Promise.allSettled(notifyLineDetached(entries, entry));
		await entry.client.close();
	};
	const removeMissing = (data) => {
		let removed = 0;
		for (const [instanceId, entry] of records) {
			if (data.auth[instanceId]) continue;
			records.delete(instanceId);
			const index = entries.indexOf(entry);
			if (index >= 0) entries.splice(index, 1);
			removed += 1;
			retire(entry).catch((error) => {
				authLog.warn("failed to retire imessage line", {
					...instanceAttrs(instanceId),
					...errorAttrs(error)
				}, error instanceof Error ? error : void 0);
			});
		}
		return removed;
	};
	const addOrSync = (data) => {
		let added = 0;
		for (const [instanceId, token] of Object.entries(data.auth)) {
			const phone = data.numbers?.[instanceId];
			const existing = records.get(instanceId);
			if (existing) {
				if (phone) existing.phone = phone;
				else authLog.warn("imessage line lost its phone number; keeping the last known number", instanceAttrs(instanceId));
				notifyLineAttached(entries, existing);
				continue;
			}
			if (!phone) {
				authLog.warn("skipping imessage line without a phone number", instanceAttrs(instanceId));
				continue;
			}
			const entry = buildEntry(instanceId, phone, token);
			setLineId(entry, instanceId);
			records.set(instanceId, entry);
			entries.push(entry);
			notifyLineAttached(entries, entry);
			added += 1;
		}
		return added;
	};
	/**
	* Brings the client set in line with the token payload, which is the only
	* inventory the cloud exposes: keys present but untracked are newly
	* provisioned, tracked keys that vanished were deprovisioned.
	*
	* An empty payload means the project has no lines, not that the response is
	* suspect — keeping entries the payload no longer covers would leave the
	* client routing through channels whose tokens have stopped being refreshed.
	* A genuinely malformed payload (no `auth` at all) throws instead, which the
	* caller contains before any line is removed.
	*/
	const reconcile = (data) => {
		const removed = removeMissing(data);
		const added = addOrSync(data);
		if (added > 0 || removed > 0) authLog.info("imessage lines reconciled", {
			"spectrum.imessage.lines.added": added,
			"spectrum.imessage.lines.removed": removed,
			"spectrum.imessage.lines.total": entries.length
		});
	};
	const renewal = createTokenRenewal({
		expiresInSeconds: () => tokenData.expiresIn,
		name: "imessage",
		refresh: async () => {
			tokenData = await cloud.issueImessageTokens(projectId, projectSecret);
			lastRefreshAt = Date.now();
			if (tokenData.type !== "dedicated") return;
			try {
				reconcile(tokenData);
			} catch (error) {
				authLog.error("imessage line reconcile failed", errorAttrs(error), error instanceof Error ? error : void 0);
			}
		}
	});
	const forceRefresh = async () => {
		if (Date.now() - lastRefreshAt < FORCE_REFRESH_MIN_INTERVAL_MS) return;
		await renewal.forceRefresh();
	};
	const cloudAuth = {
		dispose: renewal.dispose,
		forceRefresh
	};
	if (tokenData.type === "shared") {
		const address = process.env.SPECTRUM_IMESSAGE_ADDRESS ?? "imessage.spectrum.photon.codes:443";
		entries.push({
			phone: SHARED_PHONE,
			client: createGrpcClient({
				address,
				autoIdempotency: true,
				retry: true,
				tls: true,
				token: async () => {
					await renewal.refreshIfNeeded();
					return tokenData.token;
				}
			})
		});
		cloudAuthState.set(entries, cloudAuth);
		return entries;
	}
	reconcile(tokenData);
	cloudAuthState.set(entries, cloudAuth);
	return entries;
}
async function disposeCloudAuth(clients) {
	clearLineObservers(clients);
	const auth = cloudAuthState.get(clients);
	if (auth) {
		auth.dispose();
		cloudAuthState.delete(clients);
	}
}
/**
* The recover hook for a cloud-backed client array: forces a token re-mint so a
* persistently-failing stream (server rejecting an unexpired token after a
* restart) gets a fresh bearer on its next reconnect. Returns undefined for
* explicitly-configured (static-token) clients, which have nothing to re-mint.
*/
function getCloudRecover(clients) {
	return cloudAuthState.get(clients)?.forceRefresh;
}
//#endregion
//#region src/cache.ts
const DEFAULT_MAX = 1e3;
/**
* Bounded insertion-order cache of recently-seen iMessage messages, keyed by
* guid. Provides O(1) lookup for reaction target resolution. When capacity is
* exceeded, the oldest entry is evicted. Access does not promote recency —
* this is a bounded FIFO, not an LRU. The workload (reactions arriving shortly
* after the message they target) doesn't benefit from LRU semantics, and FIFO
* avoids a dependency.
*/
var MessageCache = class {
	map = /* @__PURE__ */ new Map();
	max;
	constructor(max = DEFAULT_MAX) {
		this.max = max;
	}
	get(id) {
		return this.map.get(id);
	}
	set(id, message) {
		if (this.map.has(id)) this.map.delete(id);
		this.map.set(id, message);
		if (this.map.size > this.max) {
			const first = this.map.keys().next().value;
			if (first !== void 0) this.map.delete(first);
		}
	}
	clear() {
		this.map.clear();
	}
};
/**
* Bounded insertion-order cache of recently-seen iMessage polls, keyed by
* poll message guid. The public poll shape deliberately hides provider ids;
* `optionsByIdentifier` keeps the private lookup table needed to correlate
* vote events back to public `PollChoice` objects.
*/
var PollCache = class {
	map = /* @__PURE__ */ new Map();
	max;
	constructor(max = DEFAULT_MAX) {
		this.max = max;
	}
	get(id) {
		return this.map.get(id);
	}
	set(id, poll) {
		if (this.map.has(id)) this.map.delete(id);
		this.map.set(id, poll);
		if (this.map.size > this.max) {
			const first = this.map.keys().next().value;
			if (first !== void 0) this.map.delete(first);
		}
	}
	clear() {
		this.map.clear();
	}
};
const messageCaches = /* @__PURE__ */ new WeakMap();
const pollCaches = /* @__PURE__ */ new WeakMap();
/**
* Returns a per-client message cache. Keyed by the client array so each
* iMessage provider instance has its own cache and multiple providers don't
* share state accidentally.
*/
const getMessageCache = (owner) => {
	let cache = messageCaches.get(owner);
	if (!cache) {
		cache = new MessageCache();
		messageCaches.set(owner, cache);
	}
	return cache;
};
const getPollCache = (owner) => {
	let cache = pollCaches.get(owner);
	if (!cache) {
		cache = new PollCache();
		pollCaches.set(owner, cache);
	}
	return cache;
};
//#endregion
//#region src/platform.ts
const IMESSAGE_PLATFORM = "imessage";
//#endregion
//#region src/remote/ids.ts
const PART_PREFIX = /^p:(\d+)\//;
const dmChatGuid = (address) => `any;-;${address}`;
const chatTypeFromGuid = (guid) => guid.includes(";+;") ? "group" : "dm";
const dmPeerFromChatGuid = (guid) => {
	if (chatTypeFromGuid(guid) !== "dm") return;
	const separator = guid.indexOf(";-;");
	if (separator < 0) return;
	return guid.slice(separator + 3) || void 0;
};
const toChatGuid = (value) => value;
const toMessageGuid = (value) => value;
const formatChildId = (partIndex, parentGuid) => `p:${partIndex}/${parentGuid}`;
const parseChildId = (id) => {
	const match = id.match(PART_PREFIX);
	if (!match) return null;
	return {
		parentGuid: id.replace(PART_PREFIX, ""),
		partIndex: Number(match[1])
	};
};
//#endregion
//#region src/remote/avatar.ts
/**
* Apply an `Avatar` content value to a remote iMessage group chat.
*
* `set` uploads the icon bytes via `groups.setIcon`; `clear` removes the
* current icon via `groups.removeIcon`. Both surfaces are fire-and-forget —
* no message id is produced. The caller (`handleAvatar` in the iMessage
* provider) is responsible for the group-only / remote-only guards.
*/
const setIcon$1 = async (remote, spaceId, content) => {
	const chat = toChatGuid(spaceId);
	if (content.action.kind === "clear") {
		await remote.groups.removeIcon(chat);
		return;
	}
	const buffer = await content.action.read();
	await remote.groups.setIcon(chat, new Uint8Array(buffer.buffer, buffer.byteOffset, buffer.byteLength));
};
/**
* Download the current icon of a remote iMessage group chat. Resolves
* `undefined` when the group has no icon (the SDK's `NotFoundError` with
* code `groupIconNotFound`); any other error — including a `chatNotFound`
* `NotFoundError` — propagates. Bytes are copied into a Buffer so the result
* round-trips into `setIcon` / `space.avatar(...)`. The caller (`getAvatar`
* in the iMessage provider) owns the group-only / remote-only guards.
*/
const getIcon$1 = async (remote, spaceId) => {
	try {
		const icon = await remote.groups.getIcon(toChatGuid(spaceId));
		return {
			data: Buffer.from(icon.data),
			mimeType: icon.mimeType
		};
	} catch (err) {
		if (err instanceof NotFoundError && err.code === "groupIconNotFound") return;
		throw err;
	}
};
//#endregion
//#region src/remote/background.ts
/**
* Apply a `Background` content value to a remote iMessage chat.
*
* `set` uploads the photo bytes via `chats.setBackground`; `clear` removes
* any current background via `chats.removeBackground`. Both surfaces are
* fire-and-forget — no message id is produced.
*/
const setBackground$1 = async (remote, spaceId, content) => {
	const chat = toChatGuid(spaceId);
	if (content.action.kind === "clear") {
		await remote.chats.removeBackground(chat);
		return;
	}
	const buffer = await content.action.read();
	await remote.chats.setBackground(chat, new Uint8Array(buffer.buffer, buffer.byteOffset, buffer.byteLength));
};
//#endregion
//#region src/remote/contact-card.ts
/**
* Share the local account's native contact card (name + photo) with the chat.
*
* The SDK exposes a single chat-level `chats.shareContactInfo(chatGuid)` — the
* card shared is always the bot account's own, so there is no payload beyond
* the chat. `send` has already resolved the space into `spaceId` by the time
* the dispatcher reaches here.
*
* On-demand and unconditional: unlike the proactive `ContactShareTracker` in
* `contact-share.ts` (24h dedupe, gated behind the `imessageSynced` profile),
* this fires every time the caller asks.
*/
const shareContactCard$1 = async (remote, spaceId) => {
	await remote.chats.shareContactInfo(toChatGuid(spaceId));
};
//#endregion
//#region src/remote/customized-mini-app.ts
const toProviderRecord = (result, content, spaceId) => ({
	id: result.guid,
	content,
	direction: "outbound",
	miniAppCardSession: result.miniAppCardSession,
	space: { id: spaceId },
	timestamp: result.dateCreated
});
/**
* Send a `CustomizedMiniApp` card to a remote iMessage chat.
*
* Unlike `setBackground`, this produces a real outbound message, so it returns
* a `ProviderMessageRecord`. The `content` carries extra `type` / `__platform`
* tags the SDK ignores; it is passed as a variable (not an object literal) so
* no excess-property check applies, and the wire serializer reads only the
* fields it knows.
*/
const sendCustomizedMiniApp$1 = async (remote, spaceId, content) => {
	const chat = toChatGuid(spaceId);
	return toProviderRecord(await remote.messages.sendCustomizedMiniApp(chat, content), content, spaceId);
};
const updateCustomizedMiniApp$1 = async (remote, spaceId, session, content) => {
	return toProviderRecord(await remote.messages.updateCustomizedMiniApp(session, content), content, spaceId);
};
//#endregion
//#region src/shared/audio.ts
const AUDIO_MIME_PATTERN = /^audio\//i;
const CAF_MIME_TYPE = "audio/x-caf";
const CAF_UTI = "com.apple.coreaudio-format";
const GENERIC_BINARY_MIME_TYPE = "application/octet-stream";
const isCafAttachment = (attachment) => attachment.uti?.toLowerCase() === CAF_UTI || attachment.fileName?.toLowerCase().endsWith(".caf") === true;
const normalizeAppleAttachmentMimeType = (attachment) => attachment.mimeType.toLowerCase() === GENERIC_BINARY_MIME_TYPE && isCafAttachment(attachment) ? CAF_MIME_TYPE : attachment.mimeType;
const appleAudioMimeType = (attachment) => {
	const mimeType = normalizeAppleAttachmentMimeType(attachment);
	return AUDIO_MIME_PATTERN.test(mimeType) ? mimeType : void 0;
};
const addTextPart = (parts, text) => {
	const trimmed = text?.trim();
	if (trimmed) parts.push({
		type: "text",
		text: trimmed
	});
};
const addAttachmentParts = (parts, attachments) => {
	for (const attachment of attachments) if (attachment) parts.push({
		type: "attachment",
		attachment
	});
};
const toOrderedParts = (text, attachments) => {
	const parts = [];
	if (!text) {
		addAttachmentParts(parts, attachments);
		return parts;
	}
	if (!text.includes("￼")) {
		addAttachmentParts(parts, attachments);
		addTextPart(parts, text);
		return parts;
	}
	const textSegments = text.split("￼");
	for (let i = 0; i < attachments.length; i++) {
		addTextPart(parts, textSegments[i]);
		const attachment = attachments[i];
		if (attachment) parts.push({
			type: "attachment",
			attachment
		});
	}
	addTextPart(parts, textSegments.slice(attachments.length).join(""));
	return parts;
};
//#endregion
//#region src/shared/vcard.ts
const VCARD_MIME_TYPES = new Set([
	"text/vcard",
	"text/x-vcard",
	"text/directory",
	"application/vcard",
	"application/x-vcard"
]);
const normalizeMimeType = (mimeType) => (mimeType.split(";")[0] ?? "").trim().toLowerCase();
const isVCardAttachment = (mimeType, fileName) => {
	if (mimeType && VCARD_MIME_TYPES.has(normalizeMimeType(mimeType))) return true;
	return Boolean(fileName?.toLowerCase().endsWith(".vcf"));
};
const vcardFileName = (contact) => {
	return `${(contact.name?.formatted ?? contact.user?.id ?? "contact").replace(/[^a-zA-Z0-9_\-.]/g, "_")}.vcf`;
};
//#endregion
//#region src/remote/attachments.ts
/**
* Stream the primary file bytes of an attachment as a `ReadableStream`.
* Skips header and Live Photo companion frames; emits only `primaryChunk`
* payloads. Cleans up the underlying gRPC iterator on cancel and on error.
*/
const downloadPrimaryAttachmentStream = (client, attachmentGuid) => {
	const frames = client.attachments.downloadStream(attachmentGuid);
	const iterator = frames[Symbol.asyncIterator]();
	let closed = false;
	const closeFrames = async () => {
		if (closed) return;
		closed = true;
		try {
			await iterator.return?.();
		} finally {
			await frames.close();
		}
	};
	return new ReadableStream({
		async cancel() {
			await closeFrames();
		},
		async pull(controller) {
			try {
				while (true) {
					const result = await iterator.next();
					if (result.done) {
						controller.close();
						await closeFrames();
						return;
					}
					if (result.value.type === "primaryChunk") {
						controller.enqueue(result.value.data);
						return;
					}
				}
			} catch (error) {
				await closeFrames();
				throw error;
			}
		}
	});
};
/**
* Collect the primary file bytes of an attachment into a single `Buffer`.
* Skips header and Live Photo companion frames.
*/
const downloadPrimaryAttachment = async (client, attachmentGuid) => {
	const chunks = [];
	const frames = client.attachments.downloadStream(attachmentGuid);
	try {
		for await (const frame of frames) if (frame.type === "primaryChunk") chunks.push(Buffer.from(frame.data));
	} finally {
		await frames.close();
	}
	return Buffer.concat(chunks);
};
/**
* Fetch an attachment by GUID and wrap it as a spectrum `Attachment`. The
* returned object is lazy: `.read()` triggers a Buffer download, `.stream()`
* opens a fresh byte stream. Calling both issues two independent gRPC
* downloads — cache `.read()` if you need the bytes more than once.
*
* Returns `undefined` when the GUID is unknown to the server.
*/
const getRemoteAttachment = async (client, guid) => {
	let info;
	try {
		info = await client.attachments.get(guid);
	} catch (err) {
		if (err instanceof NotFoundError) return;
		throw err;
	}
	return asAttachment({
		id: info.guid,
		name: info.fileName,
		mimeType: normalizeAppleAttachmentMimeType(info),
		size: info.totalBytes,
		read: () => downloadPrimaryAttachment(client, info.guid),
		stream: async () => downloadPrimaryAttachmentStream(client, info.guid)
	});
};
//#endregion
//#region src/remote/message-metadata.ts
const toTextFormat = (format) => ({
	effectName: format.effectName,
	length: format.length,
	start: format.start,
	type: format.type
});
const toMention = (mention) => ({
	address: mention.address,
	length: mention.length,
	start: mention.start
});
const toAttachmentMetadata = (attachment) => ({
	companionKind: attachment.companionKind,
	fileName: attachment.fileName,
	guid: attachment.guid,
	isHidden: attachment.isHidden,
	isSticker: attachment.isSticker,
	mimeType: attachment.mimeType,
	originalGuid: attachment.originalGuid,
	totalBytes: attachment.totalBytes,
	transferState: attachment.transferState,
	uti: attachment.uti
});
const toServiceAddress = (sender) => ({
	address: sender.address,
	country: sender.country,
	service: sender.service
});
const toReaction = (reaction) => ({
	emoji: reaction.emoji,
	kind: reaction.kind
});
const toAppliedReaction = (applied) => ({
	dateCreated: applied.dateCreated,
	isFromMe: applied.isFromMe,
	messageGuid: applied.messageGuid,
	reaction: toReaction(applied.reaction),
	sender: applied.sender ? toServiceAddress(applied.sender) : void 0,
	targetPartIndex: applied.targetPartIndex
});
const toStickerPlacement = (placement) => ({
	rotation: placement.rotation,
	scale: placement.scale,
	width: placement.width,
	x: placement.x,
	y: placement.y
});
const toPlacedSticker = (placed) => ({
	dateCreated: placed.dateCreated,
	isFromMe: placed.isFromMe,
	messageGuid: placed.messageGuid,
	placement: placed.placement ? toStickerPlacement(placed.placement) : void 0,
	sender: placed.sender ? toServiceAddress(placed.sender) : void 0,
	sticker: placed.sticker ? toAttachmentMetadata(placed.sticker) : void 0,
	targetPartIndex: placed.targetPartIndex
});
const toReactionRecord = (message) => {
	if (!(message.reaction && message.reactionTargetGuid)) return;
	return {
		reaction: toReaction(message.reaction),
		selected: message.reactionSelected,
		targetGuid: message.reactionTargetGuid,
		targetPartIndex: message.reactionTargetPartIndex
	};
};
/**
* Copy the curated, developer-actionable subset of an Advanced iMessage
* message into Spectrum-owned data. Keeping this mapper at the native boundary
* prevents private Apple fields from leaking through normal messages.
*/
const toMessageMetadata = (native) => ({
	dateDelivered: native.dateDelivered,
	dateEdited: native.dateEdited,
	dateExpressiveSendPlayed: native.dateExpressiveSendPlayed,
	datePlayed: native.datePlayed,
	dateRead: native.dateRead,
	dateRetracted: native.dateRetracted,
	isSent: native.isSent,
	isDelivered: native.isDelivered,
	isDeliveredQuietly: native.isDeliveredQuietly,
	didNotifyRecipient: native.didNotifyRecipient,
	isDelayed: native.isDelayed,
	sendErrorCode: native.sendErrorCode,
	nativeText: native.content?.text,
	formatting: native.content?.formatting?.map(toTextFormat) ?? [],
	mentions: native.content?.mentions?.map(toMention) ?? [],
	subject: native.subject,
	balloonBundleId: native.content?.balloonBundleId,
	expressiveSendStyleId: native.content?.expressiveSendStyleId,
	attachmentMetadata: native.content?.attachments?.map(toAttachmentMetadata) ?? [],
	appliedReactions: native.appliedReactions?.map(toAppliedReaction) ?? [],
	placedStickers: native.placedStickers?.map(toPlacedSticker) ?? [],
	reactionRecord: toReactionRecord(native),
	itemType: native.itemType,
	groupTitle: native.groupTitle,
	partCount: native.partCount,
	isAutoReply: native.isAutoReply,
	isCorrupt: native.isCorrupt,
	isExpirable: native.isExpirable,
	isServiceMessage: native.isServiceMessage,
	isSpam: native.isSpam,
	isSystemMessage: native.isSystemMessage
});
//#endregion
//#region src/remote/inbound.ts
const log$5 = createLogger("spectrum.imessage.inbound");
const messageAttachments = (message) => message.content.attachments;
const resolveChatGuid = (message, hint) => {
	if (hint) return hint;
	return message.chatGuids?.[0] ?? "";
};
/**
* Normalize an Apple address (`message.sender` or an event `actor`) into the
* spectrum sender ref. `id` stays the cross-provider identity key (the
* address); `address`/`country`/`service` are surfaced when present so apps
* can tell iMessage from SMS/RCS. Empty fields are omitted.
*/
const toSenderRef = (addr) => ({
	id: addr?.address ?? "",
	...addr?.address ? { address: addr.address } : {},
	...addr?.country ? { country: addr.country } : {},
	...addr?.service ? { service: addr.service } : {}
});
const isIMessageMessage = (value) => {
	if (typeof value !== "object" || value === null) return false;
	const record = value;
	return typeof record.id === "string" && record.id.length > 0 && typeof record.content === "object" && record.content !== null && typeof record.space === "object" && record.space !== null;
};
const asProviderGroup = (items) => groupSchema.parse({
	type: "group",
	items
});
const asProviderReply = (content, target) => asReply({
	content,
	target
});
const buildMessageBase = (message, chatGuidHint, timestamp, phone) => {
	const chat = resolveChatGuid(message, chatGuidHint);
	return {
		...toMessageMetadata(message),
		direction: message.isFromMe ? "outbound" : "inbound",
		sender: toSenderRef(message.sender),
		space: {
			id: chat,
			type: chatTypeFromGuid(chat),
			phone
		},
		timestamp
	};
};
const toAttachmentContent = (client, info) => asAttachment({
	id: info.guid,
	name: info.fileName,
	mimeType: normalizeAppleAttachmentMimeType(info),
	size: info.totalBytes,
	read: async () => await downloadPrimaryAttachment(client, info.guid),
	stream: async () => downloadPrimaryAttachmentStream(client, info.guid)
});
const toVoiceContent = (client, info, mimeType) => asVoice({
	id: info.guid,
	name: info.fileName,
	mimeType,
	size: info.totalBytes,
	read: async () => await downloadPrimaryAttachment(client, info.guid),
	stream: async () => downloadPrimaryAttachmentStream(client, info.guid)
});
const toVCardContent = async (client, info) => {
	try {
		return asContact(fromVCard((await downloadPrimaryAttachment(client, info.guid)).toString("utf8")));
	} catch (err) {
		log$5.warn("failed to parse vCard attachment; falling back to attachment content", {
			"spectrum.imessage.attachment.guid": info.guid,
			...errorAttrs(err)
		}, err);
		return toAttachmentContent(client, info);
	}
};
const attachmentContent = async (client, info, isVoice) => {
	if (isVCardAttachment(info.mimeType, info.fileName)) return await toVCardContent(client, info);
	const audioMimeType = isVoice ? appleAudioMimeType(info) : void 0;
	return audioMimeType ? toVoiceContent(client, info, audioMimeType) : toAttachmentContent(client, info);
};
const buildAttachmentMessage = async (client, base, info, id, partIndex, parentId, isVoice = false) => {
	const content = await attachmentContent(client, info, isVoice);
	const msg = {
		...base,
		id,
		content,
		partIndex
	};
	if (parentId !== void 0) msg.parentId = parentId;
	return msg;
};
const buildTextMessage = (base, text, id, partIndex, parentId) => {
	const msg = {
		...base,
		id,
		content: asText(text),
		partIndex
	};
	if (parentId !== void 0) msg.parentId = parentId;
	return msg;
};
const buildOrderedPartMessage = async (client, base, part, id, partIndex, parentId, voiceAttachmentGuid) => part.type === "text" ? buildTextMessage(base, part.text, id, partIndex, parentId) : await buildAttachmentMessage(client, base, part.attachment, id, partIndex, parentId, part.attachment.guid === voiceAttachmentGuid);
const unsupportedMessageContent = () => asCustom({ imessage_type: "unsupported-message" });
const buildUnwrappedContentMessage = async (client, base, message, messageGuidStr) => {
	const attachments = messageAttachments(message);
	const voiceAttachmentGuid = message.isAudioMessage ? attachments.find((attachment) => appleAudioMimeType(attachment))?.guid : void 0;
	if (attachments.length === 0) {
		const text = message.content.text;
		return {
			...base,
			id: messageGuidStr,
			content: text ? asText(text) : unsupportedMessageContent()
		};
	}
	const parts = toOrderedParts(message.content.text, attachments);
	if (parts.length === 0) return {
		...base,
		id: messageGuidStr,
		content: unsupportedMessageContent()
	};
	if (parts.length === 1) {
		const part = parts[0];
		if (!part) throw new Error("Unreachable: parts.length === 1 but no element");
		return buildOrderedPartMessage(client, base, part, messageGuidStr, 0, void 0, voiceAttachmentGuid);
	}
	const items = [];
	for (let i = 0; i < parts.length; i++) {
		const part = parts[i];
		if (!part) continue;
		items.push(await buildOrderedPartMessage(client, base, part, formatChildId(i, messageGuidStr), i, messageGuidStr, voiceAttachmentGuid));
	}
	return {
		...base,
		id: messageGuidStr,
		content: asProviderGroup(items)
	};
};
const replyTargetGuid = (message) => message.replyTargetGuid ?? message.threadOriginatorGuid;
const stubReplyTarget = (base, targetGuid) => ({
	id: targetGuid,
	content: asCustom({
		imessage_type: "reply-target",
		stub: true
	}),
	space: base.space
});
const resolveReplyTarget = async (client, base, targetGuid, currentGuid, options) => {
	if (targetGuid === currentGuid || options.visitedReplyGuids?.has(targetGuid)) return stubReplyTarget(base, targetGuid);
	const cached = options.cache?.get(targetGuid);
	if (cached) return cached;
	try {
		const visitedReplyGuids = new Set(options.visitedReplyGuids);
		visitedReplyGuids.add(currentGuid);
		const rebuilt = await rebuildFromAppleMessage(client, await client.messages.get(toMessageGuid(targetGuid)), options.phone, base.space.id, options.cache, visitedReplyGuids);
		if (options.cache) cacheMessage(options.cache, rebuilt);
		return rebuilt;
	} catch (err) {
		if (!(err instanceof NotFoundError)) log$5.warn("failed to resolve iMessage reply target; falling back to stub target", {
			"spectrum.imessage.message.guid": currentGuid,
			"spectrum.imessage.reply.target_guid": targetGuid,
			...errorAttrs(err)
		}, err);
		return stubReplyTarget(base, targetGuid);
	}
};
const buildContentMessage = async (client, base, message, messageGuidStr, options) => {
	const msg = await buildUnwrappedContentMessage(client, base, message, messageGuidStr);
	const targetGuid = replyTargetGuid(message);
	if (!targetGuid) return msg;
	const target = await resolveReplyTarget(client, base, targetGuid, messageGuidStr, options);
	return {
		...msg,
		content: asProviderReply(msg.content, target)
	};
};
const messageGroupContent = (message) => {
	if (message.content.type === "group") return message.content;
	if (message.content.type === "reply" && message.content.content.type === "group") return message.content.content;
};
const rebuildFromAppleMessage = async (client, message, phone, chatGuidHint, cache, visitedReplyGuids) => {
	const messageGuidStr = message.guid;
	return buildContentMessage(client, buildMessageBase(message, chatGuidHint, message.dateCreated ?? /* @__PURE__ */ new Date(), phone), message, messageGuidStr, {
		cache,
		phone,
		visitedReplyGuids
	});
};
const cacheMessage = (cache, message) => {
	cache.set(message.id, message);
	const group = messageGroupContent(message);
	if (group) {
		for (const item of group.items) if (isIMessageMessage(item)) cache.set(item.id, item);
	}
};
/**
* Resolve a guid to the spectrum message it maps to, for events that reference
* their target only by guid (reactions, read receipts). Cache first — outbound
* sends are cached at send time by `cacheRemoteOutbound`, inbound messages when
* they are converted — then one `messages.get` + rebuild, which also warms the
* cache so sibling events for the same guid (a group's other participants) are
* free.
*
* Any failure drops the event: these are secondary signals and must never wedge
* the stream. Deliberately unlike `getMessage` below (the public
* `space.getMessage` action), which rethrows non-`NotFoundError` failures; and
* unlike it this never resolves `p:N/guid` child ids, because event payloads
* always carry the parent guid.
*/
const resolveTargetMessage = async (client, cache, chatGuid, targetGuid, phone) => {
	const cached = cache.get(targetGuid);
	if (cached) return cached;
	try {
		const rebuilt = await rebuildFromAppleMessage(client, await client.messages.get(toMessageGuid(targetGuid)), phone, chatGuid);
		cacheMessage(cache, rebuilt);
		return rebuilt;
	} catch (error) {
		log$5.debug("event target could not be resolved; dropping the event", {
			"spectrum.imessage.target_guid": targetGuid,
			"spectrum.imessage.chat_guid": chatGuid,
			...errorAttrs(error)
		}, error instanceof Error ? error : void 0);
		return;
	}
};
const toInboundMessages = async (client, cache, event, phone) => {
	const base = buildMessageBase(event.message, event.chatGuid, event.occurredAt, phone);
	const messageGuidStr = event.message.guid;
	const msg = await buildContentMessage(client, base, event.message, messageGuidStr, {
		cache,
		phone
	});
	cacheMessage(cache, msg);
	return [msg];
};
const getMessage$1 = async (remote, spaceId, msgId, phone) => {
	const cache = getMessageCache(remote);
	const cached = cache.get(msgId);
	if (cached) return cached;
	const childRef = parseChildId(msgId);
	if (childRef) try {
		const parent = await rebuildFromAppleMessage(remote, await remote.messages.get(toMessageGuid(childRef.parentGuid)), phone, spaceId, cache);
		cacheMessage(cache, parent);
		const group = messageGroupContent(parent);
		if (!group) return;
		const item = group.items[childRef.partIndex];
		return isIMessageMessage(item) ? item : void 0;
	} catch (err) {
		if (err instanceof NotFoundError) return;
		throw err;
	}
	try {
		const rebuilt = await rebuildFromAppleMessage(remote, await remote.messages.get(toMessageGuid(msgId)), phone, spaceId, cache);
		cacheMessage(cache, rebuilt);
		return rebuilt;
	} catch (err) {
		if (err instanceof NotFoundError) return;
		throw err;
	}
};
//#endregion
//#region src/remote/members.ts
/**
* Apply an `AddMember` content value to a remote iMessage group chat.
* Fire-and-forget — the `Chat` returned by `addParticipants` is discarded.
* The caller (`handleAddMember` in the iMessage provider) is responsible
* for the group-only / remote-only guards.
*/
const addParticipants$1 = async (remote, spaceId, content) => {
	await remote.groups.addParticipants(toChatGuid(spaceId), content.members);
};
/**
* Apply a `RemoveMember` content value to a remote iMessage group chat.
* Fire-and-forget — the `Chat` returned by `removeParticipants` is
* discarded.
*/
const removeParticipants$1 = async (remote, spaceId, content) => {
	await remote.groups.removeParticipants(toChatGuid(spaceId), content.members);
};
/**
* Make the agent's own account leave a remote iMessage group chat.
* Fire-and-forget.
*/
const leaveGroup$1 = async (remote, spaceId) => {
	await remote.groups.leave(toChatGuid(spaceId));
};
/**
* List a remote group chat's current participants, excluding the agent's own
* handle (`selfPhone` — the dedicated number that owns the chat; the shared
* sentinel never matches a canonical address, so shared mode returns the
* full roster). The caller (`getMembers` in the iMessage provider) owns the
* group-only / remote-only guards.
*/
const listParticipants$1 = async (remote, spaceId, selfPhone) => {
	const { participants } = await remote.chats.get(toChatGuid(spaceId));
	return participants.filter((p) => p.address !== selfPhone).map((p) => ({
		id: p.address,
		address: p.address,
		country: p.country,
		service: p.service
	}));
};
//#endregion
//#region src/remote/reactions.ts
const EMOJI_TO_TAPBACK = {
	"❤️": "love",
	"👍": "like",
	"👎": "dislike",
	"😂": "laugh",
	"‼️": "emphasize",
	"❓": "question"
};
const TAPBACK_TO_EMOJI = Object.fromEntries(Object.entries(EMOJI_TO_TAPBACK).map(([emoji, kind]) => [kind, emoji]));
const reactionEmoji = (reaction) => reaction.kind === "emoji" ? reaction.emoji : TAPBACK_TO_EMOJI[reaction.kind];
const asProviderReaction = (emoji, target) => reactionSchema.parse({
	emoji,
	target,
	type: "reaction"
});
const resolveReactionTarget = async (client, cache, chat, targetGuid, partIndex, phone) => {
	const candidate = await resolveTargetMessage(client, cache, chat, targetGuid, phone);
	if (!candidate) return;
	if (candidate.content.type === "group") {
		const items = candidate.content.items;
		if (!Array.isArray(items)) return candidate;
		const item = items[partIndex ?? 0];
		return isIMessageMessage(item) ? item : candidate;
	}
	return candidate;
};
const toReactionMessages = async (client, cache, event, phone) => {
	const emoji = reactionEmoji(event.reaction);
	if (!emoji) return [];
	if (!event.actor?.address) return [];
	const resolved = await resolveReactionTarget(client, cache, event.chatGuid, event.messageGuid, event.targetPartIndex, phone);
	if (!resolved) return [];
	const partSuffix = typeof event.targetPartIndex === "number" ? `:${event.targetPartIndex}` : "";
	return [{
		sender: toSenderRef(event.actor),
		space: {
			id: event.chatGuid,
			type: chatTypeFromGuid(event.chatGuid),
			phone
		},
		timestamp: event.occurredAt,
		id: `${event.messageGuid}:reaction:${event.sequence}${partSuffix}`,
		content: asProviderReaction(emoji, resolved)
	}];
};
const toSettableReaction = (emoji) => {
	const native = EMOJI_TO_TAPBACK[emoji];
	return native ? { kind: native } : {
		kind: "emoji",
		emoji
	};
};
const tapbackTarget = (target) => ({
	guid: toMessageGuid(target.parentId ?? target.id),
	opts: typeof target.partIndex === "number" ? { partIndex: target.partIndex } : void 0
});
const reactToMessage$1 = async (remote, spaceId, target, reaction) => {
	const { guid, opts } = tapbackTarget(target);
	const sent = await remote.messages.setReaction(toChatGuid(spaceId), guid, toSettableReaction(reaction), true, opts);
	return {
		...toMessageMetadata(sent),
		id: sent.guid,
		content: asProviderReaction(reaction, target),
		direction: "outbound",
		space: { id: spaceId },
		timestamp: sent.dateCreated
	};
};
const unsendReaction$1 = async (remote, spaceId, target, reaction) => {
	const { guid, opts } = tapbackTarget(target);
	await remote.messages.setReaction(toChatGuid(spaceId), guid, toSettableReaction(reaction), false, opts);
};
//#endregion
//#region src/remote/read.ts
/**
* Mark every unread message in the chat as read.
*
* The SDK exposes only a chat-level `chats.markRead(chatGuid)` — there is no
* per-message API. The `Read` content's `target` is used by the caller to
* derive the chat, which `send` has already resolved into `spaceId` by the
* time the dispatcher reaches here.
*/
const markRead$1 = async (remote, spaceId) => {
	await remote.chats.markRead(toChatGuid(spaceId));
};
//#endregion
//#region src/remote/rename.ts
/**
* Apply a `Rename` content value to a remote iMessage group chat.
* Fire-and-forget — the `Chat` returned by `setDisplayName` is discarded.
*/
const setDisplayName$1 = async (remote, spaceId, content) => {
	await remote.groups.setDisplayName(toChatGuid(spaceId), content.displayName);
};
/**
* Read a remote iMessage group chat's title. The SDK returns an empty
* `Chat.displayName` for an unnamed group; normalized to `undefined`. The
* group-only guard lives at the action layer (see `remoteGroupClient`).
*/
const getDisplayName$1 = async (remote, spaceId) => {
	const { displayName } = await remote.chats.get(toChatGuid(spaceId));
	return displayName || void 0;
};
//#endregion
//#region src/shared/errors.ts
const unsupportedRemoteContent = (type, detail) => UnsupportedError.content(type, IMESSAGE_PLATFORM, detail);
//#endregion
//#region src/remote/markdown.ts
const markdownLexer = new Marked();
const BULLET = "• ";
const HR_LINE = "———";
const NESTED_LIST_INDENT = "  ";
const BLOCK_SEPARATOR = "\n\n";
const TABLE_CELL_SEPARATOR = " | ";
const DEFAULT_LIST_START = 1;
const LEADING_WHITESPACE = /^\s+/;
const TRAILING_WHITESPACE = /\s+$/;
const MONOSPACE_UPPER_A = 120432;
const MONOSPACE_LOWER_A = 120458;
const MONOSPACE_DIGIT_ZERO = 120822;
const UPPER_A = 65;
const UPPER_Z = 90;
const LOWER_A = 97;
const LOWER_Z = 122;
const DIGIT_ZERO = 48;
const DIGIT_NINE = 57;
const monospaceCodePoint = (codePoint) => {
	if (codePoint >= UPPER_A && codePoint <= UPPER_Z) return MONOSPACE_UPPER_A + (codePoint - UPPER_A);
	if (codePoint >= LOWER_A && codePoint <= LOWER_Z) return MONOSPACE_LOWER_A + (codePoint - LOWER_A);
	if (codePoint >= DIGIT_ZERO && codePoint <= DIGIT_NINE) return MONOSPACE_DIGIT_ZERO + (codePoint - DIGIT_ZERO);
	return codePoint;
};
const toMonospace = (text) => {
	let out = "";
	for (const char of text) {
		const codePoint = char.codePointAt(0);
		out += codePoint === void 0 ? char : String.fromCodePoint(monospaceCodePoint(codePoint));
	}
	return out;
};
const STYLE_ORDER = [
	"bold",
	"italic",
	"strikethrough"
];
const plain = (text) => ({
	text,
	styles: []
});
const withStyle = (spans, style) => spans.map((span) => span.styles.includes(style) ? span : {
	...span,
	styles: [...span.styles, style]
});
const spanText = (spans) => {
	let out = "";
	for (const span of spans) out += span.text;
	return out;
};
const joinSpans = (blocks, separator) => {
	const out = [];
	for (const [index, block] of blocks.entries()) {
		if (index > 0) out.push(plain(separator));
		out.push(...block);
	}
	return out;
};
const splitSpanLines = (spans) => {
	let current = [];
	const lines = [current];
	for (const span of spans) {
		const parts = span.text.split("\n");
		for (const [index, part] of parts.entries()) {
			if (index > 0) {
				current = [];
				lines.push(current);
			}
			if (part) current.push({
				...span,
				text: part
			});
		}
	}
	return lines;
};
const asMarkedToken = (token) => token;
const checkboxPrefix = (item) => {
	if (!item.task) return "";
	return item.checked ? "[x] " : "[ ] ";
};
const listMarker = (list, index) => {
	if (!list.ordered) return BULLET;
	return `${(list.start === "" ? DEFAULT_LIST_START : list.start) + index}. `;
};
const renderLink = (token) => {
	if (token.text === token.href) return [{
		text: token.href,
		styles: []
	}];
	return [...renderInlineTokens(token.tokens), {
		text: ` (${token.href})`,
		styles: []
	}];
};
const renderImage = (token) => [{
	text: token.text ? `${token.text} (${token.href})` : token.href,
	styles: []
}];
const renderInlineToken = (token) => {
	switch (token.type) {
		case "strong": return withStyle(renderInlineTokens(token.tokens), "bold");
		case "em": return withStyle(renderInlineTokens(token.tokens), "italic");
		case "del": return withStyle(renderInlineTokens(token.tokens), "strikethrough");
		case "codespan": return [plain(toMonospace(token.text))];
		case "br": return [plain("\n")];
		case "link": return renderLink(token);
		case "image": return renderImage(token);
		case "escape": return [plain(token.text)];
		case "text": return token.tokens ? renderInlineTokens(token.tokens) : [plain(token.text)];
		case "html": return [plain(token.text)];
		case "checkbox": return [];
		default: return "raw" in token ? [plain(String(token.raw))] : [];
	}
};
const renderInlineTokens = (tokens) => {
	const out = [];
	for (const token of tokens) out.push(...renderInlineToken(asMarkedToken(token)));
	return out;
};
const renderBlockquote = (quote) => {
	const lines = splitSpanLines(renderBlockTokens(quote.tokens));
	const out = [];
	for (const [index, line] of lines.entries()) {
		if (index > 0) out.push(plain("\n"));
		out.push(plain(line.length > 0 ? "> " : ">"), ...line);
	}
	return out;
};
const renderList = (list) => {
	const out = [];
	for (const [index, item] of list.items.entries()) {
		const prefix = `${listMarker(list, index)}${checkboxPrefix(item)}`;
		const blocks = [];
		for (const token of item.tokens) {
			const rendered = renderBlockToken(asMarkedToken(token));
			if (spanText(rendered)) blocks.push(rendered);
		}
		const [first = [], ...rest] = splitSpanLines(joinSpans(blocks, "\n"));
		if (out.length > 0) out.push(plain("\n"));
		out.push(plain(prefix), ...first);
		for (const line of rest) out.push(plain(`\n${NESTED_LIST_INDENT}`), ...line);
	}
	return out;
};
const renderTable = (table) => {
	const out = [];
	const pushRow = (cells, rowIndex) => {
		if (rowIndex > 0) out.push(plain("\n"));
		for (const [cellIndex, cell] of cells.entries()) {
			if (cellIndex > 0) out.push(plain(TABLE_CELL_SEPARATOR));
			out.push(...renderInlineTokens(cell.tokens));
		}
	};
	pushRow(table.header, 0);
	for (const [index, row] of table.rows.entries()) pushRow(row, index + 1);
	return out;
};
const renderBlockToken = (token) => {
	switch (token.type) {
		case "heading": return withStyle(renderInlineTokens(token.tokens), "bold");
		case "paragraph": return renderInlineTokens(token.tokens);
		case "code": return [plain(toMonospace(token.text))];
		case "blockquote": return renderBlockquote(token);
		case "list": return renderList(token);
		case "table": return renderTable(token);
		case "hr": return [plain(HR_LINE)];
		case "space":
		case "def": return [];
		default: return renderInlineToken(token);
	}
};
const renderBlockTokens = (tokens) => {
	const blocks = [];
	for (const token of tokens) {
		const rendered = renderBlockToken(asMarkedToken(token));
		if (spanText(rendered)) blocks.push(rendered);
	}
	return joinSpans(blocks, BLOCK_SEPARATOR);
};
const trimSpans = (spans) => {
	const trimmed = [...spans];
	while (trimmed.length > 0) {
		const first = trimmed.at(0);
		const text = first?.text.replace(LEADING_WHITESPACE, "");
		if (first && text) {
			trimmed[0] = {
				...first,
				text
			};
			break;
		}
		trimmed.shift();
	}
	while (trimmed.length > 0) {
		const last = trimmed.at(-1);
		const text = last?.text.replace(TRAILING_WHITESPACE, "");
		if (last && text) {
			trimmed[trimmed.length - 1] = {
				...last,
				text
			};
			break;
		}
		trimmed.pop();
	}
	return trimmed;
};
const finalize = (spans) => {
	let text = "";
	const open = /* @__PURE__ */ new Map();
	const ranges = [];
	const close = (style, end) => {
		const start = open.get(style);
		open.delete(style);
		if (start !== void 0 && end > start) ranges.push({
			type: style,
			start,
			length: end - start
		});
	};
	for (const span of spans) {
		if (!span.text) continue;
		const offset = text.length;
		for (const style of STYLE_ORDER) if (span.styles.includes(style)) {
			if (!open.has(style)) open.set(style, offset);
		} else close(style, offset);
		text += span.text;
	}
	for (const style of STYLE_ORDER) close(style, text.length);
	ranges.sort((a, b) => a.start - b.start || STYLE_ORDER.indexOf(a.type) - STYLE_ORDER.indexOf(b.type));
	return {
		text,
		formatting: ranges
	};
};
/**
* Render standard markdown (CommonMark + GFM) to iMessage styled text: a
* plain string plus UTF-16 formatting ranges for `messages.sendText`'s
* `formatting` option. Block layout matches the plain-text renderer (list
* bullets, `label (url)` links); inline emphasis becomes native
* bold/italic/strikethrough ranges instead of being stripped, headings
* render as bold, and code maps to Unicode mathematical monospace
* characters.
*/
const markdownToIMessageText = (markdown) => finalize(trimSpans(renderBlockTokens(markdownLexer.lexer(markdown))));
//#endregion
//#region src/remote/send.ts
const GROUP_ITEM_ALLOWED = new Set([
	"text",
	"markdown",
	"attachment",
	"contact",
	"voice"
]);
const GROUP_TEXT_TYPES = new Set(["text", "markdown"]);
const MAX_GROUP_TEXT_ITEMS = 1;
const outboundRecord = (spaceId, id, content, timestamp, extras) => ({
	id,
	content,
	direction: "outbound",
	space: { id: spaceId },
	timestamp,
	...extras
});
const outboundGroupItem = (spaceId, id, content, timestamp, partIndex, parentId, metadata) => outboundRecord(spaceId, id, content, timestamp, {
	...metadata,
	partIndex,
	parentId
});
const providerGroup = (items) => asGroup({ items });
const withReply = (options, replyTo) => replyTo ? {
	...options,
	replyTo
} : options;
const replyOptions = (replyTo) => replyTo ? { replyTo } : void 0;
const effectOption = (effect) => effect ? { effect } : {};
const formattingOption = (formatting) => formatting.length > 0 ? { formatting } : {};
const renderMarkdown = (markdown) => {
	const rendered = markdownToIMessageText(markdown);
	if (!rendered.text) throw unsupportedRemoteContent("markdown", "renders to empty text — nothing to send");
	return rendered;
};
const replyTargetFromId = (messageId) => {
	const childRef = parseChildId(messageId);
	if (childRef) return {
		guid: toMessageGuid(childRef.parentGuid),
		partIndex: childRef.partIndex
	};
	return toMessageGuid(messageId);
};
const outboundMessage = (spaceId, message, content) => outboundRecord(spaceId, message.guid, content, message.dateCreated, toMessageMetadata(message));
const outboundPollTitles = /* @__PURE__ */ new Map();
const outboundPoll = (spaceId, poll, content) => {
	if (outboundPollTitles.size === 128) {
		const oldest = outboundPollTitles.keys().next().value;
		if (oldest) outboundPollTitles.delete(oldest);
	}
	outboundPollTitles.set(poll.pollMessageGuid, content.title);
	return outboundRecord(spaceId, poll.pollMessageGuid, content, /* @__PURE__ */ new Date());
};
const sendVCardAttachment = (remote, name, vcf) => remote.attachments.upload({
	data: Buffer.from(vcf, "utf8"),
	fileName: name
});
const sendContactAttachment = async (remote, content) => {
	const vcf = await toVCard(content);
	const name = vcardFileName(content);
	return {
		guid: (await sendVCardAttachment(remote, name, vcf)).attachment.guid,
		name
	};
};
const uploadAttachment = async (remote, content) => {
	return {
		guid: (await remote.attachments.upload({
			data: await content.read(),
			fileName: content.name
		})).attachment.guid,
		name: content.name
	};
};
const uploadVoice = async (remote, content) => {
	const { buffer } = await ensureM4a(await content.read(), content.mimeType);
	const name = content.name ?? "voice.m4a";
	return {
		guid: (await remote.attachments.upload({
			data: buffer,
			fileName: name
		})).attachment.guid,
		name
	};
};
const sendContent = async (remote, spaceId, chat, content, replyTo, effect) => {
	switch (content.type) {
		case "effect": return sendContent(remote, spaceId, chat, content.content, replyTo, content.effect);
		case "text": return outboundMessage(spaceId, await remote.messages.sendText(chat, content.text, withReply(effectOption(effect), replyTo)), content);
		case "markdown": {
			const rendered = renderMarkdown(content.markdown);
			return outboundMessage(spaceId, await remote.messages.sendText(chat, rendered.text, withReply({
				...effectOption(effect),
				...formattingOption(rendered.formatting)
			}, replyTo)), content);
		}
		case "richlink": return outboundMessage(spaceId, await remote.messages.sendText(chat, content.url, withReply({ enableLinkPreview: true }, replyTo)), content);
		case "attachment": {
			const { guid } = await uploadAttachment(remote, content);
			return outboundMessage(spaceId, await remote.messages.sendAttachment(chat, guid, withReply(effectOption(effect), replyTo)), content);
		}
		case "contact": {
			const { guid } = await sendContactAttachment(remote, content);
			return outboundMessage(spaceId, await remote.messages.sendAttachment(chat, guid, replyOptions(replyTo)), content);
		}
		case "voice": {
			const { guid } = await uploadVoice(remote, content);
			return outboundMessage(spaceId, await remote.messages.sendAttachment(chat, guid, {
				isAudioMessage: true,
				...replyOptions(replyTo)
			}), content);
		}
		case "poll":
			if (replyTo) throw unsupportedRemoteContent("poll", "polls cannot be sent as replies");
			return outboundPoll(spaceId, await remote.polls.create(chat, content.title, content.options.map((option) => option.title)), content);
		default: throw unsupportedRemoteContent(content.type);
	}
};
const validateGroupContent = (content) => {
	let textCount = 0;
	for (const sub of content.items) {
		const itemType = sub.content.type;
		if (!GROUP_ITEM_ALLOWED.has(itemType)) throw unsupportedRemoteContent("group", `"${itemType}" items are not supported inside a group`);
		if (GROUP_TEXT_TYPES.has(itemType) && ++textCount > MAX_GROUP_TEXT_ITEMS) throw unsupportedRemoteContent("group", `groups can contain at most ${MAX_GROUP_TEXT_ITEMS} text item`);
	}
};
const resolvePart = async (remote, content) => {
	switch (content.type) {
		case "text": return { text: content.text };
		case "markdown": {
			const rendered = renderMarkdown(content.markdown);
			return {
				text: rendered.text,
				...formattingOption(rendered.formatting)
			};
		}
		case "attachment": {
			const { guid, name } = await uploadAttachment(remote, content);
			return {
				attachmentGuid: guid,
				attachmentName: name
			};
		}
		case "contact": {
			const { guid, name } = await sendContactAttachment(remote, content);
			return {
				attachmentGuid: guid,
				attachmentName: name
			};
		}
		case "voice": {
			const { guid, name } = await uploadVoice(remote, content);
			return {
				attachmentGuid: guid,
				attachmentName: name
			};
		}
		default: throw unsupportedRemoteContent(content.type);
	}
};
const send$1 = async (remote, spaceId, content) => {
	const chat = toChatGuid(spaceId);
	if (content.type === "group") {
		validateGroupContent(content);
		const resolved = await Promise.all(content.items.map((sub) => resolvePart(remote, sub.content)));
		const message = await remote.messages.sendMultipart(chat, resolved.map((part, idx) => ({
			...part,
			bubbleIndex: idx
		})));
		const parentGuid = message.guid;
		const timestamp = message.dateCreated;
		const metadata = toMessageMetadata(message);
		return outboundRecord(spaceId, parentGuid, providerGroup(content.items.map((sub, idx) => outboundGroupItem(spaceId, formatChildId(idx, parentGuid), sub.content, timestamp, idx, parentGuid, metadata))), timestamp, metadata);
	}
	return sendContent(remote, spaceId, chat, content);
};
const replyToMessage$1 = async (remote, spaceId, msgId, content) => {
	return sendContent(remote, spaceId, toChatGuid(spaceId), content, replyTargetFromId(msgId));
};
const editMessage$1 = async (remote, spaceId, msgId, content) => {
	if (content.type !== "text") throw unsupportedRemoteContent(content.type, "only text content can be edited");
	const childRef = parseChildId(msgId);
	await remote.messages.edit(toChatGuid(spaceId), toMessageGuid(childRef?.parentGuid ?? msgId), content.text, childRef ? { partIndex: childRef.partIndex } : void 0);
};
const unsendMessage$1 = async (remote, spaceId, msgId) => {
	const childRef = parseChildId(msgId);
	await remote.messages.unsend(toChatGuid(spaceId), toMessageGuid(childRef?.parentGuid ?? msgId), childRef ? { partIndex: childRef.partIndex } : void 0);
};
//#endregion
//#region src/remote/client.ts
const isSharedMode = (clients) => clients.length === 1 && clients[0]?.phone === "shared";
const availablePhones = (clients) => clients.map((c) => c.phone);
const clientForPhone = (clients, phone) => {
	if (isSharedMode(clients)) {
		const entry = clients[0];
		if (!entry) throw new Error("No iMessage clients configured");
		return entry.client;
	}
	const entry = clients.find((c) => c.phone === phone);
	if (!entry) {
		const list = availablePhones(clients).join(", ") || "<none>";
		throw new Error(`No iMessage client serves phone ${phone}. Available: ${list}`);
	}
	return entry.client;
};
const randomPhone = (clients) => {
	if (clients.length === 0) throw new Error("No iMessage phones configured for this account");
	if (isSharedMode(clients)) return SHARED_PHONE;
	const entry = clients[Math.floor(Math.random() * clients.length)];
	if (!entry) throw new Error("No iMessage phones configured for this account");
	return entry.phone;
};
//#endregion
//#region src/remote/contact-share.ts
const log$4 = createLogger("spectrum.imessage.contact");
const SHARE_TTL_MS = 1440 * 60 * 1e3;
const MAX_TRACKED_CHATS = 1e4;
const isPreconditionFailure = (error) => typeof error === "object" && error !== null && "code" in error && error.code === ErrorCode.preconditionFailed;
/**
* Tracks which chats this bot's line has already proactively pushed its contact
* card to, so `im.chats.shareContactInfo` is fired at most once per chat per
* line per 24h. One tracker is created per `AdvancedIMessage` client (see
* `getContactShareTracker`), so the dedupe is naturally scoped to the line: a
* DM `chatGuid` encodes the peer, not the receiving bot line, so the same guid
* arriving on a different line shares independently.
*
* Backed by `lru-cache` for TTL + bounded memory. `ttlAutopurge: false`
* keeps eviction lazy (on access) — there is no background timer to leak
* across Spectrum lifecycles.
*/
var ContactShareTracker = class {
	cache = new LRUCache({
		max: MAX_TRACKED_CHATS,
		ttl: SHARE_TTL_MS,
		ttlAutopurge: false
	});
	client;
	constructor(client) {
		this.client = client;
	}
	/**
	* Whether this line has already attempted to share with the chat during the
	* 24-hour dedupe window. Callers use this before consulting any remote gate,
	* so repeat inbound messages stay entirely local.
	*/
	hasRecentlyShared(chatGuid) {
		return this.cache.has(chatGuid);
	}
	/**
	* Best-effort share. The cache is set eagerly so that a burst of inbound
	* messages for the same chat coalesces to a single API call. A
	* `preconditionFailed` response remains cached for the normal 24-hour TTL,
	* avoiding repeated attempts when the account cannot currently share its
	* profile. Other failures evict the entry so the next inbound retries.
	* Never awaits and never throws: the receive stream must not crash on share
	* failures.
	*/
	maybeShare(chatGuid) {
		if (this.cache.has(chatGuid)) return;
		this.cache.set(chatGuid, true);
		const safeChatGuid = sanitizeErrorMessage(chatGuid);
		this.client.chats.shareContactInfo(chatGuid).then(() => {
			log$4.info("shared contact card", { "spectrum.imessage.contact.chat": safeChatGuid });
		}).catch((error) => {
			if (!isPreconditionFailure(error)) this.cache.delete(chatGuid);
			log$4.warn("failed to share contact card", {
				"spectrum.imessage.contact.chat": safeChatGuid,
				...errorAttrs(error)
			}, error);
		});
	}
};
const trackers = /* @__PURE__ */ new WeakMap();
/**
* Returns a per-line tracker. Mirrors `getMessageCache` in ../cache.ts — keyed
* by the individual `AdvancedIMessage` client, so each line has its own dedupe
* state and multiple lines/providers don't share state accidentally. The
* WeakMap holds the client weakly, so a torn-down line's tracker is collected
* with its client (the tracker's own reference back to the client doesn't pin
* it — the entry is a collectible cycle).
*/
const getContactShareTracker = (client) => {
	let tracker = trackers.get(client);
	if (!tracker) {
		tracker = new ContactShareTracker(client);
		trackers.set(client, tracker);
	}
	return tracker;
};
//#endregion
//#region src/remote/group-events.ts
const log$3 = createLogger("spectrum.imessage.group");
/**
* Synthetic id for a `group.changed` event — shared between the stream item
* (the dedup key across live/catch-up) and the surfaced message. `sequence`
* is monotonic per line, so the id is unique across all change kinds.
*/
const groupEventMessageId = (event) => `${event.chatGuid}:group:${event.sequence}`;
/**
* The acting party of a group change. For `participantLeft` that is the
* leaver (`change.participant`) — nobody leaves on someone else's behalf
* (third-party removal is `participantRemoved`), and `leaveSpace` content
* carries no members, so the leaver's identity can only travel on
* `message.sender`. Every other change acts through `event.actor`, which the
* platform doesn't always record.
*/
const groupEventActor = (event) => event.change.type === "participantLeft" ? event.change.participant : event.actor;
const toOptionalSenderRef = (addr) => addr?.address ? toSenderRef(addr) : void 0;
const fetchIconContent = async (client, event) => {
	try {
		const icon = await getIcon$1(client, event.chatGuid);
		if (!icon) return;
		return avatarSchema.parse({
			type: "avatar",
			action: {
				kind: "set",
				mimeType: icon.mimeType,
				read: () => Promise.resolve(icon.data)
			}
		});
	} catch (e) {
		log$3.error("failed to fetch changed group icon", {
			"spectrum.imessage.group.chat": event.chatGuid,
			...errorAttrs(e)
		}, e);
		return;
	}
};
const toGroupChangeContent = async (client, event) => {
	const change = event.change;
	switch (change.type) {
		case "participantAdded": return change.participant.address ? addMemberSchema.parse({
			type: "addMember",
			members: [change.participant.address]
		}) : void 0;
		case "participantRemoved": return change.participant.address ? removeMemberSchema.parse({
			type: "removeMember",
			members: [change.participant.address]
		}) : void 0;
		case "participantLeft": return leaveSpaceSchema.parse({ type: "leaveSpace" });
		case "displayNameChanged": return change.displayName ? renameSchema.parse({
			type: "rename",
			displayName: change.displayName
		}) : void 0;
		case "iconChanged": return await fetchIconContent(client, event);
		case "iconRemoved": return avatarSchema.parse({
			type: "avatar",
			action: { kind: "clear" }
		});
		default: return;
	}
};
/**
* Convert a `group.changed` event into inbound spectrum messages. Unlike
* reactions — where an event without an actor is dropped because the actor
* is itself the substance — membership/rename/avatar changes surface even
* when the platform recorded no actor: the state change is the payload, so
* the message ships with `sender: undefined`.
*/
const toGroupEventMessages = async (client, event, phone) => {
	const content = await toGroupChangeContent(client, event);
	if (!content) return [];
	return [{
		id: groupEventMessageId(event),
		content,
		sender: toOptionalSenderRef(groupEventActor(event)),
		space: {
			id: event.chatGuid,
			type: chatTypeFromGuid(event.chatGuid),
			phone
		},
		timestamp: event.occurredAt
	}];
};
//#endregion
//#region src/remote/polls.ts
const log$2 = createLogger("spectrum.imessage.poll");
const isVotedPollEvent = (event) => event.delta.type === "voted";
const isUnvotedPollEvent = (event) => event.delta.type === "unvoted";
const toCachedPoll = (input) => {
	const poll = asPoll({
		title: input.title.trim() || "iMessage poll",
		options: input.options.map((optionInfo) => ({ title: optionInfo.text }))
	});
	const optionsByIdentifier = /* @__PURE__ */ new Map();
	for (const [index, optionInfo] of input.options.entries()) {
		const option = poll.options[index];
		if (option && optionInfo.optionIdentifier) optionsByIdentifier.set(optionInfo.optionIdentifier, option);
	}
	return {
		poll,
		optionsByIdentifier
	};
};
const resolvePollTitle = (cache, pollMessageGuid, title) => title.trim() || cache.get(pollMessageGuid)?.poll.title || outboundPollTitles.get(pollMessageGuid) || "iMessage poll";
const cachePollInfo = (cache, info) => {
	const cached = toCachedPoll({
		...info,
		title: resolvePollTitle(cache, info.pollMessageGuid, info.title)
	});
	cache.set(info.pollMessageGuid, cached);
	outboundPollTitles.delete(info.pollMessageGuid);
	return cached;
};
const cachePollEvent = (cache, event) => {
	if (event.delta.type === "created" || event.delta.type === "optionAdded") try {
		const cached = toCachedPoll({
			title: resolvePollTitle(cache, event.pollMessageGuid, event.delta.title),
			options: event.delta.options
		});
		cache.set(event.pollMessageGuid, cached);
		outboundPollTitles.delete(event.pollMessageGuid);
		return cached;
	} catch (e) {
		log$2.error("failed to cache poll", {
			"spectrum.imessage.poll.guid": event.pollMessageGuid,
			...errorAttrs(e)
		}, e);
	}
};
const fetchPollInfo = async (client, cache, event) => {
	try {
		const info = await client.polls.get(event.pollMessageGuid);
		cachePollInfo(cache, info);
		return info;
	} catch (e) {
		log$2.error("failed to fetch poll", {
			"spectrum.imessage.poll.guid": event.pollMessageGuid,
			...errorAttrs(e)
		}, e);
		return;
	}
};
const resolvePoll = async (client, cache, event) => {
	const cached = cache.get(event.pollMessageGuid);
	if (cached) return cached;
	try {
		return cachePollInfo(cache, await client.polls.get(event.pollMessageGuid));
	} catch (e) {
		log$2.error("failed to resolve poll", {
			"spectrum.imessage.poll.guid": event.pollMessageGuid,
			...errorAttrs(e)
		}, e);
		return;
	}
};
const buildPollOptionMessage = (input) => {
	const option = input.cached.optionsByIdentifier.get(input.optionId);
	if (!option) return;
	const action = input.selected ? "selected" : "deselected";
	const eventTime = input.event.occurredAt.getTime();
	return {
		id: `${input.event.pollMessageGuid}:${input.sender.id}:${input.optionId}:${action}:${eventTime}`,
		sender: input.sender,
		space: {
			id: input.chatGuid,
			type: chatTypeFromGuid(input.chatGuid),
			phone: input.phone
		},
		timestamp: input.event.occurredAt,
		content: asPollOption({
			option,
			poll: input.cached.poll,
			selected: input.selected
		})
	};
};
const refreshPollMetadata = async (client, pollCache, event) => {
	const info = await fetchPollInfo(client, pollCache, event);
	if (!info) return;
	return pollCache.get(info.pollMessageGuid);
};
const toPollOptionMessage = async (client, pollCache, event, phone) => {
	const sender = toSenderRef(event.actor);
	const optionId = event.delta.optionIdentifier;
	if (!(sender.id && optionId)) return [];
	let cached = await resolvePoll(client, pollCache, event);
	if (!cached) return [];
	if (!cached.optionsByIdentifier.has(optionId)) {
		const refreshed = await refreshPollMetadata(client, pollCache, event);
		if (refreshed) cached = refreshed;
	}
	const message = buildPollOptionMessage({
		cached,
		chatGuid: event.chatGuid,
		event,
		optionId,
		phone,
		selected: event.delta.type === "voted",
		sender
	});
	return message ? [message] : [];
};
const toPollDeltaMessages = async (client, pollCache, event, phone) => {
	if (isVotedPollEvent(event)) return toPollOptionMessage(client, pollCache, event, phone);
	if (isUnvotedPollEvent(event)) return toPollOptionMessage(client, pollCache, event, phone);
	return [];
};
//#endregion
//#region src/remote/read-receipts.ts
const log$1 = createLogger("spectrum.imessage.read");
/**
* Synthetic id for a `message.read` event — shared between the stream item
* (the dedup key across live/catch-up) and the surfaced message. `sequence` is
* monotonic per line, so N participants reading the same message produce N
* distinct ids, and the same event replayed through catch-up produces the id
* it produced live.
*/
const readReceiptMessageId = (event) => `${event.messageGuid}:read:${event.sequence}`;
const asProviderRead = (target) => readSchema.parse({
	target,
	type: "read"
});
/**
* Whether the reader could be identified at all, decided without resolving
* the target. A DM always can (the peer is in the guid); a group only if the
* platform named an actor. Lets an unattributable group receipt drop before
* paying for an RPC.
*/
const isAttributable = (event) => dmPeerFromChatGuid(event.chatGuid) !== void 0 || event.actor?.address !== void 0;
/**
* Identify the reader.
*
* `event.actor` is **not** the reader on this arm. Verified against a live
* line: in a DM where the peer read our message, `actor` came back as *our
* own* line's address, not theirs.
*
* So the chat guid comes first. In a DM there are exactly two participants
* and the target is already known to be ours, which makes the reader
* definitionally the other one — no address comparison needed, and therefore
* correct on pooled lines too, where `phone` is the `"shared"` sentinel and
* comparing it against a real address is meaningless.
*
* A group guid carries no participant list, so there `actor` is the only
* possible attribution. It is trusted only when it names neither this line
* nor the target's own sender; the latter is what catches the shared-mode
* case, since the sentinel never equals a real address. An unattributable
* receipt is dropped rather than surfaced with a wrong reader — the reader's
* identity is the entire payload.
*/
const toReader = (event, target, phone) => {
	const peer = dmPeerFromChatGuid(event.chatGuid);
	if (peer) return {
		id: peer,
		address: peer
	};
	const actor = event.actor;
	if (actor?.address && actor.address !== phone && actor.address !== target.sender?.address) return toSenderRef(actor);
};
/**
* Convert a `message.read` event into an inbound `read` message — someone read
* a message the agent sent. `sender` is the reader; `content.target` is ours.
*
* Two guards, cheapest first:
*
* 1. **The reader must be identifiable -> else drop.** Unlike a membership
*    change (where the state change itself is the payload and
*    `sender: undefined` still carries meaning), the reader's identity *is*
*    the payload of a receipt. An unattributed receipt in a group is
*    indistinguishable noise and would corrupt any "N of M have read" tally.
*    Same call as `toReactionMessages`. See `toReader`.
*
* 2. **The target must be one of ours.** `event.isFromMe` is not trustworthy
*    here: the proto carries no comment for it on this arm, and it most
*    plausibly describes the *underlying message* — which for a genuine
*    receipt is ours, i.e. `true` — so keying self-suppression off it would
*    suppress every receipt worth surfacing. The durable invariant is that a
*    peer's receipt always points at a message we sent, so the resolved target
*    must be `direction: "outbound"`. That one check also suppresses the echo
*    of our own `chats.markRead()`, which marks *their* inbound messages and
*    therefore resolves `direction: "inbound"`.
*/
const toReadReceiptMessages = async (client, cache, event, phone) => {
	if (!isAttributable(event)) {
		log$1.debug("read receipt dropped: reader could not be identified", {
			"spectrum.imessage.read.message_guid": event.messageGuid,
			"spectrum.imessage.read.sequence": event.sequence,
			"spectrum.imessage.read.chat_guid": event.chatGuid,
			"spectrum.imessage.read.has_actor": false
		});
		return [];
	}
	const target = await resolveTargetMessage(client, cache, event.chatGuid, event.messageGuid, phone);
	if (!target) {
		log$1.debug("read receipt dropped: target message could not be resolved", {
			"spectrum.imessage.read.message_guid": event.messageGuid,
			"spectrum.imessage.read.sequence": event.sequence
		});
		return [];
	}
	if (target.direction !== "outbound") {
		log$1.debug("read receipt dropped: target is not one of ours", {
			"spectrum.imessage.read.message_guid": event.messageGuid,
			"spectrum.imessage.read.sequence": event.sequence,
			"spectrum.imessage.read.target_direction": target.direction ?? "unset"
		});
		return [];
	}
	const reader = toReader(event, target, phone);
	if (!reader) {
		log$1.debug("read receipt dropped: reader could not be identified", {
			"spectrum.imessage.read.message_guid": event.messageGuid,
			"spectrum.imessage.read.sequence": event.sequence,
			"spectrum.imessage.read.chat_guid": event.chatGuid,
			"spectrum.imessage.read.has_actor": true
		});
		return [];
	}
	const readAt = event.readAt;
	log$1.debug("read receipt surfaced", {
		"spectrum.imessage.read.message_guid": event.messageGuid,
		"spectrum.imessage.read.sequence": event.sequence,
		"spectrum.imessage.read.reader": sanitizePhone(reader.id),
		"spectrum.imessage.read.used_read_at": readAt !== void 0
	});
	return [{
		id: readReceiptMessageId(event),
		content: asProviderRead(target),
		sender: reader,
		space: {
			id: event.chatGuid,
			type: chatTypeFromGuid(event.chatGuid),
			phone
		},
		timestamp: readAt ?? event.occurredAt
	}];
};
//#endregion
//#region src/remote/stream.ts
const isCursorRejectedIMessageError = (error) => error instanceof ValidationError;
const streamLabel = (kind, phone) => `imessage.${kind}:${phone === "shared" ? phone : sanitizePhone(phone)}`;
const isActorCurrentAccount = (actor, phone) => phone !== "shared" && actor?.address !== void 0 && actor.address === phone;
const isEventFromCurrentAccount = (event, phone) => event.isFromMe || isActorCurrentAccount(event.actor, phone);
const streamLog = createLogger("spectrum.imessage.stream");
const isRetryableMappingError = (error) => typeof error === "object" && error !== null && error.retryable === true;
const skipUnmappable = async (label, cursor, map) => {
	try {
		return await map();
	} catch (error) {
		if (isRetryableMappingError(error)) throw error;
		streamLog.warn("skipping unmappable imessage event", {
			"spectrum.imessage.stream": label,
			"spectrum.imessage.cursor": cursor,
			...errorAttrs(error)
		}, error instanceof Error ? error : void 0);
		return {
			cursor,
			id: `unmappable:${cursor}`,
			values: []
		};
	}
};
const toMessageItem = async (client, event, phone, cursor, onInbound) => {
	if (event.type === "message.received") {
		if (event.message.isFromMe) return {
			cursor,
			id: event.message.guid,
			values: []
		};
		const values = await toInboundMessages(client, getMessageCache(client), event, phone);
		const inboundChatGuid = event.message.chatGuids?.[0];
		if (inboundChatGuid) onInbound?.(inboundChatGuid);
		return {
			cursor,
			id: event.message.guid,
			values
		};
	}
	if (event.type === "message.reactionAdded") {
		if (isEventFromCurrentAccount(event, phone)) return {
			cursor,
			id: `${event.messageGuid}:reaction:${event.sequence}`,
			values: []
		};
		const cache = getMessageCache(client);
		return {
			cursor,
			id: `${event.messageGuid}:reaction:${event.sequence}`,
			values: await toReactionMessages(client, cache, event, phone)
		};
	}
	if (event.type === "message.read") {
		const id = readReceiptMessageId(event);
		streamLog.debug("received a read event", {
			"spectrum.imessage.read.message_guid": event.messageGuid,
			"spectrum.imessage.read.sequence": event.sequence,
			"spectrum.imessage.read.chat_guid": event.chatGuid,
			"spectrum.imessage.read.actor": event.actor?.address ? sanitizePhone(event.actor.address) : "none",
			"spectrum.imessage.read.is_from_me": event.isFromMe,
			"spectrum.imessage.read.line": phone
		});
		return {
			cursor,
			id,
			values: await toReadReceiptMessages(client, getMessageCache(client), event, phone)
		};
	}
	streamLog.debug("message event consumed without mapping", {
		"spectrum.imessage.event_type": event.type,
		"spectrum.imessage.sequence": event.sequence
	});
	return {
		cursor,
		id: `${event.type}:${"messageGuid" in event ? event.messageGuid : "unknown"}:${event.sequence}`,
		values: []
	};
};
const toPollItem = async (client, pollCache, event, phone, cursor) => {
	cachePollEvent(pollCache, event);
	if (isEventFromCurrentAccount(event, phone)) return {
		cursor,
		id: `${event.pollMessageGuid}:poll:${event.sequence}`,
		values: []
	};
	return {
		cursor,
		id: `${event.pollMessageGuid}:poll:${event.sequence}`,
		values: await toPollDeltaMessages(client, pollCache, event, phone)
	};
};
const toGroupItem = async (client, event, phone, cursor) => {
	const id = groupEventMessageId(event);
	if (isEventFromCurrentAccount({
		actor: groupEventActor(event),
		isFromMe: event.isFromMe
	}, phone)) return {
		cursor,
		id,
		values: []
	};
	return {
		cursor,
		id,
		values: await toGroupEventMessages(client, event, phone)
	};
};
const toCatchUpCompleteItem = (event) => ({
	cursor: String(event.headSequence),
	id: `${event.type}:${event.headSequence}`,
	values: []
});
const isMessageEvent = (event) => event.type.startsWith("message.");
const isPollEvent = (event) => event.type === "poll.changed";
const isGroupEvent = (event) => event.type === "group.changed";
async function* catchUpEvents(client, cursor, isWanted) {
	const since = toResumeAfter(cursor);
	if (since === void 0) return;
	for await (const event of client.events.catchUp(since)) {
		if (event.type === "catchup.complete") {
			yield event;
			return;
		}
		if (isWanted(event)) yield event;
	}
}
const toResumeAfter = (cursor) => {
	if (!cursor) return;
	const sequence = Number(cursor);
	return Number.isSafeInteger(sequence) && sequence >= 0 ? sequence : void 0;
};
async function* afterCursor(stream, cursor) {
	const resumeAfter = toResumeAfter(cursor);
	try {
		for await (const event of stream) {
			if (resumeAfter !== void 0 && event.sequence <= resumeAfter) continue;
			yield event;
		}
	} finally {
		await stream.close?.();
	}
}
const withClose = (source, cursor) => Object.assign(afterCursor(source, cursor), { close: async () => {
	await source.close?.();
} });
const messageStream = (client, phone, onInbound, recover) => resumableOrderedStream({
	fetchMissed: (cursor) => catchUpEvents(client, cursor, isMessageEvent),
	isCursorRejectedError: isCursorRejectedIMessageError,
	label: streamLabel("messages", phone),
	recover,
	processLive: (event) => skipUnmappable(streamLabel("messages", phone), String(event.sequence), () => toMessageItem(client, event, phone, String(event.sequence), onInbound)),
	processMissed: (event) => event.type === "catchup.complete" ? Promise.resolve(toCatchUpCompleteItem(event)) : skipUnmappable(streamLabel("messages", phone), String(event.sequence), () => toMessageItem(client, event, phone, String(event.sequence), onInbound)),
	subscribeLive: (cursor) => withClose(client.messages.subscribeEvents(), cursor)
});
const pollStream = (client, pollCache, phone, recover) => resumableOrderedStream({
	fetchMissed: (cursor) => catchUpEvents(client, cursor, isPollEvent),
	isCursorRejectedError: isCursorRejectedIMessageError,
	label: streamLabel("polls", phone),
	recover,
	processLive: (event) => skipUnmappable(streamLabel("polls", phone), String(event.sequence), () => toPollItem(client, pollCache, event, phone, String(event.sequence))),
	processMissed: (event) => event.type === "catchup.complete" ? Promise.resolve(toCatchUpCompleteItem(event)) : skipUnmappable(streamLabel("polls", phone), String(event.sequence), () => toPollItem(client, pollCache, event, phone, String(event.sequence))),
	subscribeLive: (cursor) => withClose(client.polls.subscribeEvents(), cursor)
});
const groupStream = (client, phone, recover) => resumableOrderedStream({
	fetchMissed: (cursor) => catchUpEvents(client, cursor, isGroupEvent),
	isCursorRejectedError: isCursorRejectedIMessageError,
	label: streamLabel("groups", phone),
	recover,
	processLive: (event) => skipUnmappable(streamLabel("groups", phone), String(event.sequence), () => toGroupItem(client, event, phone, String(event.sequence))),
	processMissed: (event) => event.type === "catchup.complete" ? Promise.resolve(toCatchUpCompleteItem(event)) : skipUnmappable(streamLabel("groups", phone), String(event.sequence), () => toGroupItem(client, event, phone, String(event.sequence))),
	subscribeLive: (cursor) => withClose(client.groups.subscribeEvents(), cursor)
});
const clientStream = (client, pollCache, phone, includeGroupEvents, onInbound, recover) => {
	const streams = [messageStream(client, phone, onInbound, recover), pollStream(client, pollCache, phone, recover)];
	if (includeGroupEvents) streams.push(groupStream(client, phone, recover));
	return mergeStreams(streams);
};
const shareWhenProfileSynced = (tracker, gate, chatGuid) => {
	if (tracker.hasRecentlyShared(chatGuid)) return;
	gate.isEnabled().then((enabled) => {
		if (enabled) tracker.maybeShare(chatGuid);
	}).catch((error) => {
		streamLog.warn("profile sync gate failed; skipping automatic contact sharing", errorAttrs(error), error instanceof Error ? error : void 0);
	});
};
const contactShareHandler = (tracker, profileSyncGate) => profileSyncGate ? (chatGuid) => shareWhenProfileSynced(tracker, profileSyncGate, chatGuid) : (chatGuid) => tracker.maybeShare(chatGuid);
const messages$1 = (clients, projectConfig, profileSyncGate) => {
	const pollCache = getPollCache(clients);
	const staticShareEnabled = projectConfig?.profile?.imessageSynced === true;
	const recover = getCloudRecover(clients);
	const shared = isSharedMode(clients);
	const includeGroupEvents = !shared;
	const build = (entry) => () => {
		const tracker = staticShareEnabled || profileSyncGate ? getContactShareTracker(entry.client) : void 0;
		return clientStream(entry.client, pollCache, entry.phone, includeGroupEvents, tracker ? contactShareHandler(tracker, profileSyncGate) : void 0, recover);
	};
	const group = createStreamGroup({ label: "imessage.messages" });
	for (const entry of clients) group.add(lineKey(entry), build(entry));
	if (shared) return group;
	const disposeObserver = addLineObserver(clients, {
		attach: (entry) => {
			group.add(lineKey(entry), build(entry));
		},
		detach: (entry) => group.remove(lineKey(entry)).then(() => void 0)
	});
	const closeGroup = group.close.bind(group);
	return Object.assign(group, { close: async () => {
		disposeObserver();
		await closeGroup();
	} });
};
//#endregion
//#region src/remote/stream-text.ts
const INITIAL_THROTTLE_MS = 1e3;
const BACKOFF_FACTOR = 2;
const MAX_EDITS = 5;
/**
* Deliver a `streamText` content by sending the first chunk as a real message
* and editing it in place as more text arrives. The stream materializes into a
* normal text message: the returned record carries `asText(fullText)` with the
* first send's id and timestamp.
*/
const sendStreamText$1 = async (remote, spaceId, content) => {
	if (content.format === "markdown") throw unsupportedRemoteContent("streamText", "markdown-formatted streams have no native iMessage delivery");
	const chat = toChatGuid(spaceId);
	let sent;
	let full = "";
	let lastSentText = "";
	let lastEditAt = 0;
	let editCount = 0;
	const flushEdit = async (text) => {
		if (!sent || text === lastSentText) return;
		await remote.messages.edit(chat, toMessageGuid(sent.guid), text);
		lastSentText = text;
		lastEditAt = Date.now();
		editCount += 1;
	};
	for await (const delta of content.stream()) {
		full += delta;
		if (!sent) {
			sent = await remote.messages.sendText(chat, full);
			lastSentText = full;
			lastEditAt = Date.now();
			continue;
		}
		const hasBudgetForInterimEdit = editCount < MAX_EDITS - 1;
		const requiredGap = INITIAL_THROTTLE_MS * BACKOFF_FACTOR ** editCount;
		if (hasBudgetForInterimEdit && Date.now() - lastEditAt >= requiredGap) await flushEdit(full);
	}
	if (!sent) throw unsupportedRemoteContent("streamText", "stream produced no text — nothing to send");
	await flushEdit(full);
	return {
		...editCount === 0 ? toMessageMetadata(sent) : {},
		id: sent.guid,
		content: asText(full),
		direction: "outbound",
		nativeText: full,
		space: { id: spaceId },
		timestamp: sent.dateCreated
	};
};
//#endregion
//#region src/remote/typing.ts
const startTyping$1 = async (remote, spaceId) => {
	await remote.chats.setTyping(toChatGuid(spaceId), true);
};
const stopTyping$1 = async (remote, spaceId) => {
	await remote.chats.setTyping(toChatGuid(spaceId), false);
};
//#endregion
//#region src/remote/api.ts
const messages = (clients, projectConfig, profileSyncGate) => messages$1(clients, projectConfig, profileSyncGate);
const setBackground = async (remote, spaceId, content) => setBackground$1(remote, spaceId, content);
const sendCustomizedMiniApp = async (remote, spaceId, content) => sendCustomizedMiniApp$1(remote, spaceId, content);
const updateCustomizedMiniApp = async (remote, spaceId, session, content) => updateCustomizedMiniApp$1(remote, spaceId, session, content);
const setDisplayName = async (remote, spaceId, content) => setDisplayName$1(remote, spaceId, content);
const getDisplayName = async (remote, spaceId) => getDisplayName$1(remote, spaceId);
const addParticipants = async (remote, spaceId, content) => addParticipants$1(remote, spaceId, content);
const removeParticipants = async (remote, spaceId, content) => removeParticipants$1(remote, spaceId, content);
const leaveGroup = async (remote, spaceId) => leaveGroup$1(remote, spaceId);
const listParticipants = async (remote, spaceId, selfPhone) => listParticipants$1(remote, spaceId, selfPhone);
const getIcon = async (remote, spaceId) => getIcon$1(remote, spaceId);
const setIcon = async (remote, spaceId, content) => setIcon$1(remote, spaceId, content);
const markRead = async (remote, spaceId) => {
	await markRead$1(remote, spaceId);
};
const shareContactCard = async (remote, spaceId) => {
	await shareContactCard$1(remote, spaceId);
};
const startTyping = async (remote, spaceId) => {
	await startTyping$1(remote, spaceId);
};
const stopTyping = async (remote, spaceId) => {
	await stopTyping$1(remote, spaceId);
};
const send = async (remote, spaceId, content) => send$1(remote, spaceId, content);
const sendStreamText = async (remote, spaceId, content) => sendStreamText$1(remote, spaceId, content);
const replyToMessage = async (remote, spaceId, msgId, content) => replyToMessage$1(remote, spaceId, msgId, content);
const editMessage = async (remote, spaceId, msgId, content) => editMessage$1(remote, spaceId, msgId, content);
const reactToMessage = async (remote, spaceId, target, reaction) => reactToMessage$1(remote, spaceId, target, reaction);
const unsendMessage = async (remote, spaceId, msgId) => unsendMessage$1(remote, spaceId, msgId);
const unsendReaction = async (remote, spaceId, target, reaction) => unsendReaction$1(remote, spaceId, target, reaction);
const getMessage = async (remote, spaceId, msgId, phone) => getMessage$1(remote, spaceId, msgId, phone);
//#endregion
//#region src/remote/app.ts
/**
* Fixed identity of Spectrum's own iMessage extension. The universal `app`
* content renders through this extension, so callers never supply (or even see)
* these constants — they pass only a URL and the card opens it inside the
* Spectrum mini app on tap. Callers shipping their *own* extension use the
* low-level `customizedMiniApp()` instead.
*/
const SPECTRUM_MINI_APP = {
	appName: "Spectrum",
	extensionBundleId: "codes.photon.Spectrum.MessagesExtension",
	teamId: "P8XT6232SL",
	appStoreId: 6777616651
};
/**
* Build the iMessage mini-app card for an `app` content: Spectrum's fixed
* identity plus the per-message `url`, optional live-rendering hint, and the
* `layout` already derived from the URL's link metadata.
*/
const toSpectrumMiniApp = (url, layout, live) => asCustomizedMiniApp({
	...SPECTRUM_MINI_APP,
	url,
	layout,
	...live === void 0 ? {} : { live }
});
//#endregion
//#region src/remote/profile-sync-gate.ts
const PROFILE_SYNC_CACHE_TTL_MS = 6e4;
const PROFILE_SYNC_RETRY_DELAY_MS = 3e4;
const log = createLogger("spectrum.imessage.profile-sync");
const createProfileSyncGate = (options) => {
	const cacheTtlMs = options.cacheTtlMs ?? PROFILE_SYNC_CACHE_TTL_MS;
	const retryDelayMs = options.retryDelayMs ?? PROFILE_SYNC_RETRY_DELAY_MS;
	let cachedEnabled = options.initialEnabled;
	let disposed = false;
	let refreshInFlight;
	let retryAfter = 0;
	let validUntil = Date.now() + cacheTtlMs;
	const refreshNow = async () => {
		try {
			const enabled = await options.refresh();
			if (disposed) return false;
			cachedEnabled = enabled;
			retryAfter = 0;
			validUntil = Date.now() + cacheTtlMs;
			return enabled;
		} catch (error) {
			cachedEnabled = false;
			retryAfter = Date.now() + retryDelayMs;
			validUntil = 0;
			log.warn("failed to refresh profile sync state; automatic contact sharing remains disabled", {
				"spectrum.imessage.profile_sync.retry_in_ms": retryDelayMs,
				...errorAttrs(error)
			}, error);
			return false;
		}
	};
	const coalescedRefresh = () => {
		if (!refreshInFlight) refreshInFlight = refreshNow().finally(() => {
			refreshInFlight = void 0;
		});
		return refreshInFlight;
	};
	return {
		dispose() {
			disposed = true;
			cachedEnabled = false;
			validUntil = 0;
		},
		async isEnabled() {
			if (disposed) return false;
			const now = Date.now();
			if (now < validUntil) return cachedEnabled;
			if (now < retryAfter) return false;
			return await coalescedRefresh();
		}
	};
};
const gates = /* @__PURE__ */ new WeakMap();
const isProfileSynced = (projectConfig) => projectConfig.profile?.imessageSynced === true;
const registerProfileSyncGate = (clients, projectId, projectSecret, projectConfig) => {
	gates.get(clients)?.dispose();
	gates.set(clients, createProfileSyncGate({
		initialEnabled: isProfileSynced(projectConfig),
		refresh: async () => isProfileSynced(await cloud.getProject(projectId, projectSecret))
	}));
};
const getProfileSyncGate = (clients) => gates.get(clients);
const disposeProfileSyncGate = (clients) => {
	gates.get(clients)?.dispose();
	gates.delete(clients);
};
//#endregion
//#region src/index.ts
const isPollContent = (content) => content.type === "poll" || content.type === "poll_option";
const cacheRemoteOutbound = (remote, space, record) => {
	if (!record) return record;
	cacheMessage(getMessageCache(remote), {
		...record,
		direction: record.direction ?? "outbound",
		space: {
			...record.space,
			id: record.space.id,
			phone: space.phone,
			type: space.type
		}
	});
	return record;
};
const handleEdit = async (client, space, content) => {
	const miniAppCardSession = content.target.miniAppCardSession;
	const updateMiniAppCardSession = (record) => {
		const nextSession = record?.miniAppCardSession;
		if (nextSession) content.target.miniAppCardSession = nextSession;
	};
	if (content.content.type === "app") {
		if (!miniAppCardSession) throw UnsupportedError.content("edit", IMESSAGE_PLATFORM, "mini app card edits require a miniAppCardSession from the original send");
		const url = await content.content.url();
		const layout = await content.content.layout();
		const remote = clientForPhone(client, space.phone);
		updateMiniAppCardSession(cacheRemoteOutbound(remote, space, await updateCustomizedMiniApp(remote, space.id, miniAppCardSession, toSpectrumMiniApp(url, layout, content.content.live))));
		return;
	}
	if (isCustomizedMiniApp(content.content)) {
		if (!miniAppCardSession) throw UnsupportedError.content("edit", IMESSAGE_PLATFORM, "customized mini app card edits require a miniAppCardSession from the original send");
		const remote = clientForPhone(client, space.phone);
		updateMiniAppCardSession(cacheRemoteOutbound(remote, space, await updateCustomizedMiniApp(remote, space.id, miniAppCardSession, content.content)));
		return;
	}
	if (content.content.type !== "text") throw UnsupportedError.content("edit", IMESSAGE_PLATFORM, `only text content can be edited (got "${content.content.type}")`);
	await editMessage(clientForPhone(client, space.phone), space.id, content.target.id, content.content);
};
const handleUnsend = async (client, space, content) => {
	if (isPollContent(content.target.content)) throw UnsupportedError.action("unsend", IMESSAGE_PLATFORM, "iMessage polls cannot be unsent");
	const remote = clientForPhone(client, space.phone);
	const targetContent = content.target.content;
	if (targetContent.type === "reaction") {
		await unsendReaction(remote, space.id, targetContent.target, targetContent.emoji);
		return;
	}
	await unsendMessage(remote, space.id, content.target.id);
};
const handleStreamText = async (client, space, content) => {
	const remote = clientForPhone(client, space.phone);
	return cacheRemoteOutbound(remote, space, await sendStreamText(remote, space.id, content));
};
const handleBackground = async (client, space, content) => {
	await setBackground(clientForPhone(client, space.phone), space.id, content);
};
const handleCustomizedMiniApp = async (client, space, content) => {
	const remote = clientForPhone(client, space.phone);
	return cacheRemoteOutbound(remote, space, await sendCustomizedMiniApp(remote, space.id, content));
};
/**
* Render the universal `app` content. On remote it becomes a native Spectrum
* mini-app card (fixed `SPECTRUM_MINI_APP` identity + the URL + the layout
* already parsed from the URL's link metadata).
*/
const handleApp = async (client, space, content) => {
	const url = await content.url();
	const layout = await content.layout();
	const remote = clientForPhone(client, space.phone);
	return cacheRemoteOutbound(remote, space, await sendCustomizedMiniApp(remote, space.id, toSpectrumMiniApp(url, layout, content.live)));
};
const handleRead = async (client, space) => {
	await markRead(clientForPhone(client, space.phone), space.id);
};
const handleShareContactCard = async (client, space) => {
	await shareContactCard(clientForPhone(client, space.phone), space.id);
};
const handleTyping = async (client, space, state) => {
	const remote = clientForPhone(client, space.phone);
	if (state === "start") await startTyping(remote, space.id);
	else await stopTyping(remote, space.id);
};
const handleRename = async (client, space, content) => {
	if (space.type !== "group") throw UnsupportedError.action("rename", IMESSAGE_PLATFORM, "only group chats can be renamed (this space is a DM)");
	await setDisplayName(clientForPhone(client, space.phone), space.id, content);
};
const handleAvatar = async (client, space, content) => {
	if (space.type !== "group") throw UnsupportedError.action("avatar", IMESSAGE_PLATFORM, "only group chats have avatars (this space is a DM)");
	await setIcon(clientForPhone(client, space.phone), space.id, content);
};
/**
* Shared guard for the membership handlers: remote-only, group-only, then
* per-phone client resolution. Mirrors the `handleRename` / `handleAvatar`
* guard sequence.
*/
const remoteGroupClient = (client, space, action, detail) => {
	if (space.type !== "group") throw UnsupportedError.action(action, IMESSAGE_PLATFORM, detail);
	return clientForPhone(client, space.phone);
};
const handleAddMember = async (client, space, content) => {
	await addParticipants(remoteGroupClient(client, space, "addMember", "only group chats can add members (this space is a DM — iMessage cannot convert a DM into a group; create a group via space.create instead)"), space.id, content);
};
const handleRemoveMember = async (client, space, content) => {
	await removeParticipants(remoteGroupClient(client, space, "removeMember", "only group chats can remove members (this space is a DM — iMessage cannot convert a DM into a group; create a group via space.create instead)"), space.id, content);
};
const handleLeaveSpace = async (client, space) => {
	await leaveGroup(remoteGroupClient(client, space, "leaveSpace", "only group chats can be left (this space is a DM)"), space.id);
};
/**
* Dispatch the iMessage-only fire-and-forget control signals that live outside
* the universal `Content` union (`background`, `contactCard`). Each is narrowed
* via a runtime guard rather than a `content.type ===` check — the literals
* aren't members of `Content["type"]`. Returns `true` when it consumed the
* content so `send` can return early, keeping its dispatch chain flat.
*/
const handleProviderControlSignal = async (client, space, content) => {
	if (isBackground(content)) {
		await handleBackground(client, space, content);
		return true;
	}
	if (isContactCard(content)) {
		await handleShareContactCard(client, space);
		return true;
	}
	return false;
};
/**
* Resolve the remote client for a `reply` / `reaction` whose target is another
* message. `action` labels the error and `pollNoun` is the plural used in the
* poll-unsupported message.
*/
const remoteForMessageTarget = (client, space, target, action, pollNoun) => {
	if (isPollContent(target.content)) throw UnsupportedError.action(action, IMESSAGE_PLATFORM, `iMessage polls do not support ${pollNoun}`);
	return clientForPhone(client, space.phone);
};
const definedIMessage = definePlatform(IMESSAGE_PLATFORM, {
	config: configSchema,
	static: { effect: { message: messageEffects } },
	lifecycle: {
		createClient: async ({ config, projectConfig, projectId, projectSecret }) => {
			let clients;
			if (config.clients) clients = (Array.isArray(config.clients) ? config.clients : [config.clients]).map((e) => ({
				phone: e.phone,
				client: createGrpcClient({
					address: e.address,
					autoIdempotency: true,
					retry: true,
					tls: true,
					token: e.token
				})
			}));
			else if (projectId && projectSecret) clients = await createCloudClients(projectId, projectSecret);
			else throw new Error("Cloud iMessage requires projectId and projectSecret. Pass credentials to Spectrum() or provide explicit clients with imessage.config({ clients: [...] }). For local Messages access, install @spectrum-ts/imessage-local and use localIMessage.config().");
			if (projectId && projectSecret && projectConfig) registerProfileSyncGate(clients, projectId, projectSecret, projectConfig);
			return clients;
		},
		destroyClient: async ({ client }) => {
			disposeProfileSyncGate(client);
			await disposeCloudAuth(client);
			await Promise.all(client.map((entry) => entry.client.close()));
		}
	},
	user: {
		schema: userSchema,
		resolve: async ({ input }) => ({ id: input.userID })
	},
	space: {
		schema: spaceSchema,
		params: spaceParamsSchema,
		create: async ({ input, client }) => {
			if (input.users.length === 0) throw new Error("iMessage space creation requires at least one user");
			if (client.length === 0) throw new Error("No iMessage clients configured");
			const addresses = input.users.map((u) => u.id);
			if (isSharedMode(client)) {
				if (addresses.length > 1) throw UnsupportedError.action("space.create", "iMessage (shared mode)", "shared mode cannot create group chats — use a dedicated number, or space.get(chatGuid) for an existing group");
				return {
					id: dmChatGuid(addresses[0] ?? ""),
					type: "dm",
					phone: SHARED_PHONE
				};
			}
			const phone = input.params?.phone ?? randomPhone(client);
			const { chat } = await clientForPhone(client, phone).chats.create(addresses);
			return {
				id: chat.guid,
				type: chat.isGroup ? "group" : "dm",
				phone
			};
		},
		get: async ({ input, client }) => {
			if (client.length === 0) throw new Error("No iMessage clients configured");
			const phone = isSharedMode(client) ? SHARED_PHONE : input.params?.phone ?? (client.length === 1 ? client[0]?.phone : void 0);
			if (!phone) throw new Error(`iMessage space.get requires params.phone when multiple clients are configured. Available: ${availablePhones(client).join(", ")}`);
			return {
				id: input.id,
				type: chatTypeFromGuid(input.id),
				phone
			};
		},
		actions: {
			background: async (space, input, opts) => {
				await space.send(background(input, opts));
			},
			shareContactCard: async (space) => {
				await space.send(nativeContactCard());
			}
		}
	},
	message: { schema: messageSchema },
	messages: ({ client, projectConfig }) => messages(client, projectConfig, getProfileSyncGate(client)),
	send: async ({ space, content, client }) => {
		if (content.type === "reply") {
			const remote = remoteForMessageTarget(client, space, content.target, "reply", "replies");
			return cacheRemoteOutbound(remote, space, await replyToMessage(remote, space.id, content.target.id, content.content));
		}
		if (content.type === "reaction") {
			const remote = remoteForMessageTarget(client, space, content.target, "react", "reactions");
			return cacheRemoteOutbound(remote, space, await reactToMessage(remote, space.id, content.target, content.emoji));
		}
		if (content.type === "typing") {
			await handleTyping(client, space, content.state);
			return;
		}
		if (content.type === "edit") {
			await handleEdit(client, space, content);
			return;
		}
		if (content.type === "unsend") {
			await handleUnsend(client, space, content);
			return;
		}
		if (content.type === "streamText") return await handleStreamText(client, space, content);
		if (content.type === "rename") {
			await handleRename(client, space, content);
			return;
		}
		if (content.type === "avatar") {
			await handleAvatar(client, space, content);
			return;
		}
		if (content.type === "addMember") {
			await handleAddMember(client, space, content);
			return;
		}
		if (content.type === "removeMember") {
			await handleRemoveMember(client, space, content);
			return;
		}
		if (content.type === "leaveSpace") {
			await handleLeaveSpace(client, space);
			return;
		}
		if (content.type === "read") {
			await handleRead(client, space);
			return;
		}
		if (content.type === "app") return await handleApp(client, space, content);
		if (await handleProviderControlSignal(client, space, content)) return;
		if (isCustomizedMiniApp(content)) return await handleCustomizedMiniApp(client, space, content);
		const remote = clientForPhone(client, space.phone);
		return cacheRemoteOutbound(remote, space, await send(remote, space.id, content));
	},
	actions: {
		getMessage: async ({ client }, space, messageId) => {
			return getMessage(clientForPhone(client, space.phone), space.id, messageId, space.phone);
		},
		getMembers: async ({ client }, space) => {
			return await listParticipants(remoteGroupClient(client, space, "getMembers", "only group chats support listing members (this space is a DM)"), space.id, space.phone);
		},
		getAvatar: async ({ client }, space) => {
			return await getIcon(remoteGroupClient(client, space, "getAvatar", "only group chats have avatars (this space is a DM)"), space.id);
		},
		getDisplayName: async ({ client }, space) => {
			return await getDisplayName(remoteGroupClient(client, space, "getDisplayName", "only group chats have display names (this space is a DM)"), space.id);
		},
		getAttachment: async ({ client }, guid, phone) => {
			if (client.length === 0) throw new Error("No iMessage clients configured");
			const routedPhone = (() => {
				if (isSharedMode(client)) return SHARED_PHONE;
				if (phone) return phone;
				if (client.length === 1) return client[0].phone;
				throw new Error(`imessage.getAttachment requires a phone in multi-phone mode. Available: ${availablePhones(client).join(", ")}`);
			})();
			const remote = clientForPhone(client, routedPhone);
			return withSpan("spectrum.imessage.getAttachment", {
				"spectrum.provider": IMESSAGE_PLATFORM,
				"spectrum.imessage.attachment.guid": guid,
				"spectrum.imessage.phone": routedPhone
			}, () => getRemoteAttachment(remote, guid));
		}
	}
});
/**
* Retain iMessage's required message-schema slot in the public platform type.
* The manifest generator also validates the canonical
* `export const imessage = definePlatform(...)` form, so this provider-local
* helper performs the type refinement while preserving that export contract.
*/
const definePlatform$1 = (_platformId, platform) => platform;
const imessage = definePlatform$1(IMESSAGE_PLATFORM, definedIMessage);
//#endregion
export { background, customizedMiniApp, effect, imessage, nativeContactCard, read };
