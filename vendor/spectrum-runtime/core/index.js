import { $ as drainStreamText, A as reaction, B as removeMember, D as read, F as poll, H as asMarkdown, I as addMember, J as resolveContents, K as group, L as addMemberSchema, P as option, Q as StreamConsumedError, R as leaveSpace, S as reply, T as renameSchema, U as markdown, V as removeMemberSchema, X as text, Y as asText, a as createTokenRenewal, at as toVCard, b as richlink, c as senderAttrs, d as classifyIdentifier, dt as attachment, et as asCustom, f as envAwareConfig, g as voice, h as asVoice, i as stream, it as fromVCard, l as markdownToPlainText, mt as tracedFetch, n as createAsyncQueue, nt as asContact, o as contentAttrs, ot as avatar, p as envFor, pt as fetchUrlBytes, r as mergeStreams, rt as contact, s as errorAttrs, st as avatarSchema, t as broadcast, tt as custom, ut as asAttachment, v as unsend, w as rename, z as leaveSpaceSchema } from "./stream-t0CbOOUT.js";
import z from "zod";
import { createRequire } from "node:module";
const ogs = createRequire(import.meta.url)("open-graph-scraper");
import { createLogger, setLogLevel, setupOtel, withSpan } from "@photon-ai/otel";
import { createHmac, timingSafeEqual } from "node:crypto";
import { RawInboundEvent } from "@photon-ai/proto/photon/fusor/v1/inbound";
//#region src/utils/link-metadata.ts
const DEFAULT_TIMEOUT_MS = 5e3;
const USER_AGENT = "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_0) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Safari/605.1.15 spectrum-ts/richlink";
const normaliseImageUrl = (raw, base) => {
	try {
		return new URL(raw, base).toString();
	} catch {
		return;
	}
};
const cleanString = (v) => {
	if (typeof v !== "string") return;
	const trimmed = v.trim();
	return trimmed.length > 0 ? trimmed : void 0;
};
const fetchLinkMetadata = async (url) => {
	try {
		const result = await ogs({
			url,
			timeout: DEFAULT_TIMEOUT_MS,
			fetchOptions: { headers: { "User-Agent": USER_AGENT } }
		});
		if (result.error) return {};
		const { ogTitle, ogDescription, ogImage, ogSiteName, twitterTitle, twitterDescription, twitterImage } = result.result;
		const title = cleanString(ogTitle) ?? cleanString(twitterTitle);
		const summary = cleanString(ogDescription) ?? cleanString(twitterDescription);
		const siteName = cleanString(ogSiteName);
		const imageCandidate = ogImage?.[0] ?? twitterImage?.[0];
		const resolved = imageCandidate ? normaliseImageUrl(imageCandidate.url, url) : void 0;
		return {
			title,
			summary,
			siteName,
			image: imageCandidate && resolved ? {
				url: resolved,
				mimeType: "type" in imageCandidate && typeof imageCandidate.type === "string" ? imageCandidate.type : void 0
			} : void 0
		};
	} catch {
		return {};
	}
};
const fetchImage = (url) => fetchUrlBytes(new URL(url), {
	timeoutMs: DEFAULT_TIMEOUT_MS,
	headers: { "User-Agent": USER_AGENT }
});
//#endregion
//#region src/content/app.ts
/**
* Visible layout of an app card. Mirrors Apple's `MSMessageTemplateLayout`
* (the iMessage mini-app surface) — the iMessage provider renders it natively;
* other platforms ignore it and fall back to the bare URL. At least one of
* `caption`, `subcaption`, `trailingCaption`, `trailingSubcaption`, or `image`
* must be set so the bubble is not empty — `summary` is the fallback text shown
* on surfaces that cannot render the card and is not a visible slot on its own.
* `image` and `imageTitle` must be set together; `imageSubtitle` requires
* `image`.
*/
const appLayoutSchema = z.object({
	caption: z.string().nonempty().optional(),
	subcaption: z.string().nonempty().optional(),
	trailingCaption: z.string().nonempty().optional(),
	trailingSubcaption: z.string().nonempty().optional(),
	image: z.instanceof(Uint8Array).optional(),
	imageTitle: z.string().nonempty().optional(),
	imageSubtitle: z.string().nonempty().optional(),
	summary: z.string().nonempty().optional()
}).refine((layout) => layout.caption !== void 0 || layout.subcaption !== void 0 || layout.trailingCaption !== void 0 || layout.trailingSubcaption !== void 0 || layout.image !== void 0, { message: "layout must set at least one of caption, subcaption, trailingCaption, trailingSubcaption, image" }).refine((layout) => layout.image === void 0 === (layout.imageTitle === void 0), {
	message: "layout.image and layout.imageTitle must be set together",
	path: ["imageTitle"]
}).refine((layout) => layout.imageSubtitle === void 0 || layout.image !== void 0, {
	message: "layout.imageSubtitle requires layout.image",
	path: ["imageSubtitle"]
});
const urlAccessor = z.function({
	input: [],
	output: z.promise(z.url())
});
const layoutAccessor = z.function({
	input: [],
	output: z.promise(appLayoutSchema)
});
const appSchema = z.object({
	type: z.literal("app"),
	url: urlAccessor,
	layout: layoutAccessor,
	live: z.boolean().optional()
});
const memoize = (factory) => {
	let cached;
	return () => {
		cached ??= factory();
		return cached;
	};
};
const resolveUrl = (url) => memoize(async () => typeof url === "function" ? await url() : await url);
const WWW_PREFIX = /^www\./;
const siteHost = (url) => {
	try {
		return new URL(url).host.replace(WWW_PREFIX, "");
	} catch {
		return url;
	}
};
const JPEG_PROXY = "https://wsrv.nl/";
const MAX_IMAGE_WIDTH = 1200;
const toJpegUrl = (imageUrl) => `${JPEG_PROXY}?url=${encodeURIComponent(imageUrl)}&output=jpg&w=${MAX_IMAGE_WIDTH}`;
const isJpeg = (bytes) => bytes.length > 2 && bytes[0] === 255 && bytes[1] === 216;
const buildLayout = (metadata, url, image) => {
	const title = metadata.title ?? metadata.siteName ?? siteHost(url);
	if (image) return appLayoutSchema.parse({
		caption: title,
		subcaption: metadata.summary,
		image,
		imageTitle: metadata.siteName ?? title,
		summary: title
	});
	return appLayoutSchema.parse({
		caption: title,
		subcaption: metadata.summary,
		summary: title
	});
};
/**
* Construct an `app` content value.
*
* `url` is stored as a lazy accessor; `layout` is derived from the URL's Open
* Graph / link metadata (title → caption, og:site_name → image overlay,
* description → subcaption, og:image → JPEG-transcoded image) using the same
* machinery as `richlink`. A single metadata fetch is shared and memoized
* across `url()` / `layout()`; fetch / parse failures resolve to a host-only
* caption (no throw, no retry).
*/
const asApp = (url, options = {}) => {
	const getUrl = resolveUrl(url);
	const getMetadata = memoize(async () => fetchLinkMetadata(await getUrl()));
	const getLayout = memoize(async () => {
		const resolvedUrl = await getUrl();
		const metadata = await getMetadata();
		let image;
		if (metadata.image) try {
			const bytes = (await fetchImage(toJpegUrl(metadata.image.url))).data;
			image = isJpeg(bytes) ? bytes : void 0;
		} catch {
			image = void 0;
		}
		return buildLayout(metadata, resolvedUrl, image);
	});
	return appSchema.parse({
		type: "app",
		url: getUrl,
		layout: getLayout,
		...options
	});
};
/**
* Construct an app card from a URL.
*
* Pass `{ live: true }` to ask supported platforms to render the installed
* app extension's live UI. Other platforms retain their normal URL fallback.
*/
function app(url, options = {}) {
	return { build: async () => asApp(url, options) };
}
//#endregion
//#region src/content/edit.ts
const isMessage = (v) => typeof v === "object" && v !== null && "id" in v && "content" in v;
const isContent = (v) => typeof v === "object" && v !== null && "type" in v && typeof v.type === "string";
/**
* An `edit` rewrites the content of a previously-sent outbound message.
*
* `space.send(edit(newContent, message))` is the canonical outbound API;
* `message.edit(newContent)` and `space.edit(message, newContent)` are
* sugar that delegate here. Edits are fire-and-forget — providers handle
* them inside their `send` action and the resolved value is `undefined`
* (no new message id is produced; the existing message mutates in place).
*
* Edit cannot wrap `edit`, `reply`, `reaction`, `group`, `typing`, `rename`,
* `avatar`, `addMember`, `removeMember`, `leaveSpace`, `unsend`, or `read`
* content.
*/
const editSchema = z.object({
	type: z.literal("edit"),
	content: z.custom(isContent, { message: "edit content must be a Content value" }),
	target: z.custom(isMessage, { message: "edit target must be a Message" })
});
const asEdit = (input) => editSchema.parse({
	type: "edit",
	...input
});
/**
* Construct an `edit` content value rewriting `target`'s content.
*
* Only outbound messages (those sent by the agent) can be edited; calling
* this with an inbound target throws at build time so the misuse surfaces
* before the send pipeline runs.
*/
function edit(content, target) {
	return { build: async () => {
		if (!target) throw new Error("edit() target is undefined — the targeted message was never sent (space.send resolves undefined when a platform skips unsupported content)");
		if (target.direction !== "outbound") throw new Error(`edit() target must be an outbound message (got direction "${target.direction}", message id "${target.id}")`);
		const [resolved] = await resolveContents([content]);
		if (!resolved) throw new Error("edit() requires content");
		if (resolved.type === "edit" || resolved.type === "reply" || resolved.type === "reaction" || resolved.type === "group" || resolved.type === "typing" || resolved.type === "rename" || resolved.type === "avatar" || resolved.type === "addMember" || resolved.type === "removeMember" || resolved.type === "leaveSpace" || resolved.type === "unsend" || resolved.type === "read") throw new Error(`edit() cannot wrap "${resolved.type}" content`);
		return asEdit({
			content: resolved,
			target
		});
	} };
}
//#endregion
//#region src/content/typing.ts
/**
* A `typing` content value carries a typing-indicator signal — either
* `"start"` or `"stop"`. Like `edit`, it's fire-and-forget: providers
* dispatch on `content.type === "typing"` inside their `send()` action and
* `space.send(typing(...))` resolves to `undefined`.
*
* `space.startTyping()` / `space.stopTyping()` / `space.responding()` are
* sugar over `space.send(typing("start" | "stop"))`. Platforms that have no
* typing-indicator API (e.g. WhatsApp Business) silently no-op so the
* signal is best-effort everywhere.
*/
const typingSchema = z.object({
	type: z.literal("typing"),
	state: z.enum(["start", "stop"])
});
/**
* Construct a `typing` content value. Defaults to `"start"`.
*
* `space.send(typing())` is equivalent to `space.startTyping()`;
* `space.send(typing("stop"))` is equivalent to `space.stopTyping()`.
*/
function typing(state = "start") {
	return { build: async () => typingSchema.parse({
		type: "typing",
		state
	}) };
}
//#endregion
//#region src/emoji/generated.ts
const GeneratedEmoji = {
	_1stPlaceMedal: "🥇",
	_2ndPlaceMedal: "🥈",
	_3rdPlaceMedal: "🥉",
	abacus: "🧮",
	abButton: "🆎",
	aButton: "🅰️",
	accordion: "🪗",
	adhesiveBandage: "🩹",
	admissionTickets: "🎟️",
	aerialTramway: "🚡",
	airplane: "✈️",
	airplaneArrival: "🛬",
	airplaneDeparture: "🛫",
	alarmClock: "⏰",
	alembic: "⚗️",
	alien: "👽",
	alienMonster: "👾",
	ambulance: "🚑",
	americanFootball: "🏈",
	amphora: "🏺",
	anatomicalHeart: "🫀",
	anchor: "⚓",
	angerSymbol: "💢",
	angryFace: "😠",
	angryFaceWithHorns: "👿",
	anguishedFace: "😧",
	ant: "🐜",
	antennaBars: "📶",
	anxiousFaceWithSweat: "😰",
	aquarius: "♒",
	aries: "♈",
	articulatedLorry: "🚛",
	artist: "🧑‍🎨",
	artistPalette: "🎨",
	astonishedFace: "😲",
	astronaut: "🧑‍🚀",
	atmSign: "🏧",
	atomSymbol: "⚛️",
	automobile: "🚗",
	autoRickshaw: "🛺",
	avocado: "🥑",
	axe: "🪓",
	baby: "👶",
	babyAngel: "👼",
	babyBottle: "🍼",
	babyChick: "🐤",
	babySymbol: "🚼",
	backArrow: "🔙",
	backhandIndexPointingDown: "👇",
	backhandIndexPointingLeft: "👈",
	backhandIndexPointingRight: "👉",
	backhandIndexPointingUp: "👆",
	backpack: "🎒",
	bacon: "🥓",
	badger: "🦡",
	badminton: "🏸",
	bagel: "🥯",
	baggageClaim: "🛄",
	baguetteBread: "🥖",
	balanceScale: "⚖️",
	balletDancer: "🧑‍🩰",
	balletShoes: "🩰",
	balloon: "🎈",
	ballotBoxWithBallot: "🗳️",
	banana: "🍌",
	banjo: "🪕",
	bank: "🏦",
	barberPole: "💈",
	barChart: "📊",
	baseball: "⚾",
	basket: "🧺",
	basketball: "🏀",
	bat: "🦇",
	bathtub: "🛁",
	battery: "🔋",
	bButton: "🅱️",
	beachWithUmbrella: "🏖️",
	beamingFaceWithSmilingEyes: "😁",
	beans: "🫘",
	bear: "🐻",
	beatingHeart: "💓",
	beaver: "🦫",
	bed: "🛏️",
	beerMug: "🍺",
	beetle: "🪲",
	bell: "🔔",
	bellhopBell: "🛎️",
	bellPepper: "🫑",
	bellWithSlash: "🔕",
	bentoBox: "🍱",
	beverageBox: "🧃",
	bicycle: "🚲",
	bikini: "👙",
	billedCap: "🧢",
	biohazard: "☣️",
	bird: "🐦",
	birthdayCake: "🎂",
	bison: "🦬",
	bitingLip: "🫦",
	blackBird: "🐦‍⬛",
	blackCat: "🐈‍⬛",
	blackCircle: "⚫",
	blackFlag: "🏴",
	blackHeart: "🖤",
	blackLargeSquare: "⬛",
	blackMediumSmallSquare: "◾",
	blackMediumSquare: "◼️",
	blackNib: "✒️",
	blackSmallSquare: "▪️",
	blackSquareButton: "🔲",
	blossom: "🌼",
	blowfish: "🐡",
	blueberries: "🫐",
	blueBook: "📘",
	blueCircle: "🔵",
	blueHeart: "💙",
	blueSquare: "🟦",
	boar: "🐗",
	bomb: "💣",
	bone: "🦴",
	bookmark: "🔖",
	bookmarkTabs: "📑",
	books: "📚",
	boomerang: "🪃",
	bottleWithPoppingCork: "🍾",
	bouquet: "💐",
	bowAndArrow: "🏹",
	bowling: "🎳",
	bowlWithSpoon: "🥣",
	boxingGlove: "🥊",
	boy: "👦",
	brain: "🧠",
	bread: "🍞",
	breastFeeding: "🤱",
	brick: "🧱",
	bridgeAtNight: "🌉",
	briefcase: "💼",
	briefs: "🩲",
	brightButton: "🔆",
	broccoli: "🥦",
	brokenChain: "⛓️‍💥",
	brokenHeart: "💔",
	broom: "🧹",
	brownCircle: "🟤",
	brownHeart: "🤎",
	brownMushroom: "🍄‍🟫",
	brownSquare: "🟫",
	bubbles: "🫧",
	bubbleTea: "🧋",
	bucket: "🪣",
	bug: "🐛",
	buildingConstruction: "🏗️",
	bulletTrain: "🚅",
	bullseye: "🎯",
	burrito: "🌯",
	bus: "🚌",
	busStop: "🚏",
	bustInSilhouette: "👤",
	bustsInSilhouette: "👥",
	butter: "🧈",
	butterfly: "🦋",
	cactus: "🌵",
	calendar: "📅",
	callMeHand: "🤙",
	camel: "🐪",
	camera: "📷",
	cameraWithFlash: "📸",
	camping: "🏕️",
	cancer: "♋",
	candle: "🕯️",
	candy: "🍬",
	cannedFood: "🥫",
	canoe: "🛶",
	capricorn: "♑",
	cardFileBox: "🗃️",
	cardIndex: "📇",
	cardIndexDividers: "🗂️",
	carouselHorse: "🎠",
	carpentrySaw: "🪚",
	carpStreamer: "🎏",
	carrot: "🥕",
	castle: "🏰",
	cat: "🐈",
	catFace: "🐱",
	catWithTearsOfJoy: "😹",
	catWithWrySmile: "😼",
	chains: "⛓️",
	chair: "🪑",
	chartDecreasing: "📉",
	chartIncreasing: "📈",
	chartIncreasingWithYen: "💹",
	checkBoxWithCheck: "☑️",
	checkMark: "✔️",
	checkMarkButton: "✅",
	cheeseWedge: "🧀",
	chequeredFlag: "🏁",
	cherries: "🍒",
	cherryBlossom: "🌸",
	chessPawn: "♟️",
	chestnut: "🌰",
	chicken: "🐔",
	child: "🧒",
	childrenCrossing: "🚸",
	chipmunk: "🐿️",
	chocolateBar: "🍫",
	chopsticks: "🥢",
	christmasTree: "🎄",
	church: "⛪",
	cigarette: "🚬",
	cinema: "🎦",
	circledM: "Ⓜ️",
	circusTent: "🎪",
	cityscape: "🏙️",
	cityscapeAtDusk: "🌆",
	clamp: "🗜️",
	clapperBoard: "🎬",
	clappingHands: "👏",
	classicalBuilding: "🏛️",
	clButton: "🆑",
	clinkingBeerMugs: "🍻",
	clinkingGlasses: "🥂",
	clipboard: "📋",
	clockwiseVerticalArrows: "🔃",
	closedBook: "📕",
	closedMailboxWithLoweredFlag: "📪",
	closedMailboxWithRaisedFlag: "📫",
	closedUmbrella: "🌂",
	cloud: "☁️",
	cloudWithLightning: "🌩️",
	cloudWithLightningAndRain: "⛈️",
	cloudWithRain: "🌧️",
	cloudWithSnow: "🌨️",
	clownFace: "🤡",
	clubSuit: "♣️",
	clutchBag: "👝",
	coat: "🧥",
	cockroach: "🪳",
	cocktailGlass: "🍸",
	coconut: "🥥",
	coffin: "⚰️",
	coin: "🪙",
	coldFace: "🥶",
	collision: "💥",
	comet: "☄️",
	compass: "🧭",
	computerDisk: "💽",
	computerMouse: "🖱️",
	confettiBall: "🎊",
	confoundedFace: "😖",
	confusedFace: "😕",
	construction: "🚧",
	constructionWorker: "👷",
	controlKnobs: "🎛️",
	convenienceStore: "🏪",
	cook: "🧑‍🍳",
	cookedRice: "🍚",
	cookie: "🍪",
	cooking: "🍳",
	coolButton: "🆒",
	copyright: "©️",
	coral: "🪸",
	couchAndLamp: "🛋️",
	counterclockwiseArrowsButton: "🔄",
	coupleWithHeart: "💑",
	coupleWithHeartManMan: "👨‍❤️‍👨",
	coupleWithHeartWomanMan: "👩‍❤️‍👨",
	coupleWithHeartWomanWoman: "👩‍❤️‍👩",
	cow: "🐄",
	cowboyHatFace: "🤠",
	cowFace: "🐮",
	crab: "🦀",
	crayon: "🖍️",
	creditCard: "💳",
	crescentMoon: "🌙",
	cricket: "🦗",
	cricketGame: "🏏",
	crocodile: "🐊",
	croissant: "🥐",
	crossedFingers: "🤞",
	crossedFlags: "🎌",
	crossedSwords: "⚔️",
	crossMark: "❌",
	crossMarkButton: "❎",
	crown: "👑",
	crutch: "🩼",
	cryingCat: "😿",
	cryingFace: "😢",
	crystalBall: "🔮",
	cucumber: "🥒",
	cupcake: "🧁",
	cupWithStraw: "🥤",
	curlingStone: "🥌",
	curlyLoop: "➰",
	currencyExchange: "💱",
	curryRice: "🍛",
	custard: "🍮",
	customs: "🛃",
	cutOfMeat: "🥩",
	cyclone: "🌀",
	dagger: "🗡️",
	dango: "🍡",
	dashingAway: "💨",
	deafMan: "🧏‍♂️",
	deafPerson: "🧏",
	deafWoman: "🧏‍♀️",
	deciduousTree: "🌳",
	deer: "🦌",
	deliveryTruck: "🚚",
	departmentStore: "🏬",
	derelictHouse: "🏚️",
	desert: "🏜️",
	desertIsland: "🏝️",
	desktopComputer: "🖥️",
	detective: "🕵️",
	diamondSuit: "♦️",
	diamondWithADot: "💠",
	dimButton: "🔅",
	disappointedFace: "😞",
	disguisedFace: "🥸",
	distortedFace: "🫪",
	divide: "➗",
	divingMask: "🤿",
	diyaLamp: "🪔",
	dizzy: "💫",
	dna: "🧬",
	dodo: "🦤",
	dog: "🐕",
	dogFace: "🐶",
	dollarBanknote: "💵",
	dolphin: "🐬",
	donkey: "🫏",
	door: "🚪",
	dottedLineFace: "🫥",
	dottedSixPointedStar: "🔯",
	doubleCurlyLoop: "➿",
	doubleExclamationMark: "‼️",
	doughnut: "🍩",
	dove: "🕊️",
	downArrow: "⬇️",
	downcastFaceWithSweat: "😓",
	downLeftArrow: "↙️",
	downRightArrow: "↘️",
	downwardsButton: "🔽",
	dragon: "🐉",
	dragonFace: "🐲",
	dress: "👗",
	droolingFace: "🤤",
	droplet: "💧",
	dropOfBlood: "🩸",
	drum: "🥁",
	duck: "🦆",
	dumpling: "🥟",
	dvd: "📀",
	eagle: "🦅",
	ear: "👂",
	earOfCorn: "🌽",
	earWithHearingAid: "🦻",
	egg: "🥚",
	eggplant: "🍆",
	eightOClock: "🕗",
	eightPointedStar: "✴️",
	eightSpokedAsterisk: "✳️",
	eightThirty: "🕣",
	ejectButton: "⏏️",
	electricPlug: "🔌",
	elephant: "🐘",
	elevator: "🛗",
	elevenOClock: "🕚",
	elevenThirty: "🕦",
	elf: "🧝",
	eMail: "📧",
	emptyNest: "🪹",
	endArrow: "🔚",
	enragedFace: "😡",
	envelope: "✉️",
	envelopeWithArrow: "📩",
	euroBanknote: "💶",
	evergreenTree: "🌲",
	ewe: "🐑",
	exclamationQuestionMark: "⁉️",
	explodingHead: "🤯",
	expressionlessFace: "😑",
	eye: "👁️",
	eyeInSpeechBubble: "👁️‍🗨️",
	eyes: "👀",
	faceBlowingAKiss: "😘",
	faceExhaling: "😮‍💨",
	faceHoldingBackTears: "🥹",
	faceInClouds: "😶‍🌫️",
	faceSavoringFood: "😋",
	faceScreamingInFear: "😱",
	faceVomiting: "🤮",
	faceWithBagsUnderEyes: "🫩",
	faceWithCrossedOutEyes: "😵",
	faceWithDiagonalMouth: "🫤",
	faceWithHandOverMouth: "🤭",
	faceWithHeadBandage: "🤕",
	faceWithMedicalMask: "😷",
	faceWithMonocle: "🧐",
	faceWithOpenEyesAndHandOverMouth: "🫢",
	faceWithOpenMouth: "😮",
	faceWithoutMouth: "😶",
	faceWithPeekingEye: "🫣",
	faceWithRaisedEyebrow: "🤨",
	faceWithRollingEyes: "🙄",
	faceWithSpiralEyes: "😵‍💫",
	faceWithSteamFromNose: "😤",
	faceWithSymbolsOnMouth: "🤬",
	faceWithTearsOfJoy: "😂",
	faceWithThermometer: "🤒",
	faceWithTongue: "😛",
	factory: "🏭",
	factoryWorker: "🧑‍🏭",
	fairy: "🧚",
	falafel: "🧆",
	fallenLeaf: "🍂",
	family: "👪",
	familyAdultAdultChild: "🧑‍🧑‍🧒",
	familyAdultAdultChildChild: "🧑‍🧑‍🧒‍🧒",
	familyAdultChild: "🧑‍🧒",
	familyAdultChildChild: "🧑‍🧒‍🧒",
	familyManBoy: "👨‍👦",
	familyManBoyBoy: "👨‍👦‍👦",
	familyManGirl: "👨‍👧",
	familyManGirlBoy: "👨‍👧‍👦",
	familyManGirlGirl: "👨‍👧‍👧",
	familyManManBoy: "👨‍👨‍👦",
	familyManManBoyBoy: "👨‍👨‍👦‍👦",
	familyManManGirl: "👨‍👨‍👧",
	familyManManGirlBoy: "👨‍👨‍👧‍👦",
	familyManManGirlGirl: "👨‍👨‍👧‍👧",
	familyManWomanBoy: "👨‍👩‍👦",
	familyManWomanBoyBoy: "👨‍👩‍👦‍👦",
	familyManWomanGirl: "👨‍👩‍👧",
	familyManWomanGirlBoy: "👨‍👩‍👧‍👦",
	familyManWomanGirlGirl: "👨‍👩‍👧‍👧",
	familyWomanBoy: "👩‍👦",
	familyWomanBoyBoy: "👩‍👦‍👦",
	familyWomanGirl: "👩‍👧",
	familyWomanGirlBoy: "👩‍👧‍👦",
	familyWomanGirlGirl: "👩‍👧‍👧",
	familyWomanWomanBoy: "👩‍👩‍👦",
	familyWomanWomanBoyBoy: "👩‍👩‍👦‍👦",
	familyWomanWomanGirl: "👩‍👩‍👧",
	familyWomanWomanGirlBoy: "👩‍👩‍👧‍👦",
	familyWomanWomanGirlGirl: "👩‍👩‍👧‍👧",
	farmer: "🧑‍🌾",
	fastDownButton: "⏬",
	fastForwardButton: "⏩",
	fastReverseButton: "⏪",
	fastUpButton: "⏫",
	faxMachine: "📠",
	fearfulFace: "😨",
	feather: "🪶",
	femaleSign: "♀️",
	ferrisWheel: "🎡",
	ferry: "⛴️",
	fieldHockey: "🏑",
	fightCloud: "🫯",
	fileCabinet: "🗄️",
	fileFolder: "📁",
	filmFrames: "🎞️",
	filmProjector: "📽️",
	fingerprint: "🫆",
	fire: "🔥",
	firecracker: "🧨",
	fireEngine: "🚒",
	fireExtinguisher: "🧯",
	firefighter: "🧑‍🚒",
	fireworks: "🎆",
	firstQuarterMoon: "🌓",
	firstQuarterMoonFace: "🌛",
	fish: "🐟",
	fishCakeWithSwirl: "🍥",
	fishingPole: "🎣",
	fiveOClock: "🕔",
	fiveThirty: "🕠",
	flagAfghanistan: "🇦🇫",
	flagAlandIslands: "🇦🇽",
	flagAlbania: "🇦🇱",
	flagAlgeria: "🇩🇿",
	flagAmericanSamoa: "🇦🇸",
	flagAndorra: "🇦🇩",
	flagAngola: "🇦🇴",
	flagAnguilla: "🇦🇮",
	flagAntarctica: "🇦🇶",
	flagAntiguaBarbuda: "🇦🇬",
	flagArgentina: "🇦🇷",
	flagArmenia: "🇦🇲",
	flagAruba: "🇦🇼",
	flagAscensionIsland: "🇦🇨",
	flagAustralia: "🇦🇺",
	flagAustria: "🇦🇹",
	flagAzerbaijan: "🇦🇿",
	flagBahamas: "🇧🇸",
	flagBahrain: "🇧🇭",
	flagBangladesh: "🇧🇩",
	flagBarbados: "🇧🇧",
	flagBelarus: "🇧🇾",
	flagBelgium: "🇧🇪",
	flagBelize: "🇧🇿",
	flagBenin: "🇧🇯",
	flagBermuda: "🇧🇲",
	flagBhutan: "🇧🇹",
	flagBolivia: "🇧🇴",
	flagBosniaHerzegovina: "🇧🇦",
	flagBotswana: "🇧🇼",
	flagBouvetIsland: "🇧🇻",
	flagBrazil: "🇧🇷",
	flagBritishIndianOceanTerritory: "🇮🇴",
	flagBritishVirginIslands: "🇻🇬",
	flagBrunei: "🇧🇳",
	flagBulgaria: "🇧🇬",
	flagBurkinaFaso: "🇧🇫",
	flagBurundi: "🇧🇮",
	flagCambodia: "🇰🇭",
	flagCameroon: "🇨🇲",
	flagCanada: "🇨🇦",
	flagCanaryIslands: "🇮🇨",
	flagCapeVerde: "🇨🇻",
	flagCaribbeanNetherlands: "🇧🇶",
	flagCaymanIslands: "🇰🇾",
	flagCentralAfricanRepublic: "🇨🇫",
	flagCeutaMelilla: "🇪🇦",
	flagChad: "🇹🇩",
	flagChile: "🇨🇱",
	flagChina: "🇨🇳",
	flagChristmasIsland: "🇨🇽",
	flagClippertonIsland: "🇨🇵",
	flagCocosIslands: "🇨🇨",
	flagColombia: "🇨🇴",
	flagComoros: "🇰🇲",
	flagCongoBrazzaville: "🇨🇬",
	flagCongoKinshasa: "🇨🇩",
	flagCookIslands: "🇨🇰",
	flagCostaRica: "🇨🇷",
	flagCoteDIvoire: "🇨🇮",
	flagCroatia: "🇭🇷",
	flagCuba: "🇨🇺",
	flagCuracao: "🇨🇼",
	flagCyprus: "🇨🇾",
	flagCzechia: "🇨🇿",
	flagDenmark: "🇩🇰",
	flagDiegoGarcia: "🇩🇬",
	flagDjibouti: "🇩🇯",
	flagDominica: "🇩🇲",
	flagDominicanRepublic: "🇩🇴",
	flagEcuador: "🇪🇨",
	flagEgypt: "🇪🇬",
	flagElSalvador: "🇸🇻",
	flagEngland: "🏴󠁧󠁢󠁥󠁮󠁧󠁿",
	flagEquatorialGuinea: "🇬🇶",
	flagEritrea: "🇪🇷",
	flagEstonia: "🇪🇪",
	flagEswatini: "🇸🇿",
	flagEthiopia: "🇪🇹",
	flagEuropeanUnion: "🇪🇺",
	flagFalklandIslands: "🇫🇰",
	flagFaroeIslands: "🇫🇴",
	flagFiji: "🇫🇯",
	flagFinland: "🇫🇮",
	flagFrance: "🇫🇷",
	flagFrenchGuiana: "🇬🇫",
	flagFrenchPolynesia: "🇵🇫",
	flagFrenchSouthernTerritories: "🇹🇫",
	flagGabon: "🇬🇦",
	flagGambia: "🇬🇲",
	flagGeorgia: "🇬🇪",
	flagGermany: "🇩🇪",
	flagGhana: "🇬🇭",
	flagGibraltar: "🇬🇮",
	flagGreece: "🇬🇷",
	flagGreenland: "🇬🇱",
	flagGrenada: "🇬🇩",
	flagGuadeloupe: "🇬🇵",
	flagGuam: "🇬🇺",
	flagGuatemala: "🇬🇹",
	flagGuernsey: "🇬🇬",
	flagGuinea: "🇬🇳",
	flagGuineaBissau: "🇬🇼",
	flagGuyana: "🇬🇾",
	flagHaiti: "🇭🇹",
	flagHeardMcdonaldIslands: "🇭🇲",
	flagHonduras: "🇭🇳",
	flagHongKongSarChina: "🇭🇰",
	flagHungary: "🇭🇺",
	flagIceland: "🇮🇸",
	flagIndia: "🇮🇳",
	flagIndonesia: "🇮🇩",
	flagInHole: "⛳",
	flagIran: "🇮🇷",
	flagIraq: "🇮🇶",
	flagIreland: "🇮🇪",
	flagIsleOfMan: "🇮🇲",
	flagIsrael: "🇮🇱",
	flagItaly: "🇮🇹",
	flagJamaica: "🇯🇲",
	flagJapan: "🇯🇵",
	flagJersey: "🇯🇪",
	flagJordan: "🇯🇴",
	flagKazakhstan: "🇰🇿",
	flagKenya: "🇰🇪",
	flagKiribati: "🇰🇮",
	flagKosovo: "🇽🇰",
	flagKuwait: "🇰🇼",
	flagKyrgyzstan: "🇰🇬",
	flagLaos: "🇱🇦",
	flagLatvia: "🇱🇻",
	flagLebanon: "🇱🇧",
	flagLesotho: "🇱🇸",
	flagLiberia: "🇱🇷",
	flagLibya: "🇱🇾",
	flagLiechtenstein: "🇱🇮",
	flagLithuania: "🇱🇹",
	flagLuxembourg: "🇱🇺",
	flagMacaoSarChina: "🇲🇴",
	flagMadagascar: "🇲🇬",
	flagMalawi: "🇲🇼",
	flagMalaysia: "🇲🇾",
	flagMaldives: "🇲🇻",
	flagMali: "🇲🇱",
	flagMalta: "🇲🇹",
	flagMarshallIslands: "🇲🇭",
	flagMartinique: "🇲🇶",
	flagMauritania: "🇲🇷",
	flagMauritius: "🇲🇺",
	flagMayotte: "🇾🇹",
	flagMexico: "🇲🇽",
	flagMicronesia: "🇫🇲",
	flagMoldova: "🇲🇩",
	flagMonaco: "🇲🇨",
	flagMongolia: "🇲🇳",
	flagMontenegro: "🇲🇪",
	flagMontserrat: "🇲🇸",
	flagMorocco: "🇲🇦",
	flagMozambique: "🇲🇿",
	flagMyanmar: "🇲🇲",
	flagNamibia: "🇳🇦",
	flagNauru: "🇳🇷",
	flagNepal: "🇳🇵",
	flagNetherlands: "🇳🇱",
	flagNewCaledonia: "🇳🇨",
	flagNewZealand: "🇳🇿",
	flagNicaragua: "🇳🇮",
	flagNiger: "🇳🇪",
	flagNigeria: "🇳🇬",
	flagNiue: "🇳🇺",
	flagNorfolkIsland: "🇳🇫",
	flagNorthernMarianaIslands: "🇲🇵",
	flagNorthKorea: "🇰🇵",
	flagNorthMacedonia: "🇲🇰",
	flagNorway: "🇳🇴",
	flagOman: "🇴🇲",
	flagPakistan: "🇵🇰",
	flagPalau: "🇵🇼",
	flagPalestinianTerritories: "🇵🇸",
	flagPanama: "🇵🇦",
	flagPapuaNewGuinea: "🇵🇬",
	flagParaguay: "🇵🇾",
	flagPeru: "🇵🇪",
	flagPhilippines: "🇵🇭",
	flagPitcairnIslands: "🇵🇳",
	flagPoland: "🇵🇱",
	flagPortugal: "🇵🇹",
	flagPuertoRico: "🇵🇷",
	flagQatar: "🇶🇦",
	flagReunion: "🇷🇪",
	flagRomania: "🇷🇴",
	flagRussia: "🇷🇺",
	flagRwanda: "🇷🇼",
	flagSamoa: "🇼🇸",
	flagSanMarino: "🇸🇲",
	flagSaoTomePrincipe: "🇸🇹",
	flagSark: "🇨🇶",
	flagSaudiArabia: "🇸🇦",
	flagScotland: "🏴󠁧󠁢󠁳󠁣󠁴󠁿",
	flagSenegal: "🇸🇳",
	flagSerbia: "🇷🇸",
	flagSeychelles: "🇸🇨",
	flagSierraLeone: "🇸🇱",
	flagSingapore: "🇸🇬",
	flagSintMaarten: "🇸🇽",
	flagSlovakia: "🇸🇰",
	flagSlovenia: "🇸🇮",
	flagSolomonIslands: "🇸🇧",
	flagSomalia: "🇸🇴",
	flagSouthAfrica: "🇿🇦",
	flagSouthGeorgiaSouthSandwichIslands: "🇬🇸",
	flagSouthKorea: "🇰🇷",
	flagSouthSudan: "🇸🇸",
	flagSpain: "🇪🇸",
	flagSriLanka: "🇱🇰",
	flagStBarthelemy: "🇧🇱",
	flagStHelena: "🇸🇭",
	flagStKittsNevis: "🇰🇳",
	flagStLucia: "🇱🇨",
	flagStMartin: "🇲🇫",
	flagStPierreMiquelon: "🇵🇲",
	flagStVincentGrenadines: "🇻🇨",
	flagSudan: "🇸🇩",
	flagSuriname: "🇸🇷",
	flagSvalbardJanMayen: "🇸🇯",
	flagSweden: "🇸🇪",
	flagSwitzerland: "🇨🇭",
	flagSyria: "🇸🇾",
	flagTaiwan: "🇹🇼",
	flagTajikistan: "🇹🇯",
	flagTanzania: "🇹🇿",
	flagThailand: "🇹🇭",
	flagTimorLeste: "🇹🇱",
	flagTogo: "🇹🇬",
	flagTokelau: "🇹🇰",
	flagTonga: "🇹🇴",
	flagTrinidadTobago: "🇹🇹",
	flagTristanDaCunha: "🇹🇦",
	flagTunisia: "🇹🇳",
	flagTurkiye: "🇹🇷",
	flagTurkmenistan: "🇹🇲",
	flagTurksCaicosIslands: "🇹🇨",
	flagTuvalu: "🇹🇻",
	flagUganda: "🇺🇬",
	flagUkraine: "🇺🇦",
	flagUnitedArabEmirates: "🇦🇪",
	flagUnitedKingdom: "🇬🇧",
	flagUnitedNations: "🇺🇳",
	flagUnitedStates: "🇺🇸",
	flagUruguay: "🇺🇾",
	flagUSOutlyingIslands: "🇺🇲",
	flagUSVirginIslands: "🇻🇮",
	flagUzbekistan: "🇺🇿",
	flagVanuatu: "🇻🇺",
	flagVaticanCity: "🇻🇦",
	flagVenezuela: "🇻🇪",
	flagVietnam: "🇻🇳",
	flagWales: "🏴󠁧󠁢󠁷󠁬󠁳󠁿",
	flagWallisFutuna: "🇼🇫",
	flagWesternSahara: "🇪🇭",
	flagYemen: "🇾🇪",
	flagZambia: "🇿🇲",
	flagZimbabwe: "🇿🇼",
	flamingo: "🦩",
	flashlight: "🔦",
	flatbread: "🫓",
	flatShoe: "🥿",
	fleurDeLis: "⚜️",
	flexedBiceps: "💪",
	floppyDisk: "💾",
	flowerPlayingCards: "🎴",
	flushedFace: "😳",
	flute: "🪈",
	fly: "🪰",
	flyingDisc: "🥏",
	flyingSaucer: "🛸",
	fog: "🌫️",
	foggy: "🌁",
	foldedHands: "🙏",
	foldingHandFan: "🪭",
	fondue: "🫕",
	foot: "🦶",
	footprints: "👣",
	forkAndKnife: "🍴",
	forkAndKnifeWithPlate: "🍽️",
	fortuneCookie: "🥠",
	fountain: "⛲",
	fountainPen: "🖋️",
	fourLeafClover: "🍀",
	fourOClock: "🕓",
	fourThirty: "🕟",
	fox: "🦊",
	framedPicture: "🖼️",
	freeButton: "🆓",
	frenchFries: "🍟",
	friedShrimp: "🍤",
	frog: "🐸",
	frontFacingBabyChick: "🐥",
	frowningFace: "☹️",
	frowningFaceWithOpenMouth: "😦",
	fuelPump: "⛽",
	fullMoon: "🌕",
	fullMoonFace: "🌝",
	funeralUrn: "⚱️",
	gameDie: "🎲",
	garlic: "🧄",
	gear: "⚙️",
	gemini: "♊",
	gemStone: "💎",
	genie: "🧞",
	ghost: "👻",
	gingerRoot: "🫚",
	giraffe: "🦒",
	girl: "👧",
	glasses: "👓",
	glassOfMilk: "🥛",
	globeShowingAmericas: "🌎",
	globeShowingAsiaAustralia: "🌏",
	globeShowingEuropeAfrica: "🌍",
	globeWithMeridians: "🌐",
	gloves: "🧤",
	glowingStar: "🌟",
	goalNet: "🥅",
	goat: "🐐",
	goblin: "👺",
	goggles: "🥽",
	goose: "🪿",
	gorilla: "🦍",
	graduationCap: "🎓",
	grapes: "🍇",
	greenApple: "🍏",
	greenBook: "📗",
	greenCircle: "🟢",
	greenHeart: "💚",
	greenSalad: "🥗",
	greenSquare: "🟩",
	greyHeart: "🩶",
	grimacingFace: "😬",
	grinningCat: "😺",
	grinningCatWithSmilingEyes: "😸",
	grinningFace: "😀",
	grinningFaceWithBigEyes: "😃",
	grinningFaceWithSmilingEyes: "😄",
	grinningFaceWithSweat: "😅",
	grinningSquintingFace: "😆",
	growingHeart: "💗",
	guard: "💂",
	guideDog: "🦮",
	guitar: "🎸",
	hairPick: "🪮",
	hairyCreature: "🫈",
	hamburger: "🍔",
	hammer: "🔨",
	hammerAndPick: "⚒️",
	hammerAndWrench: "🛠️",
	hamsa: "🪬",
	hamster: "🐹",
	handbag: "👜",
	handshake: "🤝",
	handWithFingersSplayed: "🖐️",
	handWithIndexFingerAndThumbCrossed: "🫰",
	harp: "🪉",
	hatchingChick: "🐣",
	headphone: "🎧",
	headShakingHorizontally: "🙂‍↔️",
	headShakingVertically: "🙂‍↕️",
	headstone: "🪦",
	healthWorker: "🧑‍⚕️",
	hearNoEvilMonkey: "🙉",
	heartDecoration: "💟",
	heartExclamation: "❣️",
	heartHands: "🫶",
	heartOnFire: "❤️‍🔥",
	heartSuit: "♥️",
	heartWithArrow: "💘",
	heartWithRibbon: "💝",
	heavyDollarSign: "💲",
	heavyEqualsSign: "🟰",
	hedgehog: "🦔",
	helicopter: "🚁",
	herb: "🌿",
	hibiscus: "🌺",
	highHeeledShoe: "👠",
	highSpeedTrain: "🚄",
	highVoltage: "⚡",
	hikingBoot: "🥾",
	hinduTemple: "🛕",
	hippopotamus: "🦛",
	hole: "🕳️",
	hollowRedCircle: "⭕",
	honeybee: "🐝",
	honeyPot: "🍯",
	hook: "🪝",
	horizontalTrafficLight: "🚥",
	horse: "🐎",
	horseFace: "🐴",
	horseRacing: "🏇",
	hospital: "🏥",
	hotBeverage: "☕",
	hotDog: "🌭",
	hotel: "🏨",
	hotFace: "🥵",
	hotPepper: "🌶️",
	hotSprings: "♨️",
	hourglassDone: "⌛",
	hourglassNotDone: "⏳",
	house: "🏠",
	houses: "🏘️",
	houseWithGarden: "🏡",
	hundredPoints: "💯",
	hushedFace: "😯",
	hut: "🛖",
	hyacinth: "🪻",
	ice: "🧊",
	iceCream: "🍨",
	iceHockey: "🏒",
	iceSkate: "⛸️",
	idButton: "🆔",
	identificationCard: "🪪",
	inboxTray: "📥",
	incomingEnvelope: "📨",
	indexPointingAtTheViewer: "🫵",
	indexPointingUp: "☝️",
	infinity: "♾️",
	information: "ℹ️",
	inputLatinLetters: "🔤",
	inputLatinLowercase: "🔡",
	inputLatinUppercase: "🔠",
	inputNumbers: "🔢",
	inputSymbols: "🔣",
	jackOLantern: "🎃",
	japaneseAcceptableButton: "🉑",
	japaneseApplicationButton: "🈸",
	japaneseBargainButton: "🉐",
	japaneseCastle: "🏯",
	japaneseCongratulationsButton: "㊗️",
	japaneseDiscountButton: "🈹",
	japaneseDolls: "🎎",
	japaneseFreeOfChargeButton: "🈚",
	japaneseHereButton: "🈁",
	japaneseMonthlyAmountButton: "🈷️",
	japaneseNotFreeOfChargeButton: "🈶",
	japaneseNoVacancyButton: "🈵",
	japaneseOpenForBusinessButton: "🈺",
	japanesePassingGradeButton: "🈴",
	japanesePostOffice: "🏣",
	japaneseProhibitedButton: "🈲",
	japaneseReservedButton: "🈯",
	japaneseSecretButton: "㊙️",
	japaneseServiceChargeButton: "🈂️",
	japaneseSymbolForBeginner: "🔰",
	japaneseVacancyButton: "🈳",
	jar: "🫙",
	jeans: "👖",
	jellyfish: "🪼",
	joker: "🃏",
	joystick: "🕹️",
	judge: "🧑‍⚖️",
	kaaba: "🕋",
	kangaroo: "🦘",
	key: "🔑",
	keyboard: "⌨️",
	keycap0: "0️⃣",
	keycap1: "1️⃣",
	keycap10: "🔟",
	keycap2: "2️⃣",
	keycap3: "3️⃣",
	keycap4: "4️⃣",
	keycap5: "5️⃣",
	keycap6: "6️⃣",
	keycap7: "7️⃣",
	keycap8: "8️⃣",
	keycap9: "9️⃣",
	keycapAsterisk: "*️⃣",
	keycapNumberSign: "#️⃣",
	khanda: "🪯",
	kickScooter: "🛴",
	kimono: "👘",
	kiss: "💏",
	kissingCat: "😽",
	kissingFace: "😗",
	kissingFaceWithClosedEyes: "😚",
	kissingFaceWithSmilingEyes: "😙",
	kissManMan: "👨‍❤️‍💋‍👨",
	kissMark: "💋",
	kissWomanMan: "👩‍❤️‍💋‍👨",
	kissWomanWoman: "👩‍❤️‍💋‍👩",
	kitchenKnife: "🔪",
	kite: "🪁",
	kiwiFruit: "🥝",
	knot: "🪢",
	koala: "🐨",
	labCoat: "🥼",
	label: "🏷️",
	lacrosse: "🥍",
	ladder: "🪜",
	ladyBeetle: "🐞",
	landslide: "🛘",
	laptop: "💻",
	largeBlueDiamond: "🔷",
	largeOrangeDiamond: "🔶",
	lastQuarterMoon: "🌗",
	lastQuarterMoonFace: "🌜",
	lastTrackButton: "⏮️",
	latinCross: "✝️",
	leafFlutteringInWind: "🍃",
	leaflessTree: "🪾",
	leafyGreen: "🥬",
	ledger: "📒",
	leftArrow: "⬅️",
	leftArrowCurvingRight: "↪️",
	leftFacingFist: "🤛",
	leftLuggage: "🛅",
	leftRightArrow: "↔️",
	leftSpeechBubble: "🗨️",
	leftwardsHand: "🫲",
	leftwardsPushingHand: "🫷",
	leg: "🦵",
	lemon: "🍋",
	leo: "♌",
	leopard: "🐆",
	levelSlider: "🎚️",
	libra: "♎",
	lightBlueHeart: "🩵",
	lightBulb: "💡",
	lightRail: "🚈",
	lime: "🍋‍🟩",
	link: "🔗",
	linkedPaperclips: "🖇️",
	lion: "🦁",
	lipstick: "💄",
	litterInBinSign: "🚮",
	lizard: "🦎",
	llama: "🦙",
	lobster: "🦞",
	locked: "🔒",
	lockedWithKey: "🔐",
	lockedWithPen: "🔏",
	locomotive: "🚂",
	lollipop: "🍭",
	longDrum: "🪘",
	lotionBottle: "🧴",
	lotus: "🪷",
	loudlyCryingFace: "😭",
	loudspeaker: "📢",
	loveHotel: "🏩",
	loveLetter: "💌",
	loveYouGesture: "🤟",
	lowBattery: "🪫",
	luggage: "🧳",
	lungs: "🫁",
	lyingFace: "🤥",
	mage: "🧙",
	magicWand: "🪄",
	magnet: "🧲",
	magnifyingGlassTiltedLeft: "🔍",
	magnifyingGlassTiltedRight: "🔎",
	mahjongRedDragon: "🀄",
	maleSign: "♂️",
	mammoth: "🦣",
	man: "👨",
	manArtist: "👨‍🎨",
	manAstronaut: "👨‍🚀",
	manBald: "👨‍🦲",
	manBeard: "🧔‍♂️",
	manBiking: "🚴‍♂️",
	manBlondHair: "👱‍♂️",
	manBouncingBall: "⛹️‍♂️",
	manBowing: "🙇‍♂️",
	manCartwheeling: "🤸‍♂️",
	manClimbing: "🧗‍♂️",
	manConstructionWorker: "👷‍♂️",
	manCook: "👨‍🍳",
	manCurlyHair: "👨‍🦱",
	manDancing: "🕺",
	manDetective: "🕵️‍♂️",
	manElf: "🧝‍♂️",
	manFacepalming: "🤦‍♂️",
	manFactoryWorker: "👨‍🏭",
	manFairy: "🧚‍♂️",
	manFarmer: "👨‍🌾",
	manFeedingBaby: "👨‍🍼",
	manFirefighter: "👨‍🚒",
	manFrowning: "🙍‍♂️",
	manGenie: "🧞‍♂️",
	manGesturingNo: "🙅‍♂️",
	manGesturingOk: "🙆‍♂️",
	manGettingHaircut: "💇‍♂️",
	manGettingMassage: "💆‍♂️",
	mango: "🥭",
	manGolfing: "🏌️‍♂️",
	manGuard: "💂‍♂️",
	manHealthWorker: "👨‍⚕️",
	manInLotusPosition: "🧘‍♂️",
	manInManualWheelchair: "👨‍🦽",
	manInManualWheelchairFacingRight: "👨‍🦽‍➡️",
	manInMotorizedWheelchair: "👨‍🦼",
	manInMotorizedWheelchairFacingRight: "👨‍🦼‍➡️",
	manInSteamyRoom: "🧖‍♂️",
	manInTuxedo: "🤵‍♂️",
	manJudge: "👨‍⚖️",
	manJuggling: "🤹‍♂️",
	manKneeling: "🧎‍♂️",
	manKneelingFacingRight: "🧎‍♂️‍➡️",
	manLiftingWeights: "🏋️‍♂️",
	manMage: "🧙‍♂️",
	manMechanic: "👨‍🔧",
	manMountainBiking: "🚵‍♂️",
	manOfficeWorker: "👨‍💼",
	manPilot: "👨‍✈️",
	manPlayingHandball: "🤾‍♂️",
	manPlayingWaterPolo: "🤽‍♂️",
	manPoliceOfficer: "👮‍♂️",
	manPouting: "🙎‍♂️",
	manRaisingHand: "🙋‍♂️",
	manRedHair: "👨‍🦰",
	manRowingBoat: "🚣‍♂️",
	manRunning: "🏃‍♂️",
	manRunningFacingRight: "🏃‍♂️‍➡️",
	manScientist: "👨‍🔬",
	manShrugging: "🤷‍♂️",
	manSinger: "👨‍🎤",
	manSShoe: "👞",
	manStanding: "🧍‍♂️",
	manStudent: "👨‍🎓",
	manSuperhero: "🦸‍♂️",
	manSupervillain: "🦹‍♂️",
	manSurfing: "🏄‍♂️",
	manSwimming: "🏊‍♂️",
	manTeacher: "👨‍🏫",
	manTechnologist: "👨‍💻",
	mantelpieceClock: "🕰️",
	manTippingHand: "💁‍♂️",
	manualWheelchair: "🦽",
	manVampire: "🧛‍♂️",
	manWalking: "🚶‍♂️",
	manWalkingFacingRight: "🚶‍♂️‍➡️",
	manWearingTurban: "👳‍♂️",
	manWhiteHair: "👨‍🦳",
	manWithVeil: "👰‍♂️",
	manWithWhiteCane: "👨‍🦯",
	manWithWhiteCaneFacingRight: "👨‍🦯‍➡️",
	manZombie: "🧟‍♂️",
	mapleLeaf: "🍁",
	mapOfJapan: "🗾",
	maracas: "🪇",
	martialArtsUniform: "🥋",
	mate: "🧉",
	meatOnBone: "🍖",
	mechanic: "🧑‍🔧",
	mechanicalArm: "🦾",
	mechanicalLeg: "🦿",
	medicalSymbol: "⚕️",
	megaphone: "📣",
	melon: "🍈",
	meltingFace: "🫠",
	memo: "📝",
	mendingHeart: "❤️‍🩹",
	menHoldingHands: "👬",
	menorah: "🕎",
	menSRoom: "🚹",
	menWithBunnyEars: "👯‍♂️",
	menWrestling: "🤼‍♂️",
	mermaid: "🧜‍♀️",
	merman: "🧜‍♂️",
	merperson: "🧜",
	metro: "🚇",
	microbe: "🦠",
	microphone: "🎤",
	microscope: "🔬",
	middleFinger: "🖕",
	militaryHelmet: "🪖",
	militaryMedal: "🎖️",
	milkyWay: "🌌",
	minibus: "🚐",
	minus: "➖",
	mirror: "🪞",
	mirrorBall: "🪩",
	moai: "🗿",
	mobilePhone: "📱",
	mobilePhoneOff: "📴",
	mobilePhoneWithArrow: "📲",
	moneyBag: "💰",
	moneyMouthFace: "🤑",
	moneyWithWings: "💸",
	monkey: "🐒",
	monkeyFace: "🐵",
	monorail: "🚝",
	moonCake: "🥮",
	moonViewingCeremony: "🎑",
	moose: "🫎",
	mosque: "🕌",
	mosquito: "🦟",
	motorBoat: "🛥️",
	motorcycle: "🏍️",
	motorizedWheelchair: "🦼",
	motorScooter: "🛵",
	motorway: "🛣️",
	mountain: "⛰️",
	mountainCableway: "🚠",
	mountainRailway: "🚞",
	mountFuji: "🗻",
	mouse: "🐁",
	mouseFace: "🐭",
	mouseTrap: "🪤",
	mouth: "👄",
	movieCamera: "🎥",
	mrsClaus: "🤶",
	multiply: "✖️",
	mushroom: "🍄",
	musicalKeyboard: "🎹",
	musicalNote: "🎵",
	musicalNotes: "🎶",
	musicalScore: "🎼",
	mutedSpeaker: "🔇",
	mxClaus: "🧑‍🎄",
	nailPolish: "💅",
	nameBadge: "📛",
	nationalPark: "🏞️",
	nauseatedFace: "🤢",
	nazarAmulet: "🧿",
	necktie: "👔",
	nerdFace: "🤓",
	nestingDolls: "🪆",
	nestWithEggs: "🪺",
	neutralFace: "😐",
	newButton: "🆕",
	newMoon: "🌑",
	newMoonFace: "🌚",
	newspaper: "📰",
	nextTrackButton: "⏭️",
	ngButton: "🆖",
	nightWithStars: "🌃",
	nineOClock: "🕘",
	nineThirty: "🕤",
	ninja: "🥷",
	noBicycles: "🚳",
	noEntry: "⛔",
	noLittering: "🚯",
	noMobilePhones: "📵",
	nonPotableWater: "🚱",
	noOneUnderEighteen: "🔞",
	noPedestrians: "🚷",
	nose: "👃",
	noSmoking: "🚭",
	notebook: "📓",
	notebookWithDecorativeCover: "📔",
	nutAndBolt: "🔩",
	oButton: "🅾️",
	octopus: "🐙",
	oden: "🍢",
	officeBuilding: "🏢",
	officeWorker: "🧑‍💼",
	ogre: "👹",
	oilDrum: "🛢️",
	okButton: "🆗",
	okHand: "👌",
	olderPerson: "🧓",
	oldKey: "🗝️",
	oldMan: "👴",
	oldWoman: "👵",
	olive: "🫒",
	om: "🕉️",
	onArrow: "🔛",
	oncomingAutomobile: "🚘",
	oncomingBus: "🚍",
	oncomingFist: "👊",
	oncomingPoliceCar: "🚔",
	oncomingTaxi: "🚖",
	oneOClock: "🕐",
	onePieceSwimsuit: "🩱",
	oneThirty: "🕜",
	onion: "🧅",
	openBook: "📖",
	openFileFolder: "📂",
	openHands: "👐",
	openMailboxWithLoweredFlag: "📭",
	openMailboxWithRaisedFlag: "📬",
	ophiuchus: "⛎",
	opticalDisk: "💿",
	orangeBook: "📙",
	orangeCircle: "🟠",
	orangeHeart: "🧡",
	orangeSquare: "🟧",
	orangutan: "🦧",
	orca: "🫍",
	orthodoxCross: "☦️",
	otter: "🦦",
	outboxTray: "📤",
	owl: "🦉",
	ox: "🐂",
	oyster: "🦪",
	package_: "📦",
	pageFacingUp: "📄",
	pager: "📟",
	pageWithCurl: "📃",
	paintbrush: "🖌️",
	palmDownHand: "🫳",
	palmsUpTogether: "🤲",
	palmTree: "🌴",
	palmUpHand: "🫴",
	pancakes: "🥞",
	panda: "🐼",
	paperclip: "📎",
	parachute: "🪂",
	parrot: "🦜",
	partAlternationMark: "〽️",
	partyingFace: "🥳",
	partyPopper: "🎉",
	passengerShip: "🛳️",
	passportControl: "🛂",
	pauseButton: "⏸️",
	pawPrints: "🐾",
	pButton: "🅿️",
	peaceSymbol: "☮️",
	peach: "🍑",
	peacock: "🦚",
	peanuts: "🥜",
	peaPod: "🫛",
	pear: "🍐",
	pen: "🖊️",
	pencil: "✏️",
	penguin: "🐧",
	pensiveFace: "😔",
	peopleHoldingHands: "🧑‍🤝‍🧑",
	peopleHugging: "🫂",
	peopleWithBunnyEars: "👯",
	peopleWrestling: "🤼",
	performingArts: "🎭",
	perseveringFace: "😣",
	person: "🧑",
	personBald: "🧑‍🦲",
	personBeard: "🧔",
	personBiking: "🚴",
	personBlondHair: "👱",
	personBouncingBall: "⛹️",
	personBowing: "🙇",
	personCartwheeling: "🤸",
	personClimbing: "🧗",
	personCurlyHair: "🧑‍🦱",
	personFacepalming: "🤦",
	personFeedingBaby: "🧑‍🍼",
	personFencing: "🤺",
	personFrowning: "🙍",
	personGesturingNo: "🙅",
	personGesturingOk: "🙆",
	personGettingHaircut: "💇",
	personGettingMassage: "💆",
	personGolfing: "🏌️",
	personInBed: "🛌",
	personInLotusPosition: "🧘",
	personInManualWheelchair: "🧑‍🦽",
	personInManualWheelchairFacingRight: "🧑‍🦽‍➡️",
	personInMotorizedWheelchair: "🧑‍🦼",
	personInMotorizedWheelchairFacingRight: "🧑‍🦼‍➡️",
	personInSteamyRoom: "🧖",
	personInSuitLevitating: "🕴️",
	personInTuxedo: "🤵",
	personJuggling: "🤹",
	personKneeling: "🧎",
	personKneelingFacingRight: "🧎‍➡️",
	personLiftingWeights: "🏋️",
	personMountainBiking: "🚵",
	personPlayingHandball: "🤾",
	personPlayingWaterPolo: "🤽",
	personPouting: "🙎",
	personRaisingHand: "🙋",
	personRedHair: "🧑‍🦰",
	personRowingBoat: "🚣",
	personRunning: "🏃",
	personRunningFacingRight: "🏃‍➡️",
	personShrugging: "🤷",
	personStanding: "🧍",
	personSurfing: "🏄",
	personSwimming: "🏊",
	personTakingBath: "🛀",
	personTippingHand: "💁",
	personWalking: "🚶",
	personWalkingFacingRight: "🚶‍➡️",
	personWearingTurban: "👳",
	personWhiteHair: "🧑‍🦳",
	personWithCrown: "🫅",
	personWithSkullcap: "👲",
	personWithVeil: "👰",
	personWithWhiteCane: "🧑‍🦯",
	personWithWhiteCaneFacingRight: "🧑‍🦯‍➡️",
	petriDish: "🧫",
	phoenix: "🐦‍🔥",
	pick: "⛏️",
	pickupTruck: "🛻",
	pie: "🥧",
	pig: "🐖",
	pigFace: "🐷",
	pigNose: "🐽",
	pileOfPoo: "💩",
	pill: "💊",
	pilot: "🧑‍✈️",
	pinata: "🪅",
	pinchedFingers: "🤌",
	pinchingHand: "🤏",
	pineapple: "🍍",
	pineDecoration: "🎍",
	pingPong: "🏓",
	pinkHeart: "🩷",
	pirateFlag: "🏴‍☠️",
	pisces: "♓",
	pizza: "🍕",
	placard: "🪧",
	placeOfWorship: "🛐",
	playButton: "▶️",
	playgroundSlide: "🛝",
	playOrPauseButton: "⏯️",
	pleadingFace: "🥺",
	plunger: "🪠",
	plus: "➕",
	polarBear: "🐻‍❄️",
	policeCar: "🚓",
	policeCarLight: "🚨",
	policeOfficer: "👮",
	poodle: "🐩",
	pool8Ball: "🎱",
	popcorn: "🍿",
	postalHorn: "📯",
	postbox: "📮",
	postOffice: "🏤",
	potableWater: "🚰",
	potato: "🥔",
	potOfFood: "🍲",
	pottedPlant: "🪴",
	poultryLeg: "🍗",
	poundBanknote: "💷",
	pouringLiquid: "🫗",
	poutingCat: "😾",
	prayerBeads: "📿",
	pregnantMan: "🫃",
	pregnantPerson: "🫄",
	pregnantWoman: "🤰",
	pretzel: "🥨",
	prince: "🤴",
	princess: "👸",
	printer: "🖨️",
	prohibited: "🚫",
	purpleCircle: "🟣",
	purpleHeart: "💜",
	purpleSquare: "🟪",
	purse: "👛",
	pushpin: "📌",
	puzzlePiece: "🧩",
	rabbit: "🐇",
	rabbitFace: "🐰",
	raccoon: "🦝",
	racingCar: "🏎️",
	radio: "📻",
	radioactive: "☢️",
	radioButton: "🔘",
	railwayCar: "🚃",
	railwayTrack: "🛤️",
	rainbow: "🌈",
	rainbowFlag: "🏳️‍🌈",
	raisedBackOfHand: "🤚",
	raisedFist: "✊",
	raisedHand: "✋",
	raisingHands: "🙌",
	ram: "🐏",
	rat: "🐀",
	razor: "🪒",
	receipt: "🧾",
	recordButton: "⏺️",
	recyclingSymbol: "♻️",
	redApple: "🍎",
	redCircle: "🔴",
	redEnvelope: "🧧",
	redExclamationMark: "❗",
	redHeart: "❤️",
	redPaperLantern: "🏮",
	redQuestionMark: "❓",
	redSquare: "🟥",
	redTrianglePointedDown: "🔻",
	redTrianglePointedUp: "🔺",
	registered: "®️",
	relievedFace: "😌",
	reminderRibbon: "🎗️",
	repeatButton: "🔁",
	repeatSingleButton: "🔂",
	rescueWorkerSHelmet: "⛑️",
	restroom: "🚻",
	reverseButton: "◀️",
	revolvingHearts: "💞",
	rhinoceros: "🦏",
	ribbon: "🎀",
	riceBall: "🍙",
	riceCracker: "🍘",
	rightAngerBubble: "🗯️",
	rightArrow: "➡️",
	rightArrowCurvingDown: "⤵️",
	rightArrowCurvingLeft: "↩️",
	rightArrowCurvingUp: "⤴️",
	rightFacingFist: "🤜",
	rightwardsHand: "🫱",
	rightwardsPushingHand: "🫸",
	ring: "💍",
	ringBuoy: "🛟",
	ringedPlanet: "🪐",
	roastedSweetPotato: "🍠",
	robot: "🤖",
	rock: "🪨",
	rocket: "🚀",
	rolledUpNewspaper: "🗞️",
	rollerCoaster: "🎢",
	rollerSkate: "🛼",
	rollingOnTheFloorLaughing: "🤣",
	rollOfPaper: "🧻",
	rooster: "🐓",
	rootVegetable: "🫜",
	rose: "🌹",
	rosette: "🏵️",
	roundPushpin: "📍",
	rugbyFootball: "🏉",
	runningShirt: "🎽",
	runningShoe: "👟",
	sadButRelievedFace: "😥",
	safetyPin: "🧷",
	safetyVest: "🦺",
	sagittarius: "♐",
	sailboat: "⛵",
	sake: "🍶",
	salt: "🧂",
	salutingFace: "🫡",
	sandwich: "🥪",
	santaClaus: "🎅",
	sari: "🥻",
	satellite: "🛰️",
	satelliteAntenna: "📡",
	sauropod: "🦕",
	saxophone: "🎷",
	scarf: "🧣",
	school: "🏫",
	scientist: "🧑‍🔬",
	scissors: "✂️",
	scorpio: "♏",
	scorpion: "🦂",
	screwdriver: "🪛",
	scroll: "📜",
	seal: "🦭",
	seat: "💺",
	seedling: "🌱",
	seeNoEvilMonkey: "🙈",
	selfie: "🤳",
	serviceDog: "🐕‍🦺",
	sevenOClock: "🕖",
	sevenThirty: "🕢",
	sewingNeedle: "🪡",
	shakingFace: "🫨",
	shallowPanOfFood: "🥘",
	shamrock: "☘️",
	shark: "🦈",
	shavedIce: "🍧",
	sheafOfRice: "🌾",
	shield: "🛡️",
	shintoShrine: "⛩️",
	ship: "🚢",
	shootingStar: "🌠",
	shoppingBags: "🛍️",
	shoppingCart: "🛒",
	shortcake: "🍰",
	shorts: "🩳",
	shovel: "🪏",
	shower: "🚿",
	shrimp: "🦐",
	shuffleTracksButton: "🔀",
	shushingFace: "🤫",
	signOfTheHorns: "🤘",
	singer: "🧑‍🎤",
	sixOClock: "🕕",
	sixThirty: "🕡",
	skateboard: "🛹",
	skier: "⛷️",
	skis: "🎿",
	skull: "💀",
	skullAndCrossbones: "☠️",
	skunk: "🦨",
	sled: "🛷",
	sleepingFace: "😴",
	sleepyFace: "😪",
	slightlyFrowningFace: "🙁",
	slightlySmilingFace: "🙂",
	sloth: "🦥",
	slotMachine: "🎰",
	smallAirplane: "🛩️",
	smallBlueDiamond: "🔹",
	smallOrangeDiamond: "🔸",
	smilingCatWithHeartEyes: "😻",
	smilingFace: "☺️",
	smilingFaceWithHalo: "😇",
	smilingFaceWithHeartEyes: "😍",
	smilingFaceWithHearts: "🥰",
	smilingFaceWithHorns: "😈",
	smilingFaceWithOpenHands: "🤗",
	smilingFaceWithSmilingEyes: "😊",
	smilingFaceWithSunglasses: "😎",
	smilingFaceWithTear: "🥲",
	smirkingFace: "😏",
	snail: "🐌",
	snake: "🐍",
	sneezingFace: "🤧",
	snowboarder: "🏂",
	snowCappedMountain: "🏔️",
	snowflake: "❄️",
	snowman: "☃️",
	snowmanWithoutSnow: "⛄",
	soap: "🧼",
	soccerBall: "⚽",
	socks: "🧦",
	softball: "🥎",
	softIceCream: "🍦",
	soonArrow: "🔜",
	sosButton: "🆘",
	spadeSuit: "♠️",
	spaghetti: "🍝",
	sparkle: "❇️",
	sparkler: "🎇",
	sparkles: "✨",
	sparklingHeart: "💖",
	speakerHighVolume: "🔊",
	speakerLowVolume: "🔈",
	speakerMediumVolume: "🔉",
	speakingHead: "🗣️",
	speakNoEvilMonkey: "🙊",
	speechBalloon: "💬",
	speedboat: "🚤",
	spider: "🕷️",
	spiderWeb: "🕸️",
	spiralCalendar: "🗓️",
	spiralNotepad: "🗒️",
	spiralShell: "🐚",
	splatter: "🫟",
	sponge: "🧽",
	spoon: "🥄",
	sportsMedal: "🏅",
	sportUtilityVehicle: "🚙",
	spoutingWhale: "🐳",
	squid: "🦑",
	squintingFaceWithTongue: "😝",
	stadium: "🏟️",
	star: "⭐",
	starAndCrescent: "☪️",
	starOfDavid: "✡️",
	starStruck: "🤩",
	station: "🚉",
	statueOfLiberty: "🗽",
	steamingBowl: "🍜",
	stethoscope: "🩺",
	stopButton: "⏹️",
	stopSign: "🛑",
	stopwatch: "⏱️",
	straightRuler: "📏",
	strawberry: "🍓",
	student: "🧑‍🎓",
	studioMicrophone: "🎙️",
	stuffedFlatbread: "🥙",
	sun: "☀️",
	sunBehindCloud: "⛅",
	sunBehindLargeCloud: "🌥️",
	sunBehindRainCloud: "🌦️",
	sunBehindSmallCloud: "🌤️",
	sunflower: "🌻",
	sunglasses: "🕶️",
	sunrise: "🌅",
	sunriseOverMountains: "🌄",
	sunset: "🌇",
	sunWithFace: "🌞",
	superhero: "🦸",
	supervillain: "🦹",
	sushi: "🍣",
	suspensionRailway: "🚟",
	swan: "🦢",
	sweatDroplets: "💦",
	synagogue: "🕍",
	syringe: "💉",
	taco: "🌮",
	takeoutBox: "🥡",
	tamale: "🫔",
	tanabataTree: "🎋",
	tangerine: "🍊",
	taurus: "♉",
	taxi: "🚕",
	teacher: "🧑‍🏫",
	teacupWithoutHandle: "🍵",
	teapot: "🫖",
	tearOffCalendar: "📆",
	technologist: "🧑‍💻",
	teddyBear: "🧸",
	telephone: "☎️",
	telephoneReceiver: "📞",
	telescope: "🔭",
	television: "📺",
	tennis: "🎾",
	tenOClock: "🕙",
	tent: "⛺",
	tenThirty: "🕥",
	testTube: "🧪",
	thermometer: "🌡️",
	thinkingFace: "🤔",
	thongSandal: "🩴",
	thoughtBalloon: "💭",
	thread: "🧵",
	threeOClock: "🕒",
	threeThirty: "🕞",
	thumbsDown: "👎",
	thumbsUp: "👍",
	ticket: "🎫",
	tiger: "🐅",
	tigerFace: "🐯",
	timerClock: "⏲️",
	tiredFace: "😫",
	toilet: "🚽",
	tokyoTower: "🗼",
	tomato: "🍅",
	tongue: "👅",
	toolbox: "🧰",
	tooth: "🦷",
	toothbrush: "🪥",
	topArrow: "🔝",
	topHat: "🎩",
	tornado: "🌪️",
	trackball: "🖲️",
	tractor: "🚜",
	tradeMark: "™️",
	train: "🚆",
	tram: "🚊",
	tramCar: "🚋",
	transgenderFlag: "🏳️‍⚧️",
	transgenderSymbol: "⚧️",
	treasureChest: "🪎",
	tRex: "🦖",
	triangularFlag: "🚩",
	triangularRuler: "📐",
	tridentEmblem: "🔱",
	troll: "🧌",
	trolleybus: "🚎",
	trombone: "🪊",
	trophy: "🏆",
	tropicalDrink: "🍹",
	tropicalFish: "🐠",
	trumpet: "🎺",
	tShirt: "👕",
	tulip: "🌷",
	tumblerGlass: "🥃",
	turkey: "🦃",
	turtle: "🐢",
	twelveOClock: "🕛",
	twelveThirty: "🕧",
	twoHearts: "💕",
	twoHumpCamel: "🐫",
	twoOClock: "🕑",
	twoThirty: "🕝",
	umbrella: "☂️",
	umbrellaOnGround: "⛱️",
	umbrellaWithRainDrops: "☔",
	unamusedFace: "😒",
	unicorn: "🦄",
	unlocked: "🔓",
	upArrow: "⬆️",
	upButton: "🆙",
	upDownArrow: "↕️",
	upLeftArrow: "↖️",
	upRightArrow: "↗️",
	upsideDownFace: "🙃",
	upwardsButton: "🔼",
	vampire: "🧛",
	verticalTrafficLight: "🚦",
	vibrationMode: "📳",
	victoryHand: "✌️",
	videoCamera: "📹",
	videocassette: "📼",
	videoGame: "🎮",
	violin: "🎻",
	virgo: "♍",
	volcano: "🌋",
	volleyball: "🏐",
	vsButton: "🆚",
	vulcanSalute: "🖖",
	waffle: "🧇",
	waningCrescentMoon: "🌘",
	waningGibbousMoon: "🌖",
	warning: "⚠️",
	wastebasket: "🗑️",
	watch: "⌚",
	waterBuffalo: "🐃",
	waterCloset: "🚾",
	watermelon: "🍉",
	waterPistol: "🔫",
	waterWave: "🌊",
	wavingHand: "👋",
	wavyDash: "〰️",
	waxingCrescentMoon: "🌒",
	waxingGibbousMoon: "🌔",
	wearyCat: "🙀",
	wearyFace: "😩",
	wedding: "💒",
	whale: "🐋",
	wheel: "🛞",
	wheelchairSymbol: "♿",
	wheelOfDharma: "☸️",
	whiteCane: "🦯",
	whiteCircle: "⚪",
	whiteExclamationMark: "❕",
	whiteFlag: "🏳️",
	whiteFlower: "💮",
	whiteHeart: "🤍",
	whiteLargeSquare: "⬜",
	whiteMediumSmallSquare: "◽",
	whiteMediumSquare: "◻️",
	whiteQuestionMark: "❔",
	whiteSmallSquare: "▫️",
	whiteSquareButton: "🔳",
	wiltedFlower: "🥀",
	windChime: "🎐",
	windFace: "🌬️",
	window: "🪟",
	wineGlass: "🍷",
	wing: "🪽",
	winkingFace: "😉",
	winkingFaceWithTongue: "😜",
	wireless: "🛜",
	wolf: "🐺",
	woman: "👩",
	womanAndManHoldingHands: "👫",
	womanArtist: "👩‍🎨",
	womanAstronaut: "👩‍🚀",
	womanBald: "👩‍🦲",
	womanBeard: "🧔‍♀️",
	womanBiking: "🚴‍♀️",
	womanBlondHair: "👱‍♀️",
	womanBouncingBall: "⛹️‍♀️",
	womanBowing: "🙇‍♀️",
	womanCartwheeling: "🤸‍♀️",
	womanClimbing: "🧗‍♀️",
	womanConstructionWorker: "👷‍♀️",
	womanCook: "👩‍🍳",
	womanCurlyHair: "👩‍🦱",
	womanDancing: "💃",
	womanDetective: "🕵️‍♀️",
	womanElf: "🧝‍♀️",
	womanFacepalming: "🤦‍♀️",
	womanFactoryWorker: "👩‍🏭",
	womanFairy: "🧚‍♀️",
	womanFarmer: "👩‍🌾",
	womanFeedingBaby: "👩‍🍼",
	womanFirefighter: "👩‍🚒",
	womanFrowning: "🙍‍♀️",
	womanGenie: "🧞‍♀️",
	womanGesturingNo: "🙅‍♀️",
	womanGesturingOk: "🙆‍♀️",
	womanGettingHaircut: "💇‍♀️",
	womanGettingMassage: "💆‍♀️",
	womanGolfing: "🏌️‍♀️",
	womanGuard: "💂‍♀️",
	womanHealthWorker: "👩‍⚕️",
	womanInLotusPosition: "🧘‍♀️",
	womanInManualWheelchair: "👩‍🦽",
	womanInManualWheelchairFacingRight: "👩‍🦽‍➡️",
	womanInMotorizedWheelchair: "👩‍🦼",
	womanInMotorizedWheelchairFacingRight: "👩‍🦼‍➡️",
	womanInSteamyRoom: "🧖‍♀️",
	womanInTuxedo: "🤵‍♀️",
	womanJudge: "👩‍⚖️",
	womanJuggling: "🤹‍♀️",
	womanKneeling: "🧎‍♀️",
	womanKneelingFacingRight: "🧎‍♀️‍➡️",
	womanLiftingWeights: "🏋️‍♀️",
	womanMage: "🧙‍♀️",
	womanMechanic: "👩‍🔧",
	womanMountainBiking: "🚵‍♀️",
	womanOfficeWorker: "👩‍💼",
	womanPilot: "👩‍✈️",
	womanPlayingHandball: "🤾‍♀️",
	womanPlayingWaterPolo: "🤽‍♀️",
	womanPoliceOfficer: "👮‍♀️",
	womanPouting: "🙎‍♀️",
	womanRaisingHand: "🙋‍♀️",
	womanRedHair: "👩‍🦰",
	womanRowingBoat: "🚣‍♀️",
	womanRunning: "🏃‍♀️",
	womanRunningFacingRight: "🏃‍♀️‍➡️",
	womanSBoot: "👢",
	womanScientist: "👩‍🔬",
	womanSClothes: "👚",
	womanSHat: "👒",
	womanShrugging: "🤷‍♀️",
	womanSinger: "👩‍🎤",
	womanSSandal: "👡",
	womanStanding: "🧍‍♀️",
	womanStudent: "👩‍🎓",
	womanSuperhero: "🦸‍♀️",
	womanSupervillain: "🦹‍♀️",
	womanSurfing: "🏄‍♀️",
	womanSwimming: "🏊‍♀️",
	womanTeacher: "👩‍🏫",
	womanTechnologist: "👩‍💻",
	womanTippingHand: "💁‍♀️",
	womanVampire: "🧛‍♀️",
	womanWalking: "🚶‍♀️",
	womanWalkingFacingRight: "🚶‍♀️‍➡️",
	womanWearingTurban: "👳‍♀️",
	womanWhiteHair: "👩‍🦳",
	womanWithHeadscarf: "🧕",
	womanWithVeil: "👰‍♀️",
	womanWithWhiteCane: "👩‍🦯",
	womanWithWhiteCaneFacingRight: "👩‍🦯‍➡️",
	womanZombie: "🧟‍♀️",
	womenHoldingHands: "👭",
	womenSRoom: "🚺",
	womenWithBunnyEars: "👯‍♀️",
	womenWrestling: "🤼‍♀️",
	wood: "🪵",
	woozyFace: "🥴",
	worldMap: "🗺️",
	worm: "🪱",
	worriedFace: "😟",
	wrappedGift: "🎁",
	wrench: "🔧",
	writingHand: "✍️",
	xRay: "🩻",
	yarn: "🧶",
	yawningFace: "🥱",
	yellowCircle: "🟡",
	yellowHeart: "💛",
	yellowSquare: "🟨",
	yenBanknote: "💴",
	yinYang: "☯️",
	yoYo: "🪀",
	zanyFace: "🤪",
	zebra: "🦓",
	zipperMouthFace: "🤐",
	zombie: "🧟",
	zzz: "💤"
};
//#endregion
//#region src/emoji/index.ts
const aliases = {
	love: GeneratedEmoji.redHeart,
	like: GeneratedEmoji.thumbsUp,
	dislike: GeneratedEmoji.thumbsDown,
	laugh: GeneratedEmoji.faceWithTearsOfJoy,
	emphasize: GeneratedEmoji.doubleExclamationMark,
	question: GeneratedEmoji.redQuestionMark
};
const Emoji = {
	...GeneratedEmoji,
	...aliases
};
//#endregion
//#region src/fusor/types.ts
const FUSOR_BRAND = Symbol.for("spectrum.fusor.client");
//#endregion
//#region src/fusor/event.ts
const FUSOR_EVENT_BRAND = Symbol.for("spectrum.fusor.event");
function fusorEvent(name, data) {
	return {
		[FUSOR_EVENT_BRAND]: true,
		name,
		data
	};
}
function isFusorEvent(value) {
	return typeof value === "object" && value !== null && value[FUSOR_EVENT_BRAND] === true;
}
//#endregion
//#region src/fusor/index.ts
function fusor(platform, verify) {
	return {
		[FUSOR_BRAND]: true,
		platform,
		verify
	};
}
function isFusorClient(value) {
	return typeof value === "object" && value !== null && value[FUSOR_BRAND] === true;
}
//#endregion
//#region src/utils/errors.ts
const composeMessage = (opts) => {
	return `${opts.platform ?? "platform"} does not support ${opts.kind === "content" ? `content type "${opts.contentType ?? "unknown"}"` : `action "${opts.action ?? "unknown"}"`}${opts.detail ? `: ${opts.detail}` : ""}`;
};
var UnsupportedError = class UnsupportedError extends Error {
	kind;
	platform;
	contentType;
	action;
	detail;
	constructor(opts) {
		super(composeMessage(opts));
		this.name = "UnsupportedError";
		this.kind = opts.kind;
		this.platform = opts.platform;
		this.contentType = opts.contentType;
		this.action = opts.action;
		this.detail = opts.detail;
	}
	static content(contentType, platform, detail) {
		return new UnsupportedError({
			kind: "content",
			contentType,
			platform,
			detail
		});
	}
	static action(action, platform, detail) {
		return new UnsupportedError({
			kind: "action",
			action,
			platform,
			detail
		});
	}
	withPlatform(platform) {
		if (this.platform) return this;
		return new UnsupportedError({
			kind: this.kind,
			platform,
			contentType: this.contentType,
			action: this.action,
			detail: this.detail
		});
	}
};
//#endregion
//#region src/platform/build.ts
const platformLog$1 = createLogger("spectrum.platform");
const FIRE_AND_FORGET_TYPES = new Set([
	"typing",
	"edit",
	"rename",
	"avatar",
	"addMember",
	"removeMember",
	"leaveSpace",
	"unsend",
	"read"
]);
const isFireAndForget = (item) => FIRE_AND_FORGET_TYPES.has(item.type) || item.__fireAndForget === true;
const RESERVED_SPACE_KEYS = new Set([
	"__platform",
	"id",
	"send",
	"edit",
	"unsend",
	"read",
	"getMessage",
	"getMembers",
	"getAvatar",
	"getDisplayName",
	"rename",
	"avatar",
	"add",
	"remove",
	"leave",
	"startTyping",
	"stopTyping",
	"responding"
]);
const PLATFORM_WISE_ACTION_KEYS = new Set([
	"getMessage",
	"getMembers",
	"getAvatar",
	"getDisplayName"
]);
const RESERVED_MESSAGE_KEYS = new Set([
	"content",
	"direction",
	"edit",
	"id",
	"platform",
	"react",
	"read",
	"reply",
	"sender",
	"space",
	"timestamp",
	"unsend"
]);
const scopeLabel = (scope) => {
	if (scope === "space") return "Space";
	if (scope === "message") return "Message";
	return "PlatformInstance";
};
const warnReservedAction = (scope, name, platform) => {
	platformLog$1.warn(`${platform} declared ${scope} action "${name}" which collides with a reserved ${scopeLabel(scope)} key; skipping.`, {
		"spectrum.provider": platform,
		"spectrum.reserved.scope": scope,
		"spectrum.reserved.action": name
	});
};
const warnUnsupported = (err, fallbackPlatform) => {
	const platform = err.platform ?? fallbackPlatform;
	const subject = err.kind === "content" ? `content type "${err.contentType ?? "unknown"}"` : `action "${err.action ?? "unknown"}"`;
	const detail = err.detail ? `: ${err.detail}` : "";
	platformLog$1.warn(`${platform} does not support ${subject}${detail}; skipping.`, {
		"spectrum.provider": platform,
		"spectrum.unsupported.kind": err.kind,
		"spectrum.unsupported.content_type": err.contentType,
		"spectrum.unsupported.action": err.action
	});
};
const contentPlatform = (content) => {
	const platform = content.__platform;
	return typeof platform === "string" ? platform : void 0;
};
const findUnsupportedPlatformContent = (content, platform) => {
	const scopedPlatform = contentPlatform(content);
	if (scopedPlatform && scopedPlatform !== platform) return scopedPlatform;
	if (content.type === "reply" || content.type === "edit") return findUnsupportedPlatformContent(content.content, platform);
	if (content.type !== "group") return;
	for (const item of content.items) {
		const nested = item.content;
		if (typeof nested !== "object" || nested === null || !("type" in nested)) continue;
		const unsupported = findUnsupportedPlatformContent(nested, platform);
		if (unsupported) return unsupported;
	}
};
const unsupportedPlatformContentError = (content, platform) => {
	const requiredPlatform = findUnsupportedPlatformContent(content, platform);
	if (!requiredPlatform) return;
	return UnsupportedError.content(content.type, platform, `requires ${requiredPlatform}`);
};
const findStreamText = (item) => {
	if (item.type === "streamText") return item;
	if ((item.type === "reply" || item.type === "edit") && item.content.type === "streamText") return item.content;
};
const replaceStreamText = (item, source, full) => {
	const inner = source.format === "markdown" ? asMarkdown(full) : asText(full);
	if (item.type === "reply" || item.type === "edit") return {
		...item,
		content: inner
	};
	return inner;
};
const downgradeMarkdown = (md) => {
	const plain = markdownToPlainText(md.markdown);
	return plain ? asText(plain) : void 0;
};
const replaceMarkdown = (item) => {
	if (item.type === "markdown") return downgradeMarkdown(item) ?? item;
	if ((item.type === "reply" || item.type === "edit") && item.content.type === "markdown") {
		const downgraded = downgradeMarkdown(item.content);
		return downgraded ? {
			...item,
			content: downgraded
		} : item;
	}
	if (item.type === "group") {
		let changed = false;
		const items = item.items.map((member) => {
			if (member.content.type !== "markdown") return member;
			const downgraded = downgradeMarkdown(member.content);
			if (!downgraded) return member;
			changed = true;
			return {
				...member,
				content: downgraded
			};
		});
		return changed ? {
			...item,
			items
		} : item;
	}
	return item;
};
async function resendDrainedStream(send, item, source, platform, unsupported) {
	platformLog$1.info(`${platform} does not support streaming text; waiting for the stream to finish to send the full text as one message.`, {
		"spectrum.provider": platform,
		"spectrum.stream_text.fallback": true
	});
	let full;
	try {
		full = await drainStreamText(source);
	} catch (drainErr) {
		if (drainErr instanceof StreamConsumedError) throw unsupported;
		throw drainErr;
	}
	if (!full) throw unsupported;
	return await sendWithFallbacks(send, replaceStreamText(item, source, full), platform);
}
/**
* Dispatch `content` to the provider, downgrading on platforms that reject it
* with `UnsupportedError`:
*
* - `streamText` (top-level or inside `reply`/`edit`): wait for the stream to
*   finish and re-send the accumulated text — as `markdown` content for a
*   markdown-formatted stream, plain `text` otherwise — so `streamText`
*   works everywhere, just without live updates.
* - `markdown` (top-level, inside `reply`/`edit`, or a `group` item): re-send
*   with each markdown occurrence rendered to readable plain text — so
*   `markdown` works everywhere, just without styling.
*
* The two chain rather than compete: a drained markdown stream re-enters this
* function as `markdown` content and can downgrade once more to plain text.
* Rethrows the original error when no fallback applies; an `UnsupportedError`
* from the final fallback send itself propagates too. Both land in the
* caller's warn-and-skip handling.
*/
async function sendWithFallbacks(send, item, platform) {
	try {
		return await send(item);
	} catch (err) {
		if (!(err instanceof UnsupportedError)) throw err;
		const source = findStreamText(item);
		if (source) return await resendDrainedStream(send, item, source, platform, err);
		const downgraded = replaceMarkdown(item);
		if (downgraded === item) throw err;
		platformLog$1.info(`${platform} does not support markdown; sending the content as plain text instead.`, {
			"spectrum.provider": platform,
			"spectrum.markdown.fallback": true
		});
		return await send(downgraded);
	}
}
const providerMessageCoreKeys = new Set([
	"content",
	"direction",
	"id",
	"sender",
	"space",
	"timestamp"
]);
const extractExtras = (raw, definition) => {
	const entries = Object.entries(raw).filter(([key]) => !providerMessageCoreKeys.has(key));
	const extra = Object.fromEntries(entries);
	return definition.message?.schema ? definition.message.schema.parse(extra) : extra;
};
const rawDirection = (raw) => raw.direction === "inbound" || raw.direction === "outbound" ? raw.direction : void 0;
/**
* Wrap a raw provider message record (and any nested raw targets/items inside
* its content) into a fully-built `Message`. The same path serves inbound
* (`messages`, `actions.getMessage`) and outbound (`send`) flows — the only
* difference is `direction`, which decides whether the resulting Message
* exposes inbound (`react`/`reply`) or outbound (`edit`) affordances. A raw
* record can carry its own `direction` when the provider knows better than the
* wrapping context, which matters for inbound reactions targeting outbound
* messages.
* Recursion through `wrapNestedContent` handles reaction/reply targets and
* group items, which providers return as nested raw records.
*/
function wrapProviderMessage(raw, ctx, direction) {
	const effectiveDirection = rawDirection(raw) ?? direction;
	const wrappedContent = wrapNestedContent(raw.content, ctx, effectiveDirection);
	const base = {
		id: raw.id,
		content: wrappedContent,
		timestamp: raw.timestamp ?? /* @__PURE__ */ new Date(),
		extras: extractExtras(raw, ctx.definition),
		spaceRef: ctx.spaceRef,
		space: ctx.space,
		definition: ctx.definition,
		client: ctx.client,
		config: ctx.config,
		store: ctx.store
	};
	if (effectiveDirection === "inbound") return buildMessage({
		...base,
		sender: raw.sender,
		direction: "inbound"
	});
	return buildMessage({
		...base,
		sender: raw.sender,
		direction: "outbound"
	});
}
const wrapNestedContent = (content, ctx, direction) => {
	if (content.type === "reaction") {
		const target = content.target;
		if (isRawProviderRecord(target)) return {
			...content,
			target: wrapProviderMessage(target, ctx, "inbound")
		};
		return content;
	}
	if (content.type === "reply") {
		const wrappedInner = wrapNestedContent(content.content, ctx, direction);
		const target = content.target;
		if (isRawProviderRecord(target)) return {
			...content,
			content: wrappedInner,
			target: wrapProviderMessage(target, ctx, "inbound")
		};
		return {
			...content,
			content: wrappedInner
		};
	}
	if (content.type === "edit") {
		const target = content.target;
		if (isRawProviderRecord(target)) return {
			...content,
			target: wrapProviderMessage(target, ctx, "outbound")
		};
		return content;
	}
	if (content.type === "read") {
		const target = content.target;
		if (isRawProviderRecord(target)) return {
			...content,
			target: wrapProviderMessage(target, ctx, "outbound")
		};
		return content;
	}
	if (content.type === "group") {
		const items = content.items.map((item) => {
			const raw = item;
			if (!isRawProviderRecord(raw)) return item;
			return direction === "inbound" ? wrapProviderMessage(raw, ctx, "inbound") : wrapProviderMessage(raw, ctx, "outbound");
		});
		return {
			...content,
			items
		};
	}
	return content;
};
const isRawProviderRecord = (v) => {
	if (typeof v !== "object" || v === null) return false;
	const record = v;
	return "id" in record && "content" in record && typeof record.react !== "function" && typeof record.reply !== "function";
};
function buildSpace(params) {
	const { spaceRef, extras, actionCtx, definition, client, config, store } = params;
	let space;
	async function dispatchSend(item) {
		return withSpan("spectrum.message.send", {
			"spectrum.provider": definition.name,
			"spectrum.space.id": spaceRef.id,
			"spectrum.message.fire_and_forget": isFireAndForget(item),
			...contentAttrs(item)
		}, async () => {
			const platformError = unsupportedPlatformContentError(item, definition.name);
			if (platformError) {
				warnUnsupported(platformError, definition.name);
				return;
			}
			const providerSend = async (content) => await definition.send({
				...actionCtx,
				content
			});
			let raw;
			try {
				raw = await sendWithFallbacks(providerSend, item, definition.name);
			} catch (err) {
				if (err instanceof UnsupportedError) {
					warnUnsupported(err, definition.name);
					return;
				}
				throw err;
			}
			if (!raw?.id) {
				if (isFireAndForget(item)) return;
				throw new Error(`Platform "${definition.name}" send did not return a message id`);
			}
			return wrapProviderMessage(raw, {
				client,
				config,
				definition,
				space,
				spaceRef,
				store
			}, "outbound");
		});
	}
	async function sendImpl(...content) {
		const resolved = await resolveContents(content);
		const results = [];
		for (const item of resolved) {
			const sent = await dispatchSend(item);
			if (sent) results.push(sent);
		}
		if (content.length === 1) return results[0];
		return results;
	}
	async function getMessageImpl(id) {
		const getMessage = definition.actions?.getMessage;
		if (!getMessage) throw UnsupportedError.action("getMessage", definition.name);
		return withSpan("spectrum.message.get", {
			"spectrum.provider": definition.name,
			"spectrum.space.id": spaceRef.id,
			"spectrum.message.id": id
		}, async () => {
			const raw = await getMessage({
				client,
				config,
				store
			}, spaceRef, id);
			if (!raw) return;
			return wrapProviderMessage(raw, {
				client,
				config,
				definition,
				space,
				spaceRef,
				store
			}, "inbound");
		});
	}
	async function getMembersImpl() {
		const getMembers = definition.actions?.getMembers;
		if (!getMembers) throw UnsupportedError.action("getMembers", definition.name);
		return withSpan("spectrum.space.members.get", {
			"spectrum.provider": definition.name,
			"spectrum.space.id": spaceRef.id
		}, async () => {
			const raw = await getMembers({
				client,
				config,
				store
			}, spaceRef);
			if (!Array.isArray(raw)) throw new Error(`${definition.name} getMembers() must resolve an array of member records (got ${raw === null ? "null" : typeof raw})`);
			return raw.map((member) => ({
				...member,
				id: member.id,
				__platform: definition.name
			}));
		});
	}
	async function getAvatarImpl() {
		const getAvatar = definition.actions?.getAvatar;
		if (!getAvatar) throw UnsupportedError.action("getAvatar", definition.name);
		return withSpan("spectrum.space.avatar.get", {
			"spectrum.provider": definition.name,
			"spectrum.space.id": spaceRef.id
		}, async () => await getAvatar({
			client,
			config,
			store
		}, spaceRef));
	}
	async function getDisplayNameImpl() {
		const getDisplayName = definition.actions?.getDisplayName;
		if (!getDisplayName) throw UnsupportedError.action("getDisplayName", definition.name);
		return withSpan("spectrum.space.displayName.get", {
			"spectrum.provider": definition.name,
			"spectrum.space.id": spaceRef.id
		}, async () => await getDisplayName({
			client,
			config,
			store
		}, spaceRef));
	}
	const platformActions = {};
	const declaredActions = definition.space.actions;
	if (declaredActions) for (const [name, factory] of Object.entries(declaredActions)) {
		if (RESERVED_SPACE_KEYS.has(name)) {
			warnReservedAction("space", name, definition.name);
			continue;
		}
		platformActions[name] = async (...args) => {
			await factory(space, ...args);
		};
	}
	space = {
		...extras,
		...spaceRef,
		...platformActions,
		send: sendImpl,
		edit: async (message, newContent) => {
			await space.send(edit(newContent, message));
		},
		unsend: async (message) => {
			await space.send(unsend(message));
		},
		read: async (message) => {
			await space.send(read(message));
		},
		getMessage: getMessageImpl,
		getMembers: getMembersImpl,
		getAvatar: getAvatarImpl,
		getDisplayName: getDisplayNameImpl,
		rename: async (displayName) => {
			await space.send(rename(displayName));
		},
		avatar: (async (input, options) => {
			if (typeof input === "string" || input instanceof URL) {
				await space.send(avatar(input, options));
				return;
			}
			if (!options?.mimeType) throw new Error("space.avatar(Buffer) requires options.mimeType — pass { mimeType: '...' }");
			await space.send(avatar(input, { mimeType: options.mimeType }));
		}),
		add: async (users) => {
			await space.send(addMember(users));
		},
		remove: async (users) => {
			await space.send(removeMember(users));
		},
		leave: async () => {
			await space.send(leaveSpace());
		},
		startTyping: async () => {
			await space.send(typing("start"));
		},
		stopTyping: async () => {
			await space.send(typing("stop"));
		},
		responding: async (fn) => {
			await space.send(typing("start"));
			try {
				return await fn();
			} finally {
				await space.send(typing("stop")).catch(() => {});
			}
		}
	};
	return space;
}
function buildMessage(params) {
	const { definition, space } = params;
	let self;
	const requireBuiltMessage = (action) => {
		if (!self) throw new Error(`${action}() called before message construction completed (internal bug)`);
		return self;
	};
	const react = async (emoji) => {
		const target = requireBuiltMessage("react");
		return await space.send(reaction(emoji, target));
	};
	async function reply$1(...content) {
		const target = requireBuiltMessage("reply");
		const wrapped = content.map((c) => reply(c, target));
		return space.send(...wrapped);
	}
	const edit$1 = async (newContent) => {
		const target = requireBuiltMessage("edit");
		if (target.direction !== "outbound") throw new Error(`cannot edit message ${target.id}: only outbound messages can be edited (direction: "${target.direction}")`);
		await space.send(edit(newContent, target));
	};
	const unsend$1 = async () => {
		const target = requireBuiltMessage("unsend");
		if (target.direction !== "outbound") throw new Error(`cannot unsend message ${target.id}: only outbound messages can be unsent (direction: "${target.direction}")`);
		await space.send(unsend(target));
	};
	const read$1 = async () => {
		const target = requireBuiltMessage("read");
		if (target.direction !== "inbound") throw new Error(`cannot mark message ${target.id} as read: only inbound messages can be marked read (direction: "${target.direction}")`);
		await space.send(read(target));
	};
	const buildSenderWithPlatform = () => {
		if (params.sender === void 0) return;
		if (params.direction === "outbound") return {
			...params.sender,
			__platform: definition.name,
			kind: "agent"
		};
		return {
			...params.sender,
			__platform: definition.name
		};
	};
	const senderWithPlatform = buildSenderWithPlatform();
	const messagePlatformActions = {};
	const declaredMessageActions = definition.message?.actions;
	if (declaredMessageActions) for (const [name, factory] of Object.entries(declaredMessageActions)) {
		if (RESERVED_MESSAGE_KEYS.has(name)) {
			warnReservedAction("message", name, definition.name);
			continue;
		}
		messagePlatformActions[name] = async (...args) => {
			await factory(requireBuiltMessage(name), ...args);
		};
	}
	const message = {
		...params.extras,
		...messagePlatformActions,
		id: params.id,
		content: params.content,
		direction: params.direction,
		platform: definition.name,
		react,
		read: read$1,
		reply: reply$1,
		edit: edit$1,
		unsend: unsend$1,
		sender: senderWithPlatform,
		space,
		timestamp: params.timestamp
	};
	self = message;
	return message;
}
//#endregion
//#region src/platform/define.ts
const platformLog = createLogger("spectrum.platform");
const PLATFORM_ID_PATTERN = /^[a-z][a-z0-9]*(?:_[a-z0-9]+)*$/;
const assertValidPlatformId = (platformId) => {
	if (PLATFORM_ID_PATTERN.test(platformId)) return;
	throw new Error(`Invalid platform id "${platformId}". Platform ids must use lowercase snake_case (for example, "my_platform").`);
};
function buildInstanceActions(platformName, declared, reservedKeys, buildCtx) {
	const out = {};
	for (const key of PLATFORM_WISE_ACTION_KEYS) {
		const override = declared?.[key];
		if (override && typeof override === "function") out[key] = (...args) => override(buildCtx(), ...args);
		else out[key] = () => {
			throw UnsupportedError.action(key, platformName);
		};
	}
	if (!declared) return out;
	for (const [name, factory] of Object.entries(declared)) {
		if (PLATFORM_WISE_ACTION_KEYS.has(name)) continue;
		if (reservedKeys.has(name)) {
			warnReservedAction("instance", name, platformName);
			continue;
		}
		if (typeof factory !== "function") continue;
		out[name] = (...args) => factory(buildCtx(), ...args);
	}
	return out;
}
function createPlatformInstance(def, runtime) {
	const resolveUserID = async (userID) => {
		return {
			...await def.user.resolve({
				input: { userID },
				client: runtime.client,
				config: runtime.config,
				store: runtime.store
			}),
			__platform: def.name
		};
	};
	const providerCtx = () => ({
		client: runtime.client,
		config: runtime.config,
		store: runtime.store
	});
	const parseSpaceParams = (params) => params !== void 0 && def.space.params ? def.space.params.parse(params) : params;
	const finalizeSpace = (resolved) => {
		const parsedSpace = def.space.schema ? def.space.schema.parse(resolved) : resolved;
		const spaceRef = {
			...parsedSpace,
			id: parsedSpace.id,
			__platform: def.name
		};
		return buildSpace({
			spaceRef,
			extras: parsedSpace,
			actionCtx: {
				space: spaceRef,
				...providerCtx()
			},
			definition: def,
			client: runtime.client,
			config: runtime.config,
			store: runtime.store
		});
	};
	const base = {
		async user(userID) {
			return await resolveUserID(userID);
		},
		space: {
			create: async (users, params) => {
				const userList = Array.isArray(users) ? users : [users];
				const first = userList.length === 1 ? userList[0] : void 0;
				const single = typeof first === "string" ? classifyIdentifier(first) : void 0;
				const kind = userList.length > 1 ? "group" : single?.kind ?? "unknown";
				return await withSpan("spectrum.space.create", {
					"spectrum.provider": def.name,
					"spectrum.space.user_count": userList.length,
					"spectrum.space.identifier_kind": kind,
					"spectrum.space.identifier": kind === "unknown" ? void 0 : single?.identifier
				}, async () => {
					const resolvedUsers = await Promise.all(userList.map((u) => typeof u === "string" ? resolveUserID(u) : u));
					return finalizeSpace(await def.space.create({
						input: {
							users: resolvedUsers,
							params: parseSpaceParams(params)
						},
						...providerCtx()
					}));
				});
			},
			get: async (id, params) => await withSpan("spectrum.space.get", {
				"spectrum.provider": def.name,
				"spectrum.space.id": id
			}, async () => {
				const parsedParams = parseSpaceParams(params);
				if (def.space.get) return finalizeSpace(await def.space.get({
					input: {
						id,
						params: parsedParams
					},
					...providerCtx()
				}));
				const candidate = { id };
				if (def.space.schema) {
					const parsed = def.space.schema.safeParse(candidate);
					if (!parsed.success) throw new Error(`Platform "${def.name}" cannot construct a space from an id alone — its space schema requires more fields. Implement \`space.get\` in the "${def.name}" provider definition.`, { cause: parsed.error });
				}
				return finalizeSpace(candidate);
			})
		}
	};
	const eventProperties = {};
	const customEvents = def.events ?? {};
	for (const eventName of Object.keys(customEvents)) {
		const declared = customEvents[eventName];
		if (typeof declared === "function") {
			eventProperties[eventName] = declared({
				client: runtime.client,
				config: runtime.config,
				projectConfig: runtime.projectConfig,
				store: runtime.store
			});
			continue;
		}
		const fusorEvents = runtime.subscribeEvent?.(eventName);
		if (fusorEvents) eventProperties[eventName] = fusorEvents;
	}
	let messagesIterable;
	Object.defineProperty(base, "messages", {
		enumerable: true,
		get() {
			messagesIterable ??= runtime.subscribeMessages();
			return messagesIterable;
		}
	});
	const instanceActions = buildInstanceActions(def.name, def.actions, new Set([
		"user",
		"space",
		"messages",
		...Object.keys(customEvents)
	]), () => ({
		client: runtime.client,
		config: runtime.config,
		store: runtime.store
	}));
	return Object.assign(base, instanceActions, eventProperties);
}
function definePlatform(platformId, rawDef) {
	assertValidPlatformId(platformId);
	const def = rawDef;
	const fullDef = {
		...def,
		name: platformId,
		config: envAwareConfig(platformId, def.config)
	};
	const platformCache = /* @__PURE__ */ new WeakMap();
	const narrowSpectrum = (spectrum) => {
		const cached = platformCache.get(spectrum);
		if (cached) return cached;
		const runtime = spectrum.__internal.platforms.get(platformId);
		if (!runtime) throw new Error(`Platform "${platformId}" is not registered`);
		const instance = createPlatformInstance(fullDef, runtime);
		platformCache.set(spectrum, instance);
		return instance;
	};
	const narrowSpace = (input) => {
		if (input.__platform !== platformId) platformLog.warn("space platform mismatch; narrowing skipped", {
			"spectrum.platform.expected": platformId,
			"spectrum.platform.actual": input.__platform
		});
		return input;
	};
	const narrowMessage = (input) => {
		if (input.platform !== platformId) platformLog.warn("message platform mismatch; narrowing skipped", {
			"spectrum.platform.expected": platformId,
			"spectrum.platform.actual": input.platform
		});
		return input;
	};
	const narrower = ((input) => {
		if ("__providers" in input && "__internal" in input) return narrowSpectrum(input);
		if ("__platform" in input && "send" in input) return narrowSpace(input);
		if ("platform" in input && "sender" in input && "space" in input) return narrowMessage(input);
		throw new Error("Invalid input to platform narrowing function");
	});
	narrower.config = (config) => {
		return {
			__tag: "PlatformProviderConfig",
			__def: void 0,
			__name: platformId,
			config: config ?? {},
			__definition: fullDef
		};
	};
	narrower.is = ((input) => {
		if (typeof input !== "object" || input === null) return false;
		if ("__platform" in input) return input.__platform === platformId;
		if ("platform" in input) return input.platform === platformId;
		return false;
	});
	if (def.static) Object.assign(narrower, def.static);
	return narrower;
}
//#endregion
//#region src/build-env.ts
const SPECTRUM_SDK_VERSION = "12.7.0";
//#endregion
//#region src/utils/provider-packages.ts
const OFFICIAL_PROVIDER_PACKAGES = {
	imessage: "@spectrum-ts/imessage",
	local_imessage: "@spectrum-ts/imessage-local",
	slack: "@spectrum-ts/slack",
	telegram: "@spectrum-ts/telegram",
	terminal: "@spectrum-ts/terminal",
	whatsapp_business: "@spectrum-ts/whatsapp-business"
};
const LEGACY_SEPARATORS = /[\s-]+/g;
const normalizePlatformKey = (platform) => platform.trim().toLowerCase().replace(LEGACY_SEPARATORS, "_");
const officialProviderPackage = (platform) => OFFICIAL_PROVIDER_PACKAGES[normalizePlatformKey(platform)];
const installCommand = (pkg) => process.versions.bun ? `bun add ${pkg}` : `npm install ${pkg}`;
/**
* One-line install hint for a platform provided by an official package, or
* undefined for unknown/custom platforms. Appended to "no handler" style
* warnings — advisory only, callers must not change behavior based on it.
*/
const officialProviderInstallHint = (platform) => {
	const pkg = officialProviderPackage(platform);
	if (!pkg) return;
	return `the "${platform}" platform is provided by the optional package ${pkg} — install it (\`${installCommand(pkg)}\`) and pass it to Spectrum({ providers: [...] })`;
};
//#endregion
//#region src/utils/cloud.ts
const SPECTRUM_CLOUD_URL = process.env.SPECTRUM_CLOUD_URL ?? "https://spectrum.photon.codes";
var SpectrumCloudError = class extends Error {
	status;
	code;
	constructor(status, code, message) {
		super(message);
		this.name = "SpectrumCloudError";
		this.status = status;
		this.code = code;
	}
};
const cloudFetch = tracedFetch("spectrum-cloud");
const request = async (path, init) => {
	const response = await cloudFetch(`${SPECTRUM_CLOUD_URL}${path}`, init);
	if (!response.ok) {
		const body = await response.text().catch(() => "");
		try {
			const parsed = JSON.parse(body);
			throw new SpectrumCloudError(response.status, parsed.code, parsed.message);
		} catch (error) {
			if (error instanceof SpectrumCloudError) throw error;
			throw new SpectrumCloudError(response.status, "UNKNOWN", body || response.statusText);
		}
	}
	const json = await response.json();
	if (!json.succeed) throw new SpectrumCloudError(response.status, "UNKNOWN", "Server returned succeed=false");
	return json.data;
};
const basicAuth = (projectId, projectSecret) => `Basic ${btoa(`${projectId}:${projectSecret}`)}`;
const cloud = {
	getProject: (projectId, projectSecret) => request(`/projects/${projectId}/`, { headers: { Authorization: basicAuth(projectId, projectSecret) } }),
	getSubscription: (projectId) => request(`/projects/${projectId}/billing/subscription`),
	issueImessageTokens: (projectId, projectSecret) => request(`/projects/${projectId}/imessage/tokens`, {
		method: "POST",
		headers: { Authorization: basicAuth(projectId, projectSecret) }
	}),
	getImessageInfo: (projectId) => request(`/projects/${projectId}/imessage/`),
	issueWhatsappBusinessTokens: (projectId, projectSecret) => request(`/projects/${projectId}/whatsapp-business/tokens`, {
		method: "POST",
		headers: { Authorization: basicAuth(projectId, projectSecret) }
	}),
	issueSlackTokens: (projectId, projectSecret) => request(`/projects/${projectId}/slack/tokens`, {
		method: "POST",
		headers: { Authorization: basicAuth(projectId, projectSecret) }
	}),
	issueFusorToken: (projectId, projectSecret) => request(`/projects/${projectId}/fusor/token`, {
		method: "POST",
		headers: { Authorization: basicAuth(projectId, projectSecret) }
	}),
	getPlatforms: (projectId) => request(`/projects/${projectId}/platforms/`),
	togglePlatform: (projectId, projectSecret, platform, enabled) => request(`/projects/${projectId}/platforms/`, {
		method: "PATCH",
		headers: {
			Authorization: basicAuth(projectId, projectSecret),
			"Content-Type": "application/json"
		},
		body: JSON.stringify({
			platform,
			enabled
		})
	})
};
//#endregion
//#region src/fusor/auth.ts
/**
* Single-token provider for the fusor stream. Mirrors the renewal cadence
* of the slack provider package's auth but without per-team bookkeeping —
* fusor issues one bearer JWT per project.
*/
function createFusorTokenProvider(projectId, projectSecret) {
	return (async () => {
		let tokenData = await cloud.issueFusorToken(projectId, projectSecret);
		const renewal = createTokenRenewal({
			expiresInSeconds: () => tokenData.expiresIn,
			name: "fusor",
			refresh: async () => {
				tokenData = await cloud.issueFusorToken(projectId, projectSecret);
			}
		});
		return {
			async getToken() {
				await renewal.refreshIfNeeded();
				return tokenData.token;
			},
			invalidate() {
				renewal.invalidate();
			},
			async dispose() {
				renewal.dispose();
			}
		};
	})();
}
//#endregion
//#region src/fusor/parse.ts
const CR = 13;
const LF = 10;
function findHeaderEnd(bytes) {
	for (let i = 0; i + 3 < bytes.length; i++) if (bytes[i] === CR && bytes[i + 1] === LF && bytes[i + 2] === CR && bytes[i + 3] === LF) return i;
	return -1;
}
/**
* Parses an HTTP/1.1 wire-format request out of `raw_request` from
* `RawInboundEvent`. Headers are lowercased. Multiple header values with the
* same name are joined with ", " (RFC 7230 §3.2.2).
*/
function parseHttpRequest(bytes) {
	const headerEnd = findHeaderEnd(bytes);
	if (headerEnd < 0) throw new Error("fusor: raw_request missing CRLFCRLF header terminator");
	const headerText = new TextDecoder("utf-8").decode(bytes.subarray(0, headerEnd));
	const rawBody = bytes.subarray(headerEnd + 4);
	const lines = headerText.split("\r\n");
	const requestLine = lines[0];
	if (!requestLine) throw new Error("fusor: raw_request missing request line");
	const firstSpace = requestLine.indexOf(" ");
	const lastSpace = requestLine.lastIndexOf(" ");
	if (firstSpace < 0 || lastSpace <= firstSpace) throw new Error(`fusor: malformed request line: ${requestLine}`);
	const method = requestLine.slice(0, firstSpace);
	const path = requestLine.slice(firstSpace + 1, lastSpace);
	const headers = {};
	for (let i = 1; i < lines.length; i++) {
		const line = lines[i];
		if (!line) continue;
		const colon = line.indexOf(":");
		if (colon < 0) continue;
		const key = line.slice(0, colon).trim().toLowerCase();
		const value = line.slice(colon + 1).trim();
		if (!key) continue;
		const existing = headers[key];
		headers[key] = existing ? `${existing}, ${value}` : value;
	}
	return {
		method,
		path,
		headers,
		rawBody
	};
}
//#endregion
//#region src/fusor/websocket.ts
const log$1 = createLogger("spectrum.fusor.ws");
const FUSOR_WS_SUBPROTOCOL = "fusor.v1.json";
const STALENESS_GRACE_MS = 5e3;
var FusorWsError = class extends Error {
	closeCode;
	errorCode;
	constructor(message, closeCode, errorCode) {
		super(message);
		this.name = "FusorWsError";
		this.closeCode = closeCode;
		this.errorCode = errorCode;
	}
};
function isWsAuthError(error) {
	return error instanceof FusorWsError && (error.closeCode === 4401 || error.errorCode === "unauthenticated");
}
function decodeBase64(value) {
	if (typeof Buffer !== "undefined") return new Uint8Array(Buffer.from(value, "base64"));
	return Uint8Array.from(atob(value), (c) => c.charCodeAt(0));
}
function encodeBase64(bytes) {
	if (typeof Buffer !== "undefined") return Buffer.from(bytes).toString("base64");
	const chunkSize = 32768;
	const parts = [];
	for (let i = 0; i < bytes.length; i += chunkSize) parts.push(String.fromCharCode(...bytes.subarray(i, i + chunkSize)));
	return btoa(parts.join(""));
}
function toRawInboundEvent(frame) {
	const e = frame.event;
	return {
		eventId: e.eventId,
		projectId: e.projectId,
		platform: e.platform,
		receivedAt: e.receivedAt ? new Date(e.receivedAt) : void 0,
		sourceId: e.sourceId ?? "",
		prevSubjectSeq: e.prevSubjectSeq ?? 0,
		rawRequest: decodeBase64(e.rawRequest)
	};
}
function runFusorWsSession(options) {
	const WebSocketCtor = globalThis.WebSocket;
	if (typeof WebSocketCtor !== "function") throw new FusorWsError("global WebSocket is not available in this runtime — the fusor websocket transport needs Bun, Node >= 22, or a browser/worker environment");
	const wsOpen = WebSocketCtor.OPEN;
	const ws = new WebSocketCtor(options.url, [FUSOR_WS_SUBPROTOCOL]);
	let settled = false;
	let closedByUs = false;
	let pendingError = null;
	let stalenessBudgetMs = 65e3;
	let watchdog;
	let tail = Promise.resolve();
	let resolveDone;
	let rejectDone;
	const done = new Promise((resolve, reject) => {
		resolveDone = resolve;
		rejectDone = reject;
	});
	const settle = (error) => {
		if (settled) return;
		settled = true;
		if (watchdog) {
			clearTimeout(watchdog);
			watchdog = void 0;
		}
		if (error) rejectDone(error);
		else resolveDone();
	};
	const armWatchdog = () => {
		if (settled) return;
		if (watchdog) clearTimeout(watchdog);
		watchdog = setTimeout(() => {
			log$1.warn("fusor ws: no frame within staleness budget; closing", { "spectrum.fusor.ws.staleness_budget_ms": stalenessBudgetMs });
			settle(new FusorWsError("websocket heartbeat timeout"));
			try {
				ws.close();
			} catch {}
		}, stalenessBudgetMs);
		watchdog.unref?.();
	};
	const sendReplyFor = (eventId) => (reply) => {
		if (ws.readyState !== wsOpen) return;
		ws.send(JSON.stringify({
			type: "reply",
			eventId,
			status: reply.status,
			headers: reply.headers,
			...reply.body.length > 0 && { body: encodeBase64(reply.body) },
			...reply.errorReason && { errorReason: reply.errorReason }
		}));
	};
	const handleReadyFrame = (frame) => {
		const interval = frame.heartbeatIntervalMs;
		if (typeof interval === "number" && interval > 0) stalenessBudgetMs = 2 * interval + STALENESS_GRACE_MS;
		log$1.info("fusor ws stream ready", {
			"spectrum.fusor.ws.project_id": typeof frame.projectId === "string" ? frame.projectId : "",
			"spectrum.fusor.ws.heartbeat_interval_ms": typeof interval === "number" ? interval : 0
		});
	};
	const handleEventFrame = (frame) => {
		const eventFrame = frame;
		let event;
		try {
			event = toRawInboundEvent(eventFrame);
		} catch (error) {
			log$1.warn("fusor ws: undecodable event frame; skipping", errorAttrs(error), error);
			return;
		}
		const sendReply = eventFrame.replyExpected ? sendReplyFor(event.eventId) : void 0;
		tail = tail.then(() => options.onEvent(event, sendReply)).catch((error) => {
			log$1.warn("fusor ws: event handler failed", {
				"spectrum.fusor.ws.event_id": event.eventId,
				...errorAttrs(error)
			}, error);
		});
	};
	const handleErrorFrame = (frame) => {
		const code = typeof frame.code === "string" ? frame.code : "unknown";
		const message = typeof frame.message === "string" ? frame.message : "server error";
		const reason = typeof frame.reason === "string" ? frame.reason : void 0;
		if (frame.fatal === true) pendingError = {
			code,
			message,
			reason
		};
		else log$1.warn("fusor ws: server notice", {
			"spectrum.fusor.ws.notice_code": code,
			"spectrum.fusor.ws.notice_message": message,
			"spectrum.fusor.ws.notice_reason": reason
		});
	};
	const handleFrame = (raw) => {
		if (typeof raw !== "string") return;
		let frame;
		try {
			frame = JSON.parse(raw);
		} catch {
			log$1.warn("fusor ws: unparseable server frame; ignoring");
			return;
		}
		switch (frame.type) {
			case "ready":
				handleReadyFrame(frame);
				return;
			case "event":
				handleEventFrame(frame);
				return;
			case "error":
				handleErrorFrame(frame);
				return;
			default: return;
		}
	};
	ws.onopen = () => {
		armWatchdog();
		ws.send(JSON.stringify({
			type: "init",
			startSeq: 0,
			token: options.token
		}));
	};
	ws.onmessage = (messageEvent) => {
		armWatchdog();
		handleFrame(messageEvent.data);
	};
	ws.onerror = () => {
		log$1.debug("fusor ws: socket error event");
	};
	ws.onclose = (closeEvent) => {
		if (closedByUs) {
			settle();
			return;
		}
		const detail = pendingError ? `${pendingError.code}${pendingError.reason ? `:${pendingError.reason}` : ""} — ${pendingError.message}` : closeEvent.reason || "connection closed";
		settle(new FusorWsError(`fusor websocket closed (${closeEvent.code}): ${detail}`, closeEvent.code, pendingError?.code ?? (closeEvent.reason || void 0)));
	};
	armWatchdog();
	return {
		done,
		close() {
			closedByUs = true;
			try {
				ws.close(1e3);
			} catch {}
			setTimeout(() => settle(), 2e3).unref?.();
		}
	};
}
//#endregion
//#region src/fusor/core.ts
const DEFAULT_FUSOR_WS_URL = "wss://fusor-ws.spectrum.photon.codes/v1/subscribe";
const RECONNECT_BASE_MS = 1e3;
const RECONNECT_MAX_MS = 3e4;
const log = createLogger("spectrum.fusor");
const errorText = (error) => error instanceof Error ? error.message : String(error);
function toReplyBytes(body) {
	if (body === void 0) return new Uint8Array(0);
	if (typeof body === "string") return new TextEncoder().encode(body);
	return body;
}
function combineReplies(outcomes) {
	const successes = outcomes.filter((o) => o.ok);
	if (successes.length === 0) return {
		eventId: "",
		errorReason: outcomes[0]?.errorReason ?? "no handler succeeded",
		status: 0,
		headers: {},
		body: new Uint8Array(0)
	};
	let status = 0;
	const headers = {};
	let body = new Uint8Array(0);
	for (const outcome of successes) {
		const reply = outcome.reply;
		if (!reply) continue;
		if (reply.status !== void 0 && reply.status > status) status = reply.status;
		if (reply.headers) for (const [k, v] of Object.entries(reply.headers)) headers[k.toLowerCase()] = v;
		const candidate = toReplyBytes(reply.body);
		if (candidate.length > 0) body = candidate;
	}
	return {
		eventId: "",
		errorReason: "",
		status,
		headers,
		body
	};
}
function routeHandlerResult(result, handler, deliver) {
	if (result === void 0) return;
	const items = Array.isArray(result) ? result : [result];
	for (const item of items) {
		if (!isFusorEvent(item)) {
			deliver(item);
			continue;
		}
		if (item.name === "messages") deliver(item.data);
		else handler.pushEvent(item.name, item.data);
	}
}
function runHandlerOnce(handler, parsedRequest, deliver = handler.pushMessage) {
	return (async () => {
		try {
			const payload = await handler.verify(parsedRequest);
			let reply;
			let respondCalled = false;
			let returned = false;
			const respond = (next) => {
				if (returned) {
					log.warn("fusor.respond called after handler returned; ignoring");
					return;
				}
				if (respondCalled) log.debug("fusor.respond called more than once; last call wins");
				respondCalled = true;
				reply = next;
			};
			const result = await handler.messages({
				payload,
				respond
			});
			returned = true;
			routeHandlerResult(result, handler, deliver);
			return {
				ok: true,
				reply
			};
		} catch (error) {
			return {
				ok: false,
				errorReason: errorText(error)
			};
		}
	})();
}
var FusorCore = class {
	options;
	websocketEndpoint;
	handlers = /* @__PURE__ */ new Map();
	tokenProvider;
	wsSession;
	connectionLoop;
	started = false;
	stopped = false;
	stopResolve;
	stoppedPromise;
	reconnectTimer;
	reconnectResolve;
	constructor(options) {
		this.options = options;
		this.websocketEndpoint = options.websocketEndpoint ?? process.env.SPECTRUM_FUSOR_WS_URL ?? DEFAULT_FUSOR_WS_URL;
		this.stoppedPromise = new Promise((resolve) => {
			this.stopResolve = resolve;
		});
	}
	register(platform, handler) {
		const list = this.handlers.get(platform) ?? [];
		list.push(handler);
		this.handlers.set(platform, list);
	}
	async start() {
		if (!(this.options.projectId && this.options.projectSecret)) throw new Error("fusor: streaming via spectrum.messages requires projectId and projectSecret");
		if (this.started) return;
		this.started = true;
		this.tokenProvider = await createFusorTokenProvider(this.options.projectId, this.options.projectSecret);
		this.connectionLoop = this.runConnectionLoop().catch((error) => {
			log.error("fusor connection loop crashed", errorAttrs(error), error);
		});
	}
	async runConnectionLoop() {
		let attempt = 0;
		while (!this.stopped) {
			const wsRan = await this.tryWebsocketOnce();
			if (this.stopped) return;
			if (wsRan) {
				attempt = 0;
				continue;
			}
			attempt += 1;
			await this.backoffSleep(this.backoffMs(attempt));
		}
	}
	async tryWebsocketOnce() {
		try {
			await this.runWebsocketOnce();
			return true;
		} catch (error) {
			if (isWsAuthError(error)) this.tokenProvider?.invalidate();
			if (!this.stopped) log.warn("fusor websocket stream errored; reconnecting", errorAttrs(error), error);
			return false;
		}
	}
	backoffMs(attempt) {
		return Math.min(RECONNECT_BASE_MS * 2 ** (attempt - 1), RECONNECT_MAX_MS);
	}
	async backoffSleep(backoff) {
		await new Promise((resolve) => {
			this.reconnectResolve = resolve;
			const timer = setTimeout(resolve, backoff);
			timer.unref?.();
			this.reconnectTimer = timer;
		});
		this.reconnectTimer = void 0;
		this.reconnectResolve = void 0;
	}
	async runWebsocketOnce() {
		if (!this.tokenProvider) throw new Error("fusor: token not initialized");
		const token = await this.tokenProvider.getToken();
		const session = runFusorWsSession({
			url: this.websocketEndpoint,
			token,
			onEvent: async (event, sendReply) => {
				if (this.stopped) return;
				const reply = await this.processEvent(event);
				sendReply?.(reply);
			}
		});
		this.wsSession = session;
		try {
			await session.done;
		} finally {
			this.wsSession = void 0;
		}
	}
	async processEvent(event, deliver) {
		const handlers = this.handlers.get(event.platform) ?? [];
		if (handlers.length === 0) {
			const hint = officialProviderInstallHint(event.platform);
			log.warn(hint ? `fusor: no handler for platform — ${hint}` : "fusor: no handler for platform", {
				"spectrum.fusor.platform": event.platform,
				"spectrum.fusor.event_id": event.eventId
			});
			return {
				eventId: event.eventId,
				errorReason: `no handler for platform ${event.platform}`,
				status: 0,
				headers: {},
				body: new Uint8Array(0)
			};
		}
		let parsedRequest;
		try {
			parsedRequest = parseHttpRequest(event.rawRequest);
		} catch (error) {
			const errorReason = errorText(error);
			log.warn("fusor: failed to parse raw_request", {
				"spectrum.fusor.platform": event.platform,
				"spectrum.fusor.event_id": event.eventId,
				...errorAttrs(error)
			});
			return {
				eventId: event.eventId,
				errorReason,
				status: 0,
				headers: {},
				body: new Uint8Array(0)
			};
		}
		const combined = combineReplies(await Promise.all(handlers.map((handler) => runHandlerOnce(handler, parsedRequest, deliver))));
		combined.eventId = event.eventId;
		return combined;
	}
	async close() {
		if (this.stopped) return;
		this.stopped = true;
		this.wsSession?.close();
		if (this.reconnectTimer) {
			clearTimeout(this.reconnectTimer);
			this.reconnectTimer = void 0;
		}
		this.reconnectResolve?.();
		this.reconnectResolve = void 0;
		if (this.tokenProvider) await this.tokenProvider.dispose();
		if (this.connectionLoop) await this.connectionLoop;
		this.stopResolve?.();
	}
	async waitStopped() {
		return this.stoppedPromise;
	}
};
//#endregion
//#region src/platform/unique-names.ts
const assertUniquePlatformNames = (providers) => {
	const names = /* @__PURE__ */ new Set();
	for (const provider of providers) {
		const name = provider.__definition.name;
		if (names.has(name)) throw new Error(`Spectrum received multiple providers for platform "${name}". Register exactly one provider per platform.`);
		names.add(name);
	}
};
//#endregion
//#region src/utils/store.ts
const isRecordObject = (value) => {
	if (typeof value !== "object" || value === null || Array.isArray(value)) return false;
	const prototype = Object.getPrototypeOf(value);
	return prototype === Object.prototype || prototype === null;
};
function createStore() {
	const data = /* @__PURE__ */ new Map();
	return {
		set(key, value) {
			data.set(key, value);
		},
		get(key) {
			return data.get(key);
		},
		has(key) {
			return data.has(key);
		},
		delete(key) {
			return data.delete(key);
		},
		clear() {
			data.clear();
		},
		keys() {
			return Array.from(data.keys());
		},
		string(key) {
			const v = data.get(key);
			return typeof v === "string" ? v : void 0;
		},
		number(key) {
			const v = data.get(key);
			return typeof v === "number" ? v : void 0;
		},
		bool(key) {
			const v = data.get(key);
			return typeof v === "boolean" ? v : void 0;
		},
		object(key) {
			const v = data.get(key);
			if (!isRecordObject(v)) return;
			return v;
		},
		array(key) {
			const v = data.get(key);
			return Array.isArray(v) ? v : void 0;
		}
	};
}
//#endregion
//#region src/webhook/deserialize.ts
/** The single event type that carries a message today. */
const MESSAGES_EVENT = "messages";
const DEFAULT_ATTACHMENT_NAME = "attachment";
const DEFAULT_MIME_TYPE = "application/octet-stream";
const isRecord = (value) => typeof value === "object" && value !== null;
const asString = (value) => typeof value === "string" ? value : "";
const asOptionalDate = (value) => typeof value === "string" ? new Date(value) : void 0;
/**
* Map a native Spectrum webhook envelope to a `ProviderMessageRecord` plus the
* platform that owns it, ready for `resolveRecordToMessages`. Returns `null`
* when the delivery carries nothing to route (an unknown `event` type, or a
* message with no resolvable platform) — the caller acknowledges it (200)
* rather than failing, since neither is fixed by a retry.
*
* Reaction/reply targets and group items are emitted as **raw nested records**;
* the `wrapProviderMessage`/`wrapNestedContent` pipeline turns them into
* fully-built Messages, exactly as a provider's own `messages` handler would.
*/
function deserializeSpectrumMessage(envelope, ctx) {
	if (envelope.event !== MESSAGES_EVENT) return null;
	const message = envelope.message;
	const platform = resolvePlatform(message);
	if (!platform) return null;
	const spaceRef = { ...message.space };
	return {
		platform,
		record: {
			id: message.id,
			direction: "inbound",
			content: deserializeContent(message.content, platform, spaceRef, ctx),
			space: spaceRef,
			sender: message.sender ? { ...message.sender } : void 0,
			timestamp: asOptionalDate(message.timestamp)
		}
	};
}
const resolvePlatform = (message) => message.platform ?? message.space.platform;
const deserializeContent = (content, platform, spaceRef, ctx) => {
	try {
		return mapContent(content, platform, spaceRef, ctx);
	} catch {
		return asCustom(content);
	}
};
const mapContent = (content, platform, spaceRef, ctx) => {
	const raw = content;
	switch (content.type) {
		case "text": return {
			type: "text",
			text: asString(raw.text)
		};
		case "richlink": return {
			type: "text",
			text: asString(raw.url)
		};
		case "contact": return deserializeContact(raw);
		case "reaction": return deserializeReaction(raw, spaceRef);
		case "read": return deserializeRead(raw, spaceRef);
		case "reply": return deserializeReply(raw, platform, spaceRef, ctx);
		case "group": return deserializeGroup(raw, platform, spaceRef, ctx);
		case "attachment": return deserializeAttachment(raw, platform, spaceRef, ctx);
		case "voice": return deserializeVoice(raw, platform, spaceRef, ctx);
		case "addMember": return addMemberSchema.parse({
			type: "addMember",
			members: memberStrings(raw.members)
		});
		case "removeMember": return removeMemberSchema.parse({
			type: "removeMember",
			members: memberStrings(raw.members)
		});
		case "leaveSpace": return leaveSpaceSchema.parse({ type: "leaveSpace" });
		case "rename": return renameSchema.parse({
			type: "rename",
			displayName: asString(raw.displayName)
		});
		case "avatar": return deserializeAvatar(raw, platform);
		default: return asCustom(content);
	}
};
const memberStrings = (value) => Array.isArray(value) ? value.filter((entry) => typeof entry === "string") : [];
const deserializeAvatar = (raw, platform) => {
	const action = isRecord(raw.action) ? raw.action : {};
	if (action.kind === "clear") return avatarSchema.parse({
		type: "avatar",
		action: { kind: "clear" }
	});
	if (action.kind !== "set") throw new Error(`unknown avatar action kind: ${String(action.kind)}`);
	const unavailable = () => Promise.reject(UnsupportedError.action("getAvatar", platform, "avatar bytes are not delivered over the Spectrum webhook — fetch the current icon via space.getAvatar()"));
	return avatarSchema.parse({
		type: "avatar",
		action: {
			kind: "set",
			mimeType: asString(action.mimeType) || DEFAULT_MIME_TYPE,
			read: unavailable
		}
	});
};
const deserializeAttachment = (raw, platform, spaceRef, ctx) => {
	const id = asString(raw.id);
	const bytes = ctx.resolveAttachment?.(platform, spaceRef, id);
	const unavailable = () => Promise.reject(UnsupportedError.action("getAttachment", platform, `attachment "${id}" arrived without bytes over the Spectrum webhook and "${platform}" exposes no getAttachment`));
	return asAttachment({
		id,
		name: asString(raw.name) || DEFAULT_ATTACHMENT_NAME,
		mimeType: asString(raw.mimeType) || DEFAULT_MIME_TYPE,
		size: typeof raw.size === "number" ? raw.size : void 0,
		read: bytes ? bytes.read : unavailable,
		stream: bytes?.stream
	});
};
const deserializeVoice = (raw, platform, spaceRef, ctx) => {
	const id = asString(raw.id);
	const bytes = id ? ctx.resolveAttachment?.(platform, spaceRef, id) : void 0;
	const unavailable = () => Promise.reject(UnsupportedError.action("getAttachment", platform, `voice attachment "${id}" arrived without bytes over the Spectrum webhook and "${platform}" exposes no getAttachment`));
	return asVoice({
		...id ? { id } : {},
		name: asString(raw.name) || void 0,
		mimeType: asString(raw.mimeType),
		duration: typeof raw.duration === "number" ? raw.duration : void 0,
		size: typeof raw.size === "number" ? raw.size : void 0,
		read: bytes ? bytes.read : unavailable,
		stream: bytes?.stream
	});
};
const deserializeReaction = (raw, spaceRef) => ({
	type: "reaction",
	emoji: asString(raw.emoji),
	target: buildTargetRecord(raw.target, spaceRef)
});
const deserializeRead = (raw, spaceRef) => ({
	type: "read",
	target: {
		...buildTargetRecord(raw.target, spaceRef),
		direction: "outbound"
	}
});
const deserializeReply = (raw, platform, spaceRef, ctx) => ({
	type: "reply",
	content: isRecord(raw.content) ? deserializeContent(raw.content, platform, spaceRef, ctx) : asCustom(raw.content),
	target: buildTargetRecord(raw.target, spaceRef)
});
const buildTargetRecord = (target, spaceRef) => {
	const ref = isRecord(target) ? target : {};
	return {
		id: asString(ref.id),
		content: {
			type: "text",
			text: asString(ref.contentPreview)
		},
		space: { ...spaceRef },
		sender: ref.sender ? { ...ref.sender } : void 0,
		timestamp: asOptionalDate(ref.timestamp)
	};
};
const deserializeGroup = (raw, platform, spaceRef, ctx) => {
	return {
		type: "group",
		items: (Array.isArray(raw.items) ? raw.items : []).map((item) => buildItemRecord(item, platform, spaceRef, ctx))
	};
};
const buildItemRecord = (item, platform, spaceRef, ctx) => {
	const record = isRecord(item) ? item : {};
	const itemSpace = isRecord(record.space) ? {
		...record.space,
		id: asString(record.space.id) || spaceRef.id
	} : spaceRef;
	const content = isRecord(record.content) ? deserializeContent(record.content, platform, itemSpace, ctx) : asCustom(record.content);
	return {
		id: asString(record.id),
		content,
		space: itemSpace,
		sender: isRecord(record.sender) ? {
			...record.sender,
			id: asString(record.sender.id)
		} : void 0,
		timestamp: asOptionalDate(record.timestamp)
	};
};
const deserializeContact = (raw) => {
	const input = {};
	const name = normalizeContactName(raw.name);
	if (name) input.name = name;
	const phones = normalizeContactPhones(raw.phones);
	if (phones) input.phones = phones;
	if (typeof raw.note === "string") input.note = raw.note;
	if (raw.raw !== void 0) input.raw = raw.raw;
	return asContact(input);
};
const CONTACT_NAME_KEYS = [
	"formatted",
	"first",
	"last",
	"middle",
	"prefix",
	"suffix"
];
const normalizeContactName = (value) => {
	if (typeof value === "string") return { formatted: value };
	if (!isRecord(value)) return;
	const name = {};
	for (const key of CONTACT_NAME_KEYS) {
		const part = value[key];
		if (typeof part === "string") name[key] = part;
	}
	return Object.keys(name).length > 0 ? name : void 0;
};
const normalizeContactPhones = (value) => {
	if (!Array.isArray(value)) return;
	const phones = [];
	for (const entry of value) if (typeof entry === "string") phones.push({ value: entry });
	else if (isRecord(entry) && typeof entry.value === "string") phones.push({ value: entry.value });
	return phones.length > 0 ? phones : void 0;
};
//#endregion
//#region src/webhook/types.ts
/**
* Wire schemas for the **native Spectrum webhook**
* (https://photon.codes/docs/webhooks).
*
* Unlike the fusor webhook — which relays a raw provider request inside a
* protobuf envelope — the native webhook delivers Spectrum's own message model
* already normalized to slim JSON (methods and byte payloads stripped), signed
* with an HMAC. These schemas validate the fields the deserializer depends on
* while **preserving** unknown/extra fields (`z.looseObject`), so additive
* changes — new platform-specific space fields, future content arms — never
* break an older SDK. Content is discriminated by hand in `deserialize.ts`
* rather than here, so an unknown content `type` survives parsing instead of
* throwing.
*/
const slimSenderSchema = z.looseObject({
	id: z.string(),
	platform: z.string().optional()
});
const slimSpaceSchema = z.looseObject({
	id: z.string(),
	platform: z.string().optional()
});
const slimContentSchema = z.looseObject({ type: z.string() });
z.looseObject({
	id: z.string(),
	platform: z.string().optional(),
	timestamp: z.string().optional(),
	sender: slimSenderSchema.optional(),
	contentPreview: z.string().optional()
});
const slimMessageSchema = z.looseObject({
	id: z.string(),
	platform: z.string().optional(),
	direction: z.string().optional(),
	timestamp: z.string().optional(),
	sender: slimSenderSchema.optional(),
	space: slimSpaceSchema,
	content: slimContentSchema
});
const slimEnvelopeSchema = z.looseObject({
	event: z.string(),
	space: slimSpaceSchema.optional(),
	message: slimMessageSchema
});
//#endregion
//#region src/webhook/verify.ts
const SIGNATURE_HEADER = "x-spectrum-signature";
const TIMESTAMP_HEADER = "x-spectrum-timestamp";
const SIGNATURE_PREFIX = "v0=";
const SIGNATURE_SCHEME = "v0";
/**
* Replay-protection window, in seconds. Spectrum signs each delivery with a
* timestamp; a delivery whose timestamp is further than this from now (past or
* future) is rejected, so a captured request cannot be replayed indefinitely.
* Matches the documented 5-minute tolerance.
*/
const REPLAY_TOLERANCE_SECONDS = 300;
const MILLIS_PER_SECOND = 1e3;
/**
* Verify a native Spectrum webhook signature.
*
* The header is `X-Spectrum-Signature: v0=<lowercase-hex>` where the hex digest
* is `HMAC-SHA256(secret, "v0:" + timestamp + ":" + rawBody)` and `timestamp`
* is the `X-Spectrum-Timestamp` header (unix seconds). The base string is built
* over the **exact body bytes**: never JSON-parse-then-restringify before
* verifying, or the bytes (key order, whitespace) change and the MAC won't
* match. The digest comparison is constant-time.
*/
function verifySpectrumSignature(input) {
	const { rawBody, headers, secret, now = Date.now() } = input;
	const provided = headers[SIGNATURE_HEADER];
	const timestamp = headers[TIMESTAMP_HEADER];
	if (!(provided && timestamp)) return {
		ok: false,
		reason: "missing-headers"
	};
	const timestampSeconds = Number(timestamp);
	if (!Number.isFinite(timestampSeconds)) return {
		ok: false,
		reason: "missing-headers"
	};
	const nowSeconds = Math.floor(now / MILLIS_PER_SECOND);
	if (Math.abs(nowSeconds - timestampSeconds) > REPLAY_TOLERANCE_SECONDS) return {
		ok: false,
		reason: "expired"
	};
	const base = Buffer.concat([Buffer.from(`${SIGNATURE_SCHEME}:${timestamp}:`, "utf8"), Buffer.from(rawBody)]);
	const expected = createHmac("sha256", secret).update(base).digest();
	const providedHex = provided.startsWith(SIGNATURE_PREFIX) ? provided.slice(3) : provided;
	const providedBytes = Buffer.from(providedHex, "hex");
	if (providedBytes.length !== expected.length || !timingSafeEqual(providedBytes, expected)) return {
		ok: false,
		reason: "signature-mismatch"
	};
	return { ok: true };
}
//#endregion
//#region src/spectrum.ts
const PHOTON_OTEL_ENDPOINT = "https://otlp.photon.codes";
const STREAM_CLOSE_TIMEOUT_MS = 5e3;
const lifecycleLog = createLogger("spectrum.lifecycle");
const ignoreCleanupError = () => void 0;
const spectrumOptionsSchema = z.object({
	flattenGroups: z.boolean().optional(),
	logLevel: z.enum([
		"debug",
		"info",
		"warn",
		"error",
		"silent"
	]).optional()
}).optional();
const spectrumConfigSchema = z.union([z.object({
	projectId: z.string().min(1),
	projectSecret: z.string().min(1),
	providers: z.array(z.custom()),
	options: spectrumOptionsSchema,
	telemetry: z.boolean().optional(),
	webhookSecret: z.string().min(1).optional()
}), z.object({
	projectId: z.undefined().optional(),
	projectSecret: z.undefined().optional(),
	providers: z.array(z.custom()),
	options: spectrumOptionsSchema,
	telemetry: z.boolean().optional(),
	webhookSecret: z.string().min(1).optional()
})]);
function bootstrapTelemetry(opts) {
	const headers = {};
	if (opts.projectId && opts.projectSecret) {
		const credential = `${opts.projectId}:${opts.projectSecret}`;
		headers.Authorization = `Basic ${btoa(credential)}`;
	}
	const resourceAttributes = { "deployment.environment": process.env.DEPLOYMENT_ENV ?? "production" };
	if (opts.projectId) resourceAttributes["spectrum.project_id"] = opts.projectId;
	return setupOtel({
		serviceName: "spectrum-ts",
		serviceVersion: SPECTRUM_SDK_VERSION,
		endpoint: PHOTON_OTEL_ENDPOINT,
		headers,
		resourceAttributes,
		instrumentFetch: false,
		register: false
	});
}
function applyLogLevel(level) {
	if (level) setLogLevel(level);
}
function envValue(key) {
	const value = process.env[key];
	return value === "" ? void 0 : value;
}
function resolveProjectCredentials(options) {
	return {
		projectId: options.projectId ?? envValue(envFor("PROJECT", "ID")),
		projectSecret: options.projectSecret ?? envValue(envFor("PROJECT", "SECRET"))
	};
}
async function Spectrum(options) {
	const { projectId, projectSecret } = resolveProjectCredentials(options);
	spectrumConfigSchema.parse({
		...options,
		projectId,
		projectSecret
	});
	assertUniquePlatformNames(options.providers);
	const { options: runtimeOptions, providers, telemetry, webhookSecret } = options;
	const flattenGroups = runtimeOptions?.flattenGroups ?? false;
	applyLogLevel(runtimeOptions?.logLevel);
	const resolvedWebhookSecret = webhookSecret ?? process.env[envFor("WEBHOOK", "SECRET")];
	const otelHandle = telemetry ? bootstrapTelemetry({
		projectId,
		projectSecret
	}) : void 0;
	const projectConfig = projectId !== void 0 && projectSecret !== void 0 ? await cloud.getProject(projectId, projectSecret) : void 0;
	const platformStates = /* @__PURE__ */ new Map();
	const fusorMessageSources = /* @__PURE__ */ new Map();
	const messageBroadcasters = /* @__PURE__ */ new Map();
	const fusorEventSources = /* @__PURE__ */ new Map();
	const eventBroadcasters = /* @__PURE__ */ new Map();
	const customEventStreams = /* @__PURE__ */ new Map();
	let stopped = false;
	const adaptIterable = (iterable, project) => stream((emit, end) => {
		const iterator = iterable[Symbol.asyncIterator]();
		const pump = (async () => {
			try {
				let result = await iterator.next();
				while (!result.done) {
					if (project) await project(result.value, emit);
					else await emit(result.value);
					result = await iterator.next();
				}
				end();
			} catch (error) {
				end(error);
			}
		})();
		return async () => {
			await iterator.return?.();
			await pump.catch(ignoreCleanupError);
		};
	});
	const resolveRecordToMessages = async (record, rt) => {
		const { client, config, definition, store } = rt;
		const { space, normalizedMessage } = await withSpan("spectrum.message.receive", {
			"spectrum.provider": definition.name,
			"spectrum.message.id": record.id,
			"spectrum.space.id": record.space?.id,
			...contentAttrs(record.content),
			...senderAttrs(record.sender)
		}, () => {
			const spaceRef = {
				...record.space,
				__platform: definition.name
			};
			const space = buildSpace({
				spaceRef,
				extras: {},
				actionCtx: {
					space: spaceRef,
					client,
					config,
					store
				},
				definition,
				client,
				config,
				store
			});
			return {
				space,
				normalizedMessage: wrapProviderMessage(record, {
					client,
					config,
					definition,
					space,
					spaceRef,
					store
				}, "inbound")
			};
		});
		if (flattenGroups && normalizedMessage.content.type === "group") return normalizedMessage.content.items.map((item) => [space, item]);
		return [[space, normalizedMessage]];
	};
	const createProviderMessagesStream = (state) => {
		const { client, config, definition, store } = state;
		const fusorSource = fusorMessageSources.get(definition.name);
		return adaptIterable(fusorSource ? fusorSource.iterable : definition.messages({
			client,
			config,
			projectConfig,
			store
		}), async (record, emit) => {
			const tuples = await resolveRecordToMessages(record, {
				client,
				config,
				definition,
				store
			});
			for (const tuple of tuples) await emit(tuple);
		});
	};
	const getOrCreateMessageBroadcast = (state) => {
		if (stopped) throw new Error(`Spectrum instance has been stopped; cannot subscribe to "${state.definition.name}" messages`);
		const name = state.definition.name;
		let broadcaster = messageBroadcasters.get(name);
		if (!broadcaster) {
			broadcaster = broadcast(createProviderMessagesStream(state));
			messageBroadcasters.set(name, broadcaster);
		}
		return broadcaster;
	};
	const getOrCreateEventBroadcast = (platform, channel) => {
		const queue = fusorEventSources.get(platform)?.get(channel);
		if (!queue) return;
		if (stopped) throw new Error(`Spectrum instance has been stopped; cannot subscribe to "${platform}" event "${channel}"`);
		const key = `${platform} ${channel}`;
		let broadcaster = eventBroadcasters.get(key);
		if (!broadcaster) {
			broadcaster = broadcast(adaptIterable(queue.iterable));
			eventBroadcasters.set(key, broadcaster);
		}
		return broadcaster;
	};
	await withSpan("spectrum.init", {
		"spectrum.provider_count": providers.length,
		"spectrum.flatten_groups": flattenGroups
	}, async () => {
		for (const provider of providers) {
			const providerConfig = provider;
			const def = providerConfig.__definition;
			const userConfig = def.config.parse(providerConfig.config);
			const store = createStore();
			const state = {
				client: await withSpan("spectrum.provider.create_client", { "spectrum.provider": def.name }, () => def.lifecycle.createClient({
					config: userConfig,
					projectId,
					projectSecret,
					projectConfig,
					store
				})),
				config: userConfig,
				definition: def,
				store
			};
			platformStates.set(def.name, {
				...state,
				projectConfig,
				subscribeMessages: () => getOrCreateMessageBroadcast(state).subscribe(),
				subscribeEvent: (channel) => getOrCreateEventBroadcast(def.name, channel)?.subscribe()
			});
		}
	});
	let fusorCore;
	let fusorStartPromise;
	const fusorPlatforms = [];
	for (const [name, state] of platformStates) if (isFusorClient(state.client)) fusorPlatforms.push({
		name,
		client: state.client
	});
	if (fusorPlatforms.length > 0) {
		fusorCore = new FusorCore({
			projectId,
			projectSecret
		});
		for (const { name, client } of fusorPlatforms) {
			const queue = createAsyncQueue();
			fusorMessageSources.set(name, queue);
			const runtime = platformStates.get(name);
			if (!runtime) continue;
			const userMessages = runtime.definition.messages;
			const declaredEvents = runtime.definition.events ?? {};
			const eventQueues = /* @__PURE__ */ new Map();
			for (const channel of Object.keys(declaredEvents)) eventQueues.set(channel, createAsyncQueue());
			fusorEventSources.set(name, eventQueues);
			const handler = {
				verify: client.verify,
				messages: async (ctx) => userMessages({
					...ctx,
					config: runtime.config,
					store: runtime.store,
					projectConfig: runtime.projectConfig
				}),
				pushMessage: (record) => queue.push(record),
				pushEvent: (channel, data) => {
					const eventQueue = eventQueues.get(channel);
					if (!eventQueue) {
						lifecycleLog.warn(`spectrum: fusorEvent("${channel}", …) names a channel not declared in "${name}".events; dropping`, {
							"spectrum.lifecycle.platform": name,
							"spectrum.lifecycle.channel": channel
						});
						return;
					}
					eventQueue.push(data);
				}
			};
			fusorCore.register(client.platform, handler);
		}
	}
	const ensureFusorStarted = () => {
		if (!fusorCore) return Promise.resolve();
		if (!fusorStartPromise) fusorStartPromise = fusorCore.start();
		return fusorStartPromise;
	};
	const providerNames = providers.map((p) => p.__definition.name).join(",");
	lifecycleLog.info("Spectrum started", {
		"spectrum.lifecycle.provider_count": providers.length,
		"spectrum.lifecycle.providers": providerNames,
		"spectrum.lifecycle.telemetry": telemetry === true
	});
	if (projectConfig && projectId !== void 0) {
		const registered = new Set(Array.from(platformStates.keys(), normalizePlatformKey));
		cloud.getPlatforms(projectId).then((platforms) => {
			for (const [platform, status] of Object.entries(platforms)) {
				if (!status.enabled || registered.has(normalizePlatformKey(platform))) continue;
				const hint = officialProviderInstallHint(platform);
				lifecycleLog.warn(hint ? `spectrum: project has "${platform}" enabled but no matching provider is registered — ${hint}` : `spectrum: project has "${platform}" enabled but no matching provider is registered`, { "spectrum.lifecycle.platform": platform });
			}
		}).catch(() => {});
	}
	const createMessagesStream = () => stream((emit, end) => {
		ensureFusorStarted().catch((error) => end(error));
		const merged = mergeStreams(Array.from(platformStates.values(), (runtime) => runtime.subscribeMessages()));
		const pump = (async () => {
			try {
				for await (const value of merged) await emit(value);
				end();
			} catch (error) {
				end(error);
			}
		})();
		return async () => {
			await merged.close();
			await pump.catch(ignoreCleanupError);
		};
	});
	const createCustomEventStream = (eventName) => stream((emit, end) => {
		const providerStreams = [];
		for (const state of platformStates.values()) {
			const { client, config, definition, store } = state;
			let source = state.subscribeEvent?.(eventName);
			if (!source) {
				const producer = definition.events?.[eventName];
				if (typeof producer !== "function") continue;
				source = producer({
					client,
					config,
					projectConfig,
					store
				});
			}
			const providerEvents = source;
			providerStreams.push(adaptIterable(providerEvents, async (value, emit) => {
				await emit(await withSpan("spectrum.event", {
					"spectrum.provider": definition.name,
					"spectrum.event.name": eventName
				}, () => typeof value === "object" && value !== null ? {
					...value,
					platform: definition.name
				} : {
					platform: definition.name,
					payload: value
				}));
			}));
		}
		const merged = mergeStreams(providerStreams);
		const pump = (async () => {
			try {
				for await (const value of merged) await emit(value);
				end();
			} catch (error) {
				end(error);
			}
		})();
		return async () => {
			await merged.close();
			await pump.catch(ignoreCleanupError);
		};
	});
	const messagesStream = createMessagesStream();
	const closeFusorSources = () => {
		for (const queue of fusorMessageSources.values()) queue.close();
		fusorMessageSources.clear();
		for (const queues of fusorEventSources.values()) for (const queue of queues.values()) queue.close();
		fusorEventSources.clear();
	};
	const stopOnce = async () => {
		if (stopped) return;
		stopped = true;
		const streamShutdowns = [
			messagesStream.close(),
			...Array.from(customEventStreams.values(), (eventStream) => eventStream.close()),
			...Array.from(messageBroadcasters.values(), (broadcaster) => broadcaster.close()),
			...Array.from(eventBroadcasters.values(), (broadcaster) => broadcaster.close())
		];
		process.off("SIGINT", handleSignal);
		process.off("SIGTERM", handleSignal);
		const streamCloseStart = performance.now();
		const streamSettled = Promise.allSettled(streamShutdowns);
		let streamTimedOut = false;
		await Promise.race([streamSettled, new Promise((resolve) => {
			setTimeout(() => {
				streamTimedOut = true;
				resolve();
			}, STREAM_CLOSE_TIMEOUT_MS).unref();
		})]);
		if (streamTimedOut) lifecycleLog.warn("stream close timed out; proceeding to teardown", { "spectrum.lifecycle.stream_close_timeout_ms": STREAM_CLOSE_TIMEOUT_MS });
		let fusorCloseMs = 0;
		if (fusorCore) {
			const fusorCloseStart = performance.now();
			if (fusorStartPromise) await fusorStartPromise.catch(ignoreCleanupError);
			await fusorCore.close().catch((error) => {
				lifecycleLog.warn("fusor core close failed", errorAttrs(error), error);
			});
			fusorCloseMs = Math.round(performance.now() - fusorCloseStart);
			closeFusorSources();
		}
		const clientShutdowns = [];
		for (const state of platformStates.values()) {
			const destroy = state.definition.lifecycle.destroyClient;
			if (!destroy) continue;
			clientShutdowns.push(withSpan("spectrum.provider.destroy_client", { "spectrum.provider": state.definition.name }, () => destroy({
				client: state.client,
				store: state.store
			})));
		}
		const clientCloseStart = performance.now();
		await Promise.allSettled(clientShutdowns);
		const clientCloseMs = Math.round(performance.now() - clientCloseStart);
		await streamSettled.catch(() => void 0);
		const streamCloseMs = Math.round(performance.now() - streamCloseStart);
		customEventStreams.clear();
		messageBroadcasters.clear();
		eventBroadcasters.clear();
		platformStates.clear();
		lifecycleLog.info("Spectrum stopped", {
			"spectrum.lifecycle.providers": providerNames,
			"spectrum.lifecycle.stream_close_ms": streamCloseMs,
			"spectrum.lifecycle.fusor_close_ms": fusorCloseMs,
			"spectrum.lifecycle.client_close_ms": clientCloseMs
		});
		if (otelHandle) await otelHandle.shutdown();
	};
	const handleSignal = () => {
		setTimeout(() => process.exit(1), 3e3).unref();
		stopOnce().then(() => process.exit(0)).catch(() => process.exit(1));
	};
	process.on("SIGINT", handleSignal);
	process.on("SIGTERM", handleSignal);
	const messages = messagesStream;
	const customEventProxy = new Proxy({}, { get(_target, prop) {
		let eventStream = customEventStreams.get(prop);
		if (!eventStream) {
			eventStream = createCustomEventStream(prop);
			customEventStreams.set(prop, eventStream);
		}
		return eventStream;
	} });
	const encodeText = (s) => new TextEncoder().encode(s);
	const buildWebhookResult = (asWeb, result) => {
		if (asWeb) return new Response(result.body, {
			status: result.status,
			headers: result.headers
		});
		return result;
	};
	const readWebhookInput = async (request) => {
		if (typeof Request !== "undefined" && request instanceof Request) {
			const headers = {};
			for (const [key, value] of request.headers) headers[key.toLowerCase()] = value;
			return {
				asWeb: true,
				bodyBytes: new Uint8Array(await request.arrayBuffer()),
				headers
			};
		}
		const raw = request;
		const bodyBytes = raw.body instanceof ArrayBuffer ? new Uint8Array(raw.body) : raw.body;
		const headers = {};
		for (const [key, value] of Object.entries(raw.headers ?? {})) headers[key.toLowerCase()] = String(value);
		return {
			asWeb: false,
			bodyBytes,
			headers
		};
	};
	const deliverWebhookMessages = async (collected, runtime, handler, context) => {
		for (const record of collected) {
			const tuples = await resolveRecordToMessages(record, runtime);
			for (const [space, message] of tuples) try {
				await handler(space, message);
			} catch (error) {
				lifecycleLog.error("spectrum.webhook: handler threw (async)", {
					"spectrum.webhook.event_id": context.eventId,
					"spectrum.webhook.platform": context.platform,
					"spectrum.webhook.message_id": message.id,
					...errorAttrs(error)
				}, error);
			}
		}
	};
	const decodeWebhookEvent = (bodyBytes) => {
		try {
			return RawInboundEvent.decode(bodyBytes);
		} catch (error) {
			lifecycleLog.warn("spectrum.webhook: undecodable RawInboundEvent body", errorAttrs(error), error);
			return null;
		}
	};
	const processWebhookEvent = async (core, event, handler) => {
		const collected = [];
		const reply = await core.processEvent(event, (record) => {
			collected.push(record);
		});
		if (reply.errorReason) return {
			status: 400,
			headers: reply.headers ?? {},
			body: encodeText(reply.errorReason)
		};
		const result = {
			status: reply.status === 0 ? 200 : reply.status,
			headers: reply.headers ?? {},
			body: reply.body ?? new Uint8Array(0)
		};
		const runtime = platformStates.get(event.platform);
		if (runtime && collected.length > 0) deliverWebhookMessages(collected, runtime, handler, event).catch((error) => {
			lifecycleLog.error("spectrum.webhook: delivery failed (async)", {
				"spectrum.webhook.event_id": event.eventId,
				"spectrum.webhook.platform": event.platform,
				...errorAttrs(error)
			}, error);
		});
		return result;
	};
	const looksLikeNativePayload = (bodyBytes) => {
		for (const byte of bodyBytes) {
			if (byte === 32 || byte === 9 || byte === 10 || byte === 13) continue;
			return byte === 123;
		}
		return false;
	};
	const webhookText = (status, text) => ({
		status,
		headers: {},
		body: encodeText(text)
	});
	const resolveWebhookAttachment = (platform, spaceRef, attachmentId) => {
		const runtime = platformStates.get(platform);
		const action = (runtime?.definition)?.actions?.getAttachment;
		if (!runtime || typeof action !== "function") return;
		const getAttachment = action;
		const phone = typeof spaceRef.phone === "string" ? spaceRef.phone : void 0;
		let cached;
		const fetchOnce = () => {
			cached ??= getAttachment({
				client: runtime.client,
				config: runtime.config,
				store: runtime.store
			}, attachmentId, phone);
			return cached;
		};
		return {
			read: async () => {
				const found = await fetchOnce();
				if (!found) throw new Error(`Spectrum webhook attachment "${attachmentId}" not found on "${platform}"`);
				return found.read();
			},
			stream: async () => {
				const found = await fetchOnce();
				if (!found?.stream) throw new Error(`Spectrum webhook attachment "${attachmentId}" has no stream on "${platform}"`);
				return found.stream();
			}
		};
	};
	const handleSpectrumWebhook = async (bodyBytes, headers, handler) => {
		if (!resolvedWebhookSecret) {
			lifecycleLog.error("spectrum.webhook: received a signed Spectrum webhook but no webhookSecret is configured (set Spectrum({ webhookSecret }) or SPECTRUM_WEBHOOK_SECRET)", { "spectrum.webhook.reason": "missing-secret" });
			return webhookText(500, "webhook secret not configured");
		}
		const verification = verifySpectrumSignature({
			rawBody: bodyBytes,
			headers,
			secret: resolvedWebhookSecret
		});
		if (!verification.ok) return webhookText(verification.reason === "missing-headers" ? 400 : 401, verification.reason);
		let envelope;
		try {
			const parsed = JSON.parse(new TextDecoder().decode(bodyBytes));
			envelope = slimEnvelopeSchema.parse(parsed);
		} catch (error) {
			lifecycleLog.warn("spectrum.webhook: malformed Spectrum webhook payload", errorAttrs(error), error);
			return webhookText(400, "malformed payload");
		}
		const deserialized = deserializeSpectrumMessage(envelope, { resolveAttachment: resolveWebhookAttachment });
		if (!deserialized) return webhookText(200, "ok");
		const { platform, record } = deserialized;
		const runtime = platformStates.get(platform);
		if (!runtime) {
			lifecycleLog.warn(`spectrum.webhook: no provider configured for platform "${platform}"; acknowledging without delivery`, { "spectrum.webhook.platform": platform });
			return webhookText(200, "ok");
		}
		deliverWebhookMessages([record], runtime, handler, { platform }).catch((error) => {
			lifecycleLog.error("spectrum.webhook: Spectrum delivery failed (async)", {
				"spectrum.webhook.platform": platform,
				"spectrum.webhook.message_id": record.id,
				...errorAttrs(error)
			}, error);
		});
		return webhookText(200, "ok");
	};
	const handleWebhook = async (request, handler) => {
		const { asWeb, bodyBytes, headers } = await readWebhookInput(request);
		if (looksLikeNativePayload(bodyBytes)) return buildWebhookResult(asWeb, await handleSpectrumWebhook(bodyBytes, headers, handler));
		if (!fusorCore) throw new Error("spectrum.webhook() received a non-Spectrum (fusor) request but no fusor provider is configured");
		const event = decodeWebhookEvent(bodyBytes);
		if (!event) return buildWebhookResult(asWeb, {
			status: 400,
			headers: {},
			body: new Uint8Array(0)
		});
		return buildWebhookResult(asWeb, await processWebhookEvent(fusorCore, event, handler));
	};
	return new Proxy({
		__providers: providers,
		__internal: { platforms: platformStates },
		config: projectConfig,
		messages,
		stop: stopOnce,
		webhook: handleWebhook,
		send: (async (space, ...content) => content.length === 1 ? await space.send(content[0]) : await space.send(...content)),
		edit: async (message, newContent) => {
			await message.edit(newContent);
		},
		responding: async (space, fn) => space.responding(fn)
	}, { get(target, prop, receiver) {
		if (prop in target) return Reflect.get(target, prop, receiver);
		if (typeof prop === "string") return customEventProxy[prop];
	} });
}
//#endregion
export { Emoji, Spectrum, SpectrumCloudError, UnsupportedError, addMember, app, appLayoutSchema, attachment, avatar, broadcast, cloud, contact, custom, definePlatform, edit, fromVCard, fusor, fusorEvent, group, isFusorClient, isFusorEvent, leaveSpace, markdown, mergeStreams, option, poll, reaction, read, removeMember, rename, reply, resolveContents, richlink, stream, text, toVCard, typing, unsend, voice };
