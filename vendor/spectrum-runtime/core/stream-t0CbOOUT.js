import z from "zod";
import { createInstrumentedFetch, createLogger, sanitizeEmail, sanitizeErrorMessage, sanitizePhone, sanitizeUrl } from "@photon-ai/otel";
import { randomUUID } from "node:crypto";
import { createReadStream } from "node:fs";
import { readFile, stat } from "node:fs/promises";
import { basename } from "node:path";
import { Readable } from "node:stream";
import { lookup } from "mime-types";
import vCard from "vcf";
import { Marked } from "marked";
import { Repeater } from "@repeaterjs/repeater";
//#region src/utils/instrumented-fetch.ts
/**
* Build a fetch that traces spectrum's OWN outbound HTTP: each request through
* the returned fetch emits a CLIENT span (tagged with `peer.service`) and
* carries W3C trace context downstream. It never mutates `globalThis.fetch`, so
* a consumer's unrelated requests stay untouched.
*
* The base delegates to `globalThis.fetch` at call time — deliberately NOT
* `createInstrumentedFetch(globalThis.fetch, …)`, which would capture the
* original and bypass a test's `spyOn(globalThis, "fetch")`. When telemetry is
* off (no `setupOtel`), the tracer is a no-op and this is a transparent
* pass-through. Create one per module: `const tf = tracedFetch("…")`.
*
* Pass `redactUrl` to strip secrets from a request URL before it is recorded as
* the span's `url.full` (e.g. a token in the path or query); the real request
* still uses the original URL. Pair with `sanitizeUrl` for semconv-style query
* redaction.
*/
const tracedFetch = (peerService, options) => createInstrumentedFetch(((input, init) => globalThis.fetch(input, init)), {
	attributes: { "peer.service": peerService },
	...options
});
//#endregion
//#region src/utils/io.ts
const readSchema$1 = z.function({
	input: [],
	output: z.promise(z.instanceof(Buffer))
});
const streamSchema = z.function({
	input: [],
	output: z.promise(z.instanceof(ReadableStream))
});
const bufferToStream = (buf) => new ReadableStream({ start(controller) {
	controller.enqueue(buf);
	controller.close();
} });
const DEFAULT_FETCH_TIMEOUT_MS = 1e4;
const mediaFetch = tracedFetch("media", { redactUrl: sanitizeUrl });
/**
* Fetch URL bytes into memory — never touches the filesystem, so callers
* remain safe in read-only environments. Returns the response's Content-Type
* alongside the bytes so callers that want a soft MIME fallback can use it.
*/
const fetchUrlBytes = async (url, options) => {
	const controller = new AbortController();
	const timer = setTimeout(() => controller.abort(), options?.timeoutMs ?? DEFAULT_FETCH_TIMEOUT_MS);
	try {
		const res = await mediaFetch(url, {
			signal: controller.signal,
			headers: options?.headers
		});
		if (!res.ok) throw new Error(`URL fetch ${url.toString()} returned ${res.status}`);
		return {
			data: Buffer.from(await res.arrayBuffer()),
			mimeType: res.headers.get("content-type") ?? void 0
		};
	} finally {
		clearTimeout(timer);
	}
};
//#endregion
//#region src/content/attachment.ts
const DEFAULT_ATTACHMENT_NAME = "attachment";
const attachmentSchema = z.object({
	type: z.literal("attachment"),
	id: z.string().nonempty(),
	name: z.string().nonempty(),
	mimeType: z.string().nonempty(),
	size: z.number().int().nonnegative().optional(),
	read: readSchema$1,
	stream: streamSchema
});
const resolveAttachmentName = (input, name) => {
	if (name) return name;
	if (input instanceof URL) return basename(input.pathname) || DEFAULT_ATTACHMENT_NAME;
	if (typeof input === "string") return basename(input);
	return DEFAULT_ATTACHMENT_NAME;
};
const resolveAttachmentMimeType = (name, mimeType) => {
	if (mimeType) return mimeType;
	const resolvedMimeType = lookup(name);
	if (!resolvedMimeType) throw new Error(`Unable to resolve MIME type for attachment "${name}". Pass options.mimeType explicitly.`);
	return resolvedMimeType;
};
const asAttachment = (input) => {
	let cached;
	const read = () => {
		cached ??= input.read().catch((err) => {
			cached = void 0;
			throw err;
		});
		return cached;
	};
	const stream = input.stream ?? (async () => bufferToStream(await read()));
	return attachmentSchema.parse({
		type: "attachment",
		id: input.id ?? randomUUID(),
		name: input.name,
		mimeType: input.mimeType,
		size: input.size,
		read,
		stream
	});
};
function attachment(input, options) {
	return { build: async () => {
		const id = options?.id;
		const name = resolveAttachmentName(input, options?.name);
		const mimeType = resolveAttachmentMimeType(name, options?.mimeType);
		if (input instanceof URL) return asAttachment({
			id,
			name,
			mimeType,
			read: async () => (await fetchUrlBytes(input)).data
		});
		if (typeof input === "string") return asAttachment({
			id,
			name,
			mimeType,
			size: (await stat(input)).size,
			read: () => readFile(input),
			stream: async () => Readable.toWeb(createReadStream(input))
		});
		return asAttachment({
			id,
			name,
			mimeType,
			size: input.byteLength,
			read: async () => input,
			stream: async () => bufferToStream(input)
		});
	} };
}
const photoActionSchema = z.discriminatedUnion("kind", [z.object({
	kind: z.literal("set"),
	read: readSchema$1,
	mimeType: z.string().nonempty()
}), z.object({ kind: z.literal("clear") })]);
const resolveMimeType = (input, mimeType, contentLabel) => {
	if (mimeType) return mimeType;
	if (input instanceof URL) {
		const resolved = lookup(basename(input.pathname));
		if (resolved) return resolved;
	} else if (typeof input === "string") {
		const resolved = lookup(basename(input));
		if (resolved) return resolved;
	}
	throw new Error(`Unable to resolve MIME type for ${contentLabel}. Pass options.mimeType explicitly.`);
};
const cachedRead = (read) => {
	let cached;
	return () => {
		cached ??= read().catch((err) => {
			cached = void 0;
			throw err;
		});
		return cached;
	};
};
/**
* Convert a photo-content input into the discriminated `PhotoAction` shape.
*
* - `"clear"` → `{ kind: "clear" }` (reserved sentinel — to send a literal
*   file named `clear`, pass `"./clear"` or load it as a `Buffer`).
* - `string` path → reads the file lazily via `node:fs/promises.readFile`;
*   MIME type inferred from the filename extension.
* - `URL` → fetched lazily over the network into memory; never touches the
*   filesystem, so it works in read-only environments. MIME type is inferred
*   from the URL pathname extension at build time (override with
*   `options.mimeType` if the URL has no usable extension).
* - `Buffer` → in-memory bytes; `options.mimeType` is required.
*
* Called at builder-construction time so a missing MIME type fails fast
* rather than at send time. The returned `read()` is memoized so repeated
* `build()` / send cycles don't re-read the same file.
*/
const buildPhotoAction = (input, options, contentLabel) => {
	if (input === "clear") return { kind: "clear" };
	const mimeType = resolveMimeType(input, options?.mimeType, contentLabel);
	let read;
	if (input instanceof URL) read = cachedRead(async () => (await fetchUrlBytes(input)).data);
	else if (typeof input === "string") read = cachedRead(() => readFile(input));
	else {
		const snapshot = Buffer.from(input);
		read = cachedRead(async () => snapshot);
	}
	return {
		kind: "set",
		read,
		mimeType
	};
};
//#endregion
//#region src/content/avatar.ts
/**
* Set or clear the chat avatar (group icon). Universal content — providers
* dispatch by `content.type === "avatar"` in their `send` action and decide
* their own support story (e.g. iMessage only supports it for remote group
* chats).
*
* `space.send(avatar(...))` is the canonical form; `space.avatar(...)` is
* universal sugar that delegates here. Per-platform constraints (e.g.
* group-only, remote-only) surface as `UnsupportedError` from the provider's
* `send` action so the canonical and sugar forms share one error path.
*
* Bidirectional: providers also surface platform icon changes as inbound
* `Message`s carrying this content — `action.kind === "set"` arrives with
* the fetched icon behind `read()`, `"clear"` mirrors icon removal.
* `message.sender` is the user who changed it (may be `undefined` when the
* platform recorded no actor).
*/
const avatarSchema = z.object({
	type: z.literal("avatar"),
	action: photoActionSchema
});
function avatar(input, options) {
	const action = buildPhotoAction(input, options, "avatar");
	return { build: async () => avatarSchema.parse({
		type: "avatar",
		action
	}) };
}
//#endregion
//#region src/utils/vcard.ts
const asPropertyArray = (prop) => {
	if (!prop) return [];
	return Array.isArray(prop) ? prop : [prop];
};
const propString = (prop) => {
	const [first] = asPropertyArray(prop);
	const value = first?.valueOf().trim();
	return value ? value : void 0;
};
const paramTypes = (prop) => {
	const { type } = prop;
	if (!type) return [];
	return (Array.isArray(type) ? type : [type]).map((t) => t.toLowerCase());
};
const mapPhoneType = (prop) => {
	const types = paramTypes(prop);
	if (types.some((t) => t === "cell" || t === "mobile" || t === "iphone")) return "mobile";
	if (types.includes("home")) return "home";
	if (types.includes("work")) return "work";
	if (types.length > 0) return "other";
};
const mapSimpleType = (prop) => {
	const types = paramTypes(prop);
	if (types.includes("home")) return "home";
	if (types.includes("work")) return "work";
	if (types.length > 0) return "other";
};
const splitStructured = (value) => value.split(";").map((part) => part.trim());
const extractName = (card) => {
	const fn = propString(card.data.fn);
	const n = propString(card.data.n);
	if (!(fn || n)) return;
	const result = {};
	if (fn) result.formatted = fn;
	if (n) {
		const [last, first, middle, prefix, suffix] = splitStructured(n);
		if (first) result.first = first;
		if (last) result.last = last;
		if (middle) result.middle = middle;
		if (prefix) result.prefix = prefix;
		if (suffix) result.suffix = suffix;
	}
	return result;
};
const extractPhones = (card) => {
	const props = asPropertyArray(card.data.tel);
	if (props.length === 0) return;
	return props.map((p) => {
		const entry = { value: p.valueOf().trim() };
		const type = mapPhoneType(p);
		if (type) entry.type = type;
		return entry;
	});
};
const extractEmails = (card) => {
	const props = asPropertyArray(card.data.email);
	if (props.length === 0) return;
	return props.map((p) => {
		const entry = { value: p.valueOf().trim() };
		const type = mapSimpleType(p);
		if (type) entry.type = type;
		return entry;
	});
};
const extractAddresses = (card) => {
	const props = asPropertyArray(card.data.adr);
	if (props.length === 0) return;
	return props.map((p) => {
		const [, , street, city, region, postalCode, country] = splitStructured(p.valueOf());
		const entry = {};
		if (street) entry.street = street;
		if (city) entry.city = city;
		if (region) entry.region = region;
		if (postalCode) entry.postalCode = postalCode;
		if (country) entry.country = country;
		const type = mapSimpleType(p);
		if (type) entry.type = type;
		return entry;
	});
};
const extractOrg = (card) => {
	const orgStr = propString(card.data.org);
	const title = propString(card.data.title);
	if (!(orgStr || title)) return;
	const result = {};
	if (orgStr) {
		const [name, department] = splitStructured(orgStr);
		if (name) result.name = name;
		if (department) result.department = department;
	}
	if (title) result.title = title;
	return result;
};
const extractUrls = (card) => {
	const props = asPropertyArray(card.data.url);
	if (props.length === 0) return;
	return props.map((p) => p.valueOf().trim());
};
const photoMimeFromType = (type) => {
	if (!type) return "image/jpeg";
	const lower = type.toLowerCase();
	if (lower.startsWith("image/")) return lower;
	return `image/${lower}`;
};
const DATA_URI_PATTERN = /^data:([^;,]+);base64,(.*)$/i;
const extractPhoto = (card) => {
	const [prop] = asPropertyArray(card.data.photo);
	if (!prop) return;
	const value = prop.valueOf();
	const dataUriMatch = DATA_URI_PATTERN.exec(value);
	if (dataUriMatch) {
		const [, mimeType, base64] = dataUriMatch;
		const buf = Buffer.from(base64 ?? "", "base64");
		return {
			mimeType: mimeType ?? "image/jpeg",
			read: async () => buf
		};
	}
	const type = Array.isArray(prop.type) ? prop.type[0] : prop.type;
	const buf = Buffer.from(value, "base64");
	return {
		mimeType: photoMimeFromType(type),
		read: async () => buf
	};
};
const normalizeVCardInput = (vcf) => {
	return (vcf.charCodeAt(0) === 65279 ? vcf.slice(1) : vcf).replace(/\r\n|\r|\n/g, "\r\n");
};
const fromVCard = (vcf) => {
	const [card] = vCard.parse(normalizeVCardInput(vcf));
	if (!card) throw new Error("Invalid vCard: no cards parsed");
	const input = { raw: vcf };
	const name = extractName(card);
	if (name) input.name = name;
	const phones = extractPhones(card);
	if (phones) input.phones = phones;
	const emails = extractEmails(card);
	if (emails) input.emails = emails;
	const addresses = extractAddresses(card);
	if (addresses) input.addresses = addresses;
	const org = extractOrg(card);
	if (org) input.org = org;
	const urls = extractUrls(card);
	if (urls) input.urls = urls;
	const birthday = propString(card.data.bday);
	if (birthday) input.birthday = birthday;
	const note = propString(card.data.note);
	if (note) input.note = note;
	const photo = extractPhoto(card);
	if (photo) input.photo = photo;
	return input;
};
const formattedNameFor = (name) => {
	if (name?.formatted) return name.formatted;
	const parts = [
		name?.first,
		name?.middle,
		name?.last
	].filter((p) => Boolean(p));
	if (parts.length > 0) return parts.join(" ");
	return "Unknown";
};
const phoneTypeParam = (type) => {
	if (type === "mobile") return "CELL";
	if (type === "home" || type === "work" || type === "other") return type.toUpperCase();
};
const simpleTypeParam = (type) => type ? type.toUpperCase() : void 0;
const photoTypeParam = (mimeType) => {
	return (mimeType.split("/")[1] ?? "jpeg").toUpperCase();
};
const writeName = (card, name) => {
	card.set("fn", formattedNameFor(name));
	if (!name) return;
	if (name.first || name.last || name.middle || name.prefix || name.suffix) card.set("n", [
		name.last ?? "",
		name.first ?? "",
		name.middle ?? "",
		name.prefix ?? "",
		name.suffix ?? ""
	].join(";"));
};
const writePhones = (card, phones) => {
	for (const phone of phones ?? []) {
		const type = phoneTypeParam(phone.type);
		card.add("tel", phone.value, type ? { type } : void 0);
	}
};
const writeEmails = (card, emails) => {
	for (const email of emails ?? []) {
		const type = simpleTypeParam(email.type);
		card.add("email", email.value, type ? { type } : void 0);
	}
};
const writeAddresses = (card, addresses) => {
	for (const addr of addresses ?? []) {
		const value = [
			"",
			"",
			addr.street ?? "",
			addr.city ?? "",
			addr.region ?? "",
			addr.postalCode ?? "",
			addr.country ?? ""
		].join(";");
		const type = simpleTypeParam(addr.type);
		card.add("adr", value, type ? { type } : void 0);
	}
};
const writeOrg = (card, org) => {
	if (!org) return;
	if (org.name || org.department) card.set("org", [org.name ?? "", org.department ?? ""].join(";"));
	if (org.title) card.set("title", org.title);
};
const writeUrls = (card, urls) => {
	for (const url of urls ?? []) card.add("url", url);
};
const writePhoto = async (card, photo) => {
	if (!photo) return;
	const buf = await photo.read();
	card.set("photo", buf.toString("base64"), {
		encoding: "b",
		type: photoTypeParam(photo.mimeType)
	});
};
const toVCard = async (contact) => {
	if (typeof contact.raw === "string" && contact.raw.startsWith("BEGIN:VCARD")) return contact.raw;
	const card = new vCard();
	writeName(card, contact.name);
	writePhones(card, contact.phones);
	writeEmails(card, contact.emails);
	writeAddresses(card, contact.addresses);
	writeOrg(card, contact.org);
	writeUrls(card, contact.urls);
	if (contact.birthday) card.set("bday", contact.birthday);
	if (contact.note) card.set("note", contact.note);
	await writePhoto(card, contact.photo);
	return card.toString();
};
//#endregion
//#region src/content/contact.ts
const userRefSchema = z.object({
	__platform: z.string(),
	id: z.string()
});
const nameSchema = z.object({
	formatted: z.string().optional(),
	first: z.string().optional(),
	last: z.string().optional(),
	middle: z.string().optional(),
	prefix: z.string().optional(),
	suffix: z.string().optional()
});
const phoneTypeSchema = z.enum([
	"mobile",
	"home",
	"work",
	"other"
]);
const emailTypeSchema = z.enum([
	"home",
	"work",
	"other"
]);
const addressTypeSchema = z.enum([
	"home",
	"work",
	"other"
]);
const phoneSchema = z.object({
	value: z.string(),
	type: phoneTypeSchema.optional()
});
const emailSchema = z.object({
	value: z.string(),
	type: emailTypeSchema.optional()
});
const addressSchema = z.object({
	street: z.string().optional(),
	city: z.string().optional(),
	region: z.string().optional(),
	postalCode: z.string().optional(),
	country: z.string().optional(),
	type: addressTypeSchema.optional()
});
const orgSchema = z.object({
	name: z.string().optional(),
	title: z.string().optional(),
	department: z.string().optional()
});
const photoSchema = z.object({
	mimeType: z.string(),
	read: readSchema$1
});
const contactSchema = z.object({
	type: z.literal("contact"),
	user: userRefSchema.optional(),
	name: nameSchema.optional(),
	phones: z.array(phoneSchema).optional(),
	emails: z.array(emailSchema).optional(),
	addresses: z.array(addressSchema).optional(),
	org: orgSchema.optional(),
	urls: z.array(z.string()).optional(),
	birthday: z.string().optional(),
	note: z.string().optional(),
	photo: photoSchema.optional(),
	raw: z.unknown().optional()
});
const asContact = (input) => contactSchema.parse({
	type: "contact",
	...input
});
const isUser = (value) => typeof value === "object" && value !== null && "__platform" in value && "id" in value && typeof value.__platform === "string" && typeof value.id === "string";
function contact(input, details) {
	return { build: async () => {
		if (typeof input === "string") return asContact(fromVCard(input));
		if (input instanceof vCard) return asContact(fromVCard(input.toString()));
		if (isUser(input)) return asContact({
			user: {
				__platform: input.__platform,
				id: input.id
			},
			...details
		});
		return asContact(input);
	} };
}
//#endregion
//#region src/content/custom.ts
const customSchema = z.object({
	type: z.literal("custom"),
	raw: z.unknown()
});
const asCustom = (raw) => customSchema.parse({
	type: "custom",
	raw
});
function custom(raw) {
	return { build: async () => asCustom(raw) };
}
//#endregion
//#region src/content/stream-text.ts
const streamTextSchema = z.object({
	type: z.literal("streamText"),
	stream: z.custom((v) => typeof v === "function", { message: "streamText.stream must be a function returning AsyncIterable<string>" }),
	format: z.enum(["plain", "markdown"]).optional()
});
const asRecord = (value) => typeof value === "object" && value !== null ? value : void 0;
const SKIP_EVENT_TYPES = new Set([
	"message_start",
	"message_delta",
	"message_stop",
	"content_block_start",
	"content_block_stop",
	"ping"
]);
const fromOpenAIResponses = (obj) => {
	const type = obj.type;
	if (typeof type !== "string" || !type.startsWith("response.")) return;
	if (type === "response.output_text.delta" && typeof obj.delta === "string") return obj.delta;
	return null;
};
const fromAnthropicDelta = (obj) => {
	if (obj.type !== "content_block_delta") return;
	const delta = asRecord(obj.delta);
	if (delta?.type === "text_delta" && typeof delta.text === "string") return delta.text;
	return null;
};
const fromAiSdkPart = (obj) => {
	if (obj.type !== "text-delta") return;
	if (typeof obj.textDelta === "string") return obj.textDelta;
	return typeof obj.text === "string" ? obj.text : null;
};
const fromOpenAIChat = (obj) => {
	if (!Array.isArray(obj.choices)) return;
	const content = asRecord(asRecord(obj.choices[0])?.delta)?.content;
	return typeof content === "string" ? content : null;
};
const fromControlEvent = (obj) => typeof obj.type === "string" && SKIP_EVENT_TYPES.has(obj.type) ? null : void 0;
const OBJECT_EXTRACTORS = [
	fromOpenAIResponses,
	fromAnthropicDelta,
	fromAiSdkPart,
	fromOpenAIChat,
	fromControlEvent
];
/**
* Auto-detect the text delta in a chunk from a popular LLM SDK. Pass a custom
* `extract` to `text()` / `markdown()` for any shape this doesn't recognize.
*/
const defaultExtract = (chunk) => {
	if (typeof chunk === "string") return chunk;
	const record = asRecord(chunk);
	if (!record) throw new Error(`text stream: cannot extract a text delta from a ${typeof chunk} chunk. Pass { extract } to map your stream's chunks to text.`);
	for (const extractor of OBJECT_EXTRACTORS) {
		const result = extractor(record);
		if (result !== void 0) return result;
	}
	throw new Error(`text stream: unrecognized chunk shape (type=${String(record.type)}). Pass an { extract } function to map your provider's chunk to a text delta.`);
};
const isReadableStream = (value) => typeof value?.getReader === "function";
const isAsyncIterable = (value) => typeof value?.[Symbol.asyncIterator] === "function";
async function* readableToAsync(source) {
	if (isAsyncIterable(source)) {
		yield* source;
		return;
	}
	const reader = source.getReader();
	try {
		while (true) {
			const { done, value } = await reader.read();
			if (done) return;
			yield value;
		}
	} finally {
		reader.releaseLock();
	}
}
const resolveChunkIterable = (source) => {
	const textStream = source.textStream;
	if (textStream != null) {
		if (isReadableStream(textStream)) return readableToAsync(textStream);
		if (isAsyncIterable(textStream)) return textStream;
		throw new Error("text stream: `.textStream` must be an AsyncIterable or a ReadableStream.");
	}
	if (isReadableStream(source)) return readableToAsync(source);
	if (isAsyncIterable(source)) return source;
	throw new Error("text stream: source must be an AsyncIterable, a ReadableStream, or an object with a `.textStream` (e.g. the AI SDK streamText() result).");
};
/**
* Thrown when a single-use stream source is consumed a second time. The send
* pipeline's plain-text fallback matches on this to tell "the provider already
* consumed the stream" apart from a stream that errored mid-drain.
*/
var StreamConsumedError = class extends Error {
	constructor() {
		super("text stream: this source has already been consumed — a stream can only be sent once.");
		this.name = "StreamConsumedError";
	}
};
const normalize$1 = (source, options) => {
	const extract = options?.extract ? options.extract : defaultExtract;
	let consumed = false;
	return async function* normalized() {
		if (consumed) throw new StreamConsumedError();
		consumed = true;
		for await (const chunk of resolveChunkIterable(source)) {
			const delta = extract(chunk);
			if (delta) yield delta;
		}
	};
};
const asStreamText = (input) => streamTextSchema.parse({
	type: "streamText",
	stream: input.stream,
	...input.format ? { format: input.format } : {}
});
/**
* Consume a `streamText` content's stream to completion and return the full
* accumulated text. Used by the send pipeline's plain-text fallback for
* platforms that can't stream. Consumes the single-use stream — the content
* cannot be sent afterwards.
*/
const drainStreamText = async (content) => {
	let full = "";
	for await (const delta of content.stream()) full += delta;
	return full;
};
/**
* Shared backing for the stream overloads of `text()` and `markdown()`: the
* constructor name picks the wire format and labels the eager guard against
* passing a content builder where a raw stream source belongs.
*/
const streamTextBuilder = (kind, source, options) => {
	if (typeof source.build === "function") throw new Error(`${kind}(): pass the stream source itself (an AsyncIterable, a ReadableStream, or an SDK result with .textStream), not another content builder.`);
	return { build: async () => asStreamText({
		stream: normalize$1(source, options),
		format: kind === "markdown" ? "markdown" : void 0
	}) };
};
//#endregion
//#region src/content/text.ts
const textSchema = z.object({
	type: z.literal("text"),
	text: z.string().nonempty()
});
const asText = (text) => textSchema.parse({
	type: "text",
	text
});
function text(source, options) {
	if (typeof source === "string") return { build: async () => asText(source) };
	return streamTextBuilder("text", source, options);
}
//#endregion
//#region src/content/resolve.ts
const resolveContents = (items) => Promise.all(items.map((c) => typeof c === "string" ? text(c).build() : c.build()));
//#endregion
//#region src/content/group.ts
const isMessage$4 = (v) => typeof v === "object" && v !== null && "id" in v && "content" in v;
/**
* A `group` bundles multiple messages into one logical unit (e.g. an album
* of images sent together). Each item is a full `Message` — addressable by
* id, reactable via `.react()`, replyable via `.reply()`.
*
* Groups do not nest, and reactions cannot be group members. Enforced by the
* `group()` builder; platforms may additionally reject unsupported item
* content types at send time.
*/
const groupSchema = z.object({
	type: z.literal("group"),
	items: z.array(z.custom(isMessage$4)).min(2)
});
const asGroup = (input) => groupSchema.parse({
	type: "group",
	items: input.items
});
const stubOutboundMessage = (content) => ({
	id: "",
	content
});
function group(...items) {
	return { build: async () => {
		const resolved = await resolveContents(items);
		const members = [];
		for (const item of resolved) {
			if (item.type === "group" || item.type === "reaction") throw new Error(`group() cannot contain "${item.type}" items`);
			members.push(stubOutboundMessage(item));
		}
		return asGroup({ items: members });
	} };
}
//#endregion
//#region src/content/markdown.ts
/**
* Styled text written in standard markdown (CommonMark plus GFM tables and
* strikethrough). Outbound-only by design: inbound messages always surface as
* `text` content — no provider maps platform formatting back to markdown.
* Each platform renders the markdown to its native format (Telegram: HTML via
* `parse_mode`; remote iMessage: styled text via UTF-16 formatting ranges);
* platforms without native support receive readable plain text via the send
* pipeline's markdown fallback.
*/
const markdownSchema = z.object({
	type: z.literal("markdown"),
	markdown: z.string().nonempty()
});
const asMarkdown = (markdown) => markdownSchema.parse({
	type: "markdown",
	markdown
});
function markdown(source, options) {
	if (typeof source === "string") return { build: async () => asMarkdown(source) };
	return streamTextBuilder("markdown", source, options);
}
//#endregion
//#region src/content/membership.ts
/**
* Group-membership management content. Universal content — providers
* dispatch by `content.type` in their `send` action and decide their own
* support story (e.g. iMessage only supports it for remote group chats;
* on a DM it surfaces an `UnsupportedError` telling the caller to create
* a group via `space.create` instead).
*
* `space.send(addMember(users))` is the canonical form; `space.add(...)`,
* `space.remove(...)`, and `space.leave()` are universal sugar that
* delegate here. All three are fire-and-forget — no `Message` is produced.
*
* Bidirectional: providers also surface platform membership events as
* inbound `Message`s carrying this content. `message.sender` is the acting
* user — who added/removed the members — and may be `undefined` when the
* platform recorded no actor. For `leaveSpace` the sender is the leaver
* (the content carries no `members`). The agent's own actions are
* suppressed: `space.add(...)` does not echo back as an inbound event.
*
* Members are id strings (or `User`s, normalized to their `id`) in the same
* platform handle format `space.create` accepts. No async resolution happens
* in the builder — ids reach the provider verbatim.
*/
const addMemberSchema = z.object({
	type: z.literal("addMember"),
	members: z.array(z.string()).min(1, "addMember() requires at least one member")
});
const removeMemberSchema = z.object({
	type: z.literal("removeMember"),
	members: z.array(z.string()).min(1, "removeMember() requires at least one member")
});
const leaveSpaceSchema = z.object({ type: z.literal("leaveSpace") });
const toMemberIds = (users) => (Array.isArray(users) ? users : [users]).map((u) => typeof u === "string" ? u : u.id);
/**
* Build an `AddMember` content value inviting `users` into the current
* group chat. Accepts a single `User` or id string, or an array of either
* — batches land in one provider call.
*/
function addMember(users) {
	return { build: async () => addMemberSchema.parse({
		type: "addMember",
		members: toMemberIds(users)
	}) };
}
/**
* Build a `RemoveMember` content value removing `users` from the current
* group chat. Accepts the same input shapes as `addMember`.
*/
function removeMember(users) {
	return { build: async () => removeMemberSchema.parse({
		type: "removeMember",
		members: toMemberIds(users)
	}) };
}
/**
* Build a `LeaveSpace` content value making the agent's own account leave
* the current group chat.
*/
function leaveSpace() {
	return { build: async () => leaveSpaceSchema.parse({ type: "leaveSpace" }) };
}
//#endregion
//#region src/content/poll.ts
const pollChoiceSchema = z.object({ title: z.string().nonempty() });
const pollSchema = z.object({
	type: z.literal("poll"),
	title: z.string().nonempty().max(300),
	options: z.array(pollChoiceSchema).min(2).max(10)
});
const pollOptionSchema = z.object({
	type: z.literal("poll_option"),
	option: pollChoiceSchema,
	poll: pollSchema,
	selected: z.boolean(),
	title: z.string().nonempty()
}).superRefine((value, ctx) => {
	if (value.title !== value.option.title) ctx.addIssue({
		code: "custom",
		message: "poll_option title must match option.title",
		path: ["title"]
	});
	if (!value.poll.options.some((pollOption) => pollOption.title === value.option.title)) ctx.addIssue({
		code: "custom",
		message: "poll_option option must exist in poll.options",
		path: ["option"]
	});
});
const asPoll = (input) => pollSchema.parse({
	type: "poll",
	...input
});
const asPollOption = (input) => pollOptionSchema.parse({
	type: "poll_option",
	...input,
	title: input.option.title
});
const option = (title) => ({ title });
const normalize = (raw) => typeof raw === "string" ? { title: raw } : { title: raw.title };
const collectOptions = (args) => {
	const [first] = args;
	if (args.length === 1 && Array.isArray(first)) return first;
	return args;
};
function poll(title, ...rest) {
	return { build: async () => asPoll({
		title,
		options: collectOptions(rest).map(normalize)
	}) };
}
//#endregion
//#region src/content/reaction.ts
const isMessage$3 = (v) => typeof v === "object" && v !== null && "id" in v && "content" in v;
const reactionSchema = z.object({
	type: z.literal("reaction"),
	emoji: z.string().min(1),
	target: z.custom(isMessage$3, { message: "reaction target must be a Message" })
});
const asReaction = (input) => reactionSchema.parse({
	type: "reaction",
	...input
});
/**
* Construct a `reaction` content value targeting the given message.
*
* `space.send(reaction(emoji, message))` is the canonical form of
* `message.react(emoji)`. It resolves to the reaction `Message`
* (`content.type === "reaction"`) — keep it as the handle to `unsend()`
* later. Resolves `undefined` only when the platform does not support
* reactions (warned and skipped).
*
* Accepts `Message | undefined` so `space.send` results chain without
* narrowing (`send` resolves `undefined` when a platform skips unsupported
* content); an undefined target throws at build time.
*
* To react to a message known only by id, resolve it first via
* `space.getMessage(id)`.
*/
function reaction(emoji, target) {
	return { build: async () => {
		if (!target) throw new Error("reaction() target is undefined — the targeted message was never sent (space.send resolves undefined when a platform skips unsupported content)");
		if (target.content.type === "reaction") throw new Error("reaction() cannot target \"reaction\" content");
		return asReaction({
			emoji,
			target
		});
	} };
}
//#endregion
//#region src/content/read.ts
const isMessage$2 = (v) => typeof v === "object" && v !== null && "id" in v && "content" in v;
/**
* A `read` marks the conversation as read **up to** `target`, surfacing a
* read receipt to the sender where the platform supports one.
*
* `space.send(read(message))` is the canonical outbound API;
* `message.read()` and `space.read(message)` are sugar that delegate here.
* Reads are fire-and-forget — providers handle them inside their `send`
* action and the resolved value is `undefined`.
*
* Granularity is per-platform:
*
* - WhatsApp Business: per-message receipt via `markRead(target.id)`, which
*   also marks every earlier message in the conversation as read.
* - iMessage (remote): chat-level `chats.markRead(chatGuid)` — `target` only
*   identifies the chat, and **every** unread message in it is marked read.
*   Local mode rejects with `UnsupportedError` (warned and skipped).
* - Telegram / Slack: silently no-op. Neither surfaces read state for bot
*   conversations (Telegram bot chats are effectively auto-read), so the
*   signal is vacuously satisfied — same best-effort contract as `typing`.
*/
const readSchema = z.object({
	type: z.literal("read"),
	target: z.custom(isMessage$2, { message: "read target must be a Message" })
});
const asRead = (input) => readSchema.parse({
	type: "read",
	...input
});
/**
* Construct a `read` content value marking the conversation read up to
* `target`.
*
* Only inbound messages (those received from a user) can be marked read;
* calling this with an outbound target throws at build time so the misuse
* surfaces before the send pipeline runs. The target is required (not
* `Message | undefined` like `unsend`): read targets come from the inbound
* stream, never from a chainable `send()` result.
*/
function read(target) {
	return { build: async () => {
		if (target.direction !== "inbound") throw new Error(`read() target must be an inbound message (got direction "${target.direction}", message id "${target.id}")`);
		return asRead({ target });
	} };
}
//#endregion
//#region src/content/rename.ts
/**
* Rename the current chat. Universal content — providers dispatch by
* `content.type === "rename"` in their `send` action and decide their own
* support story (e.g. iMessage only supports it for remote group chats).
*
* `space.send(rename("New Name"))` is the canonical form; `space.rename(...)`
* is universal sugar that delegates here.
*
* Bidirectional: providers also surface platform rename events as inbound
* `Message`s carrying this content; `message.sender` is the user who renamed
* the chat (may be `undefined` when the platform recorded no actor).
*
* Throws at build time if `displayName` is empty. Per-platform constraints
* (e.g. group-only, remote-only) surface as `UnsupportedError` from the
* provider's `send` action so the canonical and sugar forms share one
* error path.
*/
const renameSchema = z.object({
	type: z.literal("rename"),
	displayName: z.string().min(1, "rename() displayName must be non-empty")
});
function rename(displayName) {
	return { build: async () => renameSchema.parse({
		type: "rename",
		displayName
	}) };
}
//#endregion
//#region src/content/reply.ts
const isMessage$1 = (v) => typeof v === "object" && v !== null && "id" in v && "content" in v;
const isContent = (v) => typeof v === "object" && v !== null && "type" in v && typeof v.type === "string";
/**
* A `reply` wraps inner content with the message it replies to.
*
* `space.send(reply(content, message))` is the canonical outbound API;
* `message.reply(content)` is sugar that delegates here. Providers see
* `reply` like any other content type and route to a threaded send.
*
* Reply cannot wrap `reply`, `edit`, `reaction`, `group`, `typing`,
* `rename`, `avatar`, `addMember`, `removeMember`, `leaveSpace`, `unsend`,
* or `read` content.
*/
const replySchema = z.object({
	type: z.literal("reply"),
	content: z.custom(isContent, { message: "reply content must be a Content value" }),
	target: z.custom(isMessage$1, { message: "reply target must be a Message" })
});
const asReply = (input) => replySchema.parse({
	type: "reply",
	...input
});
function reply(content, target) {
	return { build: async () => {
		if (!target) throw new Error("reply() target is undefined — the targeted message was never sent (space.send resolves undefined when a platform skips unsupported content)");
		const [resolved] = await resolveContents([content]);
		if (!resolved) throw new Error("reply() requires content");
		if (resolved.type === "reply" || resolved.type === "edit" || resolved.type === "reaction" || resolved.type === "group" || resolved.type === "typing" || resolved.type === "rename" || resolved.type === "avatar" || resolved.type === "addMember" || resolved.type === "removeMember" || resolved.type === "leaveSpace" || resolved.type === "unsend" || resolved.type === "read") throw new Error(`reply() cannot wrap "${resolved.type}" content`);
		return asReply({
			content: resolved,
			target
		});
	} };
}
//#endregion
//#region src/content/richlink.ts
/**
* A URL the platform should render as a rich link preview.
*
* Outbound-only by design: inbound messages always surface as `text` content —
* a URL received from a platform arrives as plain text, never as `richlink`, so
* no metadata is fetched on the inbound path. On outbound, each platform asks
* its native client to render the preview (remote iMessage: `enableLinkPreview`;
* Telegram: auto-unfurls the bare URL), so the framework carries only the URL
* and fetches nothing itself.
*/
const richlinkSchema = z.object({
	type: z.literal("richlink"),
	url: z.url()
});
const asRichlink = (input) => richlinkSchema.parse({
	type: "richlink",
	url: input.url
});
function richlink(url) {
	return { build: async () => asRichlink({ url }) };
}
//#endregion
//#region src/content/unsend.ts
const isMessage = (v) => typeof v === "object" && v !== null && "id" in v && "content" in v;
/**
* An `unsend` retracts a previously-sent outbound message.
*
* `space.send(unsend(message))` is the canonical outbound API;
* `message.unsend()` and `space.unsend(message)` are sugar that delegate
* here. Unsends are fire-and-forget — providers handle them inside their
* `send` action and the resolved value is `undefined` (no new message id is
* produced; the existing message is retracted in place).
*
* Platform constraints surface from the provider at send time — e.g.
* iMessage enforces Apple's ~2-minute unsend window for regular messages
* (reaction removal is not time-limited), and a late or repeated unsend
* rejects with the provider's error. `space.getMessage(id)` results are
* wrapped as inbound, so a message cannot be unsent from a refetched id
* after a restart — keep the Message returned by `send` (same limitation
* as `edit`).
*/
const unsendSchema = z.object({
	type: z.literal("unsend"),
	target: z.custom(isMessage, { message: "unsend target must be a Message" })
});
const asUnsend = (input) => unsendSchema.parse({
	type: "unsend",
	...input
});
/**
* Construct an `unsend` content value retracting `target`.
*
* Only outbound messages (those sent by the agent) can be unsent; calling
* this with an inbound target throws at build time so the misuse surfaces
* before the send pipeline runs.
*
* Accepts `Message | undefined` so `space.send` results chain without
* narrowing (`send` resolves `undefined` when a platform skips unsupported
* content); an undefined target throws at build time.
*/
function unsend(target) {
	return { build: async () => {
		if (!target) throw new Error("unsend() target is undefined — the targeted message was never sent (space.send resolves undefined when a platform skips unsupported content)");
		if (target.direction !== "outbound") throw new Error(`unsend() target must be an outbound message (got direction "${target.direction}", message id "${target.id}")`);
		return asUnsend({ target });
	} };
}
//#endregion
//#region src/content/voice.ts
const AUDIO_MIME_PATTERN = /^audio\//i;
const audioMimeSchema = z.string().nonempty().regex(AUDIO_MIME_PATTERN, "voice content requires an audio/* MIME type");
const voiceSchema = z.object({
	type: z.literal("voice"),
	id: z.string().nonempty().optional(),
	name: z.string().nonempty().optional(),
	mimeType: audioMimeSchema,
	duration: z.number().nonnegative().optional(),
	size: z.number().int().nonnegative().optional(),
	read: readSchema$1,
	stream: streamSchema
});
const resolveVoiceName = (input, name) => {
	if (name) return name;
	if (input instanceof URL) return basename(input.pathname) || void 0;
	if (typeof input === "string") return basename(input);
};
const resolveVoiceMimeHint = (input, name) => {
	if (input instanceof URL) return basename(input.pathname) || void 0;
	if (typeof input === "string") return basename(input);
	return name;
};
const resolveVoiceMimeType = (name, mimeType) => {
	if (mimeType) {
		if (!AUDIO_MIME_PATTERN.test(mimeType)) throw new Error(`voice content requires an audio/* MIME type, got "${mimeType}".`);
		return mimeType;
	}
	if (name) {
		const resolved = lookup(name);
		if (resolved && AUDIO_MIME_PATTERN.test(resolved)) return resolved;
		if (resolved) throw new Error(`Resolved non-audio MIME type "${resolved}" from name "${name}". Pass options.mimeType explicitly with an audio/* type.`);
	}
	throw new Error("Unable to resolve MIME type for voice content. Pass options.mimeType explicitly.");
};
const asVoice = (input) => {
	let cached;
	const read = () => {
		cached ??= input.read().catch((err) => {
			cached = void 0;
			throw err;
		});
		return cached;
	};
	const stream = input.stream ?? (async () => bufferToStream(await read()));
	return voiceSchema.parse({
		type: "voice",
		...input.id ? { id: input.id } : {},
		name: input.name,
		mimeType: input.mimeType,
		duration: input.duration,
		size: input.size,
		read,
		stream
	});
};
function voice(input, options) {
	return { build: async () => {
		const name = resolveVoiceName(input, options?.name);
		const mimeType = resolveVoiceMimeType(resolveVoiceMimeHint(input, name), options?.mimeType);
		if (input instanceof URL) return asVoice({
			name,
			mimeType,
			duration: options?.duration,
			read: async () => (await fetchUrlBytes(input)).data
		});
		if (typeof input === "string") {
			const stats = await stat(input);
			if (!stats.isFile()) throw new Error(`voice content path "${input}" is not a regular file.`);
			return asVoice({
				name,
				mimeType,
				duration: options?.duration,
				size: stats.size,
				read: () => readFile(input),
				stream: async () => Readable.toWeb(createReadStream(input))
			});
		}
		return asVoice({
			name,
			mimeType,
			duration: options?.duration,
			size: input.byteLength,
			read: async () => input,
			stream: async () => bufferToStream(input)
		});
	} };
}
//#endregion
//#region src/utils/env.ts
/**
* Build a Spectrum config env-var name from a channel and a key, centralizing
* the `SPECTRUM_<CHANNEL>_<KEY>` convention so the prefix can't drift or be
* mistyped per field. Both segments are joined verbatim (callers pass them
* already upper-snake-cased), e.g. `envFor("TELEGRAM", "BOT_TOKEN")` →
* `"SPECTRUM_TELEGRAM_BOT_TOKEN"`.
*/
const envFor = (channel, key) => `SPECTRUM_${channel}_${key}`;
/**
* Wrap a config-field schema so it falls back to an environment variable when
* the field is omitted. Precedence is **explicit value > env var > the inner
* schema's own default/required check**:
*
* - An explicit value (anything other than `undefined`) is passed straight
*   through — a caller-supplied config always wins over the environment.
* - When the field is `undefined`, `process.env[envKey]` is substituted. An
*   env var set to the empty string is treated as unset (a common deploy
*   footgun) so it doesn't satisfy a `.min(1)` by accident.
* - The inner `schema` then validates the resolved value, keeping its exact
*   semantics — regex, `.min(1)`, `.url()`, `.optional()`, `.default(...)`.
*   When both the field and the env var are absent, a required inner schema
*   raises its normal "required" error.
*
* The output type is the inner schema's output type, so call sites are
* unchanged. Because substitution only happens on `undefined`, wrapping a
* field inside a `z.object` leaves it a plain key on the parsed result — union
* discriminators like `"accessToken" in config` keep working.
*
* @example
* ```ts
* botToken: fromEnv("SPECTRUM_TELEGRAM_BOT_TOKEN", z.string().regex(BOT_TOKEN_PATTERN)),
* ```
*/
const fromEnv = (envKey, schema) => z.preprocess((value) => {
	if (value !== void 0) return value;
	const envValue = process.env[envKey];
	return envValue === "" ? void 0 : envValue;
}, schema);
/**
* Normalize a platform id into the env-var prefix segment: upper-case, with any
* run of non-alphanumeric characters collapsed to a single `_`. This is a
* whole-name transform, NOT camelCase splitting, so it reproduces the prefixes
* canonical platform ids use lowercase snake_case, so `"telegram"` becomes
* `TELEGRAM` and `"whatsapp_business"` becomes `WHATSAPP_BUSINESS`.
*/
const NON_ALPHANUMERIC_RUN = /[^A-Z0-9]+/;
const normalizePlatformName = (name) => name.toUpperCase().split(NON_ALPHANUMERIC_RUN).filter(Boolean).join("_");
const CAMEL_BOUNDARY = /([a-z0-9])([A-Z])/g;
/** Convert a camelCase config field name into its UPPER_SNAKE env-var suffix. */
const toEnvKey = (field) => field.replace(CAMEL_BOUNDARY, "$1_$2").toUpperCase();
const INNER_TYPE_WRAPPERS = new Set([
	"optional",
	"nullable",
	"default",
	"readonly",
	"catch",
	"nonoptional",
	"prefault"
]);
const asInternal = (schema) => schema;
const unwrapSchema = (schema) => {
	let current = asInternal(schema);
	for (let depth = 0; depth < 20; depth++) {
		const { type } = current.def;
		if (INNER_TYPE_WRAPPERS.has(type) && current.def.innerType) {
			current = current.def.innerType;
			continue;
		}
		if (type === "pipe" && (current.def.out || current.def.in)) {
			current = current.def.out ?? current.def.in;
			continue;
		}
		break;
	}
	return current;
};
const isStringLeaf = (schema) => unwrapSchema(schema).def.type === "string";
const isStrictObject = (def) => def.catchall?.def.type === "never";
/**
* Rewrite a platform's config schema so every string-leaf field falls back to
* `SPECTRUM_<PLATFORM>_<KEY>` when omitted which is the equivalent of
* hand-wrapping each field with {@link fromEnv}. Called once by
* `definePlatform`, so adapters declare plain Zod and get env fallback for free.
*
* - **object**: each string-leaf field is wrapped with `fromEnv`; non-string
*   fields (records, arrays, nested objects, booleans, numbers) are left as-is.
*   The object is only reconstructed when a field actually changed, and
*   `.strict()` is preserved so an empty strict "cloud" branch is untouched.
* - **union**: each option is rewritten independently, so an env value only
*   satisfies the branch that declares that field (a direct/cloud union stays
*   correctly discriminated).
* - anything else is returned unchanged.
*
* The output type is identical to the input schema's. Only omitted fields gain
* an env fallback which means that `z.infer` and downstream contexts are unaffected.
*/
const envAwareConfig = (name, schema) => {
	return rewrite(`SPECTRUM_${normalizePlatformName(name)}`, schema);
};
const rewrite = (prefix, schema) => {
	const { def } = asInternal(schema);
	if (def.type === "object" && def.shape) return rewriteObject(prefix, def, schema);
	if (def.type === "union" && def.options) {
		const options = def.options.map((option) => rewrite(prefix, option));
		return z.union(options);
	}
	return schema;
};
const rewriteObject = (prefix, def, original) => {
	const shape = def.shape;
	const nextShape = {};
	let changed = false;
	for (const [key, field] of Object.entries(shape)) if (isStringLeaf(field)) {
		nextShape[key] = fromEnv(`${prefix}_${toEnvKey(key)}`, field);
		changed = true;
	} else nextShape[key] = field;
	if (!changed) return original;
	const rebuilt = z.object(nextShape);
	return isStrictObject(def) ? rebuilt.strict() : rebuilt;
};
//#endregion
//#region src/utils/identifier.ts
const PHONE_LIKE = /^\+?[\d\s()\-.]{7,}$/;
const EMAIL_LIKE = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;
function classifyIdentifier(s) {
	if (EMAIL_LIKE.test(s)) return {
		kind: "email",
		identifier: sanitizeEmail(s)
	};
	if (PHONE_LIKE.test(s) && s.replace(/\D/g, "").length >= 7) return {
		kind: "phone",
		identifier: sanitizePhone(s)
	};
	return {
		kind: "unknown",
		identifier: s
	};
}
//#endregion
//#region src/utils/markdown.ts
const markdownLexer = new Marked();
const BULLET = "• ";
const HR_LINE = "———";
const NESTED_LIST_INDENT = "  ";
const BLOCK_SEPARATOR = "\n\n";
const TABLE_CELL_SEPARATOR = " | ";
const DEFAULT_LIST_START = 1;
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
	if (token.text === token.href) return token.href;
	return `${renderInlineTokens(token.tokens)} (${token.href})`;
};
const renderImage = (token) => token.text ? `${token.text} (${token.href})` : token.href;
const renderInlineToken = (token) => {
	switch (token.type) {
		case "strong":
		case "em":
		case "del": return renderInlineTokens(token.tokens);
		case "codespan": return token.text;
		case "br": return "\n";
		case "link": return renderLink(token);
		case "image": return renderImage(token);
		case "escape": return token.text;
		case "text": return token.tokens ? renderInlineTokens(token.tokens) : token.text;
		case "html": return token.text;
		case "checkbox": return "";
		default: return "raw" in token ? String(token.raw) : "";
	}
};
/**
* Render a run of inline markdown tokens (a paragraph's or table cell's
* children) to plain text. Package-internal: platform renderers reuse it
* where their native format has no inline markup (e.g. Telegram tables).
*/
const renderInlineTokens = (tokens) => {
	let out = "";
	for (const token of tokens) out += renderInlineToken(asMarkedToken(token));
	return out;
};
const renderBlockquote = (quote) => renderBlockTokens(quote.tokens).split("\n").map((line) => line ? `> ${line}` : ">").join("\n");
const renderList = (list) => {
	const lines = [];
	for (const [index, item] of list.items.entries()) {
		const prefix = `${listMarker(list, index)}${checkboxPrefix(item)}`;
		const blocks = [];
		for (const token of item.tokens) {
			const rendered = renderBlockToken(asMarkedToken(token));
			if (rendered) blocks.push(rendered);
		}
		const [first = "", ...rest] = blocks.join("\n").split("\n");
		lines.push(`${prefix}${first}`);
		for (const line of rest) lines.push(`${NESTED_LIST_INDENT}${line}`);
	}
	return lines.join("\n");
};
const renderTable = (table) => {
	const renderRow = (cells) => cells.map((cell) => renderInlineTokens(cell.tokens)).join(TABLE_CELL_SEPARATOR);
	const lines = [renderRow(table.header)];
	for (const row of table.rows) lines.push(renderRow(row));
	return lines.join("\n");
};
const renderBlockToken = (token) => {
	switch (token.type) {
		case "heading":
		case "paragraph": return renderInlineTokens(token.tokens);
		case "code": return token.text;
		case "blockquote": return renderBlockquote(token);
		case "list": return renderList(token);
		case "table": return renderTable(token);
		case "hr": return HR_LINE;
		case "space":
		case "def": return "";
		default: return renderInlineToken(token);
	}
};
const renderBlockTokens = (tokens) => {
	const blocks = [];
	for (const token of tokens) {
		const rendered = renderBlockToken(asMarkedToken(token));
		if (rendered) blocks.push(rendered);
	}
	return blocks.join(BLOCK_SEPARATOR);
};
/**
* Render standard markdown (CommonMark + GFM) to readable plain text:
* emphasis markers stripped, links as `label (url)`, list bullets kept,
* code verbatim. Used by the send pipeline to downgrade `markdown` content
* on platforms without native support.
*/
const markdownToPlainText = (markdown) => renderBlockTokens(markdownLexer.lexer(markdown)).trim();
//#endregion
//#region src/utils/telemetry.ts
const targetId = (target) => {
	const id = target?.id;
	return typeof id === "string" ? id : void 0;
};
const targetType = (target) => {
	const type = target?.content?.type;
	return typeof type === "string" ? type : void 0;
};
const replyOrEditAttrs = (content) => {
	const target = content.target;
	const innerType = content.content?.type;
	return {
		"spectrum.message.content.target.id": targetId(target),
		"spectrum.message.content.target.type": targetType(target),
		"spectrum.message.content.inner.type": typeof innerType === "string" ? innerType : void 0
	};
};
const unsendAttrs = (content) => {
	const target = content.target;
	return {
		"spectrum.message.content.target.id": targetId(target),
		"spectrum.message.content.target.type": targetType(target)
	};
};
const reactionAttrs = (content) => {
	const target = content.target;
	const emoji = content.emoji;
	return {
		"spectrum.message.content.target.id": targetId(target),
		"spectrum.message.content.reaction.emoji": typeof emoji === "string" ? emoji : void 0
	};
};
const groupAttrs = (content) => {
	const items = content.items;
	if (!Array.isArray(items)) return {};
	const types = items.map((item) => {
		const itemType = item?.content?.type;
		return typeof itemType === "string" ? itemType : void 0;
	}).filter((t) => t !== void 0);
	return {
		"spectrum.message.content.items.count": items.length,
		"spectrum.message.content.items.types": types.length > 0 ? types.join(",") : void 0
	};
};
const typingAttrs = (content) => {
	const state = content.state;
	return { "spectrum.message.content.typing.state": typeof state === "string" ? state : void 0 };
};
const attachmentAttrs = (content) => {
	const mime = content.mimeType;
	const size = content.size;
	return {
		"spectrum.message.content.attachment.mime": typeof mime === "string" ? mime : void 0,
		"spectrum.message.content.attachment.size": typeof size === "number" ? size : void 0
	};
};
const voiceAttrs = (content) => {
	const mime = content.mimeType;
	const duration = content.duration;
	const size = content.size;
	return {
		"spectrum.message.content.voice.mime": typeof mime === "string" ? mime : void 0,
		"spectrum.message.content.voice.duration": typeof duration === "number" ? duration : void 0,
		"spectrum.message.content.voice.size": typeof size === "number" ? size : void 0
	};
};
const CONTENT_ATTR_HANDLERS = {
	reply: replyOrEditAttrs,
	edit: replyOrEditAttrs,
	unsend: unsendAttrs,
	reaction: reactionAttrs,
	group: groupAttrs,
	typing: typingAttrs,
	attachment: attachmentAttrs,
	voice: voiceAttrs
};
function contentAttrs(content) {
	const type = content?.type;
	if (!(content && type)) return { "spectrum.message.content.type": void 0 };
	const handler = CONTENT_ATTR_HANDLERS[type];
	return {
		"spectrum.message.content.type": type,
		...handler ? handler(content) : {}
	};
}
function senderAttrs(sender) {
	const id = sender?.id;
	if (typeof id !== "string" || id.length === 0) return {};
	const { kind, identifier } = classifyIdentifier(id);
	return {
		"spectrum.message.sender.id": identifier,
		"spectrum.message.sender.kind": kind
	};
}
const attrCode = (value) => {
	if (typeof value === "string" || typeof value === "number") return value;
};
const causeType = (cause) => {
	if (cause === void 0) return;
	return cause instanceof Error ? cause.name : typeof cause;
};
const causeMessage = (cause) => {
	if (cause === void 0) return;
	return sanitizeErrorMessage(cause instanceof Error ? cause.message : String(cause));
};
/**
* Structured attributes for an unknown thrown value: its type, sanitized
* message, `code`/`status` when present, and one level of `cause`. Enough to
* debug from a log line or span without leaking PII or dumping a raw `Error`
* (which stringifies to "[object Object]" inside an attrs object). The stack is
* intentionally omitted — pass the `Error` as the logger's 3rd argument (or
* wrap with `withSpan`) so it lands on the OTLP record's `exception.*` fields.
*/
function errorAttrs(error, prefix = "spectrum.error") {
	if (!(error instanceof Error)) return {
		[`${prefix}.type`]: typeof error,
		[`${prefix}.message`]: sanitizeErrorMessage(String(error))
	};
	const { code, status, cause } = error;
	return {
		[`${prefix}.type`]: error.name,
		[`${prefix}.message`]: sanitizeErrorMessage(error.message),
		[`${prefix}.code`]: attrCode(code),
		[`${prefix}.status`]: attrCode(status),
		[`${prefix}.cause.type`]: causeType(cause),
		[`${prefix}.cause.message`]: causeMessage(cause)
	};
}
//#endregion
//#region src/utils/token-renewal.ts
const RENEWAL_RATIO = .8;
const EXPIRY_BUFFER_MS = 3e4;
const MIN_RENEWAL_DELAY_MS = 5e3;
const RETRY_DELAY_MS = 3e4;
const createTokenRenewal = (options) => {
	const telemetryPrefix = `spectrum.${options.name}.auth`;
	const log = createLogger(telemetryPrefix);
	let disposed = false;
	let refreshFailures = 0;
	let refreshInFlight;
	let renewalTimer;
	let tokenExpiresAt = Date.now() + options.expiresInSeconds() * 1e3;
	const clearRenewalTimer = () => {
		if (renewalTimer !== void 0) {
			clearTimeout(renewalTimer);
			renewalTimer = void 0;
		}
	};
	const onRefreshSuccess = () => {
		if (refreshFailures > 0) {
			log.info(`${options.name} token refresh recovered`, { [`${telemetryPrefix}.attempt`]: refreshFailures });
			refreshFailures = 0;
		}
	};
	const onRefreshFailure = (error) => {
		refreshFailures += 1;
		log.warn(`${options.name} token refresh failed; retrying`, {
			[`${telemetryPrefix}.attempt`]: refreshFailures,
			[`${telemetryPrefix}.retry_in_ms`]: RETRY_DELAY_MS,
			...errorAttrs(error)
		}, error);
	};
	const refreshNow = async () => {
		await options.refresh();
		tokenExpiresAt = Date.now() + options.expiresInSeconds() * 1e3;
		onRefreshSuccess();
		scheduleRenewal();
	};
	const coalescedRefresh = () => {
		if (!refreshInFlight) refreshInFlight = refreshNow().finally(() => {
			refreshInFlight = void 0;
		});
		return refreshInFlight;
	};
	const runScheduledRefresh = () => {
		if (disposed) return;
		coalescedRefresh().catch((error) => {
			onRefreshFailure(error);
			if (disposed) return;
			renewalTimer = setTimeout(runScheduledRefresh, RETRY_DELAY_MS);
			renewalTimer?.unref?.();
		});
	};
	const scheduleRenewal = () => {
		if (disposed) return;
		clearRenewalTimer();
		const ttlMs = options.expiresInSeconds() * 1e3;
		const renewInMs = Math.max(ttlMs * RENEWAL_RATIO, MIN_RENEWAL_DELAY_MS);
		renewalTimer = setTimeout(runScheduledRefresh, renewInMs);
		renewalTimer?.unref?.();
	};
	scheduleRenewal();
	return {
		dispose() {
			disposed = true;
			clearRenewalTimer();
		},
		forceRefresh: coalescedRefresh,
		invalidate() {
			tokenExpiresAt = 0;
		},
		async refreshIfNeeded() {
			if (Date.now() < tokenExpiresAt - EXPIRY_BUFFER_MS) return;
			await coalescedRefresh();
		}
	};
};
//#endregion
//#region src/utils/stream.ts
/**
* Unbounded FIFO queue with `AsyncIterable` consumer. Used by FusorCore to feed
* the per-platform message stream — events pushed before a consumer attaches
* are buffered, and a pending `next()` is woken when a value arrives.
*/
function createAsyncQueue() {
	const buffer = [];
	const resolvers = [];
	let closed = false;
	const push = (value) => {
		if (closed) return;
		const resolver = resolvers.shift();
		if (resolver) resolver({
			value,
			done: false
		});
		else buffer.push(value);
	};
	const close = () => {
		if (closed) return;
		closed = true;
		while (resolvers.length > 0) resolvers.shift()?.({
			value: void 0,
			done: true
		});
	};
	return {
		iterable: { [Symbol.asyncIterator]() {
			return {
				next() {
					if (buffer.length > 0) {
						const value = buffer.shift();
						return Promise.resolve({
							value,
							done: false
						});
					}
					if (closed) return Promise.resolve({
						value: void 0,
						done: true
					});
					return new Promise((resolve) => {
						resolvers.push(resolve);
					});
				},
				return() {
					close();
					return Promise.resolve({
						value: void 0,
						done: true
					});
				}
			};
		} },
		push,
		close
	};
}
const ignoreCleanupError = () => void 0;
function stream(setup) {
	const repeater = new Repeater(async (push, stop) => {
		const emit = async (value) => {
			try {
				await push(value);
			} catch (error) {
				stop(error);
				throw error;
			}
		};
		const end = (error) => {
			stop(error);
		};
		const cleanup = await setup(emit, end);
		try {
			await stop;
		} finally {
			await cleanup?.();
		}
	});
	return Object.assign(repeater, { close: async () => {
		await repeater.return(void 0).catch(ignoreCleanupError);
	} });
}
/**
* Merges a fixed set of streams. The result represents all of them: it ends
* once every member has ended — immediately, for an empty set — and one
* member's error ends the whole merge.
*
* For a set that changes while the stream runs, use `createStreamGroup`
* (`@spectrum-ts/core/authoring`), which ends only on `close()` and drops a
* failed member instead of tearing down its siblings.
*/
function mergeStreams(streams) {
	return stream((emit, end) => {
		if (streams.length === 0) {
			end();
			return;
		}
		let openStreams = streams.length;
		const workers = streams.map(async (source) => {
			try {
				for await (const value of source) await emit(value);
			} catch (error) {
				end(error);
			} finally {
				openStreams -= 1;
				if (openStreams === 0) end();
			}
		});
		return async () => {
			await Promise.allSettled(streams.map((source) => source.close()));
			await Promise.allSettled(workers).catch(ignoreCleanupError);
		};
	});
}
function broadcast(source) {
	const consumers = /* @__PURE__ */ new Set();
	let pumping = false;
	let terminated = false;
	let terminalError;
	let pumpPromise;
	let closed = false;
	const closeConsumers = (error) => {
		if (consumers.size === 0) return;
		const current = Array.from(consumers);
		consumers.clear();
		for (const consumer of current) consumer.end(error);
	};
	const startPump = () => {
		if (pumping || terminated) return;
		pumping = true;
		pumpPromise = (async () => {
			try {
				for await (const value of source) {
					if (terminated) break;
					for (const consumer of Array.from(consumers)) consumer.deliveries = consumer.deliveries.then(() => consumer.emit(value).catch(() => {}));
				}
				if (terminated) return;
				terminated = true;
				await Promise.allSettled(Array.from(consumers, (consumer) => consumer.deliveries));
				closeConsumers();
			} catch (error) {
				terminated = true;
				terminalError = error;
				closeConsumers(error);
			}
		})();
	};
	return {
		subscribe() {
			return stream((emit, end) => {
				if (terminated || closed) {
					end(terminalError);
					return;
				}
				const consumer = {
					emit,
					end,
					deliveries: Promise.resolve()
				};
				consumers.add(consumer);
				startPump();
				return () => {
					consumers.delete(consumer);
				};
			});
		},
		async close() {
			if (closed) return;
			closed = true;
			terminated = true;
			closeConsumers();
			await source.close().catch(ignoreCleanupError);
			if (pumpPromise) await pumpPromise.catch(ignoreCleanupError);
		}
	};
}
//#endregion
export { drainStreamText as $, reaction as A, removeMember as B, replySchema as C, read as D, asRead as E, poll as F, asGroup as G, asMarkdown as H, addMember as I, resolveContents as J, group as K, addMemberSchema as L, asPoll as M, asPollOption as N, readSchema as O, option as P, StreamConsumedError as Q, leaveSpace as R, reply as S, renameSchema as T, markdown as U, removeMemberSchema as V, markdownSchema as W, text as X, asText as Y, textSchema as Z, asUnsend as _, createTokenRenewal as a, toVCard as at, richlink as b, senderAttrs as c, buildPhotoAction as ct, classifyIdentifier as d, attachment as dt, asCustom as et, envAwareConfig as f, attachmentSchema as ft, voice as g, asVoice as h, stream as i, fromVCard as it, reactionSchema as j, asReaction as k, markdownToPlainText as l, photoActionSchema as lt, fromEnv as m, tracedFetch as mt, createAsyncQueue as n, asContact as nt, contentAttrs as o, avatar as ot, envFor as p, fetchUrlBytes as pt, groupSchema as q, mergeStreams as r, contact as rt, errorAttrs as s, avatarSchema as st, broadcast as t, custom as tt, renderInlineTokens as u, asAttachment as ut, unsend as v, rename as w, asReply as x, asRichlink as y, leaveSpaceSchema as z };
