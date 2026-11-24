// Web Profile 宿主适配器的可测试核心。
// 只依赖注入的 DSH 服务，不在这里打开 SQLite 或网络监听。
import { timingSafeEqual } from "node:crypto";

export const PROFILE_DESKTOP = "desktop";
export const PROFILE_WEB = "web";
export const WEB_ROUTE = "/api/orchestra.request";
export const WEB_GLOBAL = "__DSH_ORCHESTRA_WEB__";

/** 只认 DSH 明确给出的 desktop / web profile，其他一律视为未支持。 */
export function classifyProfile(profile) {
	const name = typeof profile?.name === "string" ? profile.name.trim().toLowerCase() : "";
	if (name === PROFILE_DESKTOP) return PROFILE_DESKTOP;
	if (name === PROFILE_WEB) return PROFILE_WEB;
	return "unsupported";
}

export function isLoopbackHost(value) {
	if (typeof value !== "string") return false;
	const host = value.trim().toLowerCase().replace(/^\[|\]$/gu, "");
	if (host === "localhost" || host === "::1") return true;
	const match = /^127\.(\d{1,3})\.(\d{1,3})\.(\d{1,3})$/u.exec(host);
	return Boolean(match && [1, 2, 3].every((index) => Number(match[index]) <= 255));
}

function hostnameOf(authority) {
	if (typeof authority !== "string" || authority.length === 0) return "";
	try {
		return new URL(`http://${authority}`).hostname.toLowerCase().replace(/^\[|\]$/gu, "");
	} catch {
		return "";
	}
}

/**
 * Web Profile 的能力清单。缺少任何一项都不装配服务，并把缺失项交给面板显示。
 * 非本机监听且没有操作人认证时，按“未配置认证”处理，不降低认证要求。
 */
export function webHostCapabilities({ profile, webServer, services = {} } = {}) {
	const missing = [];
	const push = (code, message) => missing.push({ code, message });
	if (classifyProfile(profile) !== PROFILE_WEB) push("PROFILE_UNSUPPORTED", "当前不是 DSH Web Profile");
	if (!webServer || typeof webServer.register !== "function") push("WEB_SERVER_UNAVAILABLE", "DSH Web 服务器路由注册不可用");
	else if (!isLoopbackHost(webServer.host)) push("OPERATOR_AUTHENTICATION_REQUIRED", "Web Profile 绑定到非本机地址；未配置操作人认证时拒绝启动");
	for (const [name, predicate] of Object.entries({
		sessionController: (value) => typeof value?.resolveAgent === "function",
		workspaceRegistry: (value) => typeof value?.get === "function" && typeof value?.list === "function",
		agents: (value) => typeof value?.get === "function",
		credentials: (value) => typeof value?.modifyRecord === "function",
		llm: (value) => value !== void 0,
		tools: (value) => typeof value?.register === "function",
		workspaceFiles: (value) => typeof value?.read === "function" && typeof value?.list === "function"
	})) {
		if (!predicate(services[name])) push(`MISSING_${name.toUpperCase()}`, `缺少 DSH 服务 ${name}`);
	}
	return {
		ok: missing.length === 0,
		authenticated: Boolean(webServer && isLoopbackHost(webServer.host)),
		missing
	};
}

/** 校验 Web 面板请求：本机 Host、同源、POST 的 JSON/防 CSRF 头，以及页面内下发的一次性证明。 */
export function createWebRequestAuthorizer({ webServer, proof, human } = {}) {
	const expected = typeof proof === "string" && proof.length > 0 ? Buffer.from(proof) : void 0;
	function sameSecret(value) {
		if (!expected || typeof value !== "string") return false;
		const supplied = Buffer.from(value);
		return supplied.length === expected.length && timingSafeEqual(supplied, expected);
	}
	return {
		authorize(request) {
			const hostname = hostnameOf(request.headers.get("host") ?? "");
			if (!isLoopbackHost(hostname)) return { ok: false, status: 403, code: "FORBIDDEN", message: "此面板只接受本机请求" };
			if (!webServer || !isLoopbackHost(webServer.host)) return { ok: false, status: 503, code: "OPERATOR_AUTHENTICATION_REQUIRED", message: "Web Profile 未配置操作人认证" };
			if (request.headers.get("sec-fetch-site") === "cross-site") return { ok: false, status: 403, code: "FORBIDDEN", message: "不接受跨站请求" };
			const origin = request.headers.get("origin");
			if (origin !== null) {
				let originHost = "";
				try {
					originHost = new URL(origin).hostname.toLowerCase().replace(/^\[|\]$/gu, "");
				} catch {
					return { ok: false, status: 403, code: "FORBIDDEN", message: "Origin 无效" };
				}
				if (originHost !== hostname) return { ok: false, status: 403, code: "FORBIDDEN", message: "不接受跨源请求" };
			}
			if (!sameSecret(request.headers.get("x-orchestra-web-proof") ?? "")) return { ok: false, status: 403, code: "FORBIDDEN", message: "Web 面板凭据无效" };
			if (request.method === "POST") {
				if (request.headers.get("x-orchestra-request") !== "1") return { ok: false, status: 403, code: "FORBIDDEN", message: "缺少编排面板请求标记" };
				if (request.headers.get("content-type")?.split(";", 1)[0]?.trim().toLowerCase() !== "application/json") return { ok: false, status: 403, code: "FORBIDDEN", message: "只接受 JSON 请求" };
			}
			if (!human || typeof human.id !== "string" || human.id.length === 0) return { ok: false, status: 503, code: "OPERATOR_AUTHENTICATION_REQUIRED", message: "无法确定宿主操作人身份" };
			return { ok: true, human };
		}
	};
}

/** 工作区授权：会话必须属于该工作区、未归档，且工作区仍在注册表中。 */
export function authorizeWorkspace({ workspaceRegistry, workspaceId, sessionId } = {}) {
	if (!workspaceRegistry || typeof workspaceRegistry.get !== "function") return { ok: false, status: 503, code: "SERVICE_UNAVAILABLE", message: "工作区注册表不可用" };
	const workspace = workspaceRegistry.get(workspaceId);
	const archived = Array.isArray(workspaceRegistry.archivedSessionIds) ? workspaceRegistry.archivedSessionIds : [];
	if (!workspace || !Array.isArray(workspace.sessionIds) || !workspace.sessionIds.includes(sessionId) || archived.includes(sessionId)) return { ok: false, status: 403, code: "FORBIDDEN", message: "请选择该工作区中未归档的 DSH 会话" };
	return { ok: true, workspace };
}

/**
 * 单飞本地服务管理器：同一身份只创建一个 SQLite 写者。
 * stop/closeAll 先断开入口再关闭服务；启动失败会清理占位，允许下一次重试。
 */
export function createLocalServiceManager({ open, max = 8, onError } = {}) {
	if (typeof open !== "function") throw new TypeError("service manager 需要 open(identity)");
	const entries = new Map();
	let stopped = false;
	let generation = 0;
	const report = (error) => {
		try {
			onError?.(error);
		} catch {}
	};
	function ensure(identity, opener = open) {
		if (stopped) return Promise.reject(new Error("编排服务已停止"));
		const existing = entries.get(identity);
		if (existing) return existing.startup;
		if (entries.size >= max) return Promise.reject(new Error("已打开过多编排服务"));
		const token = ++generation;
		const entry = { state: "starting", token, startup: null, service: null };
		entry.startup = Promise.resolve()
			.then(() => opener(identity))
			.then((service) => {
				entry.service = service;
				entry.state = stopped ? "stopped" : "ready";
				return service;
			})
			.catch((error) => {
				entry.state = "failed";
				if (entries.get(identity) === entry) entries.delete(identity);
				report(error);
				throw error;
			});
		entries.set(identity, entry);
		return entry.startup;
	}
	async function close(identity) {
		const entry = entries.get(identity);
		if (!entry) return;
		entries.delete(identity);
		entry.state = "stopping";
		try {
			const service = entry.service ?? await entry.startup.catch(() => void 0);
			await service?.close?.();
			entry.state = "stopped";
		} catch (error) {
			entry.state = "failed";
			report(error);
		}
	}
	async function closeAll() {
		stopped = true;
		await Promise.allSettled([...entries.keys()].map((identity) => close(identity)));
	}
	async function restart(identity, opener = open) {
		stopped = false;
		await close(identity);
		return ensure(identity, opener);
	}
	return {
		ensure,
		close,
		closeAll,
		restart,
		snapshot: () => [...entries].map(([identity, entry]) => ({ identity, state: entry.state })),
		get size() {
			return entries.size;
		}
	};
}

/** 页面内下发的 Web 面板传输描述；客户端据此使用同源路由并附加证明。 */
export function createWebPanelTransport({ proof, missing = [] } = {}) {
	return {
		profile: PROFILE_WEB,
		route: WEB_ROUTE,
		proof: typeof proof === "string" && proof.length > 0 ? proof : null,
		missing_capabilities: missing
	};
}
