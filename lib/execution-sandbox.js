// 统一执行沙箱：目录、网络与工具权限全部由服务端策略决定。
// 默认 fail-closed：策略不完整、运行时不可用或高风险任务落到宿主进程时一律拒绝。
import { spawn } from "node:child_process";
import { randomBytes } from "node:crypto";
import { mkdtempSync, readFileSync, realpathSync, rmSync } from "node:fs";
import { isIP } from "node:net";
import { tmpdir } from "node:os";
import { dirname, isAbsolute, join, relative, resolve } from "node:path";

export const SANDBOX_KINDS = Object.freeze([
	"local_process",
	"read_only_workspace",
	"docker",
	"restricted_network"
]);
export const TASK_RISKS = Object.freeze(["low", "medium", "high"]);
export const NETWORK_MODES = Object.freeze(["none", "allowlist", "host"]);

const KIND_RULES = Object.freeze({
	local_process: Object.freeze({
		isolation: "process",
		container: false,
		network: Object.freeze(["host"]),
		networkDefault: "host",
		writable: true
	}),
	read_only_workspace: Object.freeze({
		isolation: "capability",
		container: false,
		network: Object.freeze(["none"]),
		networkDefault: "none",
		writable: false
	}),
	docker: Object.freeze({
		isolation: "container",
		container: true,
		network: Object.freeze(["none", "allowlist"]),
		networkDefault: "none",
		writable: true
	}),
	restricted_network: Object.freeze({
		isolation: "container",
		container: true,
		network: Object.freeze(["allowlist"]),
		networkDefault: "allowlist",
		writable: true
	})
});

const EFFECT_RISK = Object.freeze({
	pure: "low",
	compensable: "medium",
	irreversible: "high"
});

export const SANDBOX_ERROR_CODES = Object.freeze([
	"SANDBOX_POLICY_INVALID",
	"SANDBOX_UNAVAILABLE",
	"SANDBOX_TOOL_DENIED",
	"SANDBOX_PATH_DENIED",
	"SANDBOX_NETWORK_DENIED",
	"SANDBOX_EGRESS_UNCONFIGURED",
	"HOST_EXECUTION_NOT_GRANTED",
	"HIGH_RISK_REQUIRES_ISOLATION",
	"SANDBOX_RESULT_INVALID",
	"SANDBOX_FAILED"
]);

export class SandboxError extends Error {
	constructor(code, message, details = {}) {
		super(message);
		this.name = "SandboxError";
		this.code = SANDBOX_ERROR_CODES.includes(code) ? code : "SANDBOX_FAILED";
		this.details = details;
	}
}

function fail(code, message, details = {}) {
	throw new SandboxError(code, message, details);
}

function isPlainObject(value) {
	return value !== null && typeof value === "object" && !Array.isArray(value);
}

function insideRoot(root, target) {
	const rel = relative(root, target);
	return rel === "" || !rel.startsWith("..") && !isAbsolute(rel);
}

/**
 * 解析路径的真实位置，用于阻断符号链接逃逸。
 * 尚不存在的尾部片段按最近的已存在祖先拼接；无法解析时 fail-closed。
 */
function canonicalPath(target, code = "SANDBOX_PATH_DENIED") {
	const absolute = resolve(target);
	let current = absolute;
	const suffix = [];
	while (true) {
		try {
			const real = realpathSync(current);
			return suffix.length === 0 ? real : resolve(real, ...suffix.reverse());
		} catch (error) {
			if (error?.code !== "ENOENT" && error?.code !== "ENOTDIR") fail(code, "无法解析路径的真实位置", {
				path: absolute,
				reason: String(error?.code ?? error?.message ?? error)
			});
			const parent = dirname(current);
			if (parent === current) return absolute;
			suffix.push(current.slice(parent.length).replace(/^[\\/]/u, ""));
			current = parent;
		}
	}
}

function canonicalRoots(roots) {
	return [...new Set(roots.map((root) => canonicalPath(root, "SANDBOX_POLICY_INVALID")))];
}

function normalizeRoot(value, field) {
	if (typeof value !== "string" || value.trim() === "" || value.includes("\0")) fail("SANDBOX_POLICY_INVALID", `${field} 必须是绝对路径`, { field });
	if (!isAbsolute(value)) fail("SANDBOX_POLICY_INVALID", `${field} 必须是绝对路径`, { field, value });
	return resolve(value);
}

function normalizeRoots(value, fallback) {
	if (value === void 0) return [fallback];
	if (!Array.isArray(value) || value.length === 0) fail("SANDBOX_POLICY_INVALID", "allowedRoots 必须是非空绝对路径数组", {});
	const roots = value.map((item, index) => normalizeRoot(item, `allowedRoots[${index}]`));
	return [...new Set(roots)];
}

function normalizeKinds(value) {
	if (!Array.isArray(value)) fail("SANDBOX_POLICY_INVALID", "availableKinds 必须是数组", {});
	const kinds = [];
	for (const item of value) {
		if (!SANDBOX_KINDS.includes(item)) fail("SANDBOX_POLICY_INVALID", "存在未知沙箱类型", { kind: item });
		if (!kinds.includes(item)) kinds.push(item);
	}
	if (kinds.length === 0) fail("SANDBOX_UNAVAILABLE", "宿主没有可用的执行沙箱", {});
	return kinds;
}

function normalizeNames(value, field) {
	if (value === void 0) return [];
	if (!Array.isArray(value)) fail("SANDBOX_POLICY_INVALID", `${field} 必须是字符串数组`, { field });
	const names = [];
	for (const item of value) {
		if (typeof item !== "string" || item.trim() === "") fail("SANDBOX_POLICY_INVALID", `${field} 只能包含非空字符串`, { field });
		const name = item.trim();
		if (!names.includes(name)) names.push(name);
	}
	return names;
}

function normalizeHosts(value) {
	if (value === void 0) return [];
	if (!Array.isArray(value)) fail("SANDBOX_POLICY_INVALID", "allowHosts 必须是字符串数组", {});
	const hosts = [];
	for (const item of value) {
		if (typeof item !== "string" || item.trim() === "" || /[\s/\\]/u.test(item)) fail("SANDBOX_POLICY_INVALID", "allowHosts 只能包含主机名或 *.域名", { host: item });
		const host = item.trim().toLowerCase().replace(/\.$/u, "");
		if (!hosts.includes(host)) hosts.push(host);
	}
	return hosts;
}

// 出口控制绝不允许访问的地址段：环回、link-local（含云元数据 169.254.169.254）、组播与保留段。
const BLOCKED_IP_RANGES = Object.freeze([
	"0.0.0.0/8",
	"127.0.0.0/8",
	"169.254.0.0/16",
	"224.0.0.0/4",
	"240.0.0.0/4",
	"::/128",
	"::1/128",
	"fe80::/10",
	"ff00::/8"
]);

const RESERVED_DOCKER_NETWORKS = /* @__PURE__ */ new Set(["bridge", "host", "none", "default"]);

function normalizeIpv4Mapped(value) {
	const match = /^::ffff:(\d{1,3}(?:\.\d{1,3}){3})$/iu.exec(value);
	return match ? match[1] : value;
}

function ipv4ToBigInt(ip) {
	const parts = ip.split(".").map(Number);
	if (parts.length !== 4 || parts.some((part) => !Number.isInteger(part) || part < 0 || part > 255)) return null;
	return (BigInt(parts[0]) << 24n) | (BigInt(parts[1]) << 16n) | (BigInt(parts[2]) << 8n) | BigInt(parts[3]);
}

function ipv6ToBigInt(ip) {
	const [head, tail] = ip.split("::");
	const collect = (side) => side === "" ? [] : side.split(":");
	let groups;
	if (tail === void 0) {
		groups = collect(head ?? "");
	} else {
		const left = collect(head ?? "");
		const right = collect(tail);
		const missing = 8 - left.length - right.length;
		if (missing < 0) return null;
		groups = [...left, ...Array.from({ length: missing }, () => "0"), ...right];
	}
	if (groups.length !== 8) return null;
	let result = 0n;
	for (const group of groups) {
		if (!/^[0-9a-f]{1,4}$/iu.test(group)) return null;
		result = (result << 16n) | BigInt(Number.parseInt(group, 16));
	}
	return result;
}

function parseIp(value) {
	if (typeof value !== "string") return null;
	const ip = normalizeIpv4Mapped(value.trim());
	const version = isIP(ip);
	if (version === 4) {
		const parsed = ipv4ToBigInt(ip);
		return parsed === null ? null : Object.freeze({ version, value: parsed, text: ip });
	}
	if (version === 6) {
		const parsed = ipv6ToBigInt(ip);
		return parsed === null ? null : Object.freeze({ version, value: parsed, text: ip });
	}
	return null;
}

function hostBits(version) {
	return BigInt(version === 4 ? 32 : 128);
}

function prefixMask(version, prefix) {
	const bits = hostBits(version);
	return ((1n << bits) - 1n) ^ ((1n << (bits - BigInt(prefix))) - 1n);
}

function parseCidr(value, field) {
	if (typeof value !== "string" || value.trim() === "") fail("SANDBOX_POLICY_INVALID", `${field} 必须是 IP 或 CIDR`, { field, value });
	const parts = value.trim().split("/");
	if (parts.length > 2) fail("SANDBOX_POLICY_INVALID", `${field} 的 CIDR 格式不合法`, { field, value });
	const ip = parseIp(parts[0]);
	if (!ip) fail("SANDBOX_POLICY_INVALID", `${field} 包含非法 IP`, { field, value });
	const max = ip.version === 4 ? 32 : 128;
	let prefix = max;
	if (parts.length === 2) {
		if (!/^\d+$/u.test(parts[1])) fail("SANDBOX_POLICY_INVALID", `${field} 的 CIDR 前缀必须是整数`, { field, value });
		prefix = Number(parts[1]);
		if (prefix < 0 || prefix > max) fail("SANDBOX_POLICY_INVALID", `${field} 的 CIDR 前缀越界`, { field, value });
	}
	return Object.freeze({
		version: ip.version,
		prefix,
		network: ip.value & prefixMask(ip.version, prefix),
		text: `${ip.text}/${prefix}`
	});
}

function inCidr(ip, cidr) {
	if (ip.version !== cidr.version) return false;
	return (ip.value & prefixMask(ip.version, cidr.prefix)) === cidr.network;
}

function cidrOverlaps(a, b) {
	if (a.version !== b.version) return false;
	const left = Object.freeze({ version: a.version, value: a.network, text: "" });
	const right = Object.freeze({ version: b.version, value: b.network, text: "" });
	return inCidr(left, b) || inCidr(right, a);
}

const BLOCKED_CIDRS = Object.freeze(BLOCKED_IP_RANGES.map((range) => parseCidr(range, "blocked")));

function isBlockedIp(ip) {
	return BLOCKED_CIDRS.some((cidr) => inCidr(ip, cidr));
}

function normalizeIpAllowList(value) {
	if (value === void 0) return [];
	if (!Array.isArray(value)) fail("SANDBOX_POLICY_INVALID", "egress.ips 必须是 IP 或 CIDR 数组", {});
	const entries = [];
	for (const item of value) {
		const cidr = parseCidr(item, "egress.ips");
		if (BLOCKED_CIDRS.some((blocked) => cidrOverlaps(cidr, blocked))) fail("SANDBOX_EGRESS_UNCONFIGURED", "出口白名单不能包含环回、link-local 或元数据地址段", { cidr: cidr.text });
		if (!entries.some((entry) => entry.text === cidr.text)) entries.push(cidr);
	}
	return entries;
}

function normalizePorts(value) {
	if (value === void 0) return [443];
	if (!Array.isArray(value) || value.length === 0) fail("SANDBOX_POLICY_INVALID", "egress.ports 必须是非空端口数组", {});
	const ports = [];
	for (const item of value) {
		if (!Number.isInteger(item) || item < 1 || item > 65535) fail("SANDBOX_POLICY_INVALID", "egress.ports 只能包含 1-65535 的整数", { port: item });
		if (!ports.includes(item)) ports.push(item);
	}
	return ports.sort((a, b) => a - b);
}

function normalizeIpText(value, field) {
	const ip = parseIp(value);
	if (!ip) fail("SANDBOX_POLICY_INVALID", `${field} 必须是合法 IP`, { [field]: value });
	if (isBlockedIp(ip)) fail("SANDBOX_EGRESS_UNCONFIGURED", `${field} 不能是环回或 link-local 地址`, { [field]: value });
	return ip;
}

function normalizeDnsServers(value) {
	if (value === void 0) return [];
	if (!Array.isArray(value)) fail("SANDBOX_POLICY_INVALID", "egress.dnsServers 必须是 IP 数组", {});
	const servers = [];
	for (const item of value) {
		const ip = normalizeIpText(item, "egress.dnsServers");
		if (!servers.some((server) => server.text === ip.text)) servers.push(ip);
	}
	return servers;
}

/**
 * 校验 DNS pin：域名必须来自出口白名单，固定地址必须落在白名单 IP 内且不是受保护地址。
 * 未固定的域名在运行期可能被解析到任意地址（DNS Rebinding），因此静态模式下必须显式固定。
 */
function normalizeDnsPins(value, domains) {
	if (value === void 0) return Object.freeze({});
	if (!isPlainObject(value)) fail("SANDBOX_POLICY_INVALID", "egress.dnsPins 必须是 主机名 -> IP 数组 的对象", {});
	const pins = {};
	for (const [rawHost, rawIps] of Object.entries(value)) {
		const host = String(rawHost).trim().toLowerCase().replace(/\.$/u, "");
		if (!domains.includes(host) || host.startsWith("*.")) fail("SANDBOX_EGRESS_UNCONFIGURED", "DNS pin 的主机必须精确出现在出口白名单里", { host });
		if (!Array.isArray(rawIps) || rawIps.length === 0) fail("SANDBOX_EGRESS_UNCONFIGURED", "DNS pin 必须至少包含一个地址", { host });
		const ips = rawIps.map((item) => normalizeIpText(item, `egress.dnsPins.${host}`));
		pins[host] = Object.freeze([...new Set(ips.map((ip) => ip.text))]);
	}
	return Object.freeze(pins);
}

function normalizeProxy(value) {
	if (value === void 0 || value === null) return null;
	if (!isPlainObject(value)) fail("SANDBOX_POLICY_INVALID", "egress.proxy 必须是对象", {});
	if (typeof value.url !== "string" || value.url.trim() === "") fail("SANDBOX_POLICY_INVALID", "egress.proxy 缺少 url", {});
	let parsed;
	try {
		parsed = new URL(value.url.trim());
	} catch {
		fail("SANDBOX_POLICY_INVALID", "egress.proxy.url 不是合法 URL", { url: value.url });
	}
	if (!["http:", "https:", "socks5:"].includes(parsed.protocol)) fail("SANDBOX_POLICY_INVALID", "egress.proxy 只支持 http/https/socks5", { url: value.url });
	const hostname = parsed.hostname.replace(/^\[|\]$/gu, "");
	const literal = parseIp(hostname);
	const ip = value.ip === void 0
		? literal && !isBlockedIp(literal) ? literal : null
		: normalizeIpText(value.ip, "egress.proxy.ip");
	const port = Number(parsed.port || (parsed.protocol === "https:" ? 443 : parsed.protocol === "socks5:" ? 1080 : 80));
	if (!Number.isInteger(port) || port < 1 || port > 65535) fail("SANDBOX_POLICY_INVALID", "egress.proxy 端口不合法", { url: value.url });
	return Object.freeze({ url: parsed.toString(), host: hostname, port, ip });
}

function normalizeEgressNetwork(value) {
	if (typeof value !== "string" || value.trim() === "") fail("SANDBOX_EGRESS_UNCONFIGURED", "受限网络必须绑定预置的专用 Docker 网络", {});
	const name = value.trim();
	if (RESERVED_DOCKER_NETWORKS.has(name.toLowerCase())) fail("SANDBOX_EGRESS_UNCONFIGURED", "受限网络不能使用 Docker 默认网络或宿主网络", { network: name });
	if (!/^[a-zA-Z0-9][a-zA-Z0-9_.-]{0,62}$/u.test(name)) fail("SANDBOX_POLICY_INVALID", "出口网络名不合法", { network: name });
	return name;
}

/**
 * 编译服务端出口策略。授权范围只由这里的白名单决定：
 * - 域名必须提供 DNS pin，或整体经由受控代理，否则拒绝（防 DNS Rebinding）；
 * - IP/CIDR、端口受白名单约束，环回/link-local/元数据地址段一律拒绝；
 * - 必须绑定非默认 Docker 网络，未配置出口控制时不允许启用受限网络。
 */
export function compileEgressPolicy(options = {}) {
	if (!isPlainObject(options)) fail("SANDBOX_POLICY_INVALID", "出口策略必须是对象", {});
	const domains = normalizeHosts(options.domains ?? options.allowHosts);
	const ips = normalizeIpAllowList(options.ips);
	const ports = normalizePorts(options.ports);
	const proxy = normalizeProxy(options.proxy);
	const dnsServers = normalizeDnsServers(options.dnsServers);
	const dnsPins = normalizeDnsPins(options.dnsPins, domains);
	const network = normalizeEgressNetwork(options.network ?? options.networkName);
	if (domains.length === 0 && ips.length === 0) fail("SANDBOX_EGRESS_UNCONFIGURED", "受限网络缺少域名或 IP 出口白名单", {});
	if (ips.length > 0) {
		for (const [host, pinned] of Object.entries(dnsPins)) {
			for (const value of pinned) {
				const ip = parseIp(value);
				if (!ips.some((cidr) => inCidr(ip, cidr))) fail("SANDBOX_EGRESS_UNCONFIGURED", "DNS pin 地址必须落在出口白名单 IP 内", { host, ip: value });
			}
		}
	}
	if (proxy === null) {
		const wildcard = domains.filter((domain) => domain.startsWith("*."));
		if (wildcard.length > 0) fail("SANDBOX_EGRESS_UNCONFIGURED", "通配域名必须经由受控代理，否则无法阻止 DNS 重绑定", { domains: wildcard });
		for (const domain of domains) {
			if (!dnsPins[domain] || dnsPins[domain].length === 0) fail("SANDBOX_EGRESS_UNCONFIGURED", "域名出口必须配置 DNS pin 或受控代理", { domain });
		}
	} else if (proxy.ip === null && dnsServers.length === 0) {
		fail("SANDBOX_EGRESS_UNCONFIGURED", "代理地址不是 IP 时必须配置受控 DNS 服务器", { proxy: proxy.url });
	}
	return Object.freeze({
		configured: true,
		network,
		domains: Object.freeze([...domains]),
		ips: Object.freeze(ips.map((ip) => ip.text)),
		ports: Object.freeze([...ports]),
		blocked: Object.freeze([...BLOCKED_IP_RANGES]),
		dns: Object.freeze({
			mode: proxy ? "proxy" : dnsServers.length > 0 ? "restricted" : "pinned",
			servers: Object.freeze(dnsServers.map((server) => server.text)),
			pins: dnsPins
		}),
		proxy
	});
}

function automaticKind(taskRisk, availableKinds) {
	const order = taskRisk === "high" ? ["docker", "restricted_network"] : ["read_only_workspace", "docker", "restricted_network", "local_process"];
	const kind = order.find((item) => availableKinds.includes(item));
	if (!kind) {
		if (taskRisk === "high") fail("HIGH_RISK_REQUIRES_ISOLATION", "高风险任务没有可用的隔离沙箱", { task_risk: taskRisk, available_kinds: availableKinds });
		fail("SANDBOX_UNAVAILABLE", "宿主没有可用的执行沙箱", { task_risk: taskRisk, available_kinds: availableKinds });
	}
	return kind;
}

/**
 * 解析服务端沙箱策略。Agent 只能收到这里算出的结果，不能自行扩大目录、网络或工具范围。
 * 高风险任务只接受容器级隔离；默认配置下没有容器就直接拒绝，而不是退回宿主进程。
 */
export function resolveSandboxPolicy(options = {}) {
	if (!isPlainObject(options)) fail("SANDBOX_POLICY_INVALID", "沙箱策略参数必须是对象", {});
	const taskRisk = options.taskRisk ?? "low";
	if (!TASK_RISKS.includes(taskRisk)) fail("SANDBOX_POLICY_INVALID", "未知任务风险等级", { task_risk: taskRisk });
	const availableKinds = options.availableKinds === void 0 ? ["read_only_workspace"] : normalizeKinds(options.availableKinds);
	const requested = options.requestedKind ?? automaticKind(taskRisk, availableKinds);
	if (!SANDBOX_KINDS.includes(requested)) fail("SANDBOX_POLICY_INVALID", "未知沙箱类型", { kind: requested });
	if (!availableKinds.includes(requested)) fail("SANDBOX_UNAVAILABLE", "请求的沙箱类型在当前宿主不可用", {
		kind: requested,
		available_kinds: availableKinds
	});
	const rule = KIND_RULES[requested];
	if (requested === "local_process" && options.allowHostExecution !== true) fail("HOST_EXECUTION_NOT_GRANTED", "本地进程沙箱需要显式宿主执行授权", { kind: requested });
	if (taskRisk === "high" && !rule.container) fail("HIGH_RISK_REQUIRES_ISOLATION", "高风险任务禁止直接在真实宿主执行", { kind: requested });
	const networkMode = options.network ?? rule.networkDefault;
	if (!NETWORK_MODES.includes(networkMode) || !rule.network.includes(networkMode)) fail("SANDBOX_POLICY_INVALID", "沙箱网络策略与该类型不兼容", {
		kind: requested,
		network: networkMode,
		allowed: rule.network
	});
	if (taskRisk === "high" && networkMode === "host") fail("HIGH_RISK_REQUIRES_ISOLATION", "高风险任务不允许宿主网络", { kind: requested });
	const allowHosts = normalizeHosts(options.allowHosts);
	const egressConfig = isPlainObject(options.egress) ? options.egress : {};
	let egress = null;
	if (networkMode === "allowlist") {
		// 出口授权范围只来自服务端配置；Agent 提交的 networkHost 不能参与这里的计算。
		egress = compileEgressPolicy({
			domains: allowHosts.length > 0 ? allowHosts : egressConfig.domains,
			ips: egressConfig.ips,
			ports: egressConfig.ports,
			dnsPins: egressConfig.dnsPins,
			dnsServers: egressConfig.dnsServers,
			proxy: egressConfig.proxy,
			network: egressConfig.network ?? egressConfig.networkName
		});
	}
	const workspaceRoot = canonicalPath(normalizeRoot(options.workspaceRoot, "workspaceRoot"), "SANDBOX_POLICY_INVALID");
	const roots = canonicalRoots(normalizeRoots(options.allowedRoots, workspaceRoot));
	if (!roots.some((root) => insideRoot(root, workspaceRoot))) fail("SANDBOX_POLICY_INVALID", "工作区不在允许的目录范围内", {
		workspace_root: workspaceRoot,
		allowed_roots: roots
	});
	const tools = isPlainObject(options.tools) ? options.tools : {};
	const allow = normalizeNames(tools.allow, "tools.allow");
	const deny = normalizeNames(tools.deny, "tools.deny");
	const readOnly = requested === "read_only_workspace" || options.allowWrite !== true;
	return Object.freeze({
		kind: requested,
		task_risk: taskRisk,
		isolation: rule.isolation,
		container: rule.container,
		filesystem: Object.freeze({
			workspace_root: workspaceRoot,
			roots: Object.freeze(roots),
			read_only: readOnly
		}),
		network: Object.freeze({
			mode: networkMode,
			allow_hosts: Object.freeze(egress ? [...egress.domains] : allowHosts),
			allow_ips: Object.freeze(egress ? [...egress.ips] : []),
			allow_ports: Object.freeze(egress ? [...egress.ports] : []),
			blocked_ips: Object.freeze([...BLOCKED_IP_RANGES]),
			dns: egress ? egress.dns : Object.freeze({
				mode: "none",
				servers: Object.freeze([]),
				pins: Object.freeze({})
			}),
			egress
		}),
		tools: Object.freeze({
			allow: Object.freeze(allow),
			deny: Object.freeze(deny),
			default: "deny"
		}),
		allow_host_execution: options.allowHostExecution === true,
		fail_closed: true
	});
}

function assertPolicy(policy) {
	if (!isPlainObject(policy) || !SANDBOX_KINDS.includes(policy.kind) || !TASK_RISKS.includes(policy.task_risk)) fail("SANDBOX_POLICY_INVALID", "沙箱策略结构不完整", {});
	if (!isPlainObject(policy.filesystem) || !Array.isArray(policy.filesystem.roots) || typeof policy.filesystem.workspace_root !== "string" || typeof policy.filesystem.read_only !== "boolean") fail("SANDBOX_POLICY_INVALID", "沙箱文件策略不完整", {});
	if (!isPlainObject(policy.network) || !NETWORK_MODES.includes(policy.network.mode) || !Array.isArray(policy.network.allow_hosts)) fail("SANDBOX_POLICY_INVALID", "沙箱网络策略不完整", {});
	if (policy.network.mode === "allowlist") {
		if (!isPlainObject(policy.network.egress) || policy.network.egress.configured !== true) fail("SANDBOX_EGRESS_UNCONFIGURED", "受限网络缺少已编译的出口控制配置", {});
		if (!Array.isArray(policy.network.allow_ips) || !Array.isArray(policy.network.allow_ports) || !Array.isArray(policy.network.blocked_ips) || !isPlainObject(policy.network.dns)) fail("SANDBOX_POLICY_INVALID", "受限网络的出口控制结构不完整", {});
	}
	if (!isPlainObject(policy.tools) || !Array.isArray(policy.tools.allow) || !Array.isArray(policy.tools.deny)) fail("SANDBOX_POLICY_INVALID", "沙箱工具策略不完整", {});
	return policy;
}

/** 高风险（不可逆）任务必须落到容器级隔离且不能使用宿主网络。 */
export function assertSandboxForTask({ sideEffectPolicy = "pure", policy } = {}) {
	assertPolicy(policy);
	const risk = EFFECT_RISK[sideEffectPolicy];
	if (!risk) fail("SANDBOX_POLICY_INVALID", "未知副作用等级", { side_effect_policy: sideEffectPolicy });
	if (risk === "high" && (policy.isolation !== "container" || policy.network.mode === "host")) fail("HIGH_RISK_REQUIRES_ISOLATION", "高风险任务禁止直接在真实宿主执行", {
		kind: policy.kind,
		network: policy.network.mode
	});
	return policy;
}

function hostMatches(allowHosts, host) {
	const target = host.toLowerCase().replace(/\.$/u, "");
	return allowHosts.some((rule) => rule.startsWith("*.") ? target === rule.slice(2) || target.endsWith(rule.slice(1)) : target === rule);
}

function normalizeRunRequest(request) {
	if (!isPlainObject(request)) fail("SANDBOX_POLICY_INVALID", "执行请求必须是对象", {});
	const { bin, args = [], cwd, timeoutMs = 30000, tool, capability = "read_only", networkHost, networkIp, networkPort, signal } = request;
	if (typeof bin !== "string" || bin === "" || bin.includes("\0")) fail("SANDBOX_POLICY_INVALID", "执行请求缺少可执行文件", {});
	if (!Array.isArray(args) || args.some((arg) => typeof arg !== "string" || arg.includes("\0"))) fail("SANDBOX_POLICY_INVALID", "执行参数必须是字符串数组", {});
	if (typeof cwd !== "string" || cwd === "" || cwd.includes("\0")) fail("SANDBOX_POLICY_INVALID", "执行请求缺少工作目录", {});
	if (!Number.isSafeInteger(timeoutMs) || timeoutMs < 1 || timeoutMs > 3600000) fail("SANDBOX_POLICY_INVALID", "执行超时须在 1 毫秒到 1 小时之间", { timeout_ms: timeoutMs });
	if (tool !== void 0 && typeof tool !== "string") fail("SANDBOX_POLICY_INVALID", "tool 必须是字符串", {});
	if (networkHost !== void 0 && (typeof networkHost !== "string" || networkHost.trim() === "")) fail("SANDBOX_POLICY_INVALID", "networkHost 必须是非空字符串", {});
	if (networkIp !== void 0 && (typeof networkIp !== "string" || networkIp.trim() === "")) fail("SANDBOX_POLICY_INVALID", "networkIp 必须是非空字符串", {});
	if (networkPort !== void 0 && (!Number.isInteger(networkPort) || networkPort < 1 || networkPort > 65535)) fail("SANDBOX_POLICY_INVALID", "networkPort 必须是 1-65535 的整数", { network_port: networkPort });
	if (networkPort !== void 0 && networkHost === void 0 && networkIp === void 0) fail("SANDBOX_POLICY_INVALID", "networkPort 必须与 networkHost 或 networkIp 一起提交", {});
	return {
		bin,
		args,
		cwd,
		timeoutMs,
		...(tool === void 0 ? {} : { tool }),
		capability,
		...(networkHost === void 0 ? {} : { networkHost }),
		...(networkIp === void 0 ? {} : { networkIp }),
		...(networkPort === void 0 ? {} : { networkPort }),
		...(signal === void 0 ? {} : { signal })
	};
}

/**
 * 创建统一执行沙箱。实际执行交给按类型注入的 adapter；
 * 未注入 adapter 或 adapter 不可用时拒绝执行，而不是回退到宿主进程。
 */
export function createExecutionSandbox({ policy, adapters = {} } = {}) {
	assertPolicy(policy);
	if (!isPlainObject(adapters)) fail("SANDBOX_POLICY_INVALID", "adapters 必须是对象", {});
	const adapter = adapters[policy.kind];
	const roots = canonicalRoots(policy.filesystem.roots);
	const workspaceRoot = canonicalPath(policy.filesystem.workspace_root, "SANDBOX_POLICY_INVALID");
	if (!roots.some((root) => insideRoot(root, workspaceRoot))) fail("SANDBOX_POLICY_INVALID", "工作区不在允许的目录范围内", {
		workspace_root: workspaceRoot,
		allowed_roots: roots
	});
	const sandbox = {
		kind: policy.kind,
		isolation: policy.isolation,
		policy,
		describe: () => Object.freeze({
			kind: policy.kind,
			isolation: policy.isolation,
			task_risk: policy.task_risk,
			workspace_root: workspaceRoot,
			read_only: policy.filesystem.read_only,
			network: policy.network.mode,
			allow_hosts: [...policy.network.allow_hosts],
			tools: [...policy.tools.allow],
			fail_closed: true
		}),
		assertTool(input = {}) {
			if (!isPlainObject(input) || typeof input.name !== "string" || input.name.trim() === "") fail("SANDBOX_POLICY_INVALID", "缺少工具名", {});
			const name = input.name.trim();
			const capability = input.capability ?? "read_only";
			if (!["read_only", "mutating", "compensating"].includes(capability)) fail("SANDBOX_POLICY_INVALID", "未知工具能力", { tool: name, capability });
			if (policy.tools.deny.includes(name)) fail("SANDBOX_TOOL_DENIED", "工具在沙箱禁用清单里", { tool: name });
			if (!policy.tools.allow.includes(name)) fail("SANDBOX_TOOL_DENIED", "工具不在沙箱授权清单里", { tool: name });
			if (capability !== "read_only" && policy.filesystem.read_only) fail("SANDBOX_TOOL_DENIED", "只读沙箱不允许执行写工具", { tool: name, capability });
			return true;
		},
		assertPath(target) {
			if (typeof target !== "string" || target === "" || target.includes("\0")) fail("SANDBOX_POLICY_INVALID", "路径必须是非空字符串", {});
			const absolute = isAbsolute(target) ? resolve(target) : resolve(workspaceRoot, target);
			if (!roots.some((root) => insideRoot(root, absolute))) fail("SANDBOX_PATH_DENIED", "路径越出沙箱允许范围", {
				path: absolute,
				allowed_roots: [...roots]
			});
			const real = canonicalPath(absolute);
			if (!roots.some((root) => insideRoot(root, real))) fail("SANDBOX_PATH_DENIED", "符号链接指向沙箱允许范围之外", {
				path: absolute,
				real_path: real,
				allowed_roots: [...roots]
			});
			return real;
		},
		assertNetwork(input = {}) {
			if (!isPlainObject(input)) fail("SANDBOX_POLICY_INVALID", "网络访问断言必须是对象", {});
			const host = typeof input.host === "string" ? input.host.trim() : void 0;
			const ipText = typeof input.ip === "string" ? input.ip.trim() : void 0;
			const port = input.port;
			if (host === void 0 && ipText === void 0) fail("SANDBOX_POLICY_INVALID", "缺少网络主机名或 IP", {});
			if (host !== void 0 && host === "") fail("SANDBOX_POLICY_INVALID", "网络主机名不能为空", {});
			if (ipText !== void 0 && ipText === "") fail("SANDBOX_POLICY_INVALID", "网络 IP 不能为空", {});
			if (port !== void 0 && (!Number.isInteger(port) || port < 1 || port > 65535)) fail("SANDBOX_POLICY_INVALID", "网络端口必须是 1-65535 的整数", { port });
			if (policy.network.mode === "none") fail("SANDBOX_NETWORK_DENIED", "沙箱不允许网络访问", { host, ip: ipText, port });
			if (policy.network.mode === "allowlist") {
				if (!isPlainObject(policy.network.egress)) fail("SANDBOX_EGRESS_UNCONFIGURED", "受限网络未配置出口控制", { host, ip: ipText });
				if (port !== void 0 && !policy.network.allow_ports.includes(port)) fail("SANDBOX_NETWORK_DENIED", "目标端口不在出口白名单里", {
					port,
					allow_ports: [...policy.network.allow_ports]
				});
				if (host !== void 0 && !hostMatches(policy.network.allow_hosts, host)) fail("SANDBOX_NETWORK_DENIED", "网络主机不在沙箱允许清单里", {
					host,
					allow_hosts: [...policy.network.allow_hosts]
				});
				const pinned = host === void 0 ? void 0 : policy.network.dns.pins[host.toLowerCase().replace(/\.$/u, "")];
				if (host !== void 0 && policy.network.dns.mode === "pinned" && !pinned) fail("SANDBOX_NETWORK_DENIED", "域名没有固定的解析地址，无法阻止 DNS 重绑定", { host });
				if (ipText !== void 0) {
					const ip = parseIp(ipText);
					if (!ip) fail("SANDBOX_POLICY_INVALID", "网络 IP 格式不合法", { ip: ipText });
					if (isBlockedIp(ip)) fail("SANDBOX_NETWORK_DENIED", "目标是环回、link-local 或元数据地址段", { ip: ip.text });
					if (pinned) {
						if (!pinned.includes(ip.text)) fail("SANDBOX_NETWORK_DENIED", "解析地址与 DNS pin 不一致", { host, ip: ip.text, pinned: [...pinned] });
					} else if (policy.network.allow_ips.length > 0) {
						if (!policy.network.allow_ips.some((cidr) => inCidr(ip, parseCidr(cidr, "policy")))) fail("SANDBOX_NETWORK_DENIED", "IP 不在出口白名单里", {
							ip: ip.text,
							allow_ips: [...policy.network.allow_ips]
						});
					} else if (!Object.values(policy.network.dns.pins).flat().includes(ip.text)) fail("SANDBOX_NETWORK_DENIED", "IP 不属于任何固定的出口地址", { ip: ip.text });
				}
			}
			return true;
		},
		assertTask(sideEffectPolicy = "pure") {
			return assertSandboxForTask({ sideEffectPolicy, policy });
		},
		async run(request) {
			const normalized = normalizeRunRequest(request);
			if (normalized.tool !== void 0) sandbox.assertTool({ name: normalized.tool, capability: normalized.capability });
			const cwd = sandbox.assertPath(normalized.cwd);
			// 这里的断言只用于校验调用方声明；实际出口范围由 policy 决定，networkHost 不参与授权计算。
			if (normalized.networkHost !== void 0 || normalized.networkIp !== void 0) {
				sandbox.assertNetwork({
					...(normalized.networkHost === void 0 ? {} : { host: normalized.networkHost }),
					...(normalized.networkIp === void 0 ? {} : { ip: normalized.networkIp }),
					...(normalized.networkPort === void 0 ? {} : { port: normalized.networkPort })
				});
			}
			if (!adapter || typeof adapter.run !== "function") fail("SANDBOX_UNAVAILABLE", "当前沙箱类型没有可用的执行器", { kind: policy.kind });
			if (typeof adapter.available === "function" && !adapter.available()) fail("SANDBOX_UNAVAILABLE", "沙箱执行器在当前宿主不可用", { kind: policy.kind });
			// TOCTOU：准入与执行之间路径可能被替换成符号链接，执行前按真实路径再复核一次。
			const rechecked = sandbox.assertPath(cwd);
			if (rechecked !== cwd) fail("SANDBOX_PATH_DENIED", "执行前路径真实位置发生变化", { cwd, real_path: rechecked });
			let result;
			try {
				result = await adapter.run({ ...normalized, cwd }, policy);
			} catch (error) {
				if (error instanceof SandboxError) throw error;
				throw new SandboxError("SANDBOX_FAILED", "沙箱执行失败", { kind: policy.kind, message: String(error?.message ?? error) });
			}
			if (!isPlainObject(result) || !Number.isInteger(result.exitCode) || typeof result.stdout !== "string" || typeof result.stderr !== "string") fail("SANDBOX_RESULT_INVALID", "沙箱执行结果格式不合法", { kind: policy.kind });
			return {
				exitCode: result.exitCode,
				stdout: result.stdout,
				stderr: result.stderr,
				boundary: result.boundary ?? `sandbox:${policy.kind}`,
				...(result.timedOut === true ? { timedOut: true } : {})
			};
		},
		async dispose() {
			if (adapter && typeof adapter.dispose === "function") await adapter.dispose();
		}
	};
	return Object.freeze(sandbox);
}

/** 只读工作区：只能跑注册的只读工具，目录固定在工作区根内。 */
export function createReadOnlyWorkspaceAdapter({ run } = {}) {
	return {
		kind: "read_only_workspace",
		available: () => typeof run === "function",
		async run(request, policy) {
			if (!policy.filesystem.read_only) fail("SANDBOX_TOOL_DENIED", "只读工作区沙箱不允许可写挂载", { kind: policy.kind });
			if (typeof run !== "function") fail("SANDBOX_UNAVAILABLE", "只读工作区执行器未装配", {});
			return run(request, policy);
		}
	};
}

/** 本地进程：只有在服务端显式授予宿主执行权限时才可用。 */
export function createLocalProcessAdapter({ run } = {}) {
	return {
		kind: "local_process",
		available: () => typeof run === "function",
		async run(request, policy) {
			if (policy.allow_host_execution !== true) fail("HOST_EXECUTION_NOT_GRANTED", "本地进程沙箱未获宿主执行授权", {});
			if (typeof run !== "function") fail("SANDBOX_UNAVAILABLE", "本地进程执行器未装配", {});
			return run(request, policy);
		}
	};
}

function posixJoin(base, rel) {
	if (rel === "" || rel === ".") return base;
	return `${base}/${rel.split(/[\\/]/u).filter(Boolean).join("/")}`;
}

function containerName(prefix) {
	return `${prefix}-${randomBytes(8).toString("hex")}`.toLowerCase().replace(/[^a-z0-9_.-]/gu, "-").slice(0, 63);
}

/**
 * 运行容器并跟踪生命周期。
 * 取消或超时时先按容器引用执行 docker rm -f，再结束 CLI 进程，
 * 避免只杀掉 docker 客户端却把容器和内部进程留在宿主上。
 */
function runContainer({ kind, spawnImpl, dockerBin, args, timeoutMs, signal, maxOutputBytes, cleanup, onSpawn }) {
	return new Promise((resolvePromise, reject) => {
		let child;
		try {
			child = spawnImpl(dockerBin, args, {
				stdio: ["ignore", "pipe", "pipe"],
				windowsHide: true
			});
		} catch (error) {
			reject(new SandboxError("SANDBOX_UNAVAILABLE", "无法启动容器运行时", { message: String(error?.message ?? error) }));
			return;
		}
		try {
			onSpawn?.(child);
		} catch {}
		let stdout = "";
		let stderr = "";
		let timedOut = false;
		let settled = false;
		let cleaned = false;
		let forceTimer;
		const runCleanup = () => {
			if (cleaned) return;
			cleaned = true;
			try {
				cleanup?.();
			} catch {}
		};
		const settle = (fn) => {
			if (settled) return;
			settled = true;
			clearTimeout(timer);
			clearTimeout(forceTimer);
			signal?.removeEventListener("abort", onAbort);
			fn();
		};
		const finish = (code) => settle(() => resolvePromise({
			exitCode: timedOut ? 124 : Number.isInteger(code) ? code : 1,
			stdout,
			stderr,
			boundary: `sandbox:${kind}`,
			...(timedOut ? { timedOut: true } : {})
		}));
		const onAbort = () => {
			runCleanup();
			try {
				child.kill("SIGKILL");
			} catch {}
			if (forceTimer) return;
			forceTimer = setTimeout(() => {
				if (timedOut) finish(124);
				else settle(() => reject(signal?.reason ?? new SandboxError("SANDBOX_FAILED", "沙箱执行已取消", { kind })));
			}, 5000);
		};
		const timer = setTimeout(() => {
			timedOut = true;
			onAbort();
		}, timeoutMs);
		signal?.addEventListener("abort", onAbort, { once: true });
		if (signal?.aborted) onAbort();
		child.stdout?.setEncoding?.("utf8");
		child.stderr?.setEncoding?.("utf8");
		child.stdout?.on?.("data", (chunk) => {
			if (stdout.length < maxOutputBytes) stdout += chunk;
		});
		child.stderr?.on?.("data", (chunk) => {
			if (stderr.length < maxOutputBytes) stderr += chunk;
		});
		child.once?.("error", (error) => {
			runCleanup();
			settle(() => reject(new SandboxError("SANDBOX_UNAVAILABLE", "容器运行时启动失败", { message: String(error?.message ?? error) })));
		});
		child.once?.("close", (code) => {
			if (signal?.aborted) {
				settle(() => reject(signal?.reason ?? new SandboxError("SANDBOX_FAILED", "沙箱执行已取消", { kind })));
				return;
			}
			finish(code);
		});
	});
}

function positiveInteger(value, fallback, field) {
	const number = value ?? fallback;
	if (!Number.isSafeInteger(number) || number < 1) fail("SANDBOX_POLICY_INVALID", `${field} 必须是正整数`, { [field]: number });
	return number;
}

function cpuLimit(value) {
	const cpus = value ?? "1";
	const text = typeof cpus === "number" ? String(cpus) : cpus;
	if (typeof text !== "string" || !/^[0-9]+(?:\.[0-9]+)?$/u.test(text) || Number(text) <= 0) fail("SANDBOX_POLICY_INVALID", "cpus 必须是正数", { cpus: value });
	return text;
}

/**
 * 把出口策略转成容器参数：固定域名解析、锁定 DNS、注入受控代理。
 * 这些参数只来自服务端编译后的 policy，调用方无法通过请求参数扩大范围。
 */
function egressContainerArgs(egress) {
	const args = [];
	for (const [host, ips] of Object.entries(egress.dns.pins ?? {})) {
		for (const ip of ips) args.push("--add-host", `${host}:${ip}`);
	}
	if (egress.proxy) {
		if (egress.proxy.ip && isIP(egress.proxy.host) === 0) args.push("--add-host", `${egress.proxy.host}:${egress.proxy.ip.text}`);
		for (const key of ["HTTP_PROXY", "HTTPS_PROXY", "ALL_PROXY", "http_proxy", "https_proxy", "all_proxy"]) args.push("-e", `${key}=${egress.proxy.url}`);
		args.push("-e", "NO_PROXY=localhost,127.0.0.1,::1");
	}
	const servers = egress.dns.servers ?? [];
	for (const server of servers) args.push("--dns", server);
	// 未配置受控 DNS 时禁用外部解析，容器只能使用 /etc/hosts 中固定的地址。
	if (servers.length === 0) args.push("--dns", "127.0.0.1");
	return args;
}

function containerAdapter(kind, options = {}) {
	if (!isPlainObject(options)) fail("SANDBOX_POLICY_INVALID", "容器沙箱参数必须是对象", {});
	const image = options.image;
	const dockerBin = options.dockerBin ?? "docker";
	const spawnImpl = options.spawnImpl ?? spawn;
	const user = options.user ?? "65534:65534";
	const memory = options.memory ?? "512m";
	const pidsLimit = positiveInteger(options.pidsLimit, 256, "pidsLimit");
	const nofile = positiveInteger(options.nofile, 256, "nofile");
	const cpus = cpuLimit(options.cpus);
	const tmpSize = options.tmpSize ?? "64m";
	const networkName = options.networkName;
	const maxOutputBytes = options.maxOutputBytes ?? 1048576;
	const available = typeof image === "string" && image.trim() !== "";
	const active = new Map();
	const removeContainer = (reference) => {
		if (typeof reference !== "string" || reference === "") return;
		try {
			spawnImpl(dockerBin, ["rm", "-f", reference], { stdio: "ignore", windowsHide: true })?.once?.("error", () => {});
		} catch {}
	};
	const adapter = {
		kind,
		available: () => available,
		buildArgs(request, policy) {
			if (!available) fail("SANDBOX_UNAVAILABLE", "容器沙箱没有配置镜像", { kind });
			const allowlist = policy.network.mode === "allowlist";
			const egress = allowlist ? policy.network.egress : null;
			if (allowlist && !isPlainObject(egress)) fail("SANDBOX_EGRESS_UNCONFIGURED", "受限网络沙箱缺少出口控制配置", { kind });
			if (allowlist && networkName !== void 0 && networkName !== egress.network) fail("SANDBOX_POLICY_INVALID", "适配器网络名与出口策略不一致", {
				kind,
				adapter_network: networkName,
				policy_network: egress.network
			});
			const root = policy.filesystem.workspace_root;
			const rel = relative(root, request.cwd);
			if (rel.startsWith("..") || isAbsolute(rel)) fail("SANDBOX_PATH_DENIED", "容器工作目录越出挂载范围", { cwd: request.cwd });
			const containerCwd = posixJoin("/workspace", rel);
			// 出口网络、固定解析与代理全部来自服务端策略；request.networkHost 不参与构造。
			const egressArgs = allowlist ? egressContainerArgs(egress) : [];
			return [
				"run",
				"--rm",
				"--network",
				allowlist ? egress.network : "none",
				...egressArgs,
				"--user",
				user,
				"--read-only",
				"--cap-drop",
				"ALL",
				"--security-opt",
				"no-new-privileges",
				"--pids-limit",
				String(pidsLimit),
				"--memory",
				memory,
				"--memory-swap",
				memory,
				"--cpus",
				cpus,
				"--ulimit",
				`nofile=${nofile}:${nofile}`,
				"--tmpfs",
				`/tmp:rw,noexec,nosuid,size=${tmpSize}`,
				"-v",
				`${root}:/workspace${policy.filesystem.read_only ? ":ro" : ":rw"}`,
				"-w",
				containerCwd,
				image.trim(),
				request.bin,
				...request.args
			];
		},
		async run(request, policy) {
			const args = adapter.buildArgs(request, policy);
			const name = containerName("dsh-orchestra");
			const directory = mkdtempSync(join(tmpdir(), "dsh-sandbox-"));
			const cidFile = join(directory, "container.cid");
			args.splice(1, 0, "--name", name, "--cidfile", cidFile);
			const reference = () => {
				try {
					const id = readFileSync(cidFile, "utf8").trim();
					if (/^[a-f0-9]{12,64}$/u.test(id)) return id;
				} catch {}
				return name;
			};
			active.set(name, { reference, child: null });
			try {
				return await runContainer({
					kind,
					spawnImpl,
					dockerBin,
					args,
					timeoutMs: request.timeoutMs,
					signal: request.signal,
					maxOutputBytes,
					onSpawn: (child) => {
						const entry = active.get(name);
						if (entry) entry.child = child;
					},
					cleanup: () => {
						active.delete(name);
						removeContainer(reference());
					}
				});
			} finally {
				active.delete(name);
				rmSync(directory, { recursive: true, force: true });
			}
		},
		dispose() {
			for (const [name, entry] of [...active]) {
				active.delete(name);
				removeContainer(entry.reference());
				try {
					entry.child?.kill?.("SIGKILL");
				} catch {}
			}
		}
	};
	return adapter;
}

/** Docker 隔离容器：默认无网络、只读根文件系统、非 root 用户、丢弃全部 capability。 */
export function createDockerAdapter(options = {}) {
	return containerAdapter("docker", options);
}

/** 受限网络沙箱：容器接入专用出口网络，只允许策略列出的主机。 */
export function createRestrictedNetworkAdapter(options = {}) {
	return containerAdapter("restricted_network", options);
}
