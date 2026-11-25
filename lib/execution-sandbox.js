// 统一执行沙箱：目录、网络与工具权限全部由服务端策略决定。
// 默认 fail-closed：策略不完整、运行时不可用或高风险任务落到宿主进程时一律拒绝。
import { spawn } from "node:child_process";
import { randomBytes } from "node:crypto";
import { mkdtempSync, readFileSync, realpathSync, rmSync } from "node:fs";
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
	if (networkMode === "allowlist" && allowHosts.length === 0) fail("SANDBOX_POLICY_INVALID", "allowlist 网络必须列出允许的主机", { kind: requested });
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
			allow_hosts: Object.freeze(allowHosts)
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
	const { bin, args = [], cwd, timeoutMs = 30000, tool, capability = "read_only", networkHost, signal } = request;
	if (typeof bin !== "string" || bin === "" || bin.includes("\0")) fail("SANDBOX_POLICY_INVALID", "执行请求缺少可执行文件", {});
	if (!Array.isArray(args) || args.some((arg) => typeof arg !== "string" || arg.includes("\0"))) fail("SANDBOX_POLICY_INVALID", "执行参数必须是字符串数组", {});
	if (typeof cwd !== "string" || cwd === "" || cwd.includes("\0")) fail("SANDBOX_POLICY_INVALID", "执行请求缺少工作目录", {});
	if (!Number.isSafeInteger(timeoutMs) || timeoutMs < 1 || timeoutMs > 3600000) fail("SANDBOX_POLICY_INVALID", "执行超时须在 1 毫秒到 1 小时之间", { timeout_ms: timeoutMs });
	if (tool !== void 0 && typeof tool !== "string") fail("SANDBOX_POLICY_INVALID", "tool 必须是字符串", {});
	if (networkHost !== void 0 && (typeof networkHost !== "string" || networkHost.trim() === "")) fail("SANDBOX_POLICY_INVALID", "networkHost 必须是非空字符串", {});
	return {
		bin,
		args,
		cwd,
		timeoutMs,
		...(tool === void 0 ? {} : { tool }),
		capability,
		...(networkHost === void 0 ? {} : { networkHost }),
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
			if (!isPlainObject(input) || typeof input.host !== "string" || input.host.trim() === "") fail("SANDBOX_POLICY_INVALID", "缺少网络主机名", {});
			const host = input.host.trim();
			if (policy.network.mode === "none") fail("SANDBOX_NETWORK_DENIED", "沙箱不允许网络访问", { host });
			if (policy.network.mode === "allowlist" && !hostMatches(policy.network.allow_hosts, host)) fail("SANDBOX_NETWORK_DENIED", "网络主机不在沙箱允许清单里", {
				host,
				allow_hosts: [...policy.network.allow_hosts]
			});
			return true;
		},
		assertTask(sideEffectPolicy = "pure") {
			return assertSandboxForTask({ sideEffectPolicy, policy });
		},
		async run(request) {
			const normalized = normalizeRunRequest(request);
			if (normalized.tool !== void 0) sandbox.assertTool({ name: normalized.tool, capability: normalized.capability });
			const cwd = sandbox.assertPath(normalized.cwd);
			if (normalized.networkHost !== void 0) sandbox.assertNetwork({ host: normalized.networkHost });
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
	const networkName = options.networkName ?? "dsh-orchestra-egress";
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
			if (allowlist && (typeof networkName !== "string" || networkName.trim() === "")) fail("SANDBOX_POLICY_INVALID", "受限网络沙箱需要专用网络名", { kind });
			const root = policy.filesystem.workspace_root;
			const rel = relative(root, request.cwd);
			if (rel.startsWith("..") || isAbsolute(rel)) fail("SANDBOX_PATH_DENIED", "容器工作目录越出挂载范围", { cwd: request.cwd });
			const containerCwd = posixJoin("/workspace", rel);
			return [
				"run",
				"--rm",
				"--network",
				allowlist ? networkName.trim() : "none",
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
