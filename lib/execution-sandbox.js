// 执行权限与工作区：目录、工具与本地执行授权全部由服务端策略决定。
// 默认 fail-closed：策略不完整、执行器不可用或未获授权时一律拒绝。
// 本地进程执行只是受限的宿主执行，不是操作系统沙箱，也不是容器隔离。
import { spawn } from "node:child_process";
import { realpathSync } from "node:fs";
import { basename, dirname, isAbsolute, relative, resolve } from "node:path";

export const EXECUTION_MODES = Object.freeze(["read_only_workspace", "local_process"]);
export const LEGACY_EXECUTION_MODES = Object.freeze(["docker", "restricted_network"]);
export const SIDE_EFFECT_POLICIES = Object.freeze(["pure", "compensable", "irreversible"]);
export const EXECUTION_TRUST_LEVELS = Object.freeze(["trusted", "untrusted"]);

const MODE_RULES = Object.freeze({
	read_only_workspace: Object.freeze({
		isolation: "capability",
		writable: false
	}),
	local_process: Object.freeze({
		isolation: "process",
		writable: true
	})
});

export const SANDBOX_ERROR_CODES = Object.freeze([
	"SANDBOX_POLICY_INVALID",
	"SANDBOX_UNAVAILABLE",
	"SANDBOX_TOOL_DENIED",
	"SANDBOX_PATH_DENIED",
	"HOST_EXECUTION_NOT_GRANTED",
	"UNTRUSTED_EXECUTION_DENIED",
	"IRREVERSIBLE_REQUIRES_APPROVAL",
	"COMMAND_NOT_ALLOWED",
	"LEGACY_EXECUTION_CONFIG",
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

function normalizeModes(value) {
	if (!Array.isArray(value)) fail("SANDBOX_POLICY_INVALID", "availableModes 必须是数组", {});
	const modes = [];
	for (const item of value) {
		if (!EXECUTION_MODES.includes(item)) fail("SANDBOX_POLICY_INVALID", "存在未知执行模式", { mode: item });
		if (!modes.includes(item)) modes.push(item);
	}
	if (modes.length === 0) fail("SANDBOX_UNAVAILABLE", "宿主没有可用的执行模式", {});
	return modes;
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

// 危险命令清单只做防御性拦截，不构成权限边界；解释器仍可能执行任意代码。
const DENIED_EXECUTABLES = Object.freeze([
	"rm",
	"rmdir",
	"del",
	"rd",
	"dd",
	"mkfs",
	"mkfs.ext4",
	"mkfs.xfs",
	"shred",
	"truncate",
	"sudo",
	"su",
	"doas",
	"runas",
	"shutdown",
	"reboot",
	"halt",
	"poweroff",
	"kill",
	"pkill",
	"killall",
	"taskkill",
	"chown",
	"chgrp",
	"mount",
	"umount",
	"systemctl",
	"launchctl",
	"diskutil",
	"format",
	"netsh",
	"reg",
	"sc",
	"certutil",
	"bcdedit",
	"sh",
	"bash",
	"zsh",
	"fish",
	"csh",
	"ksh",
	"dash",
	"cmd",
	"powershell",
	"pwsh"
]);

const SENSITIVE_ENV = /(?:^|_)(?:KEY|KEYS|TOKEN|TOKENS|SECRET|SECRETS|PASSWORD|PASSWD|CREDENTIAL|CREDENTIALS|AUTH|COOKIE|COOKIES|SESSION|PRIVATE)(?:$|_)/iu;
const UNSAFE_ENV = /^(?:NODE_OPTIONS|NODE_PATH|LD_PRELOAD|LD_LIBRARY_PATH|DYLD_[A-Z_]+|PYTHONPATH|PYTHONSTARTUP|BASH_ENV|ENV)$/u;

/** 清理敏感环境变量后再交给子进程；只保留名称，不记录值。 */
export function sanitizeExecutionEnv(env = process.env) {
	const clean = {};
	for (const [key, value] of Object.entries(env ?? {})) {
		if (value === void 0) continue;
		if (SENSITIVE_ENV.test(key) || UNSAFE_ENV.test(key)) continue;
		clean[key] = String(value);
	}
	return Object.freeze(clean);
}

function executableName(bin) {
	const name = basename(bin).toLowerCase();
	return process.platform === "win32" ? name.replace(/\.(?:exe|cmd|bat|com)$/u, "") : name;
}

/**
 * 检测旧版 Docker / 受限网络执行配置。
 * 只报告迁移建议，不做任何自动映射，也不修改输入对象。
 */
export function detectLegacyExecutionConfig(input) {
	const entries = [];
	const seen = new Set();
	const visited = new WeakSet();
	const add = (path, value, reason) => {
		if (seen.has(path)) return;
		seen.add(path);
		entries.push(Object.freeze({
			path,
			value,
			suggested_mode: "read_only_workspace",
			requires_execution_reconfirmation: true,
			reason
		}));
	};
	const walk = (value, path, depth) => {
		if (depth > 8 || value === null || typeof value !== "object") return;
		if (visited.has(value)) return;
		visited.add(value);
		if (Array.isArray(value)) {
			value.forEach((item, index) => walk(item, `${path}[${index}]`, depth + 1));
			return;
		}
		const inSandbox = path === "sandbox" || path.endsWith(".sandbox");
		for (const [key, item] of Object.entries(value)) {
			const childPath = path === "" ? key : `${path}.${key}`;
			if (inSandbox && key === "kind" && typeof item === "string") {
				add(childPath, item, LEGACY_EXECUTION_MODES.includes(item)
					? "旧版 Docker/受限网络执行模式已移除；不能自动映射为 local_process，必须重新确认执行权限。"
					: "旧字段 kind 已改为 mode；请按任务副作用与可信级别重新确认执行权限。");
			} else if (inSandbox && key === "risk" && typeof item === "string") {
				add(childPath, item, "旧版 risk 风险标签不再决定隔离方式；请改用 sideEffectPolicy 与 executionTrust，并重新确认执行权限。");
			} else if (typeof item === "string" && LEGACY_EXECUTION_MODES.includes(item) && ["mode", "kind", "execution_mode", "executionMode"].includes(key)) {
				add(childPath, item, "旧版 Docker/受限网络执行模式已移除；不能自动映射为 local_process，必须重新确认执行权限。");
			}
			walk(item, childPath, depth + 1);
		}
	};
	walk(input, "", 0);
	return Object.freeze({
		legacy: entries.length > 0,
		entries: Object.freeze(entries)
	});
}

/** 命中旧配置时明确拒绝启动或继续，避免静默降级为宿主执行。 */
export function assertNoLegacyExecutionConfig(input, source = "config") {
	const report = detectLegacyExecutionConfig(input);
	if (!report.legacy) return report;
	fail("LEGACY_EXECUTION_CONFIG", "检测到旧版 Docker 执行配置，迁移后才能继续", {
		source,
		entries: report.entries
	});
}

function automaticMode(sideEffectPolicy, executionTrust, availableModes, allowHostExecution) {
	if (executionTrust === "untrusted" || sideEffectPolicy === "pure" || !allowHostExecution) {
		if (availableModes.includes("read_only_workspace")) return "read_only_workspace";
	}
	if (availableModes.includes("local_process")) return "local_process";
	if (availableModes.includes("read_only_workspace")) return "read_only_workspace";
	fail("SANDBOX_UNAVAILABLE", "宿主没有可用的执行模式", {
		side_effect_policy: sideEffectPolicy,
		execution_trust: executionTrust,
		available_modes: availableModes
	});
}

/**
 * 解析服务端执行策略。Agent 只能收到这里算出的结果，不能自行扩大目录、工具或执行范围。
 * 副作用等级与可信级别决定能否本地执行；高风险不再自动等同于容器隔离。
 */
export function resolveSandboxPolicy(options = {}) {
	if (!isPlainObject(options)) fail("SANDBOX_POLICY_INVALID", "执行策略参数必须是对象", {});
	const sideEffectPolicy = options.sideEffectPolicy ?? "pure";
	if (!SIDE_EFFECT_POLICIES.includes(sideEffectPolicy)) fail("SANDBOX_POLICY_INVALID", "未知任务副作用等级", { side_effect_policy: sideEffectPolicy });
	const executionTrust = options.executionTrust ?? "untrusted";
	if (!EXECUTION_TRUST_LEVELS.includes(executionTrust)) fail("SANDBOX_POLICY_INVALID", "未知执行可信级别", { execution_trust: executionTrust });
	const availableModes = options.availableModes === void 0 ? ["read_only_workspace"] : normalizeModes(options.availableModes);
	const requested = options.executionMode ?? automaticMode(sideEffectPolicy, executionTrust, availableModes, options.allowHostExecution === true);
	if (!EXECUTION_MODES.includes(requested)) fail("SANDBOX_POLICY_INVALID", "未知执行模式", { execution_mode: requested });
	if (!availableModes.includes(requested)) fail("SANDBOX_UNAVAILABLE", "请求的执行模式在当前宿主不可用", {
		execution_mode: requested,
		available_modes: availableModes
	});
	if (requested === "local_process") {
		if (executionTrust !== "trusted") fail("UNTRUSTED_EXECUTION_DENIED", "不可信代码禁止直接在宿主执行", {
			execution_mode: requested,
			execution_trust: executionTrust
		});
		if (options.allowHostExecution !== true) fail("HOST_EXECUTION_NOT_GRANTED", "本地进程执行需要显式宿主执行授权", { execution_mode: requested });
		if (sideEffectPolicy === "pure") fail("SANDBOX_POLICY_INVALID", "纯任务只允许只读工作区，不能本地执行命令", {
			execution_mode: requested,
			side_effect_policy: sideEffectPolicy
		});
	}
	const rule = MODE_RULES[requested];
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
		execution_mode: requested,
		side_effect_policy: sideEffectPolicy,
		execution_trust: executionTrust,
		isolation: rule.isolation,
		filesystem: Object.freeze({
			workspace_root: workspaceRoot,
			roots: Object.freeze(roots),
			read_only: readOnly
		}),
		tools: Object.freeze({
			allow: Object.freeze(allow),
			deny: Object.freeze(deny),
			default: "deny"
		}),
		allow_host_execution: options.allowHostExecution === true,
		requires_approval: sideEffectPolicy === "irreversible",
		approved: options.approved === true,
		fail_closed: true
	});
}

function assertPolicy(policy) {
	if (!isPlainObject(policy) || !EXECUTION_MODES.includes(policy.execution_mode) || !SIDE_EFFECT_POLICIES.includes(policy.side_effect_policy) || !EXECUTION_TRUST_LEVELS.includes(policy.execution_trust)) fail("SANDBOX_POLICY_INVALID", "执行策略结构不完整", {});
	if (!isPlainObject(policy.filesystem) || !Array.isArray(policy.filesystem.roots) || typeof policy.filesystem.workspace_root !== "string" || typeof policy.filesystem.read_only !== "boolean") fail("SANDBOX_POLICY_INVALID", "执行文件策略不完整", {});
	if (!isPlainObject(policy.tools) || !Array.isArray(policy.tools.allow) || !Array.isArray(policy.tools.deny)) fail("SANDBOX_POLICY_INVALID", "执行工具策略不完整", {});
	if (typeof policy.allow_host_execution !== "boolean" || typeof policy.requires_approval !== "boolean" || typeof policy.approved !== "boolean") fail("SANDBOX_POLICY_INVALID", "执行授权状态不完整", {});
	return policy;
}

function assertPolicyForRun(policy) {
	assertPolicy(policy);
	if (policy.execution_trust !== "trusted" && policy.execution_mode !== "read_only_workspace") fail("UNTRUSTED_EXECUTION_DENIED", "不可信代码禁止直接在宿主执行", {
		execution_mode: policy.execution_mode,
		execution_trust: policy.execution_trust
	});
	if (policy.execution_mode === "local_process" && policy.allow_host_execution !== true) fail("HOST_EXECUTION_NOT_GRANTED", "本地进程执行未获宿主执行授权", {});
	if (policy.requires_approval && policy.approved !== true) fail("IRREVERSIBLE_REQUIRES_APPROVAL", "不可逆任务必须经人工审批后才能执行", {
		side_effect_policy: policy.side_effect_policy
	});
	return policy;
}

/**
 * 按任务声明的副作用与可信级别复核执行策略。
 * 标签不一致直接拒绝，防止通过修改风险标签绕过执行限制。
 */
export function assertSandboxForTask({ sideEffectPolicy = "pure", executionTrust = "untrusted", policy } = {}) {
	assertPolicy(policy);
	if (!SIDE_EFFECT_POLICIES.includes(sideEffectPolicy)) fail("SANDBOX_POLICY_INVALID", "未知任务副作用等级", { side_effect_policy: sideEffectPolicy });
	if (!EXECUTION_TRUST_LEVELS.includes(executionTrust)) fail("SANDBOX_POLICY_INVALID", "未知执行可信级别", { execution_trust: executionTrust });
	if (policy.side_effect_policy !== sideEffectPolicy) fail("SANDBOX_POLICY_INVALID", "执行策略与任务副作用声明不一致", {
		policy_side_effect: policy.side_effect_policy,
		task_side_effect: sideEffectPolicy
	});
	if (policy.execution_trust !== executionTrust) fail("SANDBOX_POLICY_INVALID", "执行策略与任务可信级别不一致", {
		policy_trust: policy.execution_trust,
		task_trust: executionTrust
	});
	return assertPolicyForRun(policy);
}

function normalizeRunRequest(request) {
	if (!isPlainObject(request)) fail("SANDBOX_POLICY_INVALID", "执行请求必须是对象", {});
	const { bin, args = [], cwd, timeoutMs = 30000, tool, capability = "read_only", signal } = request;
	if (typeof bin !== "string" || bin === "" || bin.includes("\0")) fail("SANDBOX_POLICY_INVALID", "执行请求缺少可执行文件", {});
	if (!Array.isArray(args) || args.some((arg) => typeof arg !== "string" || arg.includes("\0"))) fail("SANDBOX_POLICY_INVALID", "执行参数必须是字符串数组", {});
	if (typeof cwd !== "string" || cwd === "" || cwd.includes("\0")) fail("SANDBOX_POLICY_INVALID", "执行请求缺少工作目录", {});
	if (!Number.isSafeInteger(timeoutMs) || timeoutMs < 1 || timeoutMs > 3600000) fail("SANDBOX_POLICY_INVALID", "执行超时须在 1 毫秒到 1 小时之间", { timeout_ms: timeoutMs });
	if (tool !== void 0 && typeof tool !== "string") fail("SANDBOX_POLICY_INVALID", "tool 必须是字符串", {});
	if (signal !== void 0 && (typeof signal !== "object" || signal === null || typeof signal.addEventListener !== "function")) fail("SANDBOX_POLICY_INVALID", "signal 必须是 AbortSignal", {});
	return {
		bin,
		args,
		cwd,
		timeoutMs,
		...(tool === void 0 ? {} : { tool }),
		capability,
		...(signal === void 0 ? {} : { signal })
	};
}

/**
 * 创建统一执行入口。实际执行交给按模式注入的 adapter；
 * 未注入 adapter 或 adapter 不可用时拒绝执行，而不是回退到宿主进程。
 */
export function createExecutionSandbox({ policy, adapters = {} } = {}) {
	assertPolicy(policy);
	if (!isPlainObject(adapters)) fail("SANDBOX_POLICY_INVALID", "adapters 必须是对象", {});
	const adapter = adapters[policy.execution_mode];
	const roots = canonicalRoots(policy.filesystem.roots);
	const workspaceRoot = canonicalPath(policy.filesystem.workspace_root, "SANDBOX_POLICY_INVALID");
	if (!roots.some((root) => insideRoot(root, workspaceRoot))) fail("SANDBOX_POLICY_INVALID", "工作区不在允许的目录范围内", {
		workspace_root: workspaceRoot,
		allowed_roots: roots
	});
	const sandbox = {
		execution_mode: policy.execution_mode,
		isolation: policy.isolation,
		policy,
		describe: () => Object.freeze({
			execution_mode: policy.execution_mode,
			isolation: policy.isolation,
			side_effect_policy: policy.side_effect_policy,
			execution_trust: policy.execution_trust,
			workspace_root: workspaceRoot,
			read_only: policy.filesystem.read_only,
			tools: [...policy.tools.allow],
			requires_approval: policy.requires_approval,
			approved: policy.approved,
			fail_closed: true
		}),
		assertTool(input = {}) {
			if (!isPlainObject(input) || typeof input.name !== "string" || input.name.trim() === "") fail("SANDBOX_POLICY_INVALID", "缺少工具名", {});
			const name = input.name.trim();
			const capability = input.capability ?? "read_only";
			if (!["read_only", "mutating", "compensating"].includes(capability)) fail("SANDBOX_POLICY_INVALID", "未知工具能力", { tool: name, capability });
			if (policy.tools.deny.includes(name)) fail("SANDBOX_TOOL_DENIED", "工具在执行禁用清单里", { tool: name });
			if (!policy.tools.allow.includes(name)) fail("SANDBOX_TOOL_DENIED", "工具不在执行授权清单里", { tool: name });
			if (capability !== "read_only" && policy.filesystem.read_only) fail("SANDBOX_TOOL_DENIED", "只读工作区不允许执行写工具", { tool: name, capability });
			return true;
		},
		assertPath(target) {
			if (typeof target !== "string" || target === "" || target.includes("\0")) fail("SANDBOX_POLICY_INVALID", "路径必须是非空字符串", {});
			const absolute = isAbsolute(target) ? resolve(target) : resolve(workspaceRoot, target);
			if (!roots.some((root) => insideRoot(root, absolute))) fail("SANDBOX_PATH_DENIED", "路径越出允许范围", {
				path: absolute,
				allowed_roots: [...roots]
			});
			const real = canonicalPath(absolute);
			if (!roots.some((root) => insideRoot(root, real))) fail("SANDBOX_PATH_DENIED", "符号链接指向允许范围之外", {
				path: absolute,
				real_path: real,
				allowed_roots: [...roots]
			});
			return real;
		},
		assertTask(sideEffectPolicy = policy.side_effect_policy, executionTrust = policy.execution_trust) {
			return assertSandboxForTask({ sideEffectPolicy, executionTrust, policy });
		},
		async run(request) {
			assertPolicyForRun(policy);
			const normalized = normalizeRunRequest(request);
			if (normalized.tool !== void 0) sandbox.assertTool({ name: normalized.tool, capability: normalized.capability });
			const cwd = sandbox.assertPath(normalized.cwd);
			if (!adapter || typeof adapter.run !== "function") fail("SANDBOX_UNAVAILABLE", "当前执行模式没有可用的执行器", { execution_mode: policy.execution_mode });
			if (typeof adapter.available === "function" && !adapter.available()) fail("SANDBOX_UNAVAILABLE", "执行器在当前宿主不可用", { execution_mode: policy.execution_mode });
			// TOCTOU：准入与执行之间路径可能被替换成符号链接，执行前按真实路径再复核一次。
			const rechecked = sandbox.assertPath(cwd);
			if (rechecked !== cwd) fail("SANDBOX_PATH_DENIED", "执行前路径真实位置发生变化", { cwd, real_path: rechecked });
			let result;
			try {
				result = await adapter.run({ ...normalized, cwd }, policy);
			} catch (error) {
				if (error instanceof SandboxError) throw error;
				throw new SandboxError("SANDBOX_FAILED", "本地执行失败", { execution_mode: policy.execution_mode, message: String(error?.message ?? error) });
			}
			if (!isPlainObject(result) || !Number.isInteger(result.exitCode) || typeof result.stdout !== "string" || typeof result.stderr !== "string") fail("SANDBOX_RESULT_INVALID", "执行结果格式不合法", { execution_mode: policy.execution_mode });
			return {
				exitCode: result.exitCode,
				stdout: result.stdout,
				stderr: result.stderr,
				boundary: result.boundary ?? `execution:${policy.execution_mode}`,
				...(result.timedOut === true ? { timedOut: true } : {}),
				...(isPlainObject(result.evidence) ? { evidence: result.evidence } : {})
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
			if (!policy.filesystem.read_only) fail("SANDBOX_TOOL_DENIED", "只读工作区不允许可写挂载", { execution_mode: policy.execution_mode });
			if (typeof run !== "function") fail("SANDBOX_UNAVAILABLE", "只读工作区执行器未装配", {});
			return run(request, policy);
		}
	};
}

function positiveInteger(value, fallback, field) {
	const number = value ?? fallback;
	if (!Number.isSafeInteger(number) || number < 1) fail("SANDBOX_POLICY_INVALID", `${field} 必须是正整数`, { [field]: number });
	return number;
}

/**
 * 运行本地进程。命令以 executable + args 传入，绝不拼接 shell 字符串；
 * 取消或超时先结束整个进程组，避免留下后台子进程。
 */
function runLocalProcess({ bin, args, cwd, timeoutMs, signal, env, spawnImpl, maxOutputBytes }) {
	return new Promise((resolvePromise, reject) => {
		let child;
		try {
			child = spawnImpl(bin, args, {
				cwd,
				env,
				shell: false,
				stdio: ["ignore", "pipe", "pipe"],
				windowsHide: true,
				detached: process.platform !== "win32"
			});
		} catch (error) {
			reject(new SandboxError("SANDBOX_UNAVAILABLE", "无法启动本地进程", { message: String(error?.message ?? error) }));
			return;
		}
		let stdout = "";
		let stderr = "";
		let timedOut = false;
		let settled = false;
		let forceTimer;
		const terminate = () => {
			if (!child.pid) {
				try {
					child.kill("SIGKILL");
				} catch {}
				return;
			}
			try {
				if (process.platform !== "win32") process.kill(-child.pid, "SIGKILL");
				else child.kill("SIGKILL");
			} catch {
				try {
					child.kill("SIGKILL");
				} catch {}
			}
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
			boundary: "local_process",
			...(timedOut ? { timedOut: true } : {}),
			evidence: Object.freeze({
				argv: Object.freeze([bin, ...args]),
				cwd,
				timeout_ms: timeoutMs,
				env_keys: Object.freeze(Object.keys(env))
			})
		}));
		const onAbort = () => {
			terminate();
			if (forceTimer) return;
			forceTimer = setTimeout(() => {
				if (timedOut) finish(124);
				else settle(() => reject(signal?.reason instanceof Error ? signal.reason : new SandboxError("SANDBOX_FAILED", "本地执行已取消", { bin })));
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
			settle(() => reject(new SandboxError("SANDBOX_UNAVAILABLE", "本地进程启动失败", { message: String(error?.message ?? error) })));
		});
		child.once?.("close", (code) => {
			if (signal?.aborted && !timedOut) {
				settle(() => reject(signal.reason instanceof Error ? signal.reason : new SandboxError("SANDBOX_FAILED", "本地执行已取消", { bin })));
				return;
			}
			finish(code);
		});
	});
}

/**
 * 本地进程执行：只有服务端显式授予宿主执行权限且任务可信时才可用。
 * 命令走 executable + args；危险命令、敏感环境变量、工作目录、超时与取消都在这里收口。
 */
export function createLocalProcessAdapter(options = {}) {
	if (!isPlainObject(options)) fail("SANDBOX_POLICY_INVALID", "本地进程参数必须是对象", {});
	const run = options.run;
	const spawnImpl = options.spawnImpl ?? spawn;
	const baseEnv = options.env ?? process.env;
	const maxOutputBytes = positiveInteger(options.maxOutputBytes, 1048576, "maxOutputBytes");
	const extraDenied = normalizeNames(options.deniedExecutables, "deniedExecutables");
	const denied = new Set([...DENIED_EXECUTABLES, ...extraDenied.map((name) => name.toLowerCase())]);
	return {
		kind: "local_process",
		available: () => typeof run === "function" || typeof spawnImpl === "function",
		async run(request, policy) {
			if (policy.execution_trust !== "trusted") fail("UNTRUSTED_EXECUTION_DENIED", "不可信代码禁止直接在宿主执行", {
				execution_mode: policy.execution_mode,
				execution_trust: policy.execution_trust
			});
			if (policy.allow_host_execution !== true) fail("HOST_EXECUTION_NOT_GRANTED", "本地进程执行未获宿主执行授权", {});
			const name = executableName(request.bin);
			if (denied.has(name)) fail("COMMAND_NOT_ALLOWED", "命令在本地执行禁用清单里", { command: name });
			const env = sanitizeExecutionEnv(baseEnv);
			const normalized = Object.freeze({ ...request, env });
			if (typeof run === "function") return run(normalized, policy);
			return runLocalProcess({ ...normalized, spawnImpl, maxOutputBytes });
		}
	};
}
