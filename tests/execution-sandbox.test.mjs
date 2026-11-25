import test, { after } from "node:test";
import assert from "node:assert/strict";
import { EventEmitter } from "node:events";
import { mkdirSync, mkdtempSync, realpathSync, rmSync, symlinkSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import {
	SANDBOX_KINDS,
	SandboxError,
	assertSandboxForTask,
	createDockerAdapter,
	createExecutionSandbox,
	createLocalProcessAdapter,
	createReadOnlyWorkspaceAdapter,
	createRestrictedNetworkAdapter,
	resolveSandboxPolicy
} from "../lib/execution-sandbox.js";
import { ReadOnlyToolRunner, ToolRegistry } from "../lib/index.js";

const TEMP = realpathSync(mkdtempSync(join(tmpdir(), "dsh-sandbox-")));
const ROOT = join(TEMP, "workspace");
mkdirSync(ROOT);
after(() => rmSync(TEMP, { recursive: true, force: true }));

function readOnlyPolicy(overrides = {}) {
	return resolveSandboxPolicy({
		workspaceRoot: ROOT,
		availableKinds: ["read_only_workspace"],
		tools: { allow: ["read_file"] },
		...overrides
	});
}

function dockerPolicy(overrides = {}) {
	return resolveSandboxPolicy({
		requestedKind: "docker",
		availableKinds: ["docker"],
		workspaceRoot: ROOT,
		tools: { allow: ["read_file"] },
		...overrides
	});
}

function sandboxError(code) {
	return (error) => error instanceof SandboxError && error.code === code;
}

function fakeRuntime() {
	const calls = [];
	const exitTimers = new Set();
	const spawnImpl = (bin, args) => {
		calls.push([bin, ...args]);
		const child = new EventEmitter();
		child.stdout = new EventEmitter();
		child.stderr = new EventEmitter();
		child.kill = () => {
			const timer = setTimeout(() => {
				exitTimers.delete(timer);
				child.emit("close", 137);
			}, 1);
			exitTimers.add(timer);
		};
		return child;
	};
	return {
		calls,
		spawnImpl,
		removals: () => calls.filter((call) => call[1] === "rm" && call[2] === "-f"),
		cleanup() {
			for (const timer of exitTimers) clearTimeout(timer);
		}
	};
}

test("默认策略为只读工作区、无网络、工具拒绝优先", () => {
	const policy = readOnlyPolicy();
	assert.deepEqual(SANDBOX_KINDS, ["local_process", "read_only_workspace", "docker", "restricted_network"]);
	assert.equal(policy.kind, "read_only_workspace");
	assert.equal(policy.isolation, "capability");
	assert.equal(policy.filesystem.read_only, true);
	assert.equal(policy.network.mode, "none");
	assert.deepEqual(policy.network.allow_hosts, []);
	assert.equal(policy.tools.default, "deny");
	assert.equal(policy.fail_closed, true);
	assert.equal(Object.isFrozen(policy), true);
	assert.equal(Object.isFrozen(policy.filesystem), true);
});

test("策略拒绝相对路径、越界目录和未知类型", () => {
	assert.throws(() => readOnlyPolicy({ workspaceRoot: "relative/ws" }), sandboxError("SANDBOX_POLICY_INVALID"));
	assert.throws(() => readOnlyPolicy({ allowedRoots: ["/tmp/other-workspace"] }), sandboxError("SANDBOX_POLICY_INVALID"));
	assert.throws(() => readOnlyPolicy({ requestedKind: "firecracker" }), sandboxError("SANDBOX_POLICY_INVALID"));
	assert.throws(() => readOnlyPolicy({ availableKinds: ["unknown"] }), sandboxError("SANDBOX_POLICY_INVALID"));
	assert.throws(() => readOnlyPolicy({ taskRisk: "critical" }), sandboxError("SANDBOX_POLICY_INVALID"));
});

test("高风险任务默认拒绝宿主执行，有容器时才放行", () => {
	assert.throws(() => resolveSandboxPolicy({
		taskRisk: "high",
		workspaceRoot: ROOT,
		availableKinds: ["read_only_workspace"],
		tools: { allow: ["read_file"] }
	}), sandboxError("HIGH_RISK_REQUIRES_ISOLATION"));
	assert.throws(() => resolveSandboxPolicy({
		taskRisk: "high",
		requestedKind: "local_process",
		availableKinds: ["local_process"],
		allowHostExecution: true,
		workspaceRoot: ROOT,
		tools: { allow: ["read_file"] }
	}), sandboxError("HIGH_RISK_REQUIRES_ISOLATION"));
	const isolated = dockerPolicy({ taskRisk: "high" });
	assert.equal(isolated.kind, "docker");
	assert.equal(isolated.isolation, "container");
	assert.equal(isolated.network.mode, "none");
	assert.equal(assertSandboxForTask({ sideEffectPolicy: "irreversible", policy: isolated }), isolated);
	assert.throws(() => assertSandboxForTask({ sideEffectPolicy: "irreversible", policy: readOnlyPolicy() }), sandboxError("HIGH_RISK_REQUIRES_ISOLATION"));
	assert.equal(assertSandboxForTask({ sideEffectPolicy: "compensable", policy: readOnlyPolicy() }).kind, "read_only_workspace");
});

test("本地进程沙箱必须显式授权", () => {
	assert.throws(() => resolveSandboxPolicy({
		requestedKind: "local_process",
		availableKinds: ["local_process"],
		workspaceRoot: ROOT,
		tools: { allow: ["read_file"] }
	}), sandboxError("HOST_EXECUTION_NOT_GRANTED"));
	const granted = resolveSandboxPolicy({
		requestedKind: "local_process",
		availableKinds: ["local_process"],
		allowHostExecution: true,
		workspaceRoot: ROOT,
		tools: { allow: ["read_file"] }
	});
	assert.equal(granted.network.mode, "host");
	assert.equal(granted.allow_host_execution, true);
});

test("请求的沙箱不可用时 fail-closed，而不是回退宿主", () => {
	assert.throws(() => readOnlyPolicy({ requestedKind: "docker" }), sandboxError("SANDBOX_UNAVAILABLE"));
	assert.throws(() => resolveSandboxPolicy({
		workspaceRoot: ROOT,
		availableKinds: [],
		tools: { allow: [] }
	}), sandboxError("SANDBOX_UNAVAILABLE"));
});

test("工具白名单、禁用清单与只读能力同时生效", () => {
	const sandbox = createExecutionSandbox({ policy: readOnlyPolicy() });
	assert.equal(sandbox.assertTool({ name: "read_file" }), true);
	assert.throws(() => sandbox.assertTool({ name: "write_file" }), sandboxError("SANDBOX_TOOL_DENIED"));
	assert.throws(() => sandbox.assertTool({ name: "read_file", capability: "mutating" }), sandboxError("SANDBOX_TOOL_DENIED"));
	assert.throws(() => sandbox.assertTool({ name: "" }), sandboxError("SANDBOX_POLICY_INVALID"));

	const writable = createExecutionSandbox({
		policy: dockerPolicy({
			allowWrite: true,
			tools: { allow: ["write_file", "delete_file"], deny: ["delete_file"] }
		})
	});
	assert.equal(writable.assertTool({ name: "write_file", capability: "mutating" }), true);
	assert.throws(() => writable.assertTool({ name: "delete_file", capability: "mutating" }), sandboxError("SANDBOX_TOOL_DENIED"));
});

test("路径必须落在服务端允许的目录内，并按真实路径阻断符号链接逃逸", () => {
	const sandbox = createExecutionSandbox({ policy: readOnlyPolicy() });
	assert.equal(sandbox.assertPath("src/app.js"), `${ROOT}/src/app.js`);
	assert.equal(sandbox.assertPath(ROOT), ROOT);
	assert.throws(() => sandbox.assertPath("../outside.txt"), sandboxError("SANDBOX_PATH_DENIED"));
	assert.throws(() => sandbox.assertPath("/etc/passwd"), sandboxError("SANDBOX_PATH_DENIED"));
	assert.throws(() => sandbox.assertPath("src/../../escape"), sandboxError("SANDBOX_PATH_DENIED"));

	const outside = join(TEMP, "outside");
	mkdirSync(outside);
	symlinkSync(outside, join(ROOT, "escape"));
	assert.throws(() => sandbox.assertPath("escape/secret.txt"), sandboxError("SANDBOX_PATH_DENIED"));
	symlinkSync("/etc", join(ROOT, "etc-link"));
	assert.throws(() => sandbox.assertPath("etc-link/passwd"), sandboxError("SANDBOX_PATH_DENIED"));

	const inside = join(ROOT, "inside");
	mkdirSync(inside);
	symlinkSync(inside, join(ROOT, "inside-link"));
	assert.equal(sandbox.assertPath("inside-link"), inside);
});

test("网络默认关闭，allowlist 精确匹配与通配子域", () => {
	const closed = createExecutionSandbox({ policy: readOnlyPolicy() });
	assert.throws(() => closed.assertNetwork({ host: "example.com" }), sandboxError("SANDBOX_NETWORK_DENIED"));
	assert.throws(() => closed.assertNetwork({}), sandboxError("SANDBOX_POLICY_INVALID"));

	const allowed = createExecutionSandbox({
		policy: dockerPolicy({
			network: "allowlist",
			allowHosts: ["api.example.com", "*.trusted.test"]
		})
	});
	assert.equal(allowed.assertNetwork({ host: "api.example.com" }), true);
	assert.equal(allowed.assertNetwork({ host: "npm.trusted.test" }), true);
	assert.throws(() => allowed.assertNetwork({ host: "evil.test" }), sandboxError("SANDBOX_NETWORK_DENIED"));
	assert.throws(() => dockerPolicy({ network: "allowlist", allowHosts: [] }), sandboxError("SANDBOX_POLICY_INVALID"));
	assert.throws(() => dockerPolicy({ network: "host" }), sandboxError("SANDBOX_POLICY_INVALID"));
});

test("执行请求按顺序经过工具、目录、网络检查后才交给 adapter", async () => {
	const seen = [];
	const sandbox = createExecutionSandbox({
		policy: dockerPolicy({ network: "allowlist", allowHosts: ["api.example.com"] }),
		adapters: {
			docker: {
				run: async (request, policy) => {
					seen.push({ request, policy });
					return { exitCode: 0, stdout: "ok", stderr: "", boundary: "sandbox:test" };
				}
			}
		}
	});
	const result = await sandbox.run({
		bin: "/bin/echo",
		args: ["hello"],
		cwd: `${ROOT}/src`,
		timeoutMs: 1000,
		tool: "read_file",
		networkHost: "api.example.com"
	});
	assert.equal(result.boundary, "sandbox:test");
	assert.equal(result.stdout, "ok");
	assert.equal(seen.length, 1);
	assert.equal(seen[0].request.tool, "read_file");
	assert.equal(seen[0].policy.kind, "docker");

	await assert.rejects(sandbox.run({ bin: "/bin/echo", cwd: ROOT, timeoutMs: 1000, tool: "rm" }), sandboxError("SANDBOX_TOOL_DENIED"));
	await assert.rejects(sandbox.run({ bin: "/bin/echo", cwd: "/etc", timeoutMs: 1000, tool: "read_file" }), sandboxError("SANDBOX_PATH_DENIED"));
	await assert.rejects(sandbox.run({ bin: "/bin/echo", cwd: ROOT, timeoutMs: 1000, tool: "read_file", networkHost: "evil.test" }), sandboxError("SANDBOX_NETWORK_DENIED"));
	assert.equal(seen.length, 1);
});

test("缺少执行器、执行器不可用或结果格式不合法时拒绝", async () => {
	const noAdapter = createExecutionSandbox({ policy: readOnlyPolicy() });
	await assert.rejects(noAdapter.run({ bin: "/bin/echo", cwd: ROOT, timeoutMs: 1000 }), sandboxError("SANDBOX_UNAVAILABLE"));

	const unavailable = createExecutionSandbox({
		policy: dockerPolicy(),
		adapters: { docker: { available: () => false, run: async () => ({ exitCode: 0, stdout: "", stderr: "" }) } }
	});
	await assert.rejects(unavailable.run({ bin: "/bin/echo", cwd: ROOT, timeoutMs: 1000 }), sandboxError("SANDBOX_UNAVAILABLE"));

	const badResult = createExecutionSandbox({
		policy: readOnlyPolicy(),
		adapters: { read_only_workspace: { run: async () => ({ exitCode: "0", stdout: "", stderr: "" }) } }
	});
	await assert.rejects(badResult.run({ bin: "/bin/echo", cwd: ROOT, timeoutMs: 1000 }), sandboxError("SANDBOX_RESULT_INVALID"));
});

test("只读 adapter 拒绝可写挂载，本地 adapter 拒绝未授权", async () => {
	const writablePolicy = dockerPolicy({ allowWrite: true });
	await assert.rejects(
		createExecutionSandbox({
			policy: writablePolicy,
			adapters: { docker: createReadOnlyWorkspaceAdapter({ run: async () => ({ exitCode: 0, stdout: "", stderr: "" }) }) }
		}).run({ bin: "/bin/echo", cwd: ROOT, timeoutMs: 1000 }),
		(error) => error?.code === "SANDBOX_UNAVAILABLE" || error?.code === "SANDBOX_TOOL_DENIED"
	);
	const unprivileged = createExecutionSandbox({
		policy: readOnlyPolicy(),
		adapters: { read_only_workspace: createLocalProcessAdapter({ run: async () => ({ exitCode: 0, stdout: "", stderr: "" }) }) }
	});
	await assert.rejects(unprivileged.run({ bin: "/bin/echo", cwd: ROOT, timeoutMs: 1000 }), sandboxError("HOST_EXECUTION_NOT_GRANTED"));
});

test("Docker adapter 生成无网络、只读、非 root、去能力的隔离参数", () => {
	const adapter = createDockerAdapter({ image: "node:22-alpine" });
	assert.equal(adapter.available(), true);
	assert.throws(() => createDockerAdapter({}).buildArgs({ bin: "node", cwd: ROOT, timeoutMs: 1 }, readOnlyPolicy()), sandboxError("SANDBOX_UNAVAILABLE"));
	const args = adapter.buildArgs({ bin: "node", args: ["-e", "1"], cwd: `${ROOT}/src`, timeoutMs: 1000 }, dockerPolicy());
	assert.equal(args[args.indexOf("--network") + 1], "none");
	assert.ok(args.includes("--read-only"));
	assert.ok(args.includes("--cap-drop"));
	assert.ok(args.includes("no-new-privileges"));
	assert.ok(args.includes(`${ROOT}:/workspace:ro`));
	assert.equal(args[args.indexOf("-w") + 1], "/workspace/src");
	assert.deepEqual(args.slice(args.indexOf("node:22-alpine")), ["node:22-alpine", "node", "-e", "1"]);
});

test("受限网络 adapter 接入专用网络并保留挂载与用户隔离", () => {
	const adapter = createRestrictedNetworkAdapter({ image: "busybox", networkName: "dsh-egress-only" });
	const policy = resolveSandboxPolicy({
		requestedKind: "restricted_network",
		availableKinds: ["restricted_network"],
		workspaceRoot: ROOT,
		network: "allowlist",
		allowHosts: ["api.example.com"],
		tools: { allow: ["read_file"] }
	});
	const args = adapter.buildArgs({ bin: "curl", args: ["https://api.example.com"], cwd: ROOT, timeoutMs: 1000 }, policy);
	assert.equal(args[args.indexOf("--network") + 1], "dsh-egress-only");
	assert.ok(args.includes(`${ROOT}:/workspace:ro`));
	assert.ok(args.includes("65534:65534"));
});

test("容器参数包含 CPU、内存、PID 与临时磁盘限制", () => {
	const adapter = createDockerAdapter({ image: "node:22-alpine", cpus: "1.5", memory: "256m", pidsLimit: 128, nofile: 512, tmpSize: "32m" });
	const args = adapter.buildArgs({ bin: "node", args: [], cwd: ROOT, timeoutMs: 1000 }, dockerPolicy());
	assert.equal(args[args.indexOf("--cpus") + 1], "1.5");
	assert.equal(args[args.indexOf("--memory") + 1], "256m");
	assert.equal(args[args.indexOf("--memory-swap") + 1], "256m");
	assert.equal(args[args.indexOf("--pids-limit") + 1], "128");
	assert.equal(args[args.indexOf("--ulimit") + 1], "nofile=512:512");
	assert.ok(args.includes("/tmp:rw,noexec,nosuid,size=32m"));
	assert.throws(() => createDockerAdapter({ image: "x", cpus: 0 }), sandboxError("SANDBOX_POLICY_INVALID"));
	assert.throws(() => createDockerAdapter({ image: "x", pidsLimit: 0 }), sandboxError("SANDBOX_POLICY_INVALID"));
	assert.throws(() => createDockerAdapter({ image: "x", nofile: 1.5 }), sandboxError("SANDBOX_POLICY_INVALID"));
});

test("取消任务时按容器引用执行 docker rm -f 并结束 CLI", async () => {
	const runtime = fakeRuntime();
	const adapter = createDockerAdapter({ image: "busybox", spawnImpl: runtime.spawnImpl });
	const controller = new AbortController();
	const running = adapter.run({
		bin: "sleep",
		args: ["60"],
		cwd: ROOT,
		timeoutMs: 60000,
		signal: controller.signal
	}, dockerPolicy({ allowWrite: true }));
	await new Promise((resolvePromise) => setTimeout(resolvePromise, 5));
	const invocation = runtime.calls[0];
	assert.equal(invocation[1], "run");
	assert.match(invocation[invocation.indexOf("--name") + 1], /^dsh-orchestra-[a-z0-9]+$/u);
	assert.ok(invocation.includes("--cidfile"));
	controller.abort(new Error("用户取消"));
	await assert.rejects(running, /用户取消/u);
	const removals = runtime.removals();
	assert.equal(removals.length, 1);
	assert.match(removals[0][3], /^(?:dsh-orchestra-[a-z0-9]+|[a-f0-9]{12,64})$/u);
	runtime.cleanup();
});

test("超时后清理容器及残留进程并返回 124", async () => {
	const runtime = fakeRuntime();
	const adapter = createDockerAdapter({ image: "busybox", spawnImpl: runtime.spawnImpl });
	const result = await adapter.run({
		bin: "sleep",
		args: ["60"],
		cwd: ROOT,
		timeoutMs: 20
	}, dockerPolicy({ allowWrite: true }));
	assert.equal(result.exitCode, 124);
	assert.equal(result.timedOut, true);
	assert.equal(runtime.removals().length, 1);
	runtime.cleanup();
});

test("dispose 清理仍在运行的容器与客户端进程", async () => {
	const runtime = fakeRuntime();
	const adapter = createDockerAdapter({ image: "busybox", spawnImpl: runtime.spawnImpl });
	const controller = new AbortController();
	const running = adapter.run({
		bin: "sleep",
		args: ["60"],
		cwd: ROOT,
		timeoutMs: 60000,
		signal: controller.signal
	}, dockerPolicy({ allowWrite: true }));
	await new Promise((resolvePromise) => setTimeout(resolvePromise, 5));
	adapter.dispose();
	controller.abort(new Error("停止"));
	await assert.rejects(running, /停止/u);
	assert.ok(runtime.removals().length >= 1);
	runtime.cleanup();
});

test("只读工具执行经过统一沙箱，沙箱拒绝会转成编排错误码", async () => {
	const registry = new ToolRegistry();
	registry.register({ name: "inspect", capability: "read_only", command: { bin: "/bin/echo", args: ["ok"] } });
	let seen = null;
	const sandbox = createExecutionSandbox({
		policy: readOnlyPolicy({ tools: { allow: ["inspect"] } }),
		adapters: {
			read_only_workspace: createReadOnlyWorkspaceAdapter({
				run: async (request) => {
					seen = request;
					return { exitCode: 0, stdout: "ok", stderr: "", boundary: "sandbox:test" };
				}
			})
		}
	});
	const host = {
		readonlyAvailable: () => true,
		run: async () => {
			throw new Error("不应绕过沙箱直接执行");
		}
	};
	const runner = new ReadOnlyToolRunner(registry, host, ROOT, sandbox);
	const result = await runner.runTool({ command: "inspect", cwd: ".", timeoutMs: 1000 });
	assert.equal(result.stdout, "ok");
	assert.equal(result.evidence.boundary, "sandbox:test");
	assert.equal(seen.tool, "inspect");
	assert.equal(seen.cwd, ROOT);

	const denied = new ReadOnlyToolRunner(registry, host, ROOT, createExecutionSandbox({ policy: readOnlyPolicy({ tools: { allow: [] } }) }));
	await assert.rejects(denied.runTool({ command: "inspect", cwd: ".", timeoutMs: 1000 }), (error) => error?.code === "SANDBOX_TOOL_DENIED");
});
