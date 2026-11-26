import test, { after } from "node:test";
import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import { DatabaseSync } from "node:sqlite";
import { mkdirSync, mkdtempSync, realpathSync, rmSync, symlinkSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import {
	EXECUTION_MODES,
	EXECUTION_TRUST_LEVELS,
	LEGACY_EXECUTION_MODES,
	SIDE_EFFECT_POLICIES,
	SandboxError,
	assertNoLegacyExecutionConfig,
	assertSandboxForTask,
	createExecutionSandbox,
	createLocalProcessAdapter,
	createReadOnlyWorkspaceAdapter,
	detectLegacyExecutionConfig,
	resolveSandboxPolicy,
	sanitizeExecutionEnv
} from "../lib/execution-sandbox.js";
import { ReadOnlyToolRunner, ToolRegistry, version } from "../lib/index.js";

const TEMP = realpathSync(mkdtempSync(join(tmpdir(), "dsh-sandbox-")));
const ROOT = join(TEMP, "workspace");
mkdirSync(ROOT);
after(() => rmSync(TEMP, { recursive: true, force: true }));

function readOnlyPolicy(overrides = {}) {
	return resolveSandboxPolicy({
		sideEffectPolicy: "pure",
		executionTrust: "trusted",
		workspaceRoot: ROOT,
		availableModes: ["read_only_workspace"],
		tools: { allow: ["read_file"] },
		...overrides
	});
}

function localPolicy(overrides = {}) {
	return resolveSandboxPolicy({
		sideEffectPolicy: "compensable",
		executionTrust: "trusted",
		executionMode: "local_process",
		availableModes: ["local_process"],
		allowHostExecution: true,
		allowWrite: true,
		workspaceRoot: ROOT,
		tools: { allow: ["run"], deny: [] },
		...overrides
	});
}

function sandboxError(code) {
	return (error) => error instanceof SandboxError && error.code === code;
}

function runNode(adapter, policy, script, ...rest) {
	const last = rest.at(-1);
	const extra = rest.length > 0 && last !== null && typeof last === "object" && !Array.isArray(last) ? rest.pop() : {};
	return adapter.run({ bin: process.execPath, args: ["-e", script, ...rest], cwd: ROOT, timeoutMs: 15000, ...extra }, policy);
}

test("执行模式、副作用与可信级别枚举已收敛，默认策略为只读工作区", () => {
	assert.deepEqual(EXECUTION_MODES, ["read_only_workspace", "local_process"]);
	assert.deepEqual(LEGACY_EXECUTION_MODES, ["docker", "restricted_network"]);
	assert.deepEqual(SIDE_EFFECT_POLICIES, ["pure", "compensable", "irreversible"]);
	assert.deepEqual(EXECUTION_TRUST_LEVELS, ["trusted", "untrusted"]);
	const policy = readOnlyPolicy();
	assert.equal(policy.execution_mode, "read_only_workspace");
	assert.equal(policy.isolation, "capability");
	assert.equal(policy.filesystem.read_only, true);
	assert.equal(policy.tools.default, "deny");
	assert.equal(policy.allow_host_execution, false);
	assert.equal(policy.requires_approval, false);
	assert.equal(policy.fail_closed, true);
	assert.equal(Object.isFrozen(policy), true);
	assert.equal(Object.isFrozen(policy.filesystem), true);
	assert.equal(version, "0.1.0-beta.4");
});

test("策略拒绝相对路径、越界目录、未知模式与未知标签", () => {
	assert.throws(() => readOnlyPolicy({ workspaceRoot: "relative/ws" }), sandboxError("SANDBOX_POLICY_INVALID"));
	assert.throws(() => readOnlyPolicy({ allowedRoots: ["/tmp/other-workspace"] }), sandboxError("SANDBOX_POLICY_INVALID"));
	assert.throws(() => readOnlyPolicy({ executionMode: "firecracker" }), sandboxError("SANDBOX_POLICY_INVALID"));
	assert.throws(() => readOnlyPolicy({ availableModes: ["unknown"] }), sandboxError("SANDBOX_POLICY_INVALID"));
	assert.throws(() => readOnlyPolicy({ sideEffectPolicy: "critical" }), sandboxError("SANDBOX_POLICY_INVALID"));
	assert.throws(() => readOnlyPolicy({ executionTrust: "root" }), sandboxError("SANDBOX_POLICY_INVALID"));
});

test("本地执行必须显式授权、任务可信且不是纯任务", () => {
	assert.throws(() => resolveSandboxPolicy({
		sideEffectPolicy: "compensable",
		executionTrust: "trusted",
		executionMode: "local_process",
		availableModes: ["local_process"],
		workspaceRoot: ROOT,
		tools: { allow: ["run"] }
	}), sandboxError("HOST_EXECUTION_NOT_GRANTED"));
	assert.throws(() => resolveSandboxPolicy({
		sideEffectPolicy: "compensable",
		executionTrust: "untrusted",
		executionMode: "local_process",
		availableModes: ["local_process"],
		allowHostExecution: true,
		workspaceRoot: ROOT,
		tools: { allow: ["run"] }
	}), sandboxError("UNTRUSTED_EXECUTION_DENIED"));
	assert.throws(() => resolveSandboxPolicy({
		sideEffectPolicy: "pure",
		executionTrust: "trusted",
		executionMode: "local_process",
		availableModes: ["local_process"],
		allowHostExecution: true,
		workspaceRoot: ROOT,
		tools: { allow: ["run"] }
	}), sandboxError("SANDBOX_POLICY_INVALID"));
	const granted = localPolicy();
	assert.equal(granted.execution_mode, "local_process");
	assert.equal(granted.allow_host_execution, true);
	assert.equal(granted.requires_approval, false);
});

test("请求的执行模式不可用时 fail-closed，而不是回退宿主", () => {
	assert.throws(() => readOnlyPolicy({ executionMode: "local_process" }), sandboxError("SANDBOX_UNAVAILABLE"));
	assert.throws(() => resolveSandboxPolicy({
		workspaceRoot: ROOT,
		availableModes: [],
		tools: { allow: [] }
	}), sandboxError("SANDBOX_UNAVAILABLE"));
});

test("副作用与可信级别解耦：不可逆任务必须人工审批，标签不能互换", async () => {
	const irreversible = localPolicy({ sideEffectPolicy: "irreversible" });
	assert.equal(irreversible.execution_mode, "local_process");
	assert.equal(irreversible.requires_approval, true);
	assert.equal(irreversible.approved, false);
	const pending = createExecutionSandbox({
		policy: irreversible,
		adapters: { local_process: { run: async () => ({ exitCode: 0, stdout: "", stderr: "" }) } }
	});
	await assert.rejects(pending.run({
		bin: process.execPath,
		args: ["-e", "1"],
		cwd: ROOT,
		timeoutMs: 1000,
		tool: "run",
		capability: "mutating"
	}), sandboxError("IRREVERSIBLE_REQUIRES_APPROVAL"));

	const approved = localPolicy({ sideEffectPolicy: "irreversible", approved: true });
	const done = createExecutionSandbox({
		policy: approved,
		adapters: { local_process: { run: async () => ({ exitCode: 0, stdout: "ok", stderr: "" }) } }
	});
	const result = await done.run({
		bin: process.execPath,
		args: ["-e", "1"],
		cwd: ROOT,
		timeoutMs: 1000,
		tool: "run",
		capability: "mutating"
	});
	assert.equal(result.exitCode, 0);

	assert.throws(() => assertSandboxForTask({ sideEffectPolicy: "pure", executionTrust: "trusted", policy: approved }), sandboxError("SANDBOX_POLICY_INVALID"));
	assert.throws(() => assertSandboxForTask({ sideEffectPolicy: "irreversible", executionTrust: "untrusted", policy: approved }), sandboxError("SANDBOX_POLICY_INVALID"));
	assert.throws(() => assertSandboxForTask({ sideEffectPolicy: "irreversible", executionTrust: "trusted", policy: irreversible }), sandboxError("IRREVERSIBLE_REQUIRES_APPROVAL"));
	assert.equal(assertSandboxForTask({ sideEffectPolicy: "compensable", executionTrust: "trusted", policy: localPolicy() }).execution_mode, "local_process");

	const untrustedReadOnly = readOnlyPolicy({ executionTrust: "untrusted" });
	assert.equal(assertSandboxForTask({ sideEffectPolicy: "pure", executionTrust: "untrusted", policy: untrustedReadOnly }).execution_mode, "read_only_workspace");
});

test("工具白名单、禁用清单与只读能力同时生效", () => {
	const sandbox = createExecutionSandbox({ policy: readOnlyPolicy() });
	assert.equal(sandbox.assertTool({ name: "read_file" }), true);
	assert.throws(() => sandbox.assertTool({ name: "write_file" }), sandboxError("SANDBOX_TOOL_DENIED"));
	assert.throws(() => sandbox.assertTool({ name: "read_file", capability: "mutating" }), sandboxError("SANDBOX_TOOL_DENIED"));
	assert.throws(() => sandbox.assertTool({ name: "" }), sandboxError("SANDBOX_POLICY_INVALID"));

	const writable = createExecutionSandbox({
		policy: localPolicy({ tools: { allow: ["write_file", "delete_file"], deny: ["delete_file"] } })
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

test("执行请求先经过工具与目录检查后才交给 adapter", async () => {
	const seen = [];
	const sandbox = createExecutionSandbox({
		policy: localPolicy(),
		adapters: {
			local_process: {
				run: async (request, policy) => {
					seen.push({ request, policy });
					return { exitCode: 0, stdout: "ok", stderr: "", boundary: "execution:test" };
				}
			}
		}
	});
	const result = await sandbox.run({
		bin: process.execPath,
		args: ["-e", "1"],
		cwd: `${ROOT}/src`,
		timeoutMs: 1000,
		tool: "run",
		capability: "mutating"
	});
	assert.equal(result.boundary, "execution:test");
	assert.equal(result.stdout, "ok");
	assert.equal(seen.length, 1);
	assert.equal(seen[0].request.tool, "run");
	assert.equal(seen[0].policy.execution_mode, "local_process");

	await assert.rejects(sandbox.run({ bin: "x", cwd: ROOT, timeoutMs: 1000, tool: "rm", capability: "mutating" }), sandboxError("SANDBOX_TOOL_DENIED"));
	await assert.rejects(sandbox.run({ bin: "x", cwd: "/etc", timeoutMs: 1000, tool: "run", capability: "mutating" }), sandboxError("SANDBOX_PATH_DENIED"));
	assert.equal(seen.length, 1);
});

test("缺少执行器、执行器不可用或结果格式不合法时拒绝", async () => {
	const noAdapter = createExecutionSandbox({ policy: readOnlyPolicy() });
	await assert.rejects(noAdapter.run({ bin: "/bin/echo", cwd: ROOT, timeoutMs: 1000 }), sandboxError("SANDBOX_UNAVAILABLE"));

	const unavailable = createExecutionSandbox({
		policy: localPolicy(),
		adapters: { local_process: { available: () => false, run: async () => ({ exitCode: 0, stdout: "", stderr: "" }) } }
	});
	await assert.rejects(unavailable.run({ bin: "x", cwd: ROOT, timeoutMs: 1000, tool: "run", capability: "mutating" }), sandboxError("SANDBOX_UNAVAILABLE"));

	const badResult = createExecutionSandbox({
		policy: readOnlyPolicy(),
		adapters: { read_only_workspace: { run: async () => ({ exitCode: "0", stdout: "", stderr: "" }) } }
	});
	await assert.rejects(badResult.run({ bin: "/bin/echo", cwd: ROOT, timeoutMs: 1000 }), sandboxError("SANDBOX_RESULT_INVALID"));
});

test("只读 adapter 拒绝可写挂载，本地 adapter 拒绝未授权", async () => {
	await assert.rejects(
		createExecutionSandbox({
			policy: localPolicy(),
			adapters: { local_process: createReadOnlyWorkspaceAdapter({ run: async () => ({ exitCode: 0, stdout: "", stderr: "" }) }) }
		}).run({ bin: "x", cwd: ROOT, timeoutMs: 1000, tool: "run", capability: "mutating" }),
		(error) => error?.code === "SANDBOX_UNAVAILABLE" || error?.code === "SANDBOX_TOOL_DENIED"
	);
	const unprivileged = resolveSandboxPolicy({
		sideEffectPolicy: "compensable",
		executionTrust: "trusted",
		workspaceRoot: ROOT,
		availableModes: ["read_only_workspace"],
		tools: { allow: ["run"] }
	});
	await assert.rejects(
		createExecutionSandbox({
			policy: unprivileged,
			adapters: { read_only_workspace: createLocalProcessAdapter({ run: async () => ({ exitCode: 0, stdout: "", stderr: "" }) }) }
		}).run({ bin: "x", cwd: ROOT, timeoutMs: 1000, tool: "run" }),
		sandboxError("HOST_EXECUTION_NOT_GRANTED")
	);
});

test("本地进程拒绝未授权、不可信与危险命令", async () => {
	const adapter = createLocalProcessAdapter({ env: process.env });
	const policy = localPolicy();
	await assert.rejects(runNode(adapter, { ...policy, allow_host_execution: false }, "1"), sandboxError("HOST_EXECUTION_NOT_GRANTED"));
	await assert.rejects(runNode(adapter, { ...policy, execution_trust: "untrusted" }, "1"), sandboxError("UNTRUSTED_EXECUTION_DENIED"));
	await assert.rejects(adapter.run({ bin: "rm", args: ["-rf", "/tmp/nope"], cwd: ROOT, timeoutMs: 1000 }, policy), sandboxError("COMMAND_NOT_ALLOWED"));
	await assert.rejects(adapter.run({ bin: "/bin/sh", args: ["-c", "echo hi"], cwd: ROOT, timeoutMs: 1000 }, policy), sandboxError("COMMAND_NOT_ALLOWED"));
	await assert.rejects(adapter.run({ bin: "sudo", args: ["id"], cwd: ROOT, timeoutMs: 1000 }, policy), sandboxError("COMMAND_NOT_ALLOWED"));
});

test("命令参数不经过 shell，注入字符串按字面量传入", async () => {
	const adapter = createLocalProcessAdapter({ env: process.env });
	const result = await runNode(
		adapter,
		localPolicy(),
		"console.log(process.argv.slice(1).join('|'))",
		"safe; rm -rf /tmp/nope",
		"$(id)"
	);
	assert.equal(result.exitCode, 0, result.stderr);
	assert.equal(result.stdout.trim(), "safe; rm -rf /tmp/nope|$(id)");
	assert.deepEqual(result.evidence.argv.slice(1), ["-e", "console.log(process.argv.slice(1).join('|'))", "safe; rm -rf /tmp/nope", "$(id)"]);
});

test("敏感环境变量与不安全注入变量在子进程中清理", async () => {
	assert.deepEqual(sanitizeExecutionEnv({ PATH: "/bin", API_KEY: "x", NORMAL: "1" }), { PATH: "/bin", NORMAL: "1" });
	const adapter = createLocalProcessAdapter({
		env: {
			PATH: process.env.PATH,
			HOME: process.env.HOME,
			SECRET_TOKEN: "leak-me",
			AWS_SECRET_ACCESS_KEY: "aws",
			GITHUB_TOKEN: "gh",
			NODE_OPTIONS: "--require /tmp/hook.js",
			SAFE_VALUE: "ok"
		}
	});
	const result = await runNode(adapter, localPolicy(), "console.log(JSON.stringify(process.env))");
	assert.equal(result.exitCode, 0, result.stderr);
	const childEnv = JSON.parse(result.stdout);
	assert.equal(childEnv.SECRET_TOKEN, undefined);
	assert.equal(childEnv.AWS_SECRET_ACCESS_KEY, undefined);
	assert.equal(childEnv.GITHUB_TOKEN, undefined);
	assert.equal(childEnv.NODE_OPTIONS, undefined);
	assert.equal(childEnv.SAFE_VALUE, "ok");
	assert.ok(result.evidence.env_keys.includes("SAFE_VALUE"));
	assert.ok(!result.evidence.env_keys.includes("SECRET_TOKEN"));
});

test("本地执行超时返回 124 并结束进程", async () => {
	const adapter = createLocalProcessAdapter({ env: process.env });
	const started = Date.now();
	const result = await runNode(adapter, localPolicy(), "setTimeout(() => {}, 5000)", { timeoutMs: 100 });
	assert.equal(result.exitCode, 124);
	assert.equal(result.timedOut, true);
	assert.ok(Date.now() - started < 4000, "超时后应立即结束进程");
});

test("取消任务会结束本地进程并拒绝执行", async () => {
	const adapter = createLocalProcessAdapter({ env: process.env });
	const controller = new AbortController();
	const running = runNode(adapter, localPolicy(), "setTimeout(() => {}, 5000)", { signal: controller.signal });
	setTimeout(() => controller.abort(new Error("用户取消")), 100);
	await assert.rejects(running, /用户取消/u);
});

test("本地执行收集退出状态与执行证据", async () => {
	const adapter = createLocalProcessAdapter({ env: process.env });
	const result = await runNode(adapter, localPolicy(), "process.exit(3)");
	assert.equal(result.exitCode, 3);
	assert.equal(result.boundary, "local_process");
	assert.deepEqual(result.evidence.argv, [process.execPath, "-e", "process.exit(3)"]);
	assert.equal(result.evidence.cwd, ROOT);
});

test("只读工具执行经过统一执行入口，拒绝会转成编排错误码", async () => {
	const registry = new ToolRegistry();
	registry.register({ name: "inspect", capability: "read_only", command: { bin: "/bin/echo", args: ["ok"] } });
	let seen = null;
	const sandbox = createExecutionSandbox({
		policy: readOnlyPolicy({ tools: { allow: ["inspect"] } }),
		adapters: {
			read_only_workspace: createReadOnlyWorkspaceAdapter({
				run: async (request) => {
					seen = request;
					return { exitCode: 0, stdout: "ok", stderr: "", boundary: "execution:test" };
				}
			})
		}
	});
	const host = {
		readonlyAvailable: () => true,
		run: async () => {
			throw new Error("不应绕过执行入口直接执行");
		}
	};
	const runner = new ReadOnlyToolRunner(registry, host, ROOT, sandbox);
	const result = await runner.runTool({ command: "inspect", cwd: ".", timeoutMs: 1000 });
	assert.equal(result.stdout, "ok");
	assert.equal(result.evidence.boundary, "execution:test");
	assert.equal(seen.tool, "inspect");
	assert.equal(seen.cwd, ROOT);

	const denied = new ReadOnlyToolRunner(registry, host, ROOT, createExecutionSandbox({ policy: readOnlyPolicy({ tools: { allow: [] } }) }));
	await assert.rejects(denied.runTool({ command: "inspect", cwd: ".", timeoutMs: 1000 }), (error) => error?.code === "SANDBOX_TOOL_DENIED");
});

test("旧版 Docker 配置被显式检测，且不会自动映射或修改原对象", () => {
	const legacyConfig = { sandbox: { kind: "docker", risk: "high" } };
	const report = detectLegacyExecutionConfig(legacyConfig);
	assert.equal(report.legacy, true);
	assert.equal(report.entries.length, 2);
	assert.deepEqual(report.entries.map((entry) => entry.path).sort(), ["sandbox.kind", "sandbox.risk"]);
	for (const entry of report.entries) {
		assert.equal(entry.suggested_mode, "read_only_workspace");
		assert.equal(entry.requires_execution_reconfirmation, true);
		assert.match(entry.reason, /重新确认执行权限/u);
	}
	assert.deepEqual(legacyConfig, { sandbox: { kind: "docker", risk: "high" } });
	assert.throws(() => assertNoLegacyExecutionConfig({ sandbox: { kind: "restricted_network" } }), sandboxError("LEGACY_EXECUTION_CONFIG"));
	assert.throws(() => assertNoLegacyExecutionConfig({ execution_mode: "docker" }), sandboxError("LEGACY_EXECUTION_CONFIG"));
	assert.equal(detectLegacyExecutionConfig({ sandbox: { mode: "read_only_workspace", trust: "trusted" } }).legacy, false);
});

test("旧版 SQLite 任务记录保留并标记，不重置数据", () => {
	const file = join(TEMP, "legacy.sqlite");
	const db = new DatabaseSync(file);
	db.exec("CREATE TABLE IF NOT EXISTS workflow_versions (workflow_id TEXT, version INTEGER, definition_json TEXT)");
	const definition = JSON.stringify({ nodes: [{ node_id: "run", config: { sandbox: { kind: "docker" } } }] });
	db.prepare("INSERT INTO workflow_versions (workflow_id, version, definition_json) VALUES (?,?,?)").run("wf-legacy", 1, definition);
	const before = db.prepare("SELECT definition_json FROM workflow_versions WHERE workflow_id=? AND version=?").get("wf-legacy", 1).definition_json;
	const report = detectLegacyExecutionConfig(JSON.parse(before));
	assert.equal(report.legacy, true);
	assert.equal(report.entries[0].path, "nodes[0].config.sandbox.kind");
	assert.throws(() => assertNoLegacyExecutionConfig(JSON.parse(before)), sandboxError("LEGACY_EXECUTION_CONFIG"));
	const afterRow = db.prepare("SELECT definition_json FROM workflow_versions WHERE workflow_id=? AND version=?").get("wf-legacy", 1).definition_json;
	assert.equal(afterRow, before);
	assert.equal(db.prepare("SELECT COUNT(*) AS count FROM workflow_versions").get().count, 1);
	db.close();
	assert.ok(file.length > 0);
});

test("没有 Docker 命令时仍可启动并执行本地进程", () => {
	const sandboxUrl = new URL("../lib/execution-sandbox.js", import.meta.url).href;
	const emptyPath = join(TEMP, "empty-bin");
	mkdirSync(emptyPath);
	writeFileSync(join(emptyPath, "README"), "no executables here\n");
	const probe = spawnSync("docker", ["--version"], { env: { PATH: emptyPath }, encoding: "utf8" });
	assert.equal(probe.error?.code, "ENOENT", "测试环境必须模拟 Docker 命令不存在");
	const script = `import { resolveSandboxPolicy, createExecutionSandbox, createLocalProcessAdapter } from ${JSON.stringify(sandboxUrl)};
const cwd = process.cwd();
const policy = resolveSandboxPolicy({ sideEffectPolicy: "compensable", executionTrust: "trusted", executionMode: "local_process", availableModes: ["local_process"], allowHostExecution: true, allowWrite: true, workspaceRoot: cwd, tools: { allow: ["probe"] } });
const sandbox = createExecutionSandbox({ policy, adapters: { local_process: createLocalProcessAdapter({ env: { PATH: ${JSON.stringify(emptyPath)}, HOME: process.env.HOME ?? "", SAFE_VALUE: "ok" } }) } });
const result = await sandbox.run({ bin: process.execPath, args: ["-e", "console.log('no-docker-ok')"], cwd, timeoutMs: 15000, tool: "probe", capability: "mutating" });
if (result.exitCode !== 0 || !result.stdout.includes("no-docker-ok")) { console.error(result); process.exit(2); }
`;
	const result = spawnSync(process.execPath, ["--input-type=module", "-e", script], {
		cwd: ROOT,
		env: { PATH: emptyPath, HOME: process.env.HOME ?? "" },
		encoding: "utf8"
	});
	assert.equal(result.status, 0, result.stderr);
});
