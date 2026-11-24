import test from "node:test";
import assert from "node:assert/strict";

import {
	authorizeWorkspace,
	classifyProfile,
	createLocalServiceManager,
	createWebPanelTransport,
	createWebRequestAuthorizer,
	isLoopbackHost,
	webHostCapabilities
} from "../lib/web-profile-host.js";

const HUMAN = { id: "local-dsh:tester" };
const PROOF = "proof-value-123";
const SERVICES = {
	sessionController: { resolveAgent() {} },
	workspaceRegistry: { get() {}, list() {} },
	agents: { get() {} },
	credentials: { modifyRecord() {} },
	llm: {},
	tools: { register() {} },
	workspaceFiles: { read() {}, list() {} }
};
const WEB_SERVER = { host: "127.0.0.1", register() {} };

function request(path = "/health", { method = "GET", headers = {}, body } = {}) {
	return new Request(`http://127.0.0.1:3080${path}`, {
		method,
		headers: {
			host: "127.0.0.1:3080",
			"x-orchestra-web-proof": PROOF,
			...(method === "POST" ? { "content-type": "application/json", "x-orchestra-request": "1" } : {}),
			...headers
		},
		...(body === void 0 ? {} : { body })
	});
}

test("Web Profile：识别 desktop / web，其他标记为未支持", () => {
	assert.equal(classifyProfile({ name: "desktop" }), "desktop");
	assert.equal(classifyProfile({ name: "web" }), "web");
	assert.equal(classifyProfile({ name: "cli" }), "unsupported");
	assert.equal(classifyProfile(void 0), "unsupported");
	assert.equal(isLoopbackHost("localhost"), true);
	assert.equal(isLoopbackHost("127.0.0.1"), true);
	assert.equal(isLoopbackHost("::1"), true);
	assert.equal(isLoopbackHost("192.168.1.2"), false);
});

test("Web Profile：完整装配通过，缺失能力明确列出", () => {
	const ready = webHostCapabilities({ profile: { name: "web" }, webServer: WEB_SERVER, services: SERVICES });
	assert.equal(ready.ok, true);
	assert.equal(ready.authenticated, true);
	assert.deepEqual(ready.missing, []);

	const incomplete = webHostCapabilities({
		profile: { name: "web" },
		webServer: WEB_SERVER,
		services: { ...SERVICES, sessionController: void 0, credentials: {} }
	});
	assert.equal(incomplete.ok, false);
	assert.ok(incomplete.missing.some((item) => item.code === "MISSING_SESSIONCONTROLLER"));
	assert.ok(incomplete.missing.some((item) => item.code === "MISSING_CREDENTIALS"));
});

test("Web Profile：非本机监听且未配置认证时拒绝装配", () => {
	const result = webHostCapabilities({
		profile: { name: "web" },
		webServer: { host: "0.0.0.0", register() {} },
		services: SERVICES
	});
	assert.equal(result.ok, false);
	assert.equal(result.authenticated, false);
	assert.ok(result.missing.some((item) => item.code === "OPERATOR_AUTHENTICATION_REQUIRED"));
});

test("Web Profile：非授权请求被拒绝", () => {
	const authorizer = createWebRequestAuthorizer({ webServer: WEB_SERVER, proof: PROOF, human: HUMAN });
	assert.equal(authorizer.authorize(request()).ok, true);
	assert.equal(authorizer.authorize(request("/v1/tasks", { method: "POST", body: "{}" })).ok, true);

	const noProof = authorizer.authorize(request("/v1/tasks", { headers: { "x-orchestra-web-proof": "wrong" } }));
	assert.equal(noProof.ok, false);
	assert.equal(noProof.status, 403);

	const crossSite = authorizer.authorize(request("/v1/tasks", { headers: { "sec-fetch-site": "cross-site" } }));
	assert.equal(crossSite.ok, false);
	assert.equal(crossSite.status, 403);

	const crossOrigin = authorizer.authorize(request("/v1/tasks", { headers: { origin: "http://evil.example" } }));
	assert.equal(crossOrigin.ok, false);
	assert.equal(crossOrigin.status, 403);

	const nonLoopback = authorizer.authorize(request("/v1/tasks", { headers: { host: "10.0.0.8:3080" } }));
	assert.equal(nonLoopback.ok, false);
	assert.equal(nonLoopback.status, 403);

	const wrongContent = authorizer.authorize(request("/v1/tasks", {
		method: "POST",
		body: "{}",
		headers: { "content-type": "text/plain" }
	}));
	assert.equal(wrongContent.ok, false);
});

test("Web Profile：工作区授权失败被拒绝", () => {
	const registry = {
		archivedSessionIds: ["s_archived"],
		get(id) {
			if (id === "ws_ok") return { id: "ws_ok", sessionIds: ["s_ok", "s_archived"] };
			return void 0;
		}
	};
	assert.equal(authorizeWorkspace({ workspaceRegistry: registry, workspaceId: "ws_ok", sessionId: "s_ok" }).ok, true);
	assert.equal(authorizeWorkspace({ workspaceRegistry: registry, workspaceId: "ws_ok", sessionId: "s_foreign" }).ok, false);
	assert.equal(authorizeWorkspace({ workspaceRegistry: registry, workspaceId: "ws_ok", sessionId: "s_archived" }).ok, false);
	assert.equal(authorizeWorkspace({ workspaceRegistry: registry, workspaceId: "ws_missing", sessionId: "s_ok" }).ok, false);
	assert.equal(authorizeWorkspace({ workspaceRegistry: void 0, workspaceId: "ws_ok", sessionId: "s_ok" }).ok, false);
});

test("Web Profile：重复启动复用同一个服务，停止后释放", async () => {
	let opened = 0;
	let closed = 0;
	const manager = createLocalServiceManager({
		open: async () => {
			opened += 1;
			return { close: async () => { closed += 1; } };
		}
	});
	const [first, second] = await Promise.all([manager.ensure("a"), manager.ensure("a")]);
	assert.equal(opened, 1);
	assert.equal(first, second);
	assert.equal(manager.size, 1);
	await manager.close("a");
	assert.equal(closed, 1);
	assert.equal(manager.size, 0);
});

test("Web Profile：启动失败清理占位，重启创建新服务并恢复任务", async () => {
	let attempts = 0;
	const tasks = [];
	const manager = createLocalServiceManager({
		open: async () => {
			attempts += 1;
			if (attempts === 1) throw new Error("启动失败");
			return {
				tasks,
				close: async () => {}
			};
		}
	});
	await assert.rejects(manager.ensure("a"), /启动失败/u);
	assert.equal(manager.size, 0);
	const service = await manager.ensure("a");
	assert.equal(attempts, 2);
	assert.equal(service.tasks, tasks);

	const restarted = await manager.restart("a");
	assert.equal(attempts, 3);
	assert.equal(restarted.tasks, tasks);
	assert.equal(manager.size, 1);
	await manager.closeAll();
	assert.equal(manager.size, 0);
	await assert.rejects(manager.ensure("b"), /已停止/u);
});

test("Web Profile：服务数超限时拒绝新会话", async () => {
	const manager = createLocalServiceManager({ open: async () => ({ close: async () => {} }), max: 1 });
	await manager.ensure("a");
	await assert.rejects(manager.ensure("b"), /过多/u);
	await manager.closeAll();
});

test("Web Profile：面板传输描述携带证明，缺认证时不带证明", () => {
	const ready = createWebPanelTransport({ proof: PROOF });
	assert.equal(ready.route, "/api/orchestra.request");
	assert.equal(ready.proof, PROOF);
	assert.deepEqual(ready.missing_capabilities, []);

	const incomplete = createWebPanelTransport({ missing: [{ code: "OPERATOR_AUTHENTICATION_REQUIRED", message: "需要认证" }] });
	assert.equal(incomplete.proof, null);
	assert.equal(incomplete.missing_capabilities.length, 1);
});
