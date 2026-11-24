import test from "node:test";
import assert from "node:assert/strict";

import {
	PANEL_TRANSPORT_SERVICE,
	PANEL_TRANSPORT_VERSION,
	authorizeToolUse,
	authorizeWorkspace,
	classifyProfile,
	createLocalServiceManager,
	createOrchestraPanelTransportService,
	createWebPanelTransport,
	createWebRequestAuthorizer,
	installOrchestraPanelTransport,
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

test("Web Profile：请求授权返回宿主所有者身份，非 owner 被拒绝", () => {
	const authorizer = createWebRequestAuthorizer({ webServer: WEB_SERVER, proof: PROOF, human: HUMAN });
	const verdict = authorizer.authorize(request());
	assert.equal(verdict.ok, true);
	assert.equal(verdict.principal.kind, "owner");
	assert.equal(verdict.principal.id, HUMAN.id);
	assert.equal(verdict.principal.source, "dsh-web-owner");
	assert.equal(verdict.human.principal, verdict.principal);

	const anonymous = createWebRequestAuthorizer({ webServer: WEB_SERVER, proof: PROOF });
	assert.equal(anonymous.authorize(request()).status, 503);

	const guest = createWebRequestAuthorizer({
		webServer: WEB_SERVER,
		proof: PROOF,
		human: { ...HUMAN, principal: { kind: "guest", id: HUMAN.id } }
	});
	const denied = guest.authorize(request());
	assert.equal(denied.ok, false);
	assert.equal(denied.status, 403);
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
	const ready = createWebPanelTransport({ proof: PROOF, human: HUMAN });
	assert.equal(ready.transport_version, PANEL_TRANSPORT_VERSION);
	assert.equal(ready.profile, "web");
	assert.equal(ready.route, "/api/orchestra.request");
	assert.equal(ready.global, "__DSH_ORCHESTRA_WEB__");
	assert.equal(ready.proof, PROOF);
	assert.equal(ready.principal.kind, "owner");
	assert.equal(ready.principal.id, HUMAN.id);
	assert.equal(ready.tools.capability, "read_only");
	assert.deepEqual(ready.missing_capabilities, []);

	const incomplete = createWebPanelTransport({ missing: [{ code: "OPERATOR_AUTHENTICATION_REQUIRED", message: "需要认证" }] });
	assert.equal(incomplete.proof, null);
	assert.equal(incomplete.principal, null);
	assert.equal(incomplete.missing_capabilities.length, 1);
});

test("Web Profile：面板传输服务只向宿主所有者页面下发证明", () => {
	const authorizer = createWebRequestAuthorizer({ webServer: WEB_SERVER, proof: PROOF, human: HUMAN });
	const service = createOrchestraPanelTransportService({ human: HUMAN, proof: PROOF, authorizer });
	assert.equal(service.version, PANEL_TRANSPORT_VERSION);
	assert.equal(service.name, PANEL_TRANSPORT_SERVICE);
	assert.equal(service.profile, "web");
	assert.equal(service.route, "/api/orchestra.request");
	assert.equal(service.global, "__DSH_ORCHESTRA_WEB__");

	for (const peer of [void 0, { principal: { kind: "owner", id: HUMAN.id } }]) {
		const transport = service.transportFor(peer);
		assert.equal(transport.proof, PROOF);
		assert.equal(transport.principal.kind, "owner");
		assert.equal(transport.tools.capability, "read_only");
	}

	for (const peer of [{}, { principal: void 0 }, { principal: { kind: "guest", id: "u_guest" } }]) {
		const restricted = service.transportFor(peer);
		assert.equal(restricted.proof, null);
		assert.equal(restricted.principal, null);
		assert.ok(restricted.missing_capabilities.some((item) => item.code === "OPERATOR_AUTHENTICATION_REQUIRED"));
	}

	assert.equal(service.authorize(request()).ok, true);
	assert.equal(service.authorize(request("/v1/tasks", { headers: { "x-orchestra-web-proof": "wrong" } })).ok, false);

	const unconfigured = createOrchestraPanelTransportService({ human: HUMAN, proof: PROOF });
	assert.equal(unconfigured.authorize(request()).status, 503);
});

test("Web Profile：orchestraPanelTransport 装配成功、重复冲突与能力不可用", () => {
	const provided = [];
	const ctx = {
		provide(name, value) {
			if (provided.length > 0) throw new Error(`${name} 已提供`);
			provided.push([name, value]);
			return () => {};
		}
	};
	const service = createOrchestraPanelTransportService({ human: HUMAN, proof: PROOF });
	const installed = installOrchestraPanelTransport(ctx, service);
	assert.equal(installed.ok, true);
	assert.equal(typeof installed.dispose, "function");
	assert.equal(provided[0][0], PANEL_TRANSPORT_SERVICE);
	assert.equal(provided[0][1], service);

	const conflict = installOrchestraPanelTransport(ctx, service);
	assert.equal(conflict.ok, false);
	assert.equal(conflict.code, "PANEL_TRANSPORT_CONFLICT");

	const unavailable = installOrchestraPanelTransport({}, service);
	assert.equal(unavailable.ok, false);
	assert.equal(unavailable.code, "PANEL_TRANSPORT_UNAVAILABLE");
});

test("Web Profile：工具授权只允许已登记的只读工具", () => {
	const tools = new Map([
		["workspace_read_sample", { capability: "read_only" }],
		["workspace_write", { capability: "write" }]
	]);
	const registry = { lookup: (name) => tools.get(name) };

	assert.deepEqual(authorizeToolUse({ registry, names: ["workspace_read_sample"] }), { ok: true, denied: [] });

	const deniedWrite = authorizeToolUse({ registry, names: ["workspace_read_sample", "workspace_write"] });
	assert.equal(deniedWrite.ok, false);
	assert.equal(deniedWrite.code, "TOOL_NOT_ALLOWED");
	assert.deepEqual(deniedWrite.denied, ["workspace_write"]);

	const deniedUnknown = authorizeToolUse({ registry, names: ["missing"] });
	assert.equal(deniedUnknown.ok, false);
	assert.deepEqual(deniedUnknown.denied, ["missing"]);

	const unavailable = authorizeToolUse({ names: ["workspace_read_sample"] });
	assert.equal(unavailable.ok, false);
	assert.equal(unavailable.code, "TOOL_AUTHORIZATION_UNAVAILABLE");
	assert.deepEqual(unavailable.denied, ["workspace_read_sample"]);
});

test("Web Profile：宿主 apply 装配传输服务、证明路由与 owner 身份", async () => {
	const { apply } = await import("../lib/index.js");
	const routes = [];
	const injected = [];
	const effects = [];
	const provided = new Map();
	const webServer = {
		host: "127.0.0.1",
		register(spec) {
			routes.push(spec);
			return () => {};
		}
	};
	const host = {
		sessionController: { resolveAgent() {} },
		workspaceRegistry: { get() {}, list() {}, archivedSessionIds: [] },
		agents: { get() {} },
		credentials: { modifyRecord() {} },
		llm: {},
		tools: { register() {} },
		get(name) {
			if (name === "webServer") return webServer;
			if (name === "workspaceFiles") return { read() {}, list() {} };
			return void 0;
		},
		on(event, handler) {
			if (event === "webserver/index-inject") injected.push(handler);
		},
		provide(name, value) {
			if (provided.has(name)) throw new Error(`${name} 已提供`);
			provided.set(name, value);
			return () => {};
		},
		effect(fn) {
			effects.push(fn);
		}
	};
	const ctx = {
		get: (name) => name === "profileContext" ? { name: "web" } : void 0,
		inject(_dependencies, callback) {
			callback(host);
		},
		effect(fn) {
			effects.push(fn);
		}
	};

	apply(ctx, { local: {} });

	const transport = provided.get(PANEL_TRANSPORT_SERVICE);
	assert.ok(transport);
	assert.equal(transport.profile, "web");
	assert.deepEqual(transport.capabilities.missing, []);
	assert.equal(injected.length, 1);

	const ownerTable = [];
	injected[0](ownerTable, { principal: { kind: "owner", id: HUMAN.id } });
	assert.equal(ownerTable.length, 1);
	assert.equal(ownerTable[0].kind, "global");
	assert.equal(ownerTable[0].name, "__DSH_ORCHESTRA_WEB__");
	assert.equal(ownerTable[0].value.transport_version, PANEL_TRANSPORT_VERSION);
	assert.ok(ownerTable[0].value.proof);
	assert.equal(ownerTable[0].value.principal.kind, "owner");

	const guestTable = [];
	injected[0](guestTable, { principal: { kind: "guest", id: "u_guest" } });
	assert.equal(guestTable[0].value.proof, null);

	assert.equal(routes.length, 1);
	assert.equal(routes[0].kind, "exact");
	assert.equal(routes[0].path, "/api/orchestra.request");

	for (const fn of effects) {
		const dispose = fn();
		if (typeof dispose === "function") dispose();
	}
});

test("Desktop Profile：不装配 Web 传输服务，继续使用本地面板路由", async () => {
	const { apply } = await import("../lib/index.js");
	const fetches = [];
	const provided = new Map();
	const host = {
		sessionController: { resolveAgent() {} },
		workspaceRegistry: { get() {}, list() {}, archivedSessionIds: [] },
		agents: { get() {} },
		credentials: { modifyRecord() {} },
		llm: {},
		tools: { register() {} },
		connection: { fetch: { register(spec) { fetches.push(spec); return () => {}; } } },
		get() {},
		on() {},
		effect() {}
	};
	const ctx = {
		get: (name) => name === "profileContext" ? { name: "desktop" } : void 0,
		inject(_dependencies, callback) {
			callback(host);
		},
		provide(name, value) {
			provided.set(name, value);
			return () => {};
		},
		effect() {}
	};

	apply(ctx, { local: {} });

	assert.equal(provided.has(PANEL_TRANSPORT_SERVICE), false);
	assert.equal(fetches.length, 1);
	assert.equal(fetches[0].path, "/api/orchestra.request");
});
