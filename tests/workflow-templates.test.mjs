import test from "node:test";
import assert from "node:assert/strict";
import { mkdtempSync } from "node:fs";
import { tmpdir } from "node:os";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

import { serveOrchestra } from "../lib/index.js";
import {
	compileWorkflowTemplate,
	getWorkflowTemplate,
	listWorkflowTemplates,
	validateWorkflowTemplate
} from "../lib/workflow-templates.js";

const ROOT = join(dirname(fileURLToPath(import.meta.url)), "..");
const SECRET = "template-secret-".padEnd(64, "x");
const HUMAN = { id: "human:templates" };
const REQUIREMENTS = ["code-development", "security-audit", "quality-testing", "incident-response"];

function operation(n) {
	return `01900000-0000-7000-8000-${String(n).padStart(12, "0")}`;
}

function options(dir) {
	return {
		dbFile: join(dir, "service.sqlite"),
		backupDir: join(dir, "backups"),
		secret: SECRET,
		phase: "S6",
		preferNamedPipe: false,
		configPath: join(ROOT, "orchestra.config.json"),
		asOfSeq: () => 0,
		listTasks: () => [],
		listEvents: () => [],
		appendLedgerEvent: () => {},
		resolveHttpHuman: () => HUMAN
	};
}

async function api(service, path, { method = "GET", body } = {}) {
	const response = await fetch(service.endpoint + path, {
		method,
		headers: {
			accept: "application/json",
			...(body === undefined ? {} : { "content-type": "application/json" }),
			"x-orchestra-secret": SECRET
		},
		body: body === undefined ? undefined : JSON.stringify(body)
	});
	const text = await response.text();
	let json;
	try {
		json = JSON.parse(text);
	} catch {
		json = undefined;
	}
	return { status: response.status, json };
}

test("内置模板：四个模板都可列出并编译成合法 workflow 文档", () => {
	const items = listWorkflowTemplates();
	assert.deepEqual(items.map((item) => item.template_id), REQUIREMENTS);
	for (const item of items) {
		const template = getWorkflowTemplate(item.template_id);
		const compiled = compileWorkflowTemplate(template);
		assert.equal(compiled.validation.errors.length, 0, item.template_id);
		assert.ok(compiled.definition);
		const ids = compiled.definition.nodes.map((node) => node.node_id);
		assert.equal(new Set(ids).size, ids.length);
		assert.equal(compiled.definition.nodes[0].type, "trigger");
		const agents = compiled.definition.nodes.filter((node) => node.type === "agent");
		assert.equal(agents.length, template.nodes.length);
		for (const agent of agents) {
			assert.ok(agent.config.role);
			assert.ok(agent.config.system_prompt);
			assert.equal(agent.config.side_effect_policy, "pure");
			assert.ok(Array.isArray(agent.config.acceptance));
		}
		// 每个依赖边都来自模板声明，并且没有 trigger 入边（除 start 的出边外）。
		const declared = new Set(template.nodes.flatMap((node) => node.depends_on));
		for (const edge of compiled.definition.edges) {
			if (edge.from !== "start") assert.ok(declared.has(edge.from));
		}
	}
});

test("安全事件响应模板：默认只分析建议，不携带任何工具", () => {
	const template = getWorkflowTemplate("incident-response");
	assert.equal(template.execution_policy, "advisory_only");
	const compiled = compileWorkflowTemplate(template);
	const agents = compiled.definition.nodes.filter((node) => node.type === "agent");
	assert.ok(agents.length > 0);
	for (const node of agents) {
		assert.equal(node.config.advisory_only, true);
		assert.deepEqual(node.config.tools, []);
	}
	const instruction = JSON.stringify(compiled.definition);
	assert.match(instruction, /只做分析与建议/);
	assert.match(instruction, /禁止.*封禁、删除、隔离/);
});

test("模板校验：循环依赖、自依赖、未知依赖与重复依赖都会被拒绝", () => {
	const base = getWorkflowTemplate("security-audit");
	const cycle = JSON.parse(JSON.stringify(base));
	cycle.nodes[0].depends_on = ["report"];
	cycle.nodes[5].depends_on = ["assets"];
	assert.equal(validateWorkflowTemplate(cycle).ok, false);
	assert.ok(validateWorkflowTemplate(cycle).errors.some((error) => error.code === "TEMPLATE_DEPENDENCY_CYCLE"));

	const self = JSON.parse(JSON.stringify(base));
	self.nodes[0].depends_on = ["assets"];
	assert.ok(validateWorkflowTemplate(self).errors.some((error) => error.code === "TEMPLATE_DEPENDENCY_SELF"));

	const unknown = JSON.parse(JSON.stringify(base));
	unknown.nodes[0].depends_on = ["missing"];
	assert.ok(validateWorkflowTemplate(unknown).errors.some((error) => error.code === "TEMPLATE_DEPENDENCY_UNKNOWN"));

	const duplicate = JSON.parse(JSON.stringify(base));
	duplicate.nodes[1].depends_on = ["assets", "assets"];
	assert.ok(validateWorkflowTemplate(duplicate).errors.some((error) => error.code === "TEMPLATE_DEPENDENCY_DUPLICATE"));
});

test("模板校验：advisory_only 模板不允许携带工具", () => {
	const template = getWorkflowTemplate("incident-response");
	template.agents[0].tools = ["shell_exec"];
	const result = validateWorkflowTemplate(template);
	assert.equal(result.ok, false);
	assert.ok(result.errors.some((error) => error.code === "TEMPLATE_ADVISORY_TOOLS_FORBIDDEN"));
});

test("模板导入导出：JSON roundtrip 后语义不变", () => {
	for (const id of REQUIREMENTS) {
		const original = getWorkflowTemplate(id);
		const roundtrip = JSON.parse(JSON.stringify(original));
		assert.equal(validateWorkflowTemplate(roundtrip).ok, true, id);
		const left = compileWorkflowTemplate(original).definition;
		const right = compileWorkflowTemplate(roundtrip).definition;
		assert.deepEqual(right.nodes, left.nodes);
		assert.deepEqual(right.edges, left.edges);
		assert.deepEqual(right.name, left.name);
	}
});

test("自定义模板：角色、系统提示、验收标准、知识库、依赖与模型档位都会保留", () => {
	const template = {
		schema_version: "1",
		template_id: "custom-flow",
		name: "自定义流程",
		description: "验证模板字段",
		category: "自定义",
		execution_policy: "standard",
		knowledge_base: [{ id: "kb-one", title: "规范", content: "只读分析。" }],
		agents: [
			{ role_id: "planner", name: "规划员", model_class: "code", system_prompt: "先列计划。", tools: [] },
			{ role_id: "checker", name: "核对员", model_class: "audit", system_prompt: "逐条核对。", tools: [] }
		],
		nodes: [
			{ node_id: "plan", title: "规划", role_id: "planner", instruction: "拆解任务。", depends_on: [], acceptance: ["有步骤"], knowledge: [] },
			{ node_id: "check", title: "核对", role_id: "checker", instruction: "核对结果。", depends_on: ["plan"], acceptance: ["逐条核对"], model_class: "audit", knowledge: ["kb-one"] }
		]
	};
	const compiled = compileWorkflowTemplate(template, { name: "发布用名称" });
	assert.equal(compiled.validation.errors.length, 0);
	assert.equal(compiled.definition.name, "发布用名称");
	const plan = compiled.definition.nodes.find((node) => node.node_id === "plan");
	const check = compiled.definition.nodes.find((node) => node.node_id === "check");
	assert.equal(plan.config.role, "规划员");
	assert.equal(plan.config.system_prompt, "先列计划。");
	assert.equal(plan.config.model_class, "code");
	assert.deepEqual(plan.config.knowledge_base, [{ id: "kb-one", title: "规范", content: "只读分析。" }]);
	assert.equal(check.config.model_class, "audit");
	assert.deepEqual(check.config.acceptance, ["逐条核对"]);
	assert.ok(compiled.definition.edges.some((edge) => edge.from === "plan" && edge.to === "check"));
	assert.ok(compiled.definition.edges.some((edge) => edge.from === "start" && edge.to === "plan"));
});

test("模板 API：列表、读取、编译、导入校验与创建发布", async (t) => {
	const dir = mkdtempSync(join(tmpdir(), "dsh-orchestra-tpl-"));
	const service = await serveOrchestra(options(dir));
	t.after(async () => service.close());

	const listed = await api(service, "/v1/workflow-templates");
	assert.equal(listed.status, 200);
	assert.deepEqual(listed.json.data.items.map((item) => item.template_id), REQUIREMENTS);
	assert.equal(listed.json.data.items.find((item) => item.template_id === "incident-response").advisory_only, true);

	const detail = await api(service, "/v1/workflow-templates/get?template_id=code-development");
	assert.equal(detail.status, 200);
	assert.equal(detail.json.data.template_id, "code-development");

	const compiled = await api(service, "/v1/workflow-templates/compile", {
		method: "POST",
		body: { template_id: "code-development", name: "研发链路" }
	});
	assert.equal(compiled.status, 200);
	assert.equal(compiled.json.data.definition.name, "研发链路");
	assert.equal(compiled.json.data.save_validation.errors.length, 0);

	const imported = await api(service, "/v1/workflow-templates/compile", {
		method: "POST",
		body: { template: getWorkflowTemplate("incident-response") }
	});
	assert.equal(imported.status, 200);
	assert.ok(imported.json.data.definition.nodes.filter((node) => node.type === "agent").every((node) => node.config.advisory_only === true && node.config.tools.length === 0));

	const validated = await api(service, "/v1/workflow-templates/validate", {
		method: "POST",
		body: { template: getWorkflowTemplate("quality-testing"), name: "上传的测试模板" }
	});
	assert.equal(validated.status, 200);
	assert.equal(validated.json.data.ok, true);
	assert.equal(validated.json.data.errors.length, 0);
	assert.equal(validated.json.data.definition.name, "上传的测试模板");
	assert.equal(validated.json.data.template.template_id, "quality-testing");

	const invalidCycle = getWorkflowTemplate("code-development");
	invalidCycle.nodes[0].depends_on = ["integration"];
	invalidCycle.nodes[6].depends_on = ["review"];
	const rejectedValidation = await api(service, "/v1/workflow-templates/validate", {
		method: "POST",
		body: { template: invalidCycle }
	});
	assert.equal(rejectedValidation.status, 200);
	assert.equal(rejectedValidation.json.data.ok, false);
	assert.ok(rejectedValidation.json.data.errors.some((error) => error.code === "TEMPLATE_DEPENDENCY_CYCLE"));

	const unsafeIncident = getWorkflowTemplate("incident-response");
	unsafeIncident.agents[0].tools = ["shell_exec"];
	const rejectedUnsafe = await api(service, "/v1/workflow-templates/validate", {
		method: "POST",
		body: { template: unsafeIncident }
	});
	assert.equal(rejectedUnsafe.status, 200);
	assert.equal(rejectedUnsafe.json.data.ok, false);
	assert.ok(rejectedUnsafe.json.data.errors.some((error) => error.code === "TEMPLATE_ADVISORY_TOOLS_FORBIDDEN"));

	const missingUpload = await api(service, "/v1/workflow-templates/validate", {
		method: "POST",
		body: {}
	});
	assert.equal(missingUpload.status, 422);
	assert.equal(missingUpload.json.error.code, "PAYLOAD_SCHEMA_INVALID");

	const broken = getWorkflowTemplate("code-development");
	broken.nodes[0].depends_on = ["integration"];
	broken.nodes[6].depends_on = ["review", "integration"];
	const rejected = await api(service, "/v1/workflow-templates/compile", { method: "POST", body: { template: broken } });
	assert.equal(rejected.status, 422);
	assert.equal(rejected.json.error.code, "WORKFLOW_VALIDATION_FAILED");

	const created = await api(service, "/v1/workflows", {
		method: "POST",
		body: {
			workflow_id: "wf_from_template",
			name: compiled.json.data.definition.name,
			definition: compiled.json.data.definition,
			operation_id: operation(1)
		}
	});
	assert.equal(created.status, 200);
	assert.equal(created.json.data.validation.errors.length, 0);

	const tested = await api(service, "/v1/workflows/test", {
		method: "POST",
		body: { workflow_id: "wf_from_template", revision: 1 }
	});
	assert.equal(tested.status, 200);
	assert.equal(tested.json.data.passed, true);

	const published = await api(service, "/v1/workflows/publish", {
		method: "POST",
		body: {
			workflow_id: "wf_from_template",
			revision: 1,
			base_version: 0,
			publish_note: "模板集成测试",
			operation_id: operation(2)
		}
	});
	assert.equal(published.status, 200);
});
