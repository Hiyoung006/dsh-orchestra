// 内置编排板模板：把常见工作流沉淀成可复用、可导入导出的 JSON。
//
// 设计约束：
// - 模板只是数据，不执行任何动作；编译结果仍是现有 workflow 文档。
// - 安全事件响应默认 execution_policy = "advisory_only"：只分析建议，不执行封禁/删除/隔离。
// - 模板里的工具只允许只读工具；运行面仍按现有 pure 策略校验。

export const WORKFLOW_TEMPLATE_SCHEMA_VERSION = "1";
export const WORKFLOW_TEMPLATE_MAX_BYTES = 256 * 1024;

const TEMPLATE_ID_RE = /^[a-z][a-z0-9_-]{1,63}$/;
const NODE_ID_RE = /^[a-z][a-z0-9_]{0,63}$/;
const KNOWLEDGE_ID_RE = /^[a-z][a-z0-9_-]{0,63}$/;
export const TEMPLATE_MODEL_CLASSES = Object.freeze(["simple", "code", "audit"]);

const LIMITS = Object.freeze({
	nodes: 64,
	agents: 32,
	knowledge: 16,
	acceptance: 20,
	instruction: 8000,
	systemPrompt: 8000,
	knowledgeTitle: 120,
	knowledgeContent: 4000,
	acceptanceItem: 500,
	description: 2000
});

const text = (value) => typeof value === "string" ? value.trim() : "";
const list = (value) => Array.isArray(value) ? value : [];

function fail(errors, path, code, message) {
	errors.push({ path, code, message });
}

function byteLength(value) {
	try {
		return Buffer.byteLength(JSON.stringify(value) ?? "", "utf8");
	} catch {
		return Infinity;
	}
}

/** Kahn 拓扑排序：返回 null 表示有环。 */
function topologicalOrder(nodeIds, dependencies) {
	const indegree = new Map(nodeIds.map((id) => [id, 0]));
	const children = new Map(nodeIds.map((id) => [id, []]));
	for (const id of nodeIds) {
		for (const dep of dependencies.get(id) ?? []) {
			indegree.set(id, (indegree.get(id) ?? 0) + 1);
			children.get(dep)?.push(id);
		}
	}
	const queue = nodeIds.filter((id) => indegree.get(id) === 0);
	const order = [];
	while (queue.length > 0) {
		const id = queue.shift();
		order.push(id);
		for (const child of children.get(id) ?? []) {
			const next = (indegree.get(child) ?? 0) - 1;
			indegree.set(child, next);
			if (next === 0) queue.push(child);
		}
	}
	return order.length === nodeIds.length ? order : null;
}

function normalizeTemplate(input) {
	return {
		schema_version: text(input.schema_version) || WORKFLOW_TEMPLATE_SCHEMA_VERSION,
		template_id: text(input.template_id),
		name: text(input.name),
		description: text(input.description),
		category: text(input.category),
		execution_policy: input.execution_policy === "advisory_only" ? "advisory_only" : "standard",
		knowledge_base: list(input.knowledge_base).map((entry) => ({
			id: text(entry?.id),
			title: text(entry?.title),
			content: text(entry?.content)
		})),
		agents: list(input.agents).map((agent) => ({
			role_id: text(agent?.role_id),
			name: text(agent?.name),
			model_class: text(agent?.model_class) || null,
			system_prompt: text(agent?.system_prompt),
			tools: list(agent?.tools).map((tool) => text(tool)).filter(Boolean)
		})),
		nodes: list(input.nodes).map((node) => ({
			node_id: text(node?.node_id),
			title: text(node?.title),
			role_id: text(node?.role_id),
			instruction: text(node?.instruction),
			depends_on: list(node?.depends_on).map((id) => text(id)).filter(Boolean),
			acceptance: list(node?.acceptance).map((item) => text(item)).filter(Boolean),
			model_class: text(node?.model_class) || null,
			knowledge: list(node?.knowledge).map((id) => text(id)).filter(Boolean),
			tools: list(node?.tools).map((tool) => text(tool)).filter(Boolean),
			position: node?.position && Number.isFinite(node.position.x) && Number.isFinite(node.position.y) ? {
				x: Math.round(node.position.x),
				y: Math.round(node.position.y)
			} : null
		}))
	};
}

/**
 * 校验一个模板（内置或导入）。返回结构化错误，供 API 与测试共用。
 */
export function validateWorkflowTemplate(input) {
	const errors = [];
	if (input === null || typeof input !== "object" || Array.isArray(input)) {
		fail(errors, "", "TEMPLATE_INVALID", "模板必须是 JSON 对象");
		return { ok: false, errors };
	}
	if (byteLength(input) > WORKFLOW_TEMPLATE_MAX_BYTES) {
		fail(errors, "", "TEMPLATE_TOO_LARGE", `模板超过 ${WORKFLOW_TEMPLATE_MAX_BYTES} 字节`);
		return { ok: false, errors };
	}
	const template = normalizeTemplate(input);
	if (template.schema_version !== WORKFLOW_TEMPLATE_SCHEMA_VERSION) fail(errors, "/schema_version", "TEMPLATE_SCHEMA_UNSUPPORTED", "schema_version 必须是 \"1\"");
	if (!TEMPLATE_ID_RE.test(template.template_id)) fail(errors, "/template_id", "TEMPLATE_ID_INVALID", "template_id 必须是 ^[a-z][a-z0-9_-]{1,63}$");
	if (template.name.length < 1 || template.name.length > 80) fail(errors, "/name", "TEMPLATE_NAME_INVALID", "模板名必须是 1–80 字");
	if (template.description.length > LIMITS.description) fail(errors, "/description", "TEMPLATE_DESCRIPTION_INVALID", `描述最多 ${LIMITS.description} 字`);
	if (template.category.length > 40) fail(errors, "/category", "TEMPLATE_CATEGORY_INVALID", "分类最多 40 字");
	if (template.nodes.length === 0) fail(errors, "/nodes", "TEMPLATE_NODES_EMPTY", "模板至少需要一个节点");
	if (template.nodes.length > LIMITS.nodes) fail(errors, "/nodes", "TEMPLATE_NODES_TOO_MANY", `节点最多 ${LIMITS.nodes} 个`);
	if (template.agents.length > LIMITS.agents) fail(errors, "/agents", "TEMPLATE_AGENTS_TOO_MANY", `Agent 最多 ${LIMITS.agents} 个`);
	if (template.knowledge_base.length > LIMITS.knowledge) fail(errors, "/knowledge_base", "TEMPLATE_KNOWLEDGE_TOO_MANY", `知识库最多 ${LIMITS.knowledge} 条`);

	const roles = new Map();
	template.agents.forEach((agent, index) => {
		const at = `/agents/${index}`;
		if (!NODE_ID_RE.test(agent.role_id)) fail(errors, `${at}/role_id`, "TEMPLATE_ROLE_ID_INVALID", "role_id 必须是 ^[a-z][a-z0-9_]{0,63}$");
		if (roles.has(agent.role_id)) fail(errors, `${at}/role_id`, "TEMPLATE_ROLE_DUPLICATE", "role_id 必须唯一");
		roles.set(agent.role_id, agent);
		if (agent.name.length < 1 || agent.name.length > 80) fail(errors, `${at}/name`, "TEMPLATE_ROLE_NAME_INVALID", "Agent 名称必须是 1–80 字");
		if (agent.system_prompt.length > LIMITS.systemPrompt) fail(errors, `${at}/system_prompt`, "TEMPLATE_PROMPT_TOO_LONG", `系统提示最多 ${LIMITS.systemPrompt} 字`);
		if (agent.model_class !== null && !TEMPLATE_MODEL_CLASSES.includes(agent.model_class)) fail(errors, `${at}/model_class`, "TEMPLATE_MODEL_CLASS_INVALID", `model_class 只能是 ${TEMPLATE_MODEL_CLASSES.join(" / ")}`);
	});

	const knowledgeIds = new Set();
	template.knowledge_base.forEach((entry, index) => {
		const at = `/knowledge_base/${index}`;
		if (!KNOWLEDGE_ID_RE.test(entry.id)) fail(errors, `${at}/id`, "TEMPLATE_KNOWLEDGE_ID_INVALID", "知识库 id 必须是 ^[a-z][a-z0-9_-]{0,63}$");
		if (knowledgeIds.has(entry.id)) fail(errors, `${at}/id`, "TEMPLATE_KNOWLEDGE_DUPLICATE", "知识库 id 必须唯一");
		knowledgeIds.add(entry.id);
		if (entry.title.length < 1 || entry.title.length > LIMITS.knowledgeTitle) fail(errors, `${at}/title`, "TEMPLATE_KNOWLEDGE_TITLE_INVALID", `知识库标题必须是 1–${LIMITS.knowledgeTitle} 字`);
		if (entry.content.length < 1 || entry.content.length > LIMITS.knowledgeContent) fail(errors, `${at}/content`, "TEMPLATE_KNOWLEDGE_CONTENT_INVALID", `知识库内容必须是 1–${LIMITS.knowledgeContent} 字`);
	});

	const nodeIds = new Set();
	template.nodes.forEach((node, index) => {
		const at = `/nodes/${index}`;
		if (!NODE_ID_RE.test(node.node_id)) fail(errors, `${at}/node_id`, "TEMPLATE_NODE_ID_INVALID", "node_id 必须是 ^[a-z][a-z0-9_]{0,63}$");
		if (nodeIds.has(node.node_id)) fail(errors, `${at}/node_id`, "TEMPLATE_NODE_DUPLICATE", "node_id 必须唯一");
		nodeIds.add(node.node_id);
		if (node.title.length < 1 || node.title.length > 80) fail(errors, `${at}/title`, "TEMPLATE_NODE_TITLE_INVALID", "节点标题必须是 1–80 字");
		if (node.instruction.length < 1 || node.instruction.length > LIMITS.instruction) fail(errors, `${at}/instruction`, "TEMPLATE_INSTRUCTION_INVALID", `节点指令必须是 1–${LIMITS.instruction} 字`);
		if (!roles.has(node.role_id)) fail(errors, `${at}/role_id`, "TEMPLATE_ROLE_UNKNOWN", "节点引用的 Agent 角色不存在");
		if (node.acceptance.length > LIMITS.acceptance) fail(errors, `${at}/acceptance`, "TEMPLATE_ACCEPTANCE_TOO_MANY", `验收标准最多 ${LIMITS.acceptance} 条`);
		node.acceptance.forEach((item, itemIndex) => {
			if (item.length > LIMITS.acceptanceItem) fail(errors, `${at}/acceptance/${itemIndex}`, "TEMPLATE_ACCEPTANCE_TOO_LONG", `单条验收标准最多 ${LIMITS.acceptanceItem} 字`);
		});
		if (node.model_class !== null && !TEMPLATE_MODEL_CLASSES.includes(node.model_class)) fail(errors, `${at}/model_class`, "TEMPLATE_MODEL_CLASS_INVALID", `model_class 只能是 ${TEMPLATE_MODEL_CLASSES.join(" / ")}`);
		node.knowledge.forEach((id) => {
			if (!knowledgeIds.has(id)) fail(errors, `${at}/knowledge`, "TEMPLATE_KNOWLEDGE_UNKNOWN", `节点引用的知识库不存在：${id}`);
		});
	});

	const dependencies = new Map();
	template.nodes.forEach((node, index) => {
		const at = `/nodes/${index}/depends_on`;
		const seen = new Set();
		const deps = [];
		for (const dep of node.depends_on) {
			if (dep === node.node_id) {
				fail(errors, at, "TEMPLATE_DEPENDENCY_SELF", "任务不能依赖自己");
				continue;
			}
			if (!nodeIds.has(dep)) {
				fail(errors, at, "TEMPLATE_DEPENDENCY_UNKNOWN", `依赖的任务不存在：${dep}`);
				continue;
			}
			if (seen.has(dep)) {
				fail(errors, at, "TEMPLATE_DEPENDENCY_DUPLICATE", `依赖重复：${dep}`);
				continue;
			}
			seen.add(dep);
			deps.push(dep);
		}
		dependencies.set(node.node_id, deps);
	});
	if (errors.length === 0 && topologicalOrder(template.nodes.map((node) => node.node_id), dependencies) === null) fail(errors, "/nodes", "TEMPLATE_DEPENDENCY_CYCLE", "任务依赖存在环");

	// 安全模板不允许携带任何工具：只做分析与建议。
	if (template.execution_policy === "advisory_only") {
		const writable = template.agents.flatMap((agent, index) => agent.tools.map((tool) => ({ at: `/agents/${index}/tools`, tool }))).concat(template.nodes.flatMap((node, index) => node.tools.map((tool) => ({ at: `/nodes/${index}/tools`, tool }))));
		for (const { at, tool } of writable) fail(errors, at, "TEMPLATE_ADVISORY_TOOLS_FORBIDDEN", `advisory_only 模板不允许工具：${tool}`);
	}
	return { ok: errors.length === 0, errors };
}

function autoLayout(nodes, dependencies) {
	const depth = new Map();
	const visit = (id, stack) => {
		if (depth.has(id)) return depth.get(id);
		if (stack.has(id)) return 0;
		stack.add(id);
		const deps = dependencies.get(id) ?? [];
		const value = deps.length === 0 ? 0 : 1 + Math.max(...deps.map((dep) => visit(dep, stack)));
		stack.delete(id);
		depth.set(id, value);
		return value;
	};
	for (const node of nodes) visit(node.node_id, new Set());
	const rows = new Map();
	return new Map(nodes.map((node) => {
		const level = depth.get(node.node_id) ?? 0;
		const row = rows.get(level) ?? 0;
		rows.set(level, row + 1);
		return [node.node_id, node.position ?? { x: 60 + level * 260, y: 60 + row * 150 }];
	}));
}

function knowledgeFor(template, node) {
	const selected = node.knowledge.length > 0 ? template.knowledge_base.filter((entry) => node.knowledge.includes(entry.id)) : template.knowledge_base;
	return selected.map((entry) => ({
		id: entry.id,
		title: entry.title,
		content: entry.content.slice(0, LIMITS.knowledgeContent)
	}));
}

/**
 * 把模板编译成现有 workflow 文档。编译结果可以直接 POST /v1/workflows。
 * 返回 { definition, validation, template }；校验失败时 definition 为 null。
 */
export function compileWorkflowTemplate(input, options = {}) {
	const checked = validateWorkflowTemplate(input);
	const template = input !== null && typeof input === "object" && !Array.isArray(input) ? normalizeTemplate(input) : null;
	if (!checked.ok || !template) return {
		definition: null,
		template,
		validation: {
			errors: checked.errors,
			truncated: false,
			total: checked.errors.length
		}
	};
	const warnings = [];
	const usedRoles = new Set(template.nodes.map((node) => node.role_id));
	for (const agent of template.agents) if (!usedRoles.has(agent.role_id)) warnings.push({
		path: "/agents",
		code: "TEMPLATE_ROLE_UNUSED",
		message: `Agent 角色没有被任何任务使用：${agent.role_id}`
	});
	const roles = new Map(template.agents.map((agent) => [agent.role_id, agent]));
	const byId = new Map(template.nodes.map((node) => [node.node_id, node]));
	const dependencies = new Map(template.nodes.map((node) => [node.node_id, node.depends_on]));
	const positions = autoLayout(template.nodes, dependencies);
	const advisory = template.execution_policy === "advisory_only";
	const nodes = [{
		node_id: "start",
		type: "trigger",
		title: "开始",
		position: { x: 0, y: 60 }
	}];
	for (const node of template.nodes) {
		const agent = roles.get(node.role_id);
		const modelClass = node.model_class ?? agent.model_class;
		const knowledge = knowledgeFor(template, node);
		const config = {
			goal: node.instruction,
			role: agent.name,
			role_id: agent.role_id,
			system_prompt: agent.system_prompt,
			acceptance: node.acceptance,
			advisory_only: advisory,
			side_effect_policy: "pure",
			tools: advisory ? [] : node.tools.length > 0 ? node.tools : agent.tools
		};
		if (modelClass) config.model_class = modelClass;
		if (knowledge.length > 0) config.knowledge_base = knowledge;
		nodes.push({
			node_id: node.node_id,
			type: "agent",
			title: node.title,
			position: positions.get(node.node_id),
			config,
			contract: { description: `${agent.name}：${node.title}` }
		});
	}
	const edges = [];
	const edgeKeys = new Set();
	const pushEdge = (from, to) => {
		const key = from + "->" + to;
		if (edgeKeys.has(key)) return;
		edgeKeys.add(key);
		edges.push({ from, to });
	};
	for (const node of template.nodes) {
		if (node.depends_on.length === 0) pushEdge("start", node.node_id);
		else for (const dep of node.depends_on) pushEdge(dep, node.node_id);
	}
	const definition = {
		schema_version: "1",
		name: text(options.name) || template.name,
		scope: "reusable",
		nodes,
		edges
	};
	if (definition.name.length > 80) definition.name = definition.name.slice(0, 80);
	return {
		definition,
		template: {
			template_id: template.template_id,
			name: template.name,
			category: template.category,
			execution_policy: template.execution_policy
		},
		validation: {
			errors: [],
			warnings,
			truncated: false,
			total: 0
		}
	};
}

function agent(role_id, name, model_class, system_prompt) {
	return { role_id, name, model_class, system_prompt, tools: [] };
}

function node(node_id, title, role_id, instruction, depends_on, acceptance, extra = {}) {
	return {
		node_id,
		title,
		role_id,
		instruction,
		depends_on,
		acceptance,
		model_class: extra.model_class ?? null,
		knowledge: extra.knowledge ?? [],
		tools: extra.tools ?? [],
		position: extra.position ?? null
	};
}

const CODE_DEVELOPMENT = {
	schema_version: "1",
	template_id: "code-development",
	name: "代码开发工作流",
	description: "从需求分析到集成交付的完整研发链路，覆盖后端、前端、测试与代码审查。",
	category: "研发",
	execution_policy: "standard",
	knowledge_base: [{
		id: "engineering-standards",
		title: "工程规范",
		content: "改动保持最小化；新增行为必须有测试；提交前运行项目现有检查；不引入未经确认的依赖；交付说明包含变更点、验证方式与回滚建议。"
	}, {
		id: "review-checklist",
		title: "代码审查清单",
		content: "关注正确性、边界条件、错误处理、并发与幂等、向后兼容、安全边界、性能与可维护性；每个问题给出文件位置、影响与修复建议；结论必须区分阻塞项与建议项。"
	}],
	agents: [
		agent("requirement_analyst", "需求分析师", "simple", "你负责把原始需求转成可验证的功能点、约束与验收口径。不要假设未提供的事实，缺失信息列为待确认项。"),
		agent("architect", "架构师", "code", "你负责给出最小可行架构：模块边界、数据流、接口契约、失败与回滚策略。优先复用现有技术栈，明确不做什么。"),
		agent("backend_developer", "后端工程师", "code", "你负责后端实现与单元测试。严格遵循既有代码风格，避免无关重构，交付可运行的代码与验证结果。"),
		agent("frontend_developer", "前端工程师", "code", "你负责前端实现与交互细节。沿用现有设计系统与主题，兼容明暗模式，保证可访问性与移动端可用。"),
		agent("qa_engineer", "测试工程师", "code", "你负责测试设计与执行：正常路径、边界、异常、并发与回归。记录实际命令与结果，不以推测代替执行。"),
		agent("code_reviewer", "代码审查员", "audit", "你负责独立审查本次改动。只依据实际代码与测试证据下结论，按阻塞/建议分级，指出风险与遗漏。"),
		agent("integrator", "集成工程师", "code", "你负责集成与交付收口：确认依赖顺序、构建与测试通过、变更清单完整，并给出上线与回滚步骤。")
	],
	nodes: [
		node("requirements", "需求分析", "requirement_analyst", "梳理目标、范围、约束、角色与验收口径，输出需求清单与待确认项。", [], ["目标与非目标清晰", "每条需求可验证", "待确认项已列出"]),
		node("architecture", "架构设计", "architect", "基于需求清单设计模块边界、接口与数据流，说明关键取舍与失败策略。", ["requirements"], ["关键接口有契约", "异常与回滚路径明确", "未引入不必要的新依赖"]),
		node("backend", "后端开发", "backend_developer", "完成后端与数据层实现，补充单元测试，并记录本地验证命令与结果。", ["architecture"], ["实现覆盖需求清单", "单元测试通过", "错误处理可验证"]),
		node("frontend", "前端开发", "frontend_developer", "完成前端界面与交互实现，沿用现有主题与组件约定，补充必要测试。", ["backend"], ["主要流程可用", "明暗主题一致", "无未处理的加载与错误态"]),
		node("testing", "测试", "qa_engineer", "执行单元、集成与回归测试，覆盖边界与异常场景，输出可复现的测试记录。", ["frontend"], ["关键路径有测试证据", "失败项有复现步骤", "回归范围已说明"]),
		node("review", "Review", "code_reviewer", "独立审查实现与测试，按阻塞项与建议项给出结论。", ["testing"], ["阻塞问题已定位到文件", "结论有证据", "风险与遗漏已列出"]),
		node("integration", "集成", "integrator", "完成集成收口：确认构建、测试与变更清单，给出上线与回滚说明。", ["review"], ["构建与测试通过", "变更文件清单完整", "上线与回滚步骤可执行"])
	]
};

const SECURITY_AUDIT = {
	schema_version: "1",
	template_id: "security-audit",
	name: "安全代码审计工作流",
	description: "从资产识别到综合报告的安全审计链路，覆盖认证、权限、注入与依赖风险。",
	category: "安全",
	execution_policy: "standard",
	knowledge_base: [{
		id: "audit-baseline",
		title: "审计基线",
		content: "以实际代码、配置与依赖清单为依据；每个发现给出位置、触发条件、影响范围、证据与修复建议；无法验证的推测必须标注为待确认。"
	}, {
		id: "severity-guide",
		title: "风险分级",
		content: "严重：可直接导致未授权访问或数据泄露；高：在常见条件下可被利用；中：需要特定前置条件；低：纵深防御或加固建议。"
	}],
	agents: [
		agent("asset_scout", "资产识别审计员", "audit", "你负责识别审计范围内的入口、服务、数据与信任边界，输出资产清单和优先审计顺序。"),
		agent("auth_auditor", "认证审计员", "audit", "你负责认证机制审计：凭据存储、会话、多因素、找回与注销流程。只报告可验证的问题。"),
		agent("authz_auditor", "权限审计员", "audit", "你负责授权与访问控制审计：越权、角色边界、对象级权限与租户隔离。"),
		agent("injection_auditor", "注入漏洞审计员", "audit", "你负责注入类风险审计：SQL、命令、模板、反序列化、路径与 SSRF，结合输入来源判断可利用性。"),
		agent("dependency_auditor", "依赖审计员", "audit", "你负责依赖与供应链审计：已知漏洞、过期版本、来源可信度与许可证风险。"),
		agent("audit_lead", "审计负责人", "audit", "你负责汇总各专项结论，去重、定级、给出修复优先级与复核建议，不夸大未验证的问题。")
	],
	nodes: [
		node("assets", "资产识别", "asset_scout", "列出审计范围内的入口、服务、数据资产与信任边界，标注优先级与依据。", [], ["资产清单完整", "信任边界明确", "审计顺序有理由"], { knowledge: ["audit-baseline"] }),
		node("authentication", "认证审计", "auth_auditor", "审计认证流程与凭据处理，记录可验证问题及证据。", ["assets"], ["覆盖登录/会话/注销", "问题有代码位置", "严重级别有依据"], { knowledge: ["severity-guide"] }),
		node("authorization", "权限审计", "authz_auditor", "审计角色与对象级权限，验证越权与租户隔离路径。", ["assets"], ["覆盖水平与垂直越权", "对象级权限已验证", "问题有复现条件"], { knowledge: ["severity-guide"] }),
		node("injection", "注入漏洞审计", "injection_auditor", "审计 SQL、命令、模板、反序列化、路径与 SSRF 风险，说明输入来源与利用条件。", ["assets"], ["覆盖主要注入面", "输入来源可追溯", "可利用性判断有证据"], { knowledge: ["severity-guide"] }),
		node("dependencies", "依赖审计", "dependency_auditor", "核对依赖清单与已知漏洞，标注版本、影响与升级风险。", ["assets"], ["依赖版本可核对", "漏洞来源可信", "升级风险已说明"], { knowledge: ["audit-baseline"] }),
		node("report", "综合报告", "audit_lead", "汇总专项结论，去重定级，给出修复优先级、验证方式与复核建议。", ["authentication", "authorization", "injection", "dependencies"], ["每个结论可追溯", "定级标准一致", "修复与复核建议可执行"])
	]
};

const QUALITY_TESTING = {
	schema_version: "1",
	template_id: "quality-testing",
	name: "软件质量测试工作流",
	description: "从需求澄清到验收报告的质量链路，覆盖单元、集成与安全测试。",
	category: "质量",
	execution_policy: "standard",
	knowledge_base: [{
		id: "test-evidence",
		title: "测试证据要求",
		content: "每项结论必须附执行命令、环境与结果；失败用例给出复现步骤与预期/实际差异；不使用未执行的用例充数。"
	}],
	agents: [
		agent("requirements_analyst", "需求分析师", "simple", "你负责澄清测试范围与验收口径，识别不可测或模糊的需求。"),
		agent("test_lead", "测试负责人", "code", "你负责测试策略、分层与准入准出标准，保证覆盖关键风险而不是堆用例数量。"),
		agent("unit_tester", "单元测试工程师", "code", "你负责单元与组件测试，覆盖边界、异常与回归点，输出可重复执行的测试。"),
		agent("integration_tester", "集成测试工程师", "code", "你负责集成与端到端验证，关注接口契约、数据一致性与失败恢复。"),
		agent("security_tester", "安全测试工程师", "audit", "你负责安全相关测试：认证、权限、输入校验、敏感信息与依赖风险。"),
		agent("acceptance_author", "验收报告撰写人", "code", "你负责汇总测试结果，按验收标准给出通过/不通过结论，并列出遗留风险。")
	],
	nodes: [
		node("requirements", "需求分析", "requirements_analyst", "澄清测试目标、范围与验收口径，列出待确认项。", [], ["范围明确", "验收口径可验证", "风险点已识别"]),
		node("test_plan", "测试计划", "test_lead", "制定测试分层、环境、数据与准出标准，说明取舍。", ["requirements"], ["分层合理", "环境与数据可准备", "准出标准明确"]),
		node("unit_test", "单元测试", "unit_tester", "执行单元与组件测试，覆盖边界与异常，记录实际结果。", ["test_plan"], ["关键模块有覆盖", "失败项有复现步骤", "结果可重复"]),
		node("integration_test", "集成测试", "integration_tester", "执行集成与端到端测试，验证接口契约与失败恢复。", ["unit_test"], ["关键链路已验证", "契约一致", "异常路径有证据"]),
		node("security_test", "安全测试", "security_tester", "执行认证、权限、输入校验与敏感信息检查。", ["integration_test"], ["越权路径已检查", "输入校验有证据", "敏感信息无泄露"]),
		node("acceptance", "验收报告", "acceptance_author", "汇总测试证据，对照验收标准给出结论与遗留风险。", ["security_test"], ["结论对照验收标准", "证据完整", "遗留风险已说明"])
	]
};

const INCIDENT_RESPONSE = {
	schema_version: "1",
	template_id: "incident-response",
	name: "安全事件响应工作流",
	description: "告警研判到事件报告的只读分析链路。默认只输出分析与建议，不自动执行封禁、删除或隔离。",
	category: "安全运营",
	execution_policy: "advisory_only",
	knowledge_base: [{
		id: "incident-safety",
		title: "安全边界（强制）",
		content: "本模板只做分析与建议。禁止执行或建议自动执行封禁、删除、隔离、停机、改配置等生产环境操作。所有处置动作必须由人类审批后手动执行，并在报告中注明风险与回滚方式。"
	}, {
		id: "incident-severity",
		title: "事件分级",
		content: "P1：核心业务中断或大规模数据风险；P2：重要系统受影响但有缓解手段；P3：局部或低影响；P4：可疑但未证实。分级必须给出依据。"
	}],
	agents: [
		agent("alert_triage", "告警研判员", "audit", "你负责判断告警真伪、影响对象与紧急程度。只依据提供的日志与情报下结论，不确定就标注待确认。"),
		agent("threat_intel", "情报分析师", "audit", "你负责关联已知攻击手法、指标与影响面。区分事实、推断与假设。"),
		agent("impact_analyst", "影响评估员", "audit", "你负责评估受影响资产、数据范围与业务影响，给出分级依据。"),
		agent("response_advisor", "处置建议员", "audit", "你负责给出人工可执行的处置建议、优先级、风险与回滚方式。不得要求自动执行生产变更。"),
		agent("incident_reviewer", "复核员", "audit", "你负责复核证据链与结论一致性，指出过度推断与遗漏。"),
		agent("incident_reporter", "事件报告员", "audit", "你负责输出结构化事件报告：事实、影响、建议、待确认项与时间线。")
	],
	nodes: [
		node("triage", "告警研判", "alert_triage", "判断告警真伪、影响对象与紧急程度，列出事实与待确认项。", [], ["结论有证据", "紧急程度有依据", "待确认项已列出"], { knowledge: ["incident-safety"] }),
		node("intelligence", "情报分析", "threat_intel", "关联攻击手法与指标，区分事实、推断与假设。", ["triage"], ["指标可追溯", "推断已标注", "影响面有说明"], { knowledge: ["incident-severity"] }),
		node("impact", "影响评估", "impact_analyst", "评估受影响资产、数据范围与业务影响，给出分级与依据。", ["intelligence"], ["资产范围明确", "分级依据充分", "业务影响可解释"], { knowledge: ["incident-severity"] }),
		node("response", "处置建议", "response_advisor", "给出人工执行的处置建议、优先级、风险与回滚方式。不得建议自动执行生产变更。", ["impact"], ["建议可人工执行", "风险与回滚已说明", "无自动封禁/删除/隔离"], { knowledge: ["incident-safety"] }),
		node("review", "复核", "incident_reviewer", "复核证据链与结论一致性，指出过度推断与遗漏。", ["response"], ["证据链完整", "结论与证据一致", "遗漏已列出"]),
		node("report", "事件报告", "incident_reporter", "输出结构化事件报告：事实、影响、建议、待确认项与时间线。", ["review"], ["结构完整", "结论可追溯", "待确认项已标注"], { knowledge: ["incident-safety"] })
	]
};

const BUILTIN = [CODE_DEVELOPMENT, SECURITY_AUDIT, QUALITY_TESTING, INCIDENT_RESPONSE];

const clone = (value) => JSON.parse(JSON.stringify(value));

export function listWorkflowTemplates() {
	return BUILTIN.map((template) => ({
		template_id: template.template_id,
		name: template.name,
		description: template.description,
		category: template.category,
		execution_policy: template.execution_policy,
		advisory_only: template.execution_policy === "advisory_only",
		node_count: template.nodes.length,
		agent_count: template.agents.length
	}));
}

export function getWorkflowTemplate(templateId) {
	const found = BUILTIN.find((template) => template.template_id === templateId);
	return found ? clone(found) : null;
}
