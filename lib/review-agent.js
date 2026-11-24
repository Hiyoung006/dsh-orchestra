// 独立质量审查 Agent：只依据执行账本、交付物内容、工具执行记录与工作区差异，
// 不依赖执行 Agent 的自我描述。输出的审查结论只是建议，不能替代人工最终审批。

import { createHash, randomUUID } from "node:crypto";
import { isAbsolute, normalize, relative, resolve } from "node:path";

export const REVIEW_AGENT_ID = "independent-review-agent:v1";
export const REVIEW_STATUSES = Object.freeze(["pass", "fail", "need_evidence"]);

const SEVERITY_WEIGHT = { high: 0.5, medium: 0.1, low: 0.02 };

const SECURITY_RULES = Object.freeze([
	{ id: "private_key_material", severity: "high", pattern: /-----BEGIN (?:RSA |EC |OPENSSH |DSA )?PRIVATE KEY-----/ },
	{ id: "aws_access_key", severity: "high", pattern: /\bAKIA[0-9A-Z]{16}\b/ },
	{ id: "disabled_tls_verification", severity: "high", pattern: /rejectUnauthorized\s*:\s*false|NODE_TLS_REJECT_UNAUTHORIZED\s*=\s*["']?0/ },
	{ id: "shell_injection_surface", severity: "high", pattern: /shell\s*:\s*true|\bexecSync?\s*\(\s*`[^`]*\$\{/ },
	{ id: "dangerous_recursive_delete", severity: "high", pattern: /\brm\s+-rf\s+(?:\/|\$HOME|~)(?:\s|$)/ },
	{ id: "dynamic_code_execution", severity: "medium", pattern: /\b(?:eval|new Function)\s*\(/ },
	{ id: "hardcoded_secret", severity: "medium", pattern: /(?:api[_-]?key|secret|password|passwd|token)\s*[:=]\s*["'][^"'\s]{8,}["']/i },
	{ id: "sql_string_concat", severity: "medium", pattern: /(?:SELECT|INSERT|UPDATE|DELETE)[^;]{0,200}(?:\+\s*(?:req|input|params|body|query)\.)/i }
]);

function sha256(value) {
	return createHash("sha256").update(value).digest("hex");
}

function canonical(value) {
	if (value === null) return "null";
	if (typeof value === "number") return JSON.stringify(value);
	if (typeof value === "boolean" || typeof value === "string") return JSON.stringify(value);
	if (Array.isArray(value)) return "[" + value.map(canonical).join(",") + "]";
	if (typeof value === "object") {
		return "{" + Object.entries(value).filter(([, v]) => v !== undefined).sort(([a], [b]) => a < b ? -1 : a > b ? 1 : 0).map(([k, v]) => JSON.stringify(k) + ":" + canonical(v)).join(",") + "}";
	}
	return JSON.stringify(String(value));
}

function asBuffer(value) {
	if (Buffer.isBuffer(value)) return value;
	if (value instanceof Uint8Array) return Buffer.from(value);
	return null;
}

function evidenceDigest(evidence) {
	return sha256(canonical(evidence ?? null));
}

function addedLines(patch) {
	const lines = String(patch ?? "").split("\n");
	return lines.filter((line) => line.startsWith("+") && !line.startsWith("+++")).map((line) => line.slice(1));
}

function scanText(text, source, issues, rules = SECURITY_RULES) {
	for (const rule of rules) {
		const match = rule.pattern.exec(text);
		if (!match) continue;
		issues.push({
			rule: rule.id,
			severity: rule.severity,
			source,
			excerpt: match[0].slice(0, 160),
			detail: `命中安全规则 ${rule.id}`,
			evidence_type: "deterministic"
		});
	}
}

const SECRET_RULE_IDS = /* @__PURE__ */ new Set([
	"private_key_material",
	"aws_access_key",
	"hardcoded_secret"
]);
const NON_SECRET_RULES = Object.freeze(SECURITY_RULES.filter((rule) => !SECRET_RULE_IDS.has(rule.id)));
const SECRET_RULES = Object.freeze(SECURITY_RULES.filter((rule) => SECRET_RULE_IDS.has(rule.id)));

function redact(value) {
	return String(value).slice(0, 160).replace(/[A-Za-z0-9+/=_-]{12,}/gu, "[redacted]");
}

function redactSecrets(text) {
	let result = String(text);
	for (const rule of SECRET_RULES) {
		const flags = rule.pattern.flags.includes("g") ? rule.pattern.flags : rule.pattern.flags + "g";
		result = result.replace(new RegExp(rule.pattern.source, flags), "[redacted]");
	}
	return result;
}

/** 秘密信息检测接口：只返回脱敏片段，不复述完整秘密。 */
export function detectSecrets(text, { source = "evidence", rules = SECRET_RULES } = {}) {
	const findings = [];
	for (const rule of rules) {
		const match = rule.pattern.exec(text);
		if (!match) continue;
		findings.push({
			rule: rule.id,
			severity: rule.severity,
			source,
			excerpt: redact(match[0]),
			detail: `命中秘密信息规则 ${rule.id}`,
			evidence_type: "deterministic"
		});
	}
	return findings;
}

function artifactFact(tx, artifactId) {
	if (typeof artifactId !== "string" || artifactId === "") return { present: false, reason: "artifact_id_missing" };
	const row = tx.get(
		"SELECT a.sha256 AS sha256, a.retain_until AS retain_until, c.file_name AS file_name, c.bytes AS bytes FROM artifacts a JOIN artifact_contents c ON c.artifact_id = a.artifact_id WHERE a.artifact_id = ?",
		[artifactId]
	);
	if (!row) return { present: false, reason: "artifact_content_missing", artifact_id: artifactId };
	const bytes = asBuffer(row.bytes);
	if (!bytes) return { present: false, reason: "artifact_bytes_unreadable", artifact_id: artifactId };
	const digest = sha256(bytes);
	return {
		present: true,
		artifact_id: artifactId,
		file_name: row.file_name,
		bytes: bytes.length,
		sha256: digest,
		sha256_matches: digest === row.sha256,
		retain_until: row.retain_until ?? null,
		bytes_buffer: bytes
	};
}

function toolFact(tx, runId, executionId) {
	if (typeof executionId !== "string" || executionId === "") return { present: false, reason: "execution_id_missing" };
	const row = tx.get(
		"SELECT t.execution_id, t.tool, t.exit_code, t.finished_at FROM tool_executions t JOIN node_runs n ON n.node_run_id = t.node_run_id WHERE t.execution_id = ? AND n.run_id = ?",
		[executionId, runId]
	);
	if (!row) return { present: false, reason: "tool_execution_missing", execution_id: executionId };
	return { present: true, execution_id: row.execution_id, tool: row.tool, exit_code: Number(row.exit_code), finished_at: row.finished_at ?? null };
}

function boundaryViolations(diff, workspace) {
	const changed = Array.isArray(diff?.changedFiles) ? diff.changedFiles.map(String) : [];
	const root = workspace?.real_path ?? workspace?.realPath ?? null;
	const violations = [];
	for (const file of changed) {
		const normalized = normalize(file);
		const segments = normalized.split(/[\\/]/);
		if (normalized === ".." || segments.includes("..")) {
			violations.push({ file, reason: "path_traversal" });
			continue;
		}
		if (isAbsolute(normalized)) {
			if (!root) violations.push({ file, reason: "absolute_path_without_workspace" });
			else {
				const rel = relative(resolve(root), resolve(normalized));
				if (rel === "" || rel.startsWith("..") || isAbsolute(rel)) violations.push({ file, reason: "outside_workspace" });
			}
		}
	}
	return violations;
}

/**
 * 同时扫描 workspace diff 与本次运行的全部交付物。
 * 扫描范围、文件大小、内容缺失、二进制和上限跳过都记录在 scan 结果里。
 */
function scanEvidence(tx, runId, diff, scanOptions = {}) {
	const limits = {
		maxArtifactBytes: 262144,
		maxArtifacts: 50,
		maxDiffBytes: 1048576,
		...scanOptions.limits
	};
	const issues = [];
	const secretFindings = [];
	const artifacts = [];
	const textFiles = [];
	const patch = typeof diff?.patch === "string" ? diff.patch : "";
	const diffRecord = {
		available: Boolean(diff),
		kind: diff?.kind ?? null,
		changed_files: Array.isArray(diff?.changedFiles) ? diff.changedFiles.length : 0,
		patch_bytes: Buffer.byteLength(patch),
		patch_excerpt: patch.length === 0 ? "" : redactSecrets(patch.slice(0, 200000)),
		truncated: diff?.truncated === true,
		scanned: false,
		scanned_lines: 0,
		skipped_reason: null
	};
	if (!diff) diffRecord.skipped_reason = "workspace_diff_missing";
	else if (patch.length === 0) diffRecord.skipped_reason = "empty_patch";
	else if (diffRecord.patch_bytes > limits.maxDiffBytes) diffRecord.skipped_reason = "diff_too_large";
	else {
		const lines = addedLines(patch);
		diffRecord.scanned_lines = lines.length;
		for (const line of lines) {
			scanText(line, "workspace-diff", issues, NON_SECRET_RULES);
			secretFindings.push(...detectSecrets(line, { source: "workspace-diff" }));
		}
		diffRecord.scanned = true;
	}
	const rows = tx.all(
		"SELECT a.artifact_id, a.sha256, c.file_name, c.bytes FROM artifacts a JOIN artifact_contents c ON c.artifact_id = a.artifact_id WHERE a.run_id = ? ORDER BY a.created_at DESC, a.rowid DESC",
		[runId]
	);
	for (const row of rows) {
		const bytes = asBuffer(row.bytes);
		const record = {
			artifact_id: row.artifact_id,
			file_name: row.file_name,
			bytes: bytes?.length ?? null,
			sha256: row.sha256 ?? null,
			scanned: false,
			skipped_reason: null
		};
		if (artifacts.filter((item) => item.scanned).length >= limits.maxArtifacts) record.skipped_reason = "artifact_limit_exceeded";
		else if (!bytes) record.skipped_reason = "artifact_bytes_unreadable";
		else if (bytes.length > limits.maxArtifactBytes) record.skipped_reason = "artifact_too_large";
		else if (bytes.includes(0)) record.skipped_reason = "binary_content";
		else {
			const text = bytes.toString("utf8");
			scanText(text, `artifact:${row.file_name}`, issues, NON_SECRET_RULES);
			secretFindings.push(...detectSecrets(text, { source: `artifact:${row.file_name}` }));
			record.scanned = true;
			if (textFiles.length < 60) textFiles.push({ path: row.file_name, text });
		}
		artifacts.push(record);
	}
	return {
		limits,
		diff: diffRecord,
		artifacts,
		scope: {
			workspace_diff: diffRecord.scanned,
			artifacts: artifacts.filter((item) => item.scanned).length,
			artifacts_total: artifacts.length,
			text_files: textFiles.length
		},
		scanned_bytes: artifacts.reduce((sum, item) => sum + (item.scanned ? item.bytes : 0), 0),
		textFiles,
		issues,
		secretFindings
	};
}

const AST_EXTENSIONS = /* @__PURE__ */ new Set([".js", ".mjs", ".cjs", ".jsx", ".ts", ".tsx"]);

function walkAst(node, visit, seen = /* @__PURE__ */ new Set()) {
	if (!node || typeof node !== "object" || seen.has(node)) return;
	seen.add(node);
	visit(node);
	for (const value of Object.values(node)) {
		if (Array.isArray(value)) {
			for (const child of value) walkAst(child, visit, seen);
		} else if (value && typeof value === "object" && typeof value.type === "string") walkAst(value, visit, seen);
	}
}

/**
 * AST 静态分析接口：由宿主注入 parse(text, path) 返回 ESTree 风格 AST。
 * 未配置 parser 时明确返回 unavailable，不用正则伪装成 AST 结论。
 */
export function analyzeAst({ files = [], parse, maxFiles = 40 } = {}) {
	if (typeof parse !== "function") return {
		available: false,
		engine: "unconfigured",
		reason: "ast_parser_not_configured",
		scanned: [],
		skipped: [],
		findings: []
	};
	const findings = [];
	const scanned = [];
	const skipped = [];
	for (const file of files.slice(0, maxFiles)) {
		const extension = /\.[^.\\/]+$/u.exec(file.path)?.[0]?.toLowerCase() ?? "";
		if (!AST_EXTENSIONS.has(extension)) continue;
		let ast;
		try {
			ast = parse(file.text, file.path);
		} catch (error) {
			skipped.push({ path: file.path, reason: "parse_error", message: String(error?.message ?? error) });
			continue;
		}
		scanned.push(file.path);
		const add = (rule, severity, detail) => findings.push({
			rule,
			severity,
			source: `ast:${file.path}`,
			detail,
			evidence_type: "deterministic"
		});
		walkAst(ast, (node) => {
			if (node.type === "CallExpression" && node.callee?.type === "Identifier" && node.callee.name === "eval") add("ast_dynamic_code_execution", "high", "AST 中发现 eval 调用");
			if (node.type === "NewExpression" && node.callee?.type === "Identifier" && node.callee.name === "Function") add("ast_dynamic_code_execution", "high", "AST 中发现 new Function");
			if (node.type === "MemberExpression" && node.property?.type === "Identifier" && node.property.name === "innerHTML") add("ast_dom_xss_sink", "medium", "AST 中发现 innerHTML 赋值或访问");
			if (node.type === "ImportDeclaration" && node.source?.value === "child_process") add("ast_process_execution_import", "high", "AST 中直接引入 child_process");
			if (node.type === "CallExpression" && node.callee?.type === "Identifier" && ["exec", "execSync", "spawn", "spawnSync"].includes(node.callee.name)) add("ast_process_execution", "medium", `AST 中调用 ${node.callee.name}`);
		});
	}
	return {
		available: true,
		engine: "host_parser",
		scanned,
		skipped,
		findings: findings.slice(0, 200)
	};
}

const MANIFEST_NAMES = /* @__PURE__ */ new Set([
	"package.json",
	"package-lock.json",
	"npm-shrinkwrap.json",
	"pnpm-lock.yaml",
	"yarn.lock"
]);

/**
 * 依赖漏洞扫描接口：宿主注入 advisoryProvider({ path, text }) 返回漏洞列表。
 * 未配置 provider 时明确返回 unavailable；异步 provider 不会被同步审查误当成已完成。
 */
export function scanDependencyVulnerabilities({ files = [], advisoryProvider, maxFiles = 20 } = {}) {
	if (typeof advisoryProvider !== "function") return {
		available: false,
		engine: "unconfigured",
		reason: "advisory_provider_not_configured",
		scanned: [],
		skipped: [],
		findings: []
	};
	const findings = [];
	const scanned = [];
	const skipped = [];
	for (const file of files) {
		const name = file.path.split(/[\\/]/u).pop();
		if (!MANIFEST_NAMES.has(name)) continue;
		if (scanned.length >= maxFiles) {
			skipped.push({ path: file.path, reason: "manifest_limit_exceeded" });
			continue;
		}
		let result;
		try {
			result = advisoryProvider({ path: file.path, text: file.text });
		} catch (error) {
			skipped.push({ path: file.path, reason: "provider_error", message: String(error?.message ?? error) });
			continue;
		}
		if (result && typeof result.then === "function") {
			skipped.push({ path: file.path, reason: "async_provider_requires_async_review" });
			continue;
		}
		scanned.push(file.path);
		for (const item of Array.isArray(result) ? result : []) findings.push({
			rule: item.id ?? item.advisory ?? "dependency_vulnerability",
			severity: ["critical", "high", "medium", "low"].includes(item.severity) ? item.severity : "medium",
			source: `dependency:${file.path}`,
			package: item.package ?? null,
			version: item.version ?? null,
			detail: item.summary ?? item.title ?? "依赖漏洞",
			evidence_type: "deterministic"
		});
	}
	return { available: true, engine: "host_advisory_provider", scanned, skipped, findings: findings.slice(0, 200) };
}

export class ReviewIsolationError extends Error {
	constructor(message) {
		super(message);
		this.name = "ReviewIsolationError";
		this.code = "REVIEW_WORKSPACE_NOT_READ_ONLY";
	}
}

function assertReadOnlyReviewInput({ workspaceAccess, workspace }) {
	if (workspaceAccess !== "read_only") throw new ReviewIsolationError("审查器只能以只读方式访问证据");
	if (workspace?.writable === true || workspace?.writable === "true" || workspace?.writable_access === true || workspace?.review_access === "write") throw new ReviewIsolationError("审查器不能与执行 Agent 共享可写工作区");
}

/**
 * 对一次运行做独立审查。
 *
 * 输入必须来自系统事实：
 * - criteria：冻结的验收标准；
 * - items：逐项验收结果与证据（artifact / tool_execution）；
 * - diff：工作区差异（可为空；为空时明确标记不可核验，不当作通过）。
 *
 * 审查器默认只读：不挂载执行 Agent 的工作区，不写文件；LLM 复核也只接收脱敏证据。
 */
export function runIndependentReview(tx, { taskId, runId, criteria, items, workspace = null, diff = null, now, reviewer = REVIEW_AGENT_ID, criteriaVersion = null, scan = null, ast = null, dependency = null, llm = null, workspaceAccess = "read_only" }) {
	assertReadOnlyReviewInput({ workspaceAccess, workspace });
	const workspaceSnapshot = workspace ? Object.freeze({
		real_path: workspace.real_path ?? null,
		realPath: workspace.realPath ?? null,
		allowed_roots: workspace.allowed_roots ?? null,
		mode: "readonly"
	}) : null;
	const criteriaList = Array.isArray(criteria) ? criteria : [];
	const itemMap = new Map((Array.isArray(items) ? items : []).map((item) => [item.key, item]));
	const passedCriteria = [];
	const failedCriteria = [];
	const missingEvidence = [];
	const securityIssues = [];
	const suggestions = [];

	const toolRows = tx.all(
		"SELECT t.execution_id, t.tool, t.exit_code, t.finished_at FROM tool_executions t JOIN node_runs n ON n.node_run_id = t.node_run_id WHERE n.run_id = ? ORDER BY t.started_at ASC, t.tool_seq ASC LIMIT 200",
		[runId]
	);
	let toolCriteriaCount = 0;
	let toolCriteriaSatisfied = 0;

	for (const criterion of criteriaList) {
		const item = itemMap.get(criterion.key);
		if (!item) {
			missingEvidence.push({ key: criterion.key, title: criterion.title, kind: criterion.kind, reason: "criterion_result_missing" });
			suggestions.push(`补充验收项「${criterion.title || criterion.key}」的评估结果与证据。`);
			continue;
		}
		if (criterion.kind === "tool_evidence") toolCriteriaCount++;
		if (!item.evidence || typeof item.evidence !== "object") {
			missingEvidence.push({ key: criterion.key, title: criterion.title, kind: criterion.kind, reason: "evidence_missing" });
			suggestions.push(`验收项「${criterion.title || criterion.key}」没有对应证据，不能判定为通过。`);
			continue;
		}
		if (item.outcome === "failed") {
			failedCriteria.push({ key: criterion.key, title: criterion.title, kind: criterion.kind, reason: item.reason ?? "criterion_failed", evidence: item.evidence });
			suggestions.push(`修复验收项「${criterion.title || criterion.key}」：${item.reason ?? "未通过"}。`);
			continue;
		}
		if (item.outcome === "unverifiable") {
			missingEvidence.push({ key: criterion.key, title: criterion.title, kind: criterion.kind, reason: item.reason ?? "criterion_unverifiable", evidence: item.evidence });
			suggestions.push(`补充可核验证据后再提交「${criterion.title || criterion.key}」。`);
			continue;
		}
		if (criterion.kind === "artifact") {
			const fact = artifactFact(tx, item.evidence.artifact_id);
			if (!fact.present) {
				missingEvidence.push({ key: criterion.key, title: criterion.title, kind: criterion.kind, reason: fact.reason, artifact_id: fact.artifact_id ?? null });
				suggestions.push(`重新发布交付文件「${criterion.title || criterion.key}」，当前无法读取实际内容。`);
				continue;
			}
			if (!fact.sha256_matches) {
				failedCriteria.push({ key: criterion.key, title: criterion.title, kind: criterion.kind, reason: "artifact_hash_mismatch", evidence: { expected: item.evidence.sha256 ?? null, actual: fact.sha256, file_name: fact.file_name } });
				suggestions.push(`交付文件「${criterion.title || criterion.key}」内容哈希与登记记录不一致。`);
				continue;
			}
			passedCriteria.push({
				key: criterion.key,
				title: criterion.title,
				kind: criterion.kind,
				evidence_digest: evidenceDigest({ ...item.evidence, sha256: fact.sha256, bytes: fact.bytes }),
				file_name: fact.file_name,
				bytes: fact.bytes,
				sha256: fact.sha256
			});
			continue;
		}
		if (criterion.kind === "tool_evidence") {
			const fact = toolFact(tx, runId, item.evidence.matched_execution_id);
			const expected = typeof criterion.rule?.exit_code === "number" ? criterion.rule.exit_code : 0;
			if (!fact.present) {
				missingEvidence.push({ key: criterion.key, title: criterion.title, kind: criterion.kind, reason: fact.reason, execution_id: fact.execution_id ?? null });
				suggestions.push(`重新执行「${criterion.rule?.tool ?? criterion.title}」并保留系统执行记录。`);
				continue;
			}
			if (fact.exit_code !== expected) {
				failedCriteria.push({ key: criterion.key, title: criterion.title, kind: criterion.kind, reason: "tool_exit_code_mismatch", evidence: { tool: fact.tool, exit_code: fact.exit_code, expected_exit_code: expected } });
				suggestions.push(`「${fact.tool}」退出码为 ${fact.exit_code}，期望 ${expected}。`);
				continue;
			}
			toolCriteriaSatisfied++;
			passedCriteria.push({
				key: criterion.key,
				title: criterion.title,
				kind: criterion.kind,
				evidence_digest: evidenceDigest({ ...item.evidence, execution_id: fact.execution_id, exit_code: fact.exit_code }),
				tool: fact.tool,
				exit_code: fact.exit_code,
				execution_id: fact.execution_id
			});
			continue;
		}
		// 未知类型：没有系统证据就不能通过。
		missingEvidence.push({ key: criterion.key, title: criterion.title, kind: criterion.kind, reason: "unsupported_criterion_kind", evidence: item.evidence });
		suggestions.push(`验收项「${criterion.title || criterion.key}」类型无法独立核验。`);
	}

	const evidenceScan = scanEvidence(tx, runId, diff, scan ?? {});
	securityIssues.push(...evidenceScan.issues, ...evidenceScan.secretFindings);
	const staticAnalysis = analyzeAst({ files: evidenceScan.textFiles, ...(ast ?? {}) });
	const dependencies = scanDependencyVulnerabilities({ files: evidenceScan.textFiles, ...(dependency ?? {}) });
	const staticBlocking = [...staticAnalysis.findings, ...dependencies.findings].filter((issue) => ["critical", "high"].includes(issue.severity));
	for (const issue of securityIssues) suggestions.push(`处理安全问题 ${issue.rule}（${issue.severity}）：${issue.excerpt}`);
	for (const issue of staticAnalysis.findings) suggestions.push(`处理 AST 静态分析问题 ${issue.rule}（${issue.severity}）：${issue.detail}`);
	for (const issue of dependencies.findings) suggestions.push(`处理依赖漏洞 ${issue.rule}（${issue.severity}）：${issue.detail}`);

	const violations = boundaryViolations(diff, workspaceSnapshot);
	for (const violation of violations) suggestions.push(`修正越界修改：${violation.file}（${violation.reason}）`);
	if (!diff) suggestions.push("未提供工作区差异，代码修改范围无法独立核验。");

	const testsRequired = toolCriteriaCount > 0;
	const testsExecuted = testsRequired ? toolCriteriaSatisfied === toolCriteriaCount : null;
	if (testsRequired && !testsExecuted) suggestions.push("测试或工具验收项未全部取得真实执行记录，不能判定为通过。");

	const highIssues = securityIssues.filter((issue) => issue.severity === "high");
	const hasFailed = failedCriteria.length > 0 || highIssues.length > 0 || violations.length > 0 || staticBlocking.length > 0;
	const hasMissing = criteriaList.length === 0 || missingEvidence.length > 0 || (testsRequired && !testsExecuted);
	const reviewStatus = hasFailed ? "fail" : hasMissing ? "need_evidence" : "pass";

	const total = criteriaList.length;
	const passRatio = total === 0 ? 0 : passedCriteria.length / total;
	const securityPenalty = [...securityIssues, ...staticBlocking].reduce((sum, issue) => sum + (SEVERITY_WEIGHT[issue.severity] ?? 0), 0);
	const scopeOk = !diff ? 0.5 : violations.length === 0 ? 1 : 0;
	const testsOk = testsExecuted === false ? 0 : 1;
	const score = Math.max(0, Math.min(100, Math.round(100 * (0.6 * passRatio + 0.2 * Math.max(0, 1 - securityPenalty) + 0.1 * scopeOk + 0.1 * testsOk))));
	const resultKinds = {
		real_evidence: [
			...passedCriteria.map((item) => ({ kind: "criterion_evidence", criterion_key: item.key, evidence_digest: item.evidence_digest, evidence_type: "deterministic" })),
			...toolRows.map((row) => ({ kind: "tool_execution", ref: row.execution_id, evidence_type: "deterministic" })),
			...evidenceScan.artifacts.filter((item) => item.scanned).map((item) => ({ kind: "artifact_scan", ref: item.artifact_id, evidence_type: "deterministic" })),
			...violations.map((item) => ({ kind: "boundary_violation", ref: item.file, evidence_type: "deterministic" })),
			...(diff ? [{ kind: "workspace_diff", evidence_type: "deterministic" }] : [])
		],
		model_judgment: [],
		speculation: []
	};
	for (const item of [...passedCriteria, ...failedCriteria, ...missingEvidence]) item.evidence_type = "deterministic";

	return {
		review_id: "rv_" + randomUUID(),
		task_id: taskId,
		run_id: runId,
		criteria_version: criteriaVersion,
		reviewer,
		reviewer_kind: "independent_agent",
		reviewStatus,
		score,
		passedCriteria,
		failedCriteria,
		missingEvidence,
		securityIssues,
		suggestions: [...new Set(suggestions)],
		scan: {
			limits: evidenceScan.limits,
			diff: evidenceScan.diff,
			artifacts: evidenceScan.artifacts,
			scope: evidenceScan.scope,
			scanned_bytes: evidenceScan.scanned_bytes
		},
		staticAnalysis,
		dependencies,
		llm: {
			configured: Boolean(llm?.enabled),
			model: llm?.model ?? null
		},
		llmReview: null,
		reviewIsolation: {
			mode: "read_only_evidence",
			workspace: "not_mounted",
			workspace_mode: workspace?.mode ?? null,
			shared_with_executor: false,
			llm_workspace_access: false
		},
		resultKinds,
		checks: {
			artifactsPresent: passedCriteria.filter((item) => item.kind === "artifact").length,
			filesExist: passedCriteria.some((item) => item.kind === "artifact") ? passedCriteria.filter((item) => item.kind === "artifact").every((item) => Boolean(item.sha256)) : null,
			changeScope: diff ? (diff.kind ?? "workspace") : "unavailable",
			testsExecuted,
			testResults: testsRequired ? (testsExecuted ? "passed" : "missing") : "not_required",
			securityIssues: securityIssues.length,
			secretFindings: evidenceScan.secretFindings.length,
			staticAnalysis: staticAnalysis.available ? staticAnalysis.findings.length : "unavailable",
			dependencyScan: dependencies.available ? dependencies.findings.length : "unavailable",
			aiReview: llm?.enabled ? "pending" : "disabled",
			taskBoundary: !diff ? "unavailable" : violations.length === 0 ? "ok" : "violated",
			evidenceCoverage: total === 0 ? 0 : passedCriteria.length / total
		},
		toolExecutions: toolRows.map((row) => ({ execution_id: row.execution_id, tool: row.tool, exit_code: Number(row.exit_code), finished_at: row.finished_at ?? null })),
		created_at: now
	};
}

export const LLM_REVIEW_SECTIONS = Object.freeze([
	"correctness",
	"security",
	"maintainability",
	"test_coverage",
	"requirement_compliance"
]);
const LLM_SECTION_STATUSES = /* @__PURE__ */ new Set(["pass", "warn", "fail", "unknown"]);
const LLM_SEVERITIES = /* @__PURE__ */ new Set(["critical", "high", "medium", "low", "info"]);

function arrayOf(value) {
	return Array.isArray(value) ? value : [];
}

function shortText(value, limit = 4000) {
	return typeof value === "string" ? value.slice(0, limit) : "";
}

function normalizeLlmFinding(value, section) {
	const item = value && typeof value === "object" ? value : { detail: String(value ?? "") };
	const severityValue = LLM_SEVERITIES.has(item.severity) ? item.severity : "info";
	return {
		section,
		title: shortText(item.title ?? item.summary ?? item.detail, 500),
		detail: shortText(item.detail ?? item.summary ?? item.title),
		severity: severityValue,
		evidence_refs: arrayOf(item.evidence_refs ?? item.evidence).map((ref) => shortText(ref, 500)).filter(Boolean),
		evidence_type: "model_judgment"
	};
}

function normalizeLlmSection(value, key) {
	const source = value && typeof value === "object" ? value : {};
	const status = LLM_SECTION_STATUSES.has(source.status) ? source.status : "unknown";
	return {
		status,
		summary: shortText(source.summary),
		findings: arrayOf(source.findings).slice(0, 50).map((item) => normalizeLlmFinding(item, key))
	};
}

/**
 * 构造只读、脱敏的 LLM 证据包：不放工作区路径，不放执行器可写句柄，不放原始二进制。
 */
export function buildLlmEvidence(report) {
	return {
		schema_version: 1,
		task_id: report?.task_id ?? null,
		run_id: report?.run_id ?? null,
		criteria_version: report?.criteria_version ?? null,
		deterministic_review: {
			status: report?.reviewStatus ?? null,
			score_is_auxiliary_only: true,
			approval_requires_human: true
		},
		criteria: {
			passed: report?.passedCriteria ?? [],
			failed: report?.failedCriteria ?? [],
			missing: report?.missingEvidence ?? []
		},
		security_issues: report?.securityIssues ?? [],
		scan: report?.scan ?? null,
		static_analysis: report?.staticAnalysis ?? null,
		dependencies: report?.dependencies ?? null,
		tool_executions: report?.toolExecutions ?? [],
		constraints: {
			read_only: true,
			workspace_mounted: false,
			executor_workspace_shared: false,
			secrets_redacted: true,
			model_must_not_approve: true
		}
	};
}

function normalizeLlmReview(raw, { model = null, evidenceDigest = null } = {}) {
	const source = raw?.review && typeof raw.review === "object" ? raw.review : raw;
	const sections = {};
	for (const key of LLM_REVIEW_SECTIONS) sections[key] = normalizeLlmSection(source?.[key], key);
	const findings = LLM_REVIEW_SECTIONS.flatMap((key) => sections[key].findings);
	const blockingSections = ["correctness", "security"];
	const blocking = blockingSections.some((key) => sections[key].status === "fail") || findings.some((item) => blockingSections.includes(item.section) && ["critical", "high"].includes(item.severity));
	const score = Number.isFinite(source?.score) ? Math.max(0, Math.min(100, Math.round(source.score))) : null;
	return {
		available: true,
		engine: "independent_llm",
		model,
		evidence_digest: evidenceDigest,
		score,
		score_used_for_approval: false,
		blocking,
		approval_eligible: false,
		...sections,
		missing_evidence: arrayOf(source?.missing_evidence).slice(0, 50).map((item) => ({
			title: shortText(item?.title ?? item, 500),
			reason: shortText(item?.reason ?? ""),
			evidence_type: "model_judgment"
		})),
		false_positive_candidates: arrayOf(source?.false_positive_candidates).slice(0, 50).map((item) => ({
			claim: shortText(item?.claim ?? item, 1000),
			reason: shortText(item?.reason ?? ""),
			evidence_type: "model_judgment"
		})),
		remediation: arrayOf(source?.remediation).slice(0, 50).map((item) => ({
			action: shortText(item?.action ?? item, 1000),
			priority: LLM_SEVERITIES.has(item?.priority) ? item.priority : "medium",
			evidence_type: "model_judgment"
		})),
		speculation: arrayOf(source?.speculation).slice(0, 50).map((item) => ({
			claim: shortText(item?.claim ?? item, 1000),
			reason: shortText(item?.reason ?? ""),
			evidence_type: "speculation"
		}))
	};
}

const LLM_SYSTEM_PROMPT = [
	"你是独立的代码质量审查者，只依据给定的只读证据给出建议。",
	"不要假设你有工作区、文件写入、命令执行或工具访问权限。",
	"必须区分：确定性证据、模型判断、推测。没有证据的结论请放进 speculation。",
	"你不能批准交付；最终审批只能由人工完成。高分不能作为自动通过依据。",
	"只返回 JSON，字段：correctness, security, maintainability, test_coverage, requirement_compliance, missing_evidence, false_positive_candidates, remediation, speculation, score。",
	"每个 section 形如 {status: pass|warn|fail|unknown, summary, findings:[{title,detail,severity,evidence_refs}]}。"
].join("\n");

/** 可配置的独立 LLM Review。返回模型判断，不修改确定性结论。 */
export async function runIndependentLlmReview({ report, config, signal } = {}) {
	if (!config?.enabled) return { available: false, reason: "llm_review_disabled", evidence_type: "model_judgment" };
	if (typeof config.complete !== "function") return { available: false, reason: "llm_reviewer_not_configured", evidence_type: "model_judgment" };
	const evidence = buildLlmEvidence(report);
	const limit = Number.isFinite(config.maxEvidenceBytes) && config.maxEvidenceBytes > 0 ? config.maxEvidenceBytes : 160000;
	let body = JSON.stringify(evidence);
	if (Buffer.byteLength(body) > limit) {
		evidence.truncated = true;
		if (evidence.scan?.diff) evidence.scan = { ...evidence.scan, diff: { ...evidence.scan.diff, patch_excerpt: "" } };
		if (Array.isArray(evidence.scan?.artifacts)) evidence.scan = { ...evidence.scan, artifacts: evidence.scan.artifacts.slice(0, 20) };
		body = JSON.stringify(evidence);
	}
	const timeout = AbortSignal.timeout(Number.isFinite(config.timeoutMs) && config.timeoutMs > 0 ? config.timeoutMs : 60000);
	const combined = signal ? AbortSignal.any([signal, timeout]) : timeout;
	try {
		let raw = await config.complete({
			system: LLM_SYSTEM_PROMPT,
			user: body,
			schema: {
				sections: LLM_REVIEW_SECTIONS,
				statuses: [...LLM_SECTION_STATUSES],
				severities: [...LLM_SEVERITIES]
			},
			model: config.model ?? null,
			signal: combined
		});
		if (typeof raw === "string") raw = JSON.parse(raw.replace(/^```(?:json)?\s*/iu, "").replace(/\s*```$/u, ""));
		return normalizeLlmReview(raw, {
			model: config.model ?? null,
			evidenceDigest: sha256(body)
		});
	} catch (error) {
		return {
			available: false,
			reason: "llm_review_failed",
			message: String(error?.message ?? error).slice(0, 500),
			evidence_type: "model_judgment"
		};
	}
}

/** 合并模型复核：只能维持或收紧结论，永远不能把 fail / need_evidence 升级为 pass。 */
export function mergeLlmReview(report, llmReview) {
	if (!report) return report;
	const suggestions = [...(report.suggestions ?? [])];
	for (const item of llmReview?.remediation ?? []) {
		if (item?.action) suggestions.push(`AI 复核建议（模型判断）：${item.action}`);
	}
	const merged = {
		...report,
		llmReview,
		llm: {
			...(report.llm ?? {}),
			configured: Boolean(report.llm?.configured || llmReview?.engine === "independent_llm"),
			model: llmReview?.model ?? report.llm?.model ?? null
		},
		suggestions: [...new Set(suggestions)],
		resultKinds: {
			...report.resultKinds,
			model_judgment: [
				...LLM_REVIEW_SECTIONS.flatMap((key) => arrayOf(llmReview?.[key]?.findings)),
				...arrayOf(llmReview?.missing_evidence),
				...arrayOf(llmReview?.false_positive_candidates),
				...arrayOf(llmReview?.remediation)
			],
			speculation: [...arrayOf(llmReview?.speculation)]
		},
		checks: {
			...report.checks,
			aiReview: llmReview?.available ? llmReview.blocking ? "blocking" : "completed" : "unavailable"
		}
	};
	if (llmReview?.available && llmReview.blocking && merged.reviewStatus === "pass") merged.reviewStatus = "need_evidence";
	return merged;
}

/** 确定性审查 + 可选 LLM 复核的异步入口；模型结果不能自动批准交付。 */
export async function runIndependentReviewWithLlm(tx, options) {
	const report = runIndependentReview(tx, options);
	if (!options?.llm?.enabled) return report;
	return mergeLlmReview(report, await runIndependentLlmReview({ report, config: options.llm }));
}

export function reviewAllowsAcceptance(report) {
	if (!report || report.reviewStatus !== "pass") return false;
	// 只要启用了独立 LLM 复核，就必须保留人工审批；模型分数永远不能自动放行。
	if (report.llm?.configured) return false;
	if (report.llmReview?.available === false) return false;
	if (report.llmReview?.blocking) return false;
	return true;
}

/** 独立审查意见转成验收整改项；审查不通过不能靠分数放行。 */
export function reportRemediation(report) {
	if (!report) return [];
	const items = [];
	for (const criterion of report.failedCriteria ?? []) items.push({ criterion_key: criterion.key, title: criterion.title, outcome: "failed", reason: criterion.reason, action: `修复「${criterion.title || criterion.key}」：${criterion.reason}`, evidence_type: "deterministic" });
	for (const item of report.missingEvidence ?? []) items.push({ criterion_key: item.key, title: item.title, outcome: "unverifiable", reason: item.reason, action: `补充「${item.title || item.key}」的证据：${item.reason}`, evidence_type: "deterministic" });
	for (const issue of report.securityIssues ?? []) items.push({ criterion_key: null, title: "安全检查", outcome: "failed", reason: issue.rule, action: `处理安全问题 ${issue.rule}：${issue.excerpt}`, evidence_type: "deterministic" });
	for (const key of LLM_REVIEW_SECTIONS) {
		for (const finding of report.llmReview?.[key]?.findings ?? []) items.push({
			criterion_key: null,
			title: `AI 复核：${key}`,
			outcome: "unverifiable",
			reason: `ai_review_${key}`,
			action: `${finding.title || finding.detail}${finding.detail && finding.title ? `：${finding.detail}` : ""}`,
			evidence_type: "model_judgment"
		});
	}
	for (const item of report.llmReview?.missing_evidence ?? []) items.push({
		criterion_key: null,
		title: "AI 复核：缺失证据",
		outcome: "unverifiable",
		reason: "ai_review_missing_evidence",
		action: `补充证据：${item.title}${item.reason ? `（${item.reason}）` : ""}`,
		evidence_type: "model_judgment"
	});
	for (const suggestion of report.suggestions ?? []) {
		if (items.some((item) => item.action === suggestion)) continue;
		items.push({ criterion_key: null, title: "审查建议", outcome: "unverifiable", reason: "review_suggestion", action: suggestion, evidence_type: "deterministic" });
	}
	return items;
}

export function persistReviewReport(tx, report) {
	tx.run(
		"INSERT INTO review_reports (review_id, task_id, run_id, criteria_version, reviewer, reviewer_kind, review_status, score, report_json, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
		[
			report.review_id,
			report.task_id,
			report.run_id,
			report.criteria_version ?? null,
			report.reviewer,
			report.reviewer_kind,
			report.reviewStatus,
			report.score,
			JSON.stringify(report),
			report.created_at
		]
	);
	return report;
}

export function latestReviewReport(tx, taskId) {
	const row = tx.get("SELECT report_json FROM review_reports WHERE task_id = ? ORDER BY created_at DESC, rowid DESC LIMIT 1", [taskId]);
	if (!row) return null;
	try {
		return JSON.parse(row.report_json);
	} catch {
		return null;
	}
}

export function reviewReportForRun(tx, runId) {
	const row = tx.get("SELECT report_json FROM review_reports WHERE run_id = ? ORDER BY created_at DESC, rowid DESC LIMIT 1", [runId]);
	if (!row) return null;
	try {
		return JSON.parse(row.report_json);
	} catch {
		return null;
	}
}
