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

function scanText(text, source, issues) {
	for (const rule of SECURITY_RULES) {
		const match = rule.pattern.exec(text);
		if (!match) continue;
		issues.push({
			rule: rule.id,
			severity: rule.severity,
			source,
			excerpt: match[0].slice(0, 160),
			detail: `命中安全规则 ${rule.id}`
		});
	}
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

function collectArtifactSecurity(tx, runId, issues) {
	const rows = tx.all(
		"SELECT c.file_name, c.bytes FROM artifacts a JOIN artifact_contents c ON c.artifact_id = a.artifact_id WHERE a.run_id = ? ORDER BY a.created_at DESC LIMIT 20",
		[runId]
	);
	for (const row of rows) {
		const bytes = asBuffer(row.bytes);
		if (!bytes || bytes.length > 262144 || bytes.includes(0)) continue;
		scanText(bytes.toString("utf8"), `artifact:${row.file_name}`, issues);
	}
}

/**
 * 对一次运行做独立审查。
 *
 * 输入必须来自系统事实：
 * - criteria：冻结的验收标准；
 * - items：逐项验收结果与证据（artifact / tool_execution）；
 * - diff：工作区差异（可为空；为空时明确标记不可核验，不当作通过）。
 */
export function runIndependentReview(tx, { taskId, runId, criteria, items, workspace = null, diff = null, now, reviewer = REVIEW_AGENT_ID, criteriaVersion = null }) {
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

	for (const line of addedLines(diff?.patch)) scanText(line, diff?.kind === "git_worktree" ? "workspace-diff" : "workspace-diff", securityIssues);
	if (securityIssues.length === 0 && !diff?.patch) collectArtifactSecurity(tx, runId, securityIssues);
	for (const issue of securityIssues) {
		suggestions.push(`处理安全问题 ${issue.rule}（${issue.severity}）：${issue.excerpt}`);
	}

	const violations = boundaryViolations(diff, workspace);
	for (const violation of violations) suggestions.push(`修正越界修改：${violation.file}（${violation.reason}）`);
	if (!diff) suggestions.push("未提供工作区差异，代码修改范围无法独立核验。");

	const testsRequired = toolCriteriaCount > 0;
	const testsExecuted = testsRequired ? toolCriteriaSatisfied === toolCriteriaCount : null;
	if (testsRequired && !testsExecuted) suggestions.push("测试或工具验收项未全部取得真实执行记录，不能判定为通过。");

	const highIssues = securityIssues.filter((issue) => issue.severity === "high");
	const hasFailed = failedCriteria.length > 0 || highIssues.length > 0 || violations.length > 0;
	const hasMissing = criteriaList.length === 0 || missingEvidence.length > 0 || (testsRequired && !testsExecuted);
	const reviewStatus = hasFailed ? "fail" : hasMissing ? "need_evidence" : "pass";

	const total = criteriaList.length;
	const passRatio = total === 0 ? 0 : passedCriteria.length / total;
	const securityPenalty = securityIssues.reduce((sum, issue) => sum + (SEVERITY_WEIGHT[issue.severity] ?? 0), 0);
	const scopeOk = !diff ? 0.5 : violations.length === 0 ? 1 : 0;
	const testsOk = testsExecuted === false ? 0 : 1;
	const score = Math.max(0, Math.min(100, Math.round(100 * (0.6 * passRatio + 0.2 * Math.max(0, 1 - securityPenalty) + 0.1 * scopeOk + 0.1 * testsOk))));

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
		checks: {
			artifactsPresent: passedCriteria.filter((item) => item.kind === "artifact").length,
			filesExist: passedCriteria.some((item) => item.kind === "artifact") ? passedCriteria.filter((item) => item.kind === "artifact").every((item) => Boolean(item.sha256)) : null,
			changeScope: diff ? (diff.kind ?? "workspace") : "unavailable",
			testsExecuted,
			testResults: testsRequired ? (testsExecuted ? "passed" : "missing") : "not_required",
			securityIssues: securityIssues.length,
			taskBoundary: !diff ? "unavailable" : violations.length === 0 ? "ok" : "violated",
			evidenceCoverage: total === 0 ? 0 : passedCriteria.length / total
		},
		toolExecutions: toolRows.map((row) => ({ execution_id: row.execution_id, tool: row.tool, exit_code: Number(row.exit_code), finished_at: row.finished_at ?? null })),
		created_at: now
	};
}

export function reviewAllowsAcceptance(report) {
	return Boolean(report) && report.reviewStatus === "pass";
}

/** 独立审查意见转成验收整改项；审查不通过不能靠分数放行。 */
export function reportRemediation(report) {
	if (!report) return [];
	const items = [];
	for (const criterion of report.failedCriteria ?? []) items.push({ criterion_key: criterion.key, title: criterion.title, outcome: "failed", reason: criterion.reason, action: `修复「${criterion.title || criterion.key}」：${criterion.reason}` });
	for (const item of report.missingEvidence ?? []) items.push({ criterion_key: item.key, title: item.title, outcome: "unverifiable", reason: item.reason, action: `补充「${item.title || item.key}」的证据：${item.reason}` });
	for (const issue of report.securityIssues ?? []) items.push({ criterion_key: null, title: "安全检查", outcome: "failed", reason: issue.rule, action: `处理安全问题 ${issue.rule}：${issue.excerpt}` });
	for (const suggestion of report.suggestions ?? []) {
		if (items.some((item) => item.action === suggestion)) continue;
		items.push({ criterion_key: null, title: "审查建议", outcome: "unverifiable", reason: "review_suggestion", action: suggestion });
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
