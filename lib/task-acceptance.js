// 任务验收：独立验收状态、冻结标准、AI 预审（仅建议）与人工最终审批。
//
// 安全边界：
// - AI 只能写 acceptance_reviews（意见），不能写 acceptance_approvals（审批）；
// - 人工审批由 requireHumanApprover 在业务层强制，工具层不暴露审批动作；
// - 已冻结的验收标准只能新增版本，不能原地修改（业务层 + 数据库触发器双重约束）。

import { createHash, randomUUID } from "node:crypto";

export const ACCEPTANCE_MODES = Object.freeze(["manual", "assisted", "automatic"]);

export const ACCEPTANCE_STATUSES = Object.freeze([
	"pending",
	"reviewing",
	"ai_reviewed",
	"approved",
	"rejected",
	"not_applicable"
]);

export const ACCEPTANCE_VERDICTS = Object.freeze(["pass", "fail", "needs_human"]);

const PASSING = new Set(["approved", "not_applicable"]);
const MODE_SET = new Set(ACCEPTANCE_MODES);
const STATUS_SET = new Set(ACCEPTANCE_STATUSES);

const TRANSITIONS = Object.freeze({
	pending: ["reviewing", "ai_reviewed", "approved", "rejected", "not_applicable"],
	reviewing: ["ai_reviewed", "approved", "rejected", "pending", "not_applicable"],
	ai_reviewed: ["approved", "rejected", "pending", "not_applicable"],
	approved: [],
	rejected: ["pending", "approved"],
	not_applicable: []
});

export class AcceptanceError extends Error {
	constructor(code, message, details = {}) {
		super(message);
		this.name = "AcceptanceError";
		this.code = code;
		this.details = details;
	}
}

function canonicalJson(value) {
	if (value === null) return "null";
	if (typeof value === "number") {
		if (!Number.isFinite(value)) throw new TypeError("验收标准不接受非有限数字");
		return JSON.stringify(value);
	}
	if (typeof value === "boolean" || typeof value === "string") return JSON.stringify(value);
	if (Array.isArray(value)) return "[" + value.map(canonicalJson).join(",") + "]";
	if (typeof value === "object") {
		return "{" + Object.entries(value)
			.filter(([, v]) => v !== undefined)
			.sort(([a], [b]) => a < b ? -1 : a > b ? 1 : 0)
			.map(([k, v]) => JSON.stringify(k) + ":" + canonicalJson(v))
			.join(",") + "}";
	}
	throw new TypeError("验收标准不接受这个类型: " + typeof value);
}

function sha256Hex(input) {
	return createHash("sha256").update(input).digest("hex");
}

function parseJson(value, fallback) {
	if (typeof value !== "string" || value.length === 0) return fallback;
	try {
		return JSON.parse(value);
	} catch {
		return fallback;
	}
}

export function normalizeAcceptanceMode(value, fallback = "assisted") {
	return MODE_SET.has(value) ? value : fallback;
}

export function acceptanceCriteriaHash(criteria) {
	return sha256Hex(canonicalJson(criteria));
}

/** 所有标准都必须能在没有人工判断的情况下机器核验，才允许 automatic。 */
export function criteriaMachineCheckable(criteria) {
	return Array.isArray(criteria) && criteria.length > 0 && criteria.every((item) => item.machine_checkable === true && !item.parse_error);
}

/** 从既有的交付物评估结果生成待冻结标准；不改变原有评估逻辑。 */
export function criteriaFromResults(results) {
	return (Array.isArray(results) ? results : []).map((result) => ({
		key: String(result.deliverable_key ?? ""),
		title: String(result.title ?? ""),
		node_id: String(result.node_id ?? ""),
		kind: String(result.kind ?? "unknown"),
		rule: result.rule ?? result.evidence?.rule ?? null,
		rule_version: String(result.rule_version ?? ""),
		machine_checkable: result.machine_checkable === true,
		parse_error: result.parse_error ?? null
	}));
}

/** 每条标准必须有一条证据记录；缺失时也必须是显式的 missing 证据。 */
export function buildReviewItems(results) {
	return (Array.isArray(results) ? results : []).map((result) => ({
		key: String(result.deliverable_key ?? ""),
		title: String(result.title ?? ""),
		node_id: String(result.node_id ?? ""),
		kind: String(result.kind ?? "unknown"),
		outcome: String(result.outcome ?? "unverifiable"),
		reason: result.reason ?? null,
		evidence: result.evidence ?? { missing: true }
	}));
}

function remediationText(item) {
	const title = item.title || item.key || "验收项";
	const reason = item.reason ?? "未通过";
	if (item.kind === "artifact") {
		if (reason === "artifact_not_registered") return `交付文件「${title}」尚未登记，请发布实际完成的文件后重新提交。`;
		if (reason === "artifact_below_minimum_bytes") return `交付文件「${title}」未达到最低字节要求，请补全内容后重新提交。`;
		if (reason === "artifact_retain_until_expired") return `交付文件「${title}」已过保留期，请重新发布当前版本。`;
		if (reason === "artifact_source_node_mismatch") return `交付文件「${title}」不是本步骤产出的版本，请由承担该交付的步骤重新发布。`;
		return `交付文件「${title}」未通过验收：${reason}。`;
	}
	if (item.kind === "tool_evidence") {
		if (reason === "tool_evidence_missing") return `缺少 ${item.evidence?.tool ?? "指定工具"} 的执行证据，请实际执行并通过后重新提交。`;
		if (reason === "tool_exit_code_mismatch") return `${item.evidence?.tool ?? "指定工具"} 的退出码不符合验收要求，请修复后重新执行。`;
		return `工具证据「${title}」未通过验收：${reason}。`;
	}
	return `验收项「${title}」未通过：${reason}。`;
}

export function remediationFor(items) {
	return (Array.isArray(items) ? items : [])
		.filter((item) => item.outcome !== "passed")
		.map((item) => ({
			criterion_key: item.key,
			title: item.title,
			outcome: item.outcome,
			reason: item.reason,
			action: remediationText(item)
		}));
}

export function reviewVerdict(items) {
	const list = Array.isArray(items) ? items : [];
	if (list.some((item) => item.outcome === "failed")) return "fail";
	if (list.some((item) => item.outcome === "unverifiable")) return "needs_human";
	return "pass";
}

/**
 * 计算一次预审建议。该结果只是意见，不改变任务验收状态；
 * automatic 的自动通过必须同时满足全部机器化标准通过。
 */
export function planReview({ mode, criteria, items }) {
	const effectiveMode = normalizeAcceptanceMode(mode);
	const verdict = reviewVerdict(items);
	const remediation = remediationFor(items);
	const autoApprovable = effectiveMode === "automatic"
		&& criteriaMachineCheckable(criteria)
		&& Array.isArray(items)
		&& items.length > 0
		&& items.every((item) => item.outcome === "passed");
	const summary = verdict === "pass"
		? "所有验收项均已取得通过证据。"
		: verdict === "fail"
			? "存在未通过验收项，已生成整改意见。"
			: "存在无法自动核验的验收项，需要人工判断。";
	return { mode: effectiveMode, verdict, summary, remediation, autoApprovable };
}

/** 工具层白名单：Agent 只能查看和预审，审批动作永远不开放。 */
export function assertAgentAcceptanceAction(action) {
	if (action === "status" || action === "pre_review") return action;
	if (action === "approve" || action === "reject") throw new AcceptanceError("FORBIDDEN", "AI 不能审批自己的交付成果；最终验收只能由人工提交", { action });
	throw new AcceptanceError("PAYLOAD_SCHEMA_INVALID", "未知的验收工具动作", { action: action ?? null });
}

function approverOf(actor) {
	if (typeof actor === "string") return { id: actor, kind: undefined };
	if (actor && typeof actor === "object") return { id: actor.id, kind: actor.kind };
	return { id: undefined, kind: undefined };
}

/** 最终审批只能是人工主体；AI / Agent / 自动系统都不能冒充。 */
export function requireHumanApprover(actor) {
	const { id, kind } = approverOf(actor);
	if (kind !== undefined && kind !== "human") {
		throw new AcceptanceError("FORBIDDEN", "最终验收审批只能由人工提交", { actorKind: kind ?? null });
	}
	if (typeof id !== "string" || id.trim() === "" || /[\u0000-\u001f\u007f]/u.test(id)) {
		throw new AcceptanceError("FORBIDDEN", "最终验收审批缺少可识别的人工负责人");
	}
	if (/^(?:agent|ai|system):/i.test(id) || /^(?:agent|ai|system)$/i.test(id)) {
		throw new AcceptanceError("FORBIDDEN", "AI 或系统主体不能批准自己的交付成果", { actor: id });
	}
	return id;
}

export function assertAcceptanceTransition(from, to) {
	if (!STATUS_SET.has(from) || !STATUS_SET.has(to)) {
		throw new AcceptanceError("PAYLOAD_SCHEMA_INVALID", "未知的验收状态", { from, to });
	}
	if (from === to) return;
	if (!(TRANSITIONS[from] ?? []).includes(to)) {
		throw new AcceptanceError("STALE_GENERATION", "验收状态不允许这条迁移", { from, to });
	}
}

export function acceptanceAllowsPass(snapshot) {
	if (!snapshot) return true;
	const status = typeof snapshot === "string" ? snapshot : snapshot.status;
	return status === undefined || status === null || PASSING.has(status);
}

/** 强制派发/强制重试：必须同时具备用户授权、理由与操作者。 */
export function assertForceAuthorization({ force = false, authorize = false, reason, actor } = {}) {
	if (force !== true) return null;
	if (authorize !== true) throw new AcceptanceError("CONFIRMATION_REQUIRED", "强制派发需要用户明确授权");
	if (typeof reason !== "string" || reason.trim() === "") throw new AcceptanceError("PAYLOAD_SCHEMA_INVALID", "强制派发必须填写操作理由");
	const { id, kind } = approverOf(actor);
	if (kind !== undefined && kind !== "human") throw new AcceptanceError("FORBIDDEN", "强制派发只能由人工发起");
	if (typeof id !== "string" || id.trim() === "" || /^(?:agent|ai|system):/i.test(id)) throw new AcceptanceError("FORBIDDEN", "强制派发必须记录明确的人工操作者");
	return { authorized_by: id.trim(), reason: reason.trim() };
}

function taskRow(tx, taskId) {
	const row = tx.get(
		"SELECT task_id, current_run_id, lifecycle_state, acceptance_mode, acceptance_status, acceptance_criteria_version, acceptance_criteria_hash, acceptance_reviewed_at, acceptance_approved_by, acceptance_approved_at, acceptance_reason, acceptance_remediation_json FROM tasks WHERE task_id = ?",
		[taskId]
	);
	if (!row) throw new AcceptanceError("NOT_FOUND", "任务不存在", { taskId });
	return row;
}

function setTaskAcceptance(tx, taskId, patch, now) {
	const keys = Object.keys(patch);
	if (keys.length === 0) return;
	tx.run(
		`UPDATE tasks SET ${keys.map((key) => `${key} = ?`).join(", ")}, updated_at = ?, revision = revision + 1 WHERE task_id = ?`,
		[...keys.map((key) => patch[key]), now, taskId]
	);
}

function appendAcceptanceEvent(tx, { taskId, runId, kind, actor, actorKind, detail, now }) {
	tx.run(
		"INSERT INTO acceptance_events (task_id, run_id, kind, actor, actor_kind, detail_json, created_at) VALUES (?, ?, ?, ?, ?, ?, ?)",
		[taskId, runId ?? null, kind, actor ?? null, actorKind ?? null, JSON.stringify(detail ?? {}), now]
	);
}

export function latestCriteria(tx, taskId) {
	return tx.get(
		"SELECT * FROM acceptance_criteria WHERE task_id = ? ORDER BY version DESC LIMIT 1",
		[taskId]
	) ?? null;
}

/**
 * 冻结验收标准。相同内容复用同一版本；计划内容变化时只能追加新版本。
 * AI 不能创建或替换已冻结版本；历史版本保持原样。
 */
export function freezeCriteria(tx, { taskId, planId = null, contentHash = null, criteria, actor, actorKind, now }) {
	const list = Array.isArray(criteria) ? criteria : [];
	const hash = acceptanceCriteriaHash(list);
	const existing = latestCriteria(tx, taskId);
	if (existing && existing.criteria_hash === hash) {
		if (taskRow(tx, taskId).acceptance_criteria_version !== existing.version) {
			setTaskAcceptance(tx, taskId, {
				acceptance_criteria_version: existing.version,
				acceptance_criteria_hash: hash
			}, now);
		}
		return { criteria: { ...existing, items: parseJson(existing.criteria_json, []) }, reused: true };
	}
	if (existing && actorKind === "ai") {
		throw new AcceptanceError("FORBIDDEN", "AI 不能修改已冻结的验收标准", { taskId, version: existing.version });
	}
	const version = Number(existing?.version ?? 0) + 1;
	const row = {
		criteria_id: "ac_" + randomUUID(),
		task_id: taskId,
		plan_id: planId,
		content_hash: contentHash,
		version,
		criteria_hash: hash,
		criteria_json: JSON.stringify(list),
		machine_checkable: criteriaMachineCheckable(list) ? 1 : 0,
		source: "plan",
		frozen_at: now,
		frozen_by: String(actor ?? "service:acceptance"),
		created_at: now
	};
	tx.run(
		`INSERT INTO acceptance_criteria
		 (criteria_id, task_id, plan_id, content_hash, version, criteria_hash, criteria_json, machine_checkable, source, frozen_at, frozen_by, created_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		[row.criteria_id, row.task_id, row.plan_id, row.content_hash, row.version, row.criteria_hash, row.criteria_json, row.machine_checkable, row.source, row.frozen_at, row.frozen_by, row.created_at]
	);
	setTaskAcceptance(tx, taskId, {
		acceptance_criteria_version: version,
		acceptance_criteria_hash: hash
	}, now);
	appendAcceptanceEvent(tx, {
		taskId,
		runId: null,
		kind: "criteria.frozen",
		actor: row.frozen_by,
		actorKind: actorKind ?? "system",
		detail: { version, criteria_hash: hash, count: list.length, machine_checkable: row.machine_checkable === 1 },
		now
	});
	return {
		criteria: {
			criteria_id: row.criteria_id,
			task_id: taskId,
			version,
			criteria_hash: hash,
			criteria_json: row.criteria_json,
			items: list,
			machine_checkable: row.machine_checkable,
			frozen_at: now,
			frozen_by: row.frozen_by
		},
		reused: false
	};
}

/** 记录 AI/系统预审意见；与最终审批完全分开存储。 */
export function recordReview(tx, { taskId, runId, criteria, mode, reviewer, reviewerKind = "ai", items, now, verdict, summary, remediation }) {
	const row = taskRow(tx, taskId);
	const effectiveMode = normalizeAcceptanceMode(mode ?? row.acceptance_mode);
	const list = Array.isArray(items) ? items : [];
	const plan = planReview({ mode: effectiveMode, criteria, items: list });
	const finalVerdict = verdict ?? plan.verdict;
	if (!ACCEPTANCE_VERDICTS.includes(finalVerdict)) throw new AcceptanceError("PAYLOAD_SCHEMA_INVALID", "未知的预审结论", { verdict: finalVerdict });
	for (const item of list) {
		if (!item || typeof item !== "object" || !item.key) throw new AcceptanceError("PAYLOAD_SCHEMA_INVALID", "验收项缺少标准键");
		if (!("evidence" in item) || item.evidence === undefined) throw new AcceptanceError("ACCEPTANCE_EVIDENCE_MISSING", "每项验收标准都必须对应证据", { key: item.key });
	}
	const criterionItems = Array.isArray(criteria?.items) ? criteria.items : null;
	if (criterionItems && criterionItems.length > 0) {
		const keys = new Set(criterionItems.map((item) => item.key));
		const seen = new Set();
		for (const item of list) {
			if (!keys.has(item.key)) throw new AcceptanceError("PAYLOAD_SCHEMA_INVALID", "预审条目不属于已冻结的验收标准", { key: item.key });
			if (seen.has(item.key)) throw new AcceptanceError("PAYLOAD_SCHEMA_INVALID", "同一验收标准只能对应一条证据", { key: item.key });
			seen.add(item.key);
		}
		if (seen.size !== criterionItems.length) throw new AcceptanceError("ACCEPTANCE_EVIDENCE_MISSING", "每项验收标准都必须对应证据", { expected: criterionItems.length, received: seen.size });
	}
	const remediationList = remediation ?? plan.remediation;
	const reviewId = "ar_" + randomUUID();
	tx.run(
		`INSERT INTO acceptance_reviews
		 (review_id, task_id, run_id, criteria_version, mode, reviewer_kind, reviewer, verdict, summary, items_json, remediation_json, created_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		[
			reviewId,
			taskId,
			runId ?? null,
			criteria?.version ?? null,
			effectiveMode,
			reviewerKind,
			String(reviewer ?? "service:precheck"),
			finalVerdict,
			String(summary ?? plan.summary),
			JSON.stringify(list),
			JSON.stringify(remediationList),
			now
		]
	);
	let next = effectiveMode === "manual" ? row.acceptance_status : "ai_reviewed";
	try {
		assertAcceptanceTransition(row.acceptance_status, next);
	} catch {
		// 同一轮重复评估不改变状态，但保留这次预审历史。
		next = row.acceptance_status;
	}
	setTaskAcceptance(tx, taskId, {
		acceptance_status: next,
		acceptance_reviewed_at: now,
		acceptance_remediation_json: remediationList.length > 0 ? JSON.stringify(remediationList) : null
	}, now);
	appendAcceptanceEvent(tx, {
		taskId,
		runId,
		kind: "review.recorded",
		actor: String(reviewer ?? "service:precheck"),
		actorKind: reviewerKind,
		detail: { review_id: reviewId, verdict: finalVerdict, mode: effectiveMode, remediation: remediationList.length },
		now
	});
	return { review_id: reviewId, task_id: taskId, run_id: runId ?? null, mode: effectiveMode, verdict: finalVerdict, summary: summary ?? plan.summary, items: list, remediation: remediationList };
}

/**
 * 写入最终审批。人工审批是唯一能把任务推入 approved 的入口；
 * system_auto 只能用于 automatic 模式且全部机器标准通过的情况。
 */
export function recordApproval(tx, { taskId, runId, criteria, reviewId = null, decision, approver, approverKind = "human", reason, items, mode, now }) {
	const row = taskRow(tx, taskId);
	const effectiveMode = normalizeAcceptanceMode(mode ?? row.acceptance_mode);
	if (decision !== "approved" && decision !== "rejected") throw new AcceptanceError("PAYLOAD_SCHEMA_INVALID", "未知的审批结论", { decision });
	const status = decision === "approved" ? "approved" : "rejected";
	assertAcceptanceTransition(row.acceptance_status, status);
	let approverId;
	if (approverKind === "system_auto") {
		if (effectiveMode !== "automatic") throw new AcceptanceError("FORBIDDEN", "只有 automatic 模式允许系统自动通过");
		if (decision !== "approved") throw new AcceptanceError("FORBIDDEN", "系统自动审批只能通过");
		const list = Array.isArray(items) ? items : [];
		const criteriaList = Array.isArray(criteria?.items) ? criteria.items : Array.isArray(criteria) ? criteria : null;
		if (!criteriaMachineCheckable(criteriaList)) throw new AcceptanceError("CONFIRMATION_REQUIRED", "存在无法机器核验的验收标准，不能自动通过");
		if (list.length === 0 || list.some((item) => item.outcome !== "passed")) throw new AcceptanceError("CONFIRMATION_REQUIRED", "并非所有验收标准都已通过，不能自动通过");
		approverId = String(approver ?? "system:auto_acceptance");
	} else {
		approverId = requireHumanApprover(approver);
	}
	if (decision === "rejected" && (typeof reason !== "string" || reason.trim() === "")) {
		throw new AcceptanceError("PAYLOAD_SCHEMA_INVALID", "驳回任务必须填写审批理由");
	}
	const remediation = decision === "rejected"
		? (Array.isArray(items) && items.length > 0 ? remediationFor(items) : parseJson(row.acceptance_remediation_json, []))
		: [];
	const approvalId = "ap_" + randomUUID();
	tx.run(
		`INSERT INTO acceptance_approvals
		 (approval_id, task_id, run_id, criteria_version, review_id, decision, approver, approver_kind, reason, items_json, created_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		[
			approvalId,
			taskId,
			runId ?? null,
			criteria?.version ?? row.acceptance_criteria_version ?? null,
			reviewId ?? null,
			decision,
			approverId,
			approverKind,
			reason?.trim() || null,
			JSON.stringify(Array.isArray(items) ? items : []),
			now
		]
	);
	setTaskAcceptance(tx, taskId, {
		acceptance_status: status,
		acceptance_approved_by: approverId,
		acceptance_approved_at: now,
		acceptance_reason: reason?.trim() || null,
		acceptance_remediation_json: decision === "rejected" && remediation.length > 0 ? JSON.stringify(remediation) : null
	}, now);
	appendAcceptanceEvent(tx, {
		taskId,
		runId,
		kind: decision === "approved" ? "approval.approved" : "approval.rejected",
		actor: approverId,
		actorKind: approverKind,
		detail: { approval_id: approvalId, criteria_version: criteria?.version ?? row.acceptance_criteria_version ?? null, review_id: reviewId ?? null, reason: reason?.trim() || null, remediation: remediation.length },
		now
	});
	return { approval_id: approvalId, task_id: taskId, run_id: runId ?? null, decision, status, approver: approverId, approver_kind: approverKind, reason: reason?.trim() || null, remediation };
}

export function markNotApplicable(tx, { taskId, runId, reason, now }) {
	const row = taskRow(tx, taskId);
	if (row.acceptance_status === "not_applicable") return { changed: false, status: "not_applicable" };
	assertAcceptanceTransition(row.acceptance_status, "not_applicable");
	setTaskAcceptance(tx, taskId, { acceptance_status: "not_applicable", acceptance_reason: reason ?? null }, now);
	appendAcceptanceEvent(tx, { taskId, runId, kind: "acceptance.not_applicable", actor: "service:acceptance", actorKind: "system", detail: { reason: reason ?? null }, now });
	return { changed: true, status: "not_applicable" };
}

export function setAcceptanceMode(tx, { taskId, mode, actor, actorKind, now }) {
	if (!MODE_SET.has(mode)) throw new AcceptanceError("PAYLOAD_SCHEMA_INVALID", "未知的验收模式", { mode });
	const row = taskRow(tx, taskId);
	if (row.acceptance_mode === mode) return { task_id: taskId, mode, changed: false };
	setTaskAcceptance(tx, taskId, { acceptance_mode: mode }, now);
	appendAcceptanceEvent(tx, {
		taskId,
		runId: null,
		kind: "mode.changed",
		actor: String(actor ?? "service:acceptance"),
		actorKind: actorKind ?? "system",
		detail: { from: row.acceptance_mode, to: mode },
		now
	});
	return { task_id: taskId, mode, changed: true, previous: row.acceptance_mode };
}

function parseItems(row) {
	if (!row) return [];
	return parseJson(row.items_json, []);
}

export function acceptanceSnapshot(tx, taskId) {
	const task = taskRow(tx, taskId);
	const criteriaRow = task.acceptance_criteria_version
		? tx.get("SELECT * FROM acceptance_criteria WHERE task_id = ? AND version = ?", [taskId, task.acceptance_criteria_version])
		: latestCriteria(tx, taskId);
	const review = tx.get("SELECT * FROM acceptance_reviews WHERE task_id = ? ORDER BY created_at DESC, rowid DESC LIMIT 1", [taskId]) ?? null;
	const approval = tx.get("SELECT * FROM acceptance_approvals WHERE task_id = ? ORDER BY created_at DESC, rowid DESC LIMIT 1", [taskId]) ?? null;
	return {
		task_id: task.task_id,
		run_id: task.current_run_id,
		mode: normalizeAcceptanceMode(task.acceptance_mode),
		status: STATUS_SET.has(task.acceptance_status) ? task.acceptance_status : "pending",
		criteria_version: task.acceptance_criteria_version ?? null,
		criteria_hash: task.acceptance_criteria_hash ?? null,
		criteria: criteriaRow ? {
			version: criteriaRow.version,
			criteria_hash: criteriaRow.criteria_hash,
			machine_checkable: criteriaRow.machine_checkable === 1,
			frozen_at: criteriaRow.frozen_at,
			frozen_by: criteriaRow.frozen_by,
			items: parseJson(criteriaRow.criteria_json, [])
		} : null,
		review: review ? {
			review_id: review.review_id,
			run_id: review.run_id,
			mode: review.mode,
			reviewer_kind: review.reviewer_kind,
			reviewer: review.reviewer,
			verdict: review.verdict,
			summary: review.summary,
			items: parseItems(review),
			remediation: parseJson(review.remediation_json, []),
			created_at: review.created_at
		} : null,
		approval: approval ? {
			approval_id: approval.approval_id,
			run_id: approval.run_id,
			criteria_version: approval.criteria_version,
			review_id: approval.review_id,
			decision: approval.decision,
			approver: approval.approver,
			approver_kind: approval.approver_kind,
			reason: approval.reason,
			created_at: approval.created_at
		} : null,
		reviewed_at: task.acceptance_reviewed_at,
		approved_by: task.acceptance_approved_by,
		approved_at: task.acceptance_approved_at,
		reason: task.acceptance_reason,
		remediation: parseJson(task.acceptance_remediation_json, []),
		history: tx.all("SELECT kind, actor, actor_kind, detail_json, created_at FROM acceptance_events WHERE task_id = ? ORDER BY id ASC", [taskId]).map((item) => ({
			kind: item.kind,
			actor: item.actor,
			actor_kind: item.actor_kind,
			detail: parseJson(item.detail_json, {}),
			created_at: item.created_at
		}))
	};
}

export function acceptanceForRun(tx, runId) {
	const row = tx.get("SELECT task_id FROM runs WHERE run_id = ?", [runId]);
	if (!row) return null;
	return acceptanceSnapshot(tx, row.task_id);
}

/** 驳回后未消费的整改意见；重试时注入原始会话。 */
export function pendingRemediation(tx, taskId) {
	const row = taskRow(tx, taskId);
	if (row.acceptance_status !== "rejected") return [];
	return parseJson(row.acceptance_remediation_json, []);
}

export function recordForceAuthorization(tx, { taskId, runId, operationId, kind, authorizedBy, reason, detail, now }) {
	tx.run(
		"INSERT INTO dispatch_authorizations (task_id, run_id, operation_id, kind, authorized_by, reason, detail_json, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
		[taskId, runId ?? null, operationId ?? null, kind, authorizedBy, reason, JSON.stringify(detail ?? {}), now]
	);
}

export function acceptanceHistory(tx, taskId, limit = 100) {
	return {
		criteria: tx.all("SELECT version, criteria_hash, machine_checkable, frozen_at, frozen_by FROM acceptance_criteria WHERE task_id = ? ORDER BY version DESC LIMIT ?", [taskId, limit]),
		reviews: tx.all("SELECT review_id, run_id, criteria_version, mode, reviewer_kind, reviewer, verdict, summary, created_at FROM acceptance_reviews WHERE task_id = ? ORDER BY created_at DESC LIMIT ?", [taskId, limit]),
		approvals: tx.all("SELECT approval_id, run_id, criteria_version, review_id, decision, approver, approver_kind, reason, created_at FROM acceptance_approvals WHERE task_id = ? ORDER BY created_at DESC LIMIT ?", [taskId, limit]),
		events: tx.all("SELECT kind, actor, actor_kind, detail_json, created_at FROM acceptance_events WHERE task_id = ? ORDER BY id DESC LIMIT ?", [taskId, limit]).map((item) => ({
			kind: item.kind,
			actor: item.actor,
			actor_kind: item.actor_kind,
			detail: parseJson(item.detail_json, {}),
			created_at: item.created_at
		}))
	};
}
