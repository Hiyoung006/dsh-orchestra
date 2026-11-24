// 任务级生命周期：13 态状态机、attempt 归属校验、启动恢复分类、重试退避。
//
// 这一层不替代 node_runs 的细粒度执行状态机，而是在 tasks 之上提供一个
// 面向人的任务生命周期视图。所有状态转换都必须经过 canTaskTransition /
// applyTaskLifecycle 校验，running 不存在回到 draft 的边。
//
// 验收边界：运行成功不等于任务通过。只有独立验收状态为 approved /
// not_applicable 时，succeeded 才能投影为 passed。

import { acceptanceAllowsPass } from "./task-acceptance.js";

export const TASK_STATUSES = Object.freeze([
	"draft",
	"blocked",
	"queued",
	"dispatching",
	"running",
	"review",
	"retrying",
	"passed",
	"rejected",
	"failed",
	"timeout",
	"cancelled",
	"interrupted"
]);

export const TASK_ACTIVE_STATUSES = Object.freeze([
	"draft",
	"blocked",
	"queued",
	"dispatching",
	"running",
	"review",
	"retrying"
]);

export const TASK_TERMINAL_STATUSES = Object.freeze([
	"passed",
	"rejected",
	"failed",
	"timeout",
	"cancelled",
	"interrupted"
]);

/** 终态里允许（在满足条件时）重新执行的状态。passed 只有用户显式要求才放行。 */
export const TASK_RETRYABLE_STATUSES = Object.freeze([
	"rejected",
	"failed",
	"timeout",
	"cancelled",
	"interrupted"
]);

export class TaskLifecycleError extends Error {
	constructor(code, message, details = {}) {
		super(message);
		this.name = "TaskLifecycleError";
		this.code = code;
		this.details = details;
	}
}

function isTerminalTaskStatus(status) {
	return TASK_TERMINAL_STATUSES.includes(status);
}

/**
 * 任务生命周期迁移表。每条规则都带 id，便于审计与测试定位。
 * 终态只允许在规则显式列出时离开（例如重试），不存在隐式的“终态复位”。
 */
export const TASK_TRANSITIONS = Object.freeze([
	{ id: "T-01", from: ["draft"], to: ["queued", "blocked", "cancelled"], trigger: "admit" },
	{ id: "T-02", from: ["blocked"], to: ["queued", "rejected", "cancelled"], trigger: "unblock" },
	{ id: "T-03", from: ["queued"], to: ["dispatching", "blocked", "cancelled", "timeout", "interrupted"], trigger: "dispatch" },
	{ id: "T-04", from: ["dispatching"], to: ["running", "failed", "timeout", "interrupted", "cancelled"], trigger: "start-observed" },
	{ id: "T-05", from: ["running"], to: ["review", "passed", "failed", "timeout", "interrupted", "cancelled"], trigger: "settle" },
	{ id: "T-06", from: ["review"], to: ["passed", "rejected", "failed", "timeout", "interrupted", "cancelled"], trigger: "review" },
	{ id: "T-07", from: ["retrying"], to: ["queued", "dispatching", "cancelled", "failed", "timeout"], trigger: "retry-start" },
	{ id: "T-08", from: ["passed"], to: ["retrying"], trigger: "rerun-explicit", requires: (ctx) => ctx.force === true && ctx.actorKind === "user" },
	{ id: "T-09", from: ["rejected"], to: ["retrying"], trigger: "rework-explicit", requires: (ctx) => ctx.explicit === true },
	{ id: "T-10", from: ["failed", "timeout", "cancelled", "interrupted"], to: ["retrying"], trigger: "retry", requires: (ctx) => ctx.retryAllowed !== false },
	{ id: "T-11", from: ["retrying"], to: ["queued"], trigger: "retry-due" },
	// 已配置的触发器属于用户显式授权，允许同一任务开始下一批次。
	{ id: "T-12", from: ["passed", "rejected", "failed", "timeout", "cancelled", "interrupted"], to: ["queued"], trigger: "triggered-start", requires: (ctx) => ctx.triggered === true },
	// 审批放行、审阅返工或直接观察到执行时，活动态回到 running。
	{ id: "T-13", from: ["queued", "blocked", "review", "retrying"], to: ["running"], trigger: "resume" }
]);

function ruleMatches(rule, from, to) {
	return rule.from.includes(from) && rule.to.includes(to);
}

/** 返回 {allowed, rule, reason}，不抛错，便于恢复扫描按事实选择安全动作。 */
export function taskTransitionAllowed(from, to, context = {}) {
	if (!TASK_STATUSES.includes(from)) return { allowed: false, rule: null, reason: "unknown_from" };
	if (!TASK_STATUSES.includes(to)) return { allowed: false, rule: null, reason: "unknown_to" };
	if (from === to) return { allowed: true, rule: null, reason: "noop" };
	const candidates = TASK_TRANSITIONS.filter((rule) => ruleMatches(rule, from, to));
	if (candidates.length === 0) return { allowed: false, rule: null, reason: "no_edge" };
	for (const rule of candidates) {
		if (!rule.requires || rule.requires(context) === true) return { allowed: true, rule, reason: "allowed" };
	}
	return { allowed: false, rule: candidates[0], reason: "precondition_unmet" };
}

export function assertTaskTransition(from, to, context = {}) {
	const verdict = taskTransitionAllowed(from, to, context);
	if (!verdict.allowed) {
		throw new TaskLifecycleError("STALE_GENERATION", "任务生命周期不允许这条迁移", {
			from,
			to,
			rule: verdict.rule?.id ?? null,
			reason: verdict.reason
		});
	}
	return verdict;
}

/** 构建完整状态转换图（邻接表），供审计与前端展示。 */
export function taskTransitionGraph() {
	const graph = Object.fromEntries(TASK_STATUSES.map((status) => [status, []]));
	for (const rule of TASK_TRANSITIONS) {
		for (const from of rule.from) {
			if (!graph[from]) continue;
			for (const to of rule.to) if (!graph[from].includes(to)) graph[from].push(to);
		}
	}
	return Object.freeze(Object.fromEntries(Object.entries(graph).map(([status, targets]) => [status, Object.freeze(targets)])));
}

/**
 * 重试退避：指数增长 + 抖动，必须有上限。
 * retryCount 从 1 开始（第一次重试）。
 */
export function retryBackoff({ retryCount, baseDelayMs = 1000, maxDelayMs = 60000, jitter = 0.2, random = Math.random, now = new Date().toISOString() } = {}) {
	const attempt = Math.max(1, Number(retryCount) || 1);
	const base = Math.max(1, Number(baseDelayMs) || 1);
	const cap = Math.max(base, Number(maxDelayMs) || base);
	const exponential = Math.min(cap, base * 2 ** (attempt - 1));
	const factor = 1 + Math.max(0, Number(jitter) || 0) * Math.min(1, Math.max(0, random()));
	const delayMs = Math.min(cap, Math.round(exponential * factor));
	return {
		delayMs,
		nextRetryAt: new Date(Date.parse(now) + delayMs).toISOString()
	};
}

/** 是否允许重试。auto 严格受上限约束；manual 也要有显式 force 才能越过上限。 */
export function canRetryTask({ lifecycle, retryCount = 0, retryLimit = 3, mode = "manual", force = false, actorKind = "user", explicit = false } = {}) {
	const count = Math.max(0, Number(retryCount) || 0);
	const limit = Math.max(0, Number(retryLimit) || 0);
	if (lifecycle === "passed") {
		if (!(force === true && actorKind === "user")) return { allowed: false, reason: "passed_requires_explicit_user_request" };
	} else if (lifecycle === "rejected") {
		if (!explicit && !force) return { allowed: false, reason: "rejected_requires_explicit_request" };
	} else if (!TASK_RETRYABLE_STATUSES.includes(lifecycle)) {
		return { allowed: false, reason: "not_retryable", lifecycle };
	}
	if (mode === "auto" && count >= limit) return { allowed: false, reason: "retry_limit_reached", retryCount: count, retryLimit: limit };
	return { allowed: true, reason: "allowed", retryCount: count, retryLimit: limit };
}

export function attemptIdFor(nodeRunId, attempt) {
	return `${nodeRunId}:${Number(attempt)}`;
}

export function isAttemptCurrent(lastAttemptId, attemptId) {
	return typeof lastAttemptId === "string" && lastAttemptId.length > 0 && lastAttemptId === attemptId;
}

/** 旧尝试的迟到事件不得改写任务状态。 */
export function assertAttemptOwnership(lastAttemptId, attemptId) {
	if (!isAttemptCurrent(lastAttemptId, attemptId)) {
		throw new TaskLifecycleError("STALE_GENERATION", "事件所属的执行尝试已过期", {
			expected: lastAttemptId ?? null,
			received: attemptId ?? null
		});
	}
	return true;
}

const RUN_TERMINAL_STATE = Object.freeze({
	succeeded: "passed",
	succeeded_with_unverified: "passed",
	failed: "failed",
	failed_with_unverified: "failed",
	orphaned: "interrupted",
	cancelled: "cancelled",
	timeout: "timeout"
});

const RUN_TERMINAL = Object.freeze(Object.keys(RUN_TERMINAL_STATE));

const NODE_ACTIVE = Object.freeze([
	"dispatched",
	"started",
	"waiting",
	"verifying",
	"stopping",
	"timing_out",
	"startup_unknown",
	"paused_bridge_missing",
	"indeterminate_side_effect"
]);

const NODE_TERMINAL = Object.freeze([
	"done",
	"failed",
	"timeout",
	"abandoned_unverified",
	"failed_with_unverified",
	"orphaned",
	"superseded"
]);

export function taskStateForRunStatus(runStatus) {
	return RUN_TERMINAL_STATE[runStatus] ?? null;
}

/**
 * 依据持久化事实推导任务生命周期（在线投影，不做会话探测）。
 * 终端 run 优先；运行中的 run 由节点状态与审批卡推导。
 */
export function projectTaskLifecycle(snapshot) {
	const { lifecycle, run, nodes = [], decisions = [] } = snapshot;
	if (lifecycle === "retrying" && snapshot.task?.next_retry_at) return { state: "retrying", reason: "retry_scheduled" };
	if (!run) return { state: lifecycle, reason: "run_missing" };
	const terminal = taskStateForRunStatus(run.status);
	if (terminal) {
		if (terminal === "passed" && !acceptanceAllowsPass(snapshot.acceptance)) return { state: "review", reason: "acceptance_pending" };
		if (terminal === "failed" && nodes.length > 0 && nodes.every((node) => node.status === "timeout")) return { state: "timeout", reason: "nodes_timeout" };
		return { state: terminal, reason: `run_${run.status}` };
	}
	if (run.status === "paused") return { state: "blocked", reason: "run_paused" };
	const open = decisions.filter((decision) => ["pending", "acknowledged", "blocked_for_human"].includes(decision.status));
	if (open.length > 0) {
		const pre = open.some((decision) => decision.kind === "pre_execution");
		return { state: pre ? "blocked" : "review", reason: pre ? "pre_execution_decision" : "result_review_decision" };
	}
	if (nodes.some((node) => node.status === "blocked_post")) return { state: "review", reason: "blocked_post" };
	if (nodes.some((node) => node.status === "blocked_pre")) return { state: "blocked", reason: "blocked_pre" };
	if (nodes.some((node) => NODE_ACTIVE.includes(node.status))) return { state: "running", reason: "node_active" };
	if (nodes.some((node) => node.status === "leased")) return { state: "dispatching", reason: "node_leased" };
	if (nodes.length > 0 && nodes.every((node) => NODE_TERMINAL.includes(node.status))) return { state: "review", reason: "nodes_terminal_run_open" };
	if (nodes.length === 0 || nodes.every((node) => node.status === "queued")) return { state: "queued", reason: "nodes_queued" };
	return { state: lifecycle, reason: "unclassified" };
}

function stale(iso, nowMs, staleMs) {
	if (!iso) return true;
	const at = Date.parse(iso);
	if (!Number.isFinite(at)) return true;
	return nowMs - at > staleMs;
}

/**
 * 启动恢复分类。输入必须全部来自持久化事实 + 当前进程对子会话的探测结果。
 *
 * sessionFacts: { [nodeRunId]: { exists, running, turnEnded, state } }
 * 没有探测结果时（纯离线恢复）只用心跳判断，绝不把 running 直接当 draft。
 */
export function classifyStartupTask(snapshot) {
	const { lifecycle, run, nodes = [], decisions = [], sessionFacts = null, acceptedReport = false, now = new Date().toISOString(), staleHeartbeatMs = 120000, currentEpoch = null } = snapshot;
	if (lifecycle === "retrying" && snapshot.task?.next_retry_at) return { state: "retrying", reason: "retry_scheduled", recoverable: true };
	if (!run) {
		if (lifecycle === "draft" || lifecycle === "blocked" || lifecycle === "retrying") return { state: lifecycle, reason: "no_run", recoverable: true };
		return { state: "interrupted", reason: "run_missing", recoverable: true };
	}
	const terminal = taskStateForRunStatus(run.status);
	if (terminal) {
		if (terminal === "passed" && !acceptanceAllowsPass(snapshot.acceptance)) return { state: "review", reason: "acceptance_pending", recoverable: true };
		if (terminal === "failed" && nodes.length > 0 && nodes.every((node) => node.status === "timeout")) return { state: "timeout", reason: "nodes_timeout", recoverable: false };
		return { state: terminal, reason: `run_${run.status}`, recoverable: false };
	}
	if (run.status === "paused") return { state: "blocked", reason: "run_paused", recoverable: false };

	const open = decisions.filter((decision) => ["pending", "acknowledged", "blocked_for_human"].includes(decision.status));
	if (open.length > 0) {
		const pre = open.some((decision) => decision.kind === "pre_execution");
		return { state: pre ? "blocked" : "review", reason: pre ? "pre_execution_decision" : "result_review_decision", recoverable: false };
	}
	if (nodes.some((node) => node.status === "blocked_post")) return { state: "review", reason: "blocked_post", recoverable: false };
	if (nodes.some((node) => node.status === "blocked_pre")) return { state: "blocked", reason: "blocked_pre", recoverable: false };

	const nowMs = Date.parse(now);
	const active = nodes.filter((node) => NODE_ACTIVE.includes(node.status) || node.status === "leased");
	if (active.length === 0) {
		if (nodes.length > 0 && nodes.every((node) => NODE_TERMINAL.includes(node.status))) return { state: "review", reason: "nodes_terminal_run_open", recoverable: true };
		if (nodes.length === 0 || nodes.every((node) => node.status === "queued")) return { state: "queued", reason: "nodes_queued", recoverable: false };
		return { state: lifecycle, reason: "no_active_nodes", recoverable: false };
	}

	const agentNodes = active.filter((node) => node.executor_agent_id || node.executor_session_id);
	// owner_epoch 变化说明这些尝试属于上一个 DSH 进程，子会话不可能还活着。
	// 这是重启恢复最可靠的持久化事实，优先于心跳时间。
	if (currentEpoch != null) {
		const foreign = active.filter((node) => node.owner_epoch != null && Number(node.owner_epoch) !== Number(currentEpoch));
		if (foreign.length > 0 && (!sessionFacts || foreign.every((node) => sessionFacts[node.node_run_id]?.exists !== true))) {
			return { state: "interrupted", reason: "owner_epoch_changed", recoverable: true };
		}
	}
	if (sessionFacts && agentNodes.length > 0) {
		const facts = agentNodes.map((node) => ({ node, fact: sessionFacts[node.node_run_id] ?? null }));
		if (facts.some(({ fact }) => fact && fact.running === true)) {
			const anyFresh = facts.some(({ node, fact }) => fact && fact.running === true && !stale(node.heartbeat_at ?? node.started_at, nowMs, staleHeartbeatMs));
			return anyFresh ? { state: "running", reason: "session_running", recoverable: false } : { state: "interrupted", reason: "session_running_but_stale_heartbeat", recoverable: true };
		}
		if (facts.some(({ fact }) => fact && fact.exists === true && fact.turnEnded === true && acceptedReport === true)) {
			return { state: "review", reason: "result_pending_review", recoverable: true };
		}
		if (facts.some(({ fact }) => fact && fact.exists === true && fact.turnEnded === true && acceptedReport !== true)) {
			return { state: "interrupted", reason: "turn_ended_without_valid_result", recoverable: true };
		}
		if (facts.every(({ fact }) => fact && fact.exists === false)) return { state: "interrupted", reason: "session_missing", recoverable: true };
		if (facts.some(({ fact }) => fact && fact.exists === true && fact.running === false)) return { state: "interrupted", reason: "session_idle_without_turn_end", recoverable: true };
	}

	const withHeartbeat = active.filter((node) => node.heartbeat_at || node.started_at);
	const fresh = withHeartbeat.filter((node) => !stale(node.heartbeat_at ?? node.started_at, nowMs, staleHeartbeatMs));
	if (fresh.length > 0) return { state: "running", reason: "heartbeat_fresh", recoverable: false };
	if (agentNodes.length > 0) return { state: "interrupted", reason: "heartbeat_stale", recoverable: true };
	return { state: "running", reason: "non_agent_active", recoverable: false };
}

// ---------------------------------------------------------------------------
// 持久化辅助（全部接受 tx = { all, get, run }）
// ---------------------------------------------------------------------------

function readTask(tx, taskId) {
	const task = tx.get(
		"SELECT task_id, lifecycle_state, lifecycle_reason, lifecycle_updated_at, retry_count, retry_limit, next_retry_at, last_attempt_id, current_run_id, acceptance_mode, acceptance_status, acceptance_criteria_version FROM tasks WHERE task_id = ?",
		[taskId]
	);
	if (!task) throw new TaskLifecycleError("NOT_FOUND", "任务不存在", { taskId });
	return task;
}

/**
 * 校验并落地任务生命周期迁移，同时写 task_lifecycle_events 审计行。
 * 返回 {changed, from, to, rule}。
 */
export function applyTaskLifecycle(tx, p) {
	const task = readTask(tx, p.taskId);
	const from = String(task.lifecycle_state);
	const to = String(p.next);
	if (!TASK_STATUSES.includes(to)) throw new TaskLifecycleError("SCHEMA_VERSION_UNSUPPORTED", "未知的任务生命周期状态", { state: to });
	if (from === to) return { changed: false, from, to, rule: null };
	const verdict = assertTaskTransition(from, to, {
		force: p.force === true,
		actorKind: p.actorKind ?? "system",
		explicit: p.explicit === true,
		triggered: p.triggered === true,
		retryAllowed: p.retryAllowed,
		retryCount: Number(task.retry_count ?? 0),
		retryLimit: Number(task.retry_limit ?? 0)
	});
	tx.run(
		"UPDATE tasks SET lifecycle_state = ?, lifecycle_reason = ?, lifecycle_updated_at = ?, updated_at = ?, revision = revision + 1 WHERE task_id = ? AND lifecycle_state = ?",
		[to, p.reason ?? null, p.now, p.now, p.taskId, from]
	);
	tx.run(
		"INSERT INTO task_lifecycle_events (task_id, from_state, to_state, reason, attempt_id, actor, created_at) VALUES (?, ?, ?, ?, ?, ?, ?)",
		[p.taskId, from, to, p.reason ?? null, p.attemptId ?? task.last_attempt_id ?? null, p.actor ?? null, p.now]
	);
	return { changed: true, from, to, rule: verdict.rule?.id ?? null };
}

export function loadTaskRecoverySnapshot(tx, taskId) {
	const task = readTask(tx, taskId);
	const run = task.current_run_id
		? tx.get("SELECT run_id, task_id, status, cancel_requested_at, root_run_id, started_at, updated_at FROM runs WHERE run_id = ?", [task.current_run_id])
		: null;
	const nodes = run
		? tx.all("SELECT node_run_id, node_id, attempt, status, owner_epoch, executor_agent_id, executor_session_id, started_at, heartbeat_at, lease_expires_at, replacement_node_run_id, cancel_requested_at, hang_reason, recovery_uncertain, updated_at FROM node_runs WHERE run_id = ? ORDER BY seq ASC", [run.run_id])
		: [];
	const decisions = run
		? tx.all("SELECT decision_id, kind, status, node_run_id FROM decisions WHERE run_id = ? AND status IN ('pending','acknowledged','blocked_for_human')", [run.run_id])
		: [];
	const acceptedReport = run
		? Boolean(tx.get("SELECT 1 AS ok FROM report_registry g JOIN node_runs n ON n.node_run_id = g.node_run_id WHERE n.run_id = ? AND g.attempt = n.attempt AND g.state IN ('applied','replayed') LIMIT 1", [run.run_id]))
		: false;
	return {
		task,
		run,
		nodes,
		decisions,
		acceptedReport,
		acceptance: {
			mode: task.acceptance_mode ?? "assisted",
			status: task.acceptance_status ?? "pending",
			criteriaVersion: task.acceptance_criteria_version ?? null
		}
	};
}

/**
 * 启动恢复：先按事实分类，再走与普通迁移相同的校验函数。
 * 禁止 running -> draft（状态机里不存在这条边）。
 */
export function recoverTaskLifecycle(tx, taskId, p) {
	const snapshot = loadTaskRecoverySnapshot(tx, taskId);
	const task = snapshot.task;
	if (isTerminalTaskStatus(task.lifecycle_state)) {
		return { changed: false, state: task.lifecycle_state, reason: "terminal", snapshot };
	}
	const verdict = classifyStartupTask({
		...snapshot,
		lifecycle: task.lifecycle_state,
		sessionFacts: p.sessionFacts ?? null,
		now: p.now,
		staleHeartbeatMs: p.staleHeartbeatMs,
		currentEpoch: p.currentEpoch ?? null
	});
	if (verdict.state === task.lifecycle_state) return { changed: false, state: verdict.state, reason: verdict.reason, snapshot: { ...snapshot, acceptedReport: snapshot.acceptedReport } };
	let applied;
	try {
		applied = applyTaskLifecycle(tx, {
			taskId,
			next: verdict.state,
			now: p.now,
			reason: `startup:${verdict.reason}`,
			actorKind: "system"
		});
	} catch (error) {
		if (error instanceof TaskLifecycleError) return { changed: false, state: task.lifecycle_state, reason: verdict.reason, blocked: error.details, snapshot };
		throw error;
	}
	return { changed: applied.changed, state: verdict.state, reason: verdict.reason, snapshot };
}

/** 在线维护扫描：把运行中的任务投影到任务生命周期。 */
export function projectTaskLifecycleFromFacts(tx, taskId, p) {
	const snapshot = loadTaskRecoverySnapshot(tx, taskId);
	const task = snapshot.task;
	if (isTerminalTaskStatus(task.lifecycle_state)) return { changed: false, state: task.lifecycle_state, reason: "terminal" };
	const verdict = projectTaskLifecycle({ ...snapshot, lifecycle: task.lifecycle_state });
	if (verdict.state === task.lifecycle_state) return { changed: false, state: verdict.state, reason: verdict.reason };
	try {
		const applied = applyTaskLifecycle(tx, {
			taskId,
			next: verdict.state,
			now: p.now,
			reason: `projection:${verdict.reason}`,
			actorKind: "system"
		});
		return { changed: applied.changed, state: verdict.state, reason: verdict.reason };
	} catch (error) {
		if (error instanceof TaskLifecycleError) return { changed: false, state: task.lifecycle_state, reason: verdict.reason, blocked: error.details };
		throw error;
	}
}

/** 进入重试：校验上限/显式要求，写 retry_count 与退避时间。 */
export function planTaskRetry(tx, p) {
	const task = readTask(tx, p.taskId);
	const mode = p.mode === "auto" ? "auto" : "manual";
	const verdict = canRetryTask({
		lifecycle: task.lifecycle_state,
		retryCount: task.retry_count,
		retryLimit: task.retry_limit,
		mode,
		force: p.force === true,
		actorKind: p.actorKind ?? "user",
		explicit: p.explicit === true
	});
	if (!verdict.allowed) {
		throw new TaskLifecycleError(verdict.reason === "retry_limit_reached" ? "OPERATION_ID_CONFLICT" : "FORBIDDEN", "当前任务不允许重试", {
			taskId: p.taskId,
			lifecycle: task.lifecycle_state,
			reason: verdict.reason,
			retryCount: verdict.retryCount,
			retryLimit: verdict.retryLimit
		});
	}
	const retryCount = Number(task.retry_count ?? 0) + 1;
	const backoff = retryBackoff({
		retryCount,
		baseDelayMs: p.baseDelayMs,
		maxDelayMs: p.maxDelayMs,
		jitter: p.jitter,
		random: p.random,
		now: p.now
	});
	applyTaskLifecycle(tx, {
		taskId: p.taskId,
		next: "retrying",
		now: p.now,
		reason: p.reason ?? `retry_${mode}`,
		force: p.force === true,
		actorKind: p.actorKind ?? "user",
		explicit: p.explicit === true,
		retryAllowed: true
	});
	const nextRetryAt = mode === "auto" ? backoff.nextRetryAt : p.now;
	tx.run("UPDATE tasks SET retry_count = ?, next_retry_at = ?, updated_at = ?, revision = revision + 1 WHERE task_id = ?", [retryCount, nextRetryAt, p.now, p.taskId]);
	return { taskId: p.taskId, mode, retryCount, delayMs: mode === "auto" ? backoff.delayMs : 0, nextRetryAt };
}

export function dueRetryTasks(tx, now, limit = 10) {
	return tx.all(
		"SELECT task_id, retry_count, retry_limit, current_run_id, next_retry_at FROM tasks WHERE lifecycle_state = 'retrying' AND next_retry_at IS NOT NULL AND next_retry_at <= ? ORDER BY next_retry_at ASC LIMIT ?",
		[now, Math.max(1, Number(limit) || 1)]
	);
}

export function listTaskAttempts(tx, taskId, limit = 100) {
	return tx.all(
		"SELECT attempt_id, task_id, run_id, node_run_id, session_id, attempt_no, status, dispatched_at, completed_at, outcome, error_message, retry_count, model_ref, model_provider, model_name, reasoning_effort, parent_attempt_id, created_at, updated_at FROM task_attempts WHERE task_id = ? ORDER BY created_at DESC, attempt_id DESC LIMIT ?",
		[taskId, Math.max(1, Number(limit) || 1)]
	);
}

export function rememberAttemptSpec(tx, { attemptId, spec, now }) {
	if (!attemptId) return 0;
	return tx.run("UPDATE task_attempts SET dispatch_spec_json = ?, updated_at = ? WHERE attempt_id = ?", [JSON.stringify(spec ?? {}), now, attemptId]).changes ?? 0;
}

/** 为任务级重试取回每个节点最近一次的可复用派发规格。 */
export function loadReplaySpecs(tx, taskId, runId) {
	const rows = tx.all(
		`SELECT a.node_run_id, a.dispatch_spec_json, n.node_id
       FROM task_attempts a JOIN node_runs n ON n.node_run_id = a.node_run_id
       WHERE a.task_id = ? AND a.run_id = ? AND a.dispatch_spec_json IS NOT NULL
       ORDER BY a.attempt_no ASC, a.created_at ASC`,
		[taskId, runId]
	);
	const byNode = new Map();
	for (const row of rows) byNode.set(row.node_id, row.dispatch_spec_json);
	const specs = {};
	for (const [nodeId, json] of byNode) {
		try {
			specs[nodeId] = JSON.parse(json);
		} catch {}
	}
	return specs;
}

/** 记录一次任务级重试尝试的派发事实（触发器未覆盖的字段）。 */
export function markAttemptDispatched(tx, { attemptId, now, spec }) {
	tx.run("UPDATE task_attempts SET dispatched_at = COALESCE(dispatched_at, ?), status = CASE WHEN completed_at IS NULL THEN 'dispatching' ELSE status END, dispatch_spec_json = COALESCE(?, dispatch_spec_json), updated_at = ? WHERE attempt_id = ?", [
		now,
		spec === undefined ? null : JSON.stringify(spec),
		now,
		attemptId
	]);
}

/** 明确尝试的最终结果（取消确认、超时、中断等由命令层写入）。 */
export function settleAttempt(tx, { attemptId, status, outcome, errorMessage = null, completedAt = null, now }) {
	tx.run("UPDATE task_attempts SET status = ?, outcome = ?, error_message = ?, completed_at = COALESCE(?, completed_at, ?), updated_at = ? WHERE attempt_id = ?", [
		status,
		outcome ?? null,
		errorMessage,
		completedAt,
		now,
		now,
		attemptId
	]);
}

/**
 * 任务级 attempt 归属：返回当前任务允许接受事件的 attemptId 集合。
 * 旧 run 的迟到事件不能修改新任务状态。
 */
export function assertTaskEventOwnership(tx, { taskId, attemptId, runId = null, allowChildRuns = true }) {
	const task = readTask(tx, taskId);
	if (attemptId != null && !isAttemptCurrent(task.last_attempt_id, attemptId)) {
		throw new TaskLifecycleError("STALE_GENERATION", "事件所属的执行尝试已过期", {
			taskId,
			expected: task.last_attempt_id ?? null,
			received: attemptId
		});
	}
	if (runId != null && task.current_run_id != null && runId !== task.current_run_id) {
		const run = tx.get("SELECT run_id, root_run_id FROM runs WHERE run_id = ?", [runId]);
		if (run && (!allowChildRuns || run.root_run_id === run.run_id)) {
			throw new TaskLifecycleError("STALE_GENERATION", "事件属于已被替换的旧执行批次", {
				taskId,
				currentRunId: task.current_run_id,
				receivedRunId: runId
			});
		}
	}
	return task;
}

export { isTerminalTaskStatus, NODE_ACTIVE, NODE_TERMINAL, RUN_TERMINAL_STATE };
