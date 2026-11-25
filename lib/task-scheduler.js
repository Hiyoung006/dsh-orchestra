// 任务调度器：优先级队列、并发/超时/重试、暂停恢复、工作区冲突与费用预算。
//
// 费用只接受宿主上报的真实金额（source=host, basis=actual）；
// 绝不把 Token 数量换算成金额。预算判断使用“实际费用 + 在途预留”，
// 预留只是预算占用，不是费用，也不会计入实际费用。

import { randomUUID } from "node:crypto";

export const SCHEDULER_JOB_STATUSES = Object.freeze([
	"queued",
	"dispatching",
	"running",
	"retrying",
	"cancelling",
	"succeeded",
	"failed",
	"cancelled",
	"budget_blocked"
]);

export const MODEL_CLASSES = Object.freeze(["simple", "code", "audit"]);
export const BUDGET_INTERRUPT_POLICIES = Object.freeze(["block_new", "cancel_running"]);

const MODEL_COMPLEXITY = Object.freeze({
	simple: "routine",
	code: "advanced",
	audit: "expert"
});

const ACTIVE_STATUSES = ["dispatching", "running", "retrying", "cancelling"];

export class SchedulerError extends Error {
	constructor(code, message, details = {}) {
		super(message);
		this.name = "SchedulerError";
		this.code = code;
		this.details = details;
	}
}

function parseJson(value, fallback) {
	if (typeof value !== "string" || value === "") return fallback;
	try {
		return JSON.parse(value);
	} catch {
		return fallback;
	}
}

function int(value, fallback, min, max) {
	const number = Number(value);
	if (!Number.isFinite(number)) return fallback;
	const rounded = Math.trunc(number);
	return Math.max(min, Math.min(max, rounded));
}

export function normalizeModelClass(value) {
	return MODEL_CLASSES.includes(value) ? value : null;
}

/** 调度档位只映射到 DSH 已支持的复杂度档位，不直接选择供应商或模型。 */
export function modelComplexityForClass(modelClass, fallback = null) {
	return MODEL_COMPLEXITY[modelClass] ?? fallback;
}

export function readSchedulerSettings(tx) {
	const row = tx.get("SELECT * FROM scheduler_settings WHERE id = 1");
	return {
		maxConcurrency: row?.max_concurrency ?? 1,
		paused: row?.paused === 1,
		globalBudgetMicros: row?.global_budget_micros ?? null,
		currency: row?.currency ?? "USD",
		defaultTimeoutMs: row?.default_timeout_ms ?? 3600000,
		defaultRetryLimit: row?.default_retry_limit ?? 0,
		budgetWarnPercent: row?.budget_warn_percent ?? 80,
		budgetInterruptPolicy: row?.budget_interrupt_policy ?? "block_new",
		updatedAt: row?.updated_at ?? null,
		updatedBy: row?.updated_by ?? null
	};
}

export function updateSchedulerSettings(tx, p) {
	const current = readSchedulerSettings(tx);
	const next = {
		maxConcurrency: p.maxConcurrency === undefined ? current.maxConcurrency : int(p.maxConcurrency, current.maxConcurrency, 1, 128),
		paused: p.paused === undefined ? current.paused : p.paused === true,
		globalBudgetMicros: p.globalBudgetMicros === undefined ? current.globalBudgetMicros : p.globalBudgetMicros === null ? null : int(p.globalBudgetMicros, 0, 0, Number.MAX_SAFE_INTEGER),
		currency: p.currency === undefined ? current.currency : String(p.currency).toUpperCase(),
		defaultTimeoutMs: p.defaultTimeoutMs === undefined ? current.defaultTimeoutMs : int(p.defaultTimeoutMs, current.defaultTimeoutMs, 1000, 7 * 24 * 3600000),
		defaultRetryLimit: p.defaultRetryLimit === undefined ? current.defaultRetryLimit : int(p.defaultRetryLimit, current.defaultRetryLimit, 0, 100),
		budgetWarnPercent: p.budgetWarnPercent === undefined ? current.budgetWarnPercent : int(p.budgetWarnPercent, current.budgetWarnPercent, 1, 100),
		budgetInterruptPolicy: p.budgetInterruptPolicy === undefined ? current.budgetInterruptPolicy : p.budgetInterruptPolicy
	};
	if (!/^[A-Z]{3}$/.test(next.currency)) throw new SchedulerError("PAYLOAD_SCHEMA_INVALID", "币种必须是三位大写代码", { currency: next.currency });
	if (!BUDGET_INTERRUPT_POLICIES.includes(next.budgetInterruptPolicy)) throw new SchedulerError("PAYLOAD_SCHEMA_INVALID", "未知的预算中断策略", { policy: next.budgetInterruptPolicy });
	tx.run(
		"UPDATE scheduler_settings SET max_concurrency = ?, paused = ?, global_budget_micros = ?, currency = ?, default_timeout_ms = ?, default_retry_limit = ?, budget_warn_percent = ?, budget_interrupt_policy = ?, updated_at = ?, updated_by = ? WHERE id = 1",
		[next.maxConcurrency, next.paused ? 1 : 0, next.globalBudgetMicros, next.currency, next.defaultTimeoutMs, next.defaultRetryLimit, next.budgetWarnPercent, next.budgetInterruptPolicy, p.now, p.actor ?? null]
	);
	return readSchedulerSettings(tx);
}

function jobFromRow(row) {
	if (!row) return null;
	return {
		job_id: row.job_id,
		task_id: row.task_id,
		title: row.title,
		workflow_id: row.workflow_id,
		workflow_version: row.workflow_version,
		node_ids: parseJson(row.node_ids_json, []),
		autonomy: row.autonomy ?? null,
		escalations: parseJson(row.escalations_json, []),
		status: row.status,
		priority: Number(row.priority) || 0,
		order_seq: Number(row.order_seq) || 0,
		enqueued_at: row.enqueued_at,
		started_at: row.started_at ?? null,
		finished_at: row.finished_at ?? null,
		run_id: row.run_id ?? null,
		workspace_key: row.workspace_key ?? null,
		timeout_ms: Number(row.timeout_ms) || 0,
		retry_limit: Number(row.retry_limit) || 0,
		retry_count: Number(row.retry_count) || 0,
		allow_auto_retry: row.allow_auto_retry === 1,
		model_class: row.model_class ?? null,
		model_failure_policy: row.model_failure_policy ?? "retry",
		fallback_model_class: row.fallback_model_class ?? null,
		budget_micros: row.budget_micros ?? null,
		cancel_reason: row.cancel_reason ?? null,
		last_error: row.last_error ?? null
	};
}

export function getJob(tx, jobId) {
	return jobFromRow(tx.get("SELECT * FROM scheduler_jobs WHERE job_id = ?", [jobId]));
}

export function listJobs(tx, { taskId, status, limit = 100 } = {}) {
	const where = [];
	const params = [];
	if (taskId) {
		where.push("task_id = ?");
		params.push(taskId);
	}
	if (status) {
		where.push("status = ?");
		params.push(status);
	}
	params.push(Math.max(1, Math.min(500, Number(limit) || 100)));
	return tx.all(`SELECT * FROM scheduler_jobs ${where.length ? `WHERE ${where.join(" AND ")}` : ""} ORDER BY enqueued_at DESC, order_seq DESC LIMIT ?`, params).map(jobFromRow);
}

function dependenciesOf(tx, jobId) {
	return tx.all("SELECT depends_on_task_id FROM scheduler_job_dependencies WHERE job_id = ? ORDER BY depends_on_task_id", [jobId]).map((row) => row.depends_on_task_id);
}

function wouldCycle(tx, taskId, dependsOn) {
	const jobs = tx.all("SELECT j.job_id, j.task_id FROM scheduler_jobs j");
	const deps = /* @__PURE__ */ new Map();
	for (const row of tx.all("SELECT job_id, depends_on_task_id FROM scheduler_job_dependencies")) {
		if (!deps.has(row.job_id)) deps.set(row.job_id, []);
		deps.get(row.job_id).push(row.depends_on_task_id);
	}
	const jobByTask = new Map(jobs.map((job) => [job.task_id, job.job_id]));
	const stack = [...dependsOn];
	const seen = /* @__PURE__ */ new Set();
	while (stack.length > 0) {
		const current = stack.pop();
		if (current === taskId) return true;
		if (seen.has(current)) continue;
		seen.add(current);
		const jobId = jobByTask.get(current);
		if (jobId) stack.push(...(deps.get(jobId) ?? []));
	}
	return false;
}

export function enqueueJob(tx, p) {
	const task = tx.get("SELECT task_id, title, status, current_run_id, workspace_source, workspace_root FROM tasks WHERE task_id = ?", [p.taskId]);
	if (!task) throw new SchedulerError("NOT_FOUND", "任务不存在", { taskId: p.taskId });
	if (task.current_run_id !== null) throw new SchedulerError("STALE_GENERATION", "任务已经在执行，不能重复入队", { taskId: p.taskId });
	if (task.status !== "reserved") throw new SchedulerError("STALE_GENERATION", "只有预留任务可以进入调度队列", { taskId: p.taskId, status: task.status });
	// 同一任务只允许有一个活动调度项；否则重复点击或重放请求会派发两次。
	const active = tx.get("SELECT job_id, status FROM scheduler_jobs WHERE task_id = ? AND status IN ('queued','dispatching','running','retrying','cancelling') LIMIT 1", [p.taskId]);
	if (active) throw new SchedulerError("STALE_GENERATION", "任务已在调度队列中，不能重复派发", { taskId: p.taskId, jobId: active.job_id, status: active.status });
	const dependsOn = [...new Set((p.dependsOn ?? []).map(String))];
	if (dependsOn.includes(String(p.taskId))) throw new SchedulerError("PAYLOAD_SCHEMA_INVALID", "任务不能依赖自身", { taskId: p.taskId });
	for (const dependency of dependsOn) {
		if (!tx.get("SELECT task_id FROM tasks WHERE task_id = ?", [dependency])) throw new SchedulerError("NOT_FOUND", "前置任务不存在", { taskId: dependency });
	}
	if (wouldCycle(tx, String(p.taskId), dependsOn)) throw new SchedulerError("PAYLOAD_SCHEMA_INVALID", "任务依赖形成环路");
	const settings = readSchedulerSettings(tx);
	const modelClass = normalizeModelClass(p.modelClass);
	const fallbackClass = normalizeModelClass(p.fallbackModelClass);
	const budget = p.budgetMicros === undefined || p.budgetMicros === null ? null : int(p.budgetMicros, 0, 0, Number.MAX_SAFE_INTEGER);
	const retryLimit = p.retryLimit === undefined ? settings.defaultRetryLimit : int(p.retryLimit, settings.defaultRetryLimit, 0, 100);
	const timeoutMs = p.timeoutMs === undefined ? settings.defaultTimeoutMs : int(p.timeoutMs, settings.defaultTimeoutMs, 1000, 7 * 24 * 3600000);
	const priority = int(p.priority, 0, -1000, 1000);
	const workspaceKey = p.workspaceKey === undefined ? (task.workspace_source ?? task.workspace_root ?? null) : p.workspaceKey;
	const jobId = "sj_" + randomUUID();
	const order = (tx.get("SELECT COALESCE(MAX(order_seq), 0) AS seq FROM scheduler_jobs")?.seq ?? 0) + 1;
	tx.run(
		`INSERT INTO scheduler_jobs
		 (job_id, task_id, title, workflow_id, workflow_version, node_ids_json, autonomy, escalations_json, status, priority, order_seq, enqueued_at, workspace_key, timeout_ms, retry_limit, retry_count, allow_auto_retry, model_class, model_failure_policy, fallback_model_class, budget_micros, updated_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'queued', ?, ?, ?, ?, ?, ?, 0, 1, ?, ?, ?, ?, ?)`,
		[
			jobId,
			p.taskId,
			String(p.title ?? task.title ?? "任务"),
			String(p.workflowId),
			String(p.workflowVersion),
			JSON.stringify(Array.isArray(p.nodeIds) ? p.nodeIds : []),
			p.autonomy ?? null,
			JSON.stringify(Array.isArray(p.escalations) ? p.escalations : []),
			priority,
			order,
			p.now,
			workspaceKey,
			timeoutMs,
			retryLimit,
			modelClass,
			p.modelFailurePolicy === "downgrade" ? "downgrade" : "retry",
			fallbackClass,
			budget,
			p.now
		]
	);
	for (const dependency of dependsOn) tx.run("INSERT INTO scheduler_job_dependencies (job_id, depends_on_task_id) VALUES (?, ?)", [jobId, dependency]);
	tx.run(
		"UPDATE tasks SET scheduler_priority = ?, scheduler_model_class = ?, scheduler_model_failure_policy = ?, scheduler_fallback_model_class = ?, scheduler_budget_micros = ?, retry_limit = 0, updated_at = ?, revision = revision + 1 WHERE task_id = ?",
		[priority, modelClass, p.modelFailurePolicy === "downgrade" ? "downgrade" : "retry", fallbackClass, budget, p.now, p.taskId]
	);
	return getJob(tx, jobId);
}

export function spentMicros(tx, { taskId = null } = {}) {
	const row = taskId
		? tx.get("SELECT COALESCE(SUM(amount_micros), 0) AS total FROM scheduler_cost_events WHERE task_id = ?", [taskId])
		: tx.get("SELECT COALESCE(SUM(amount_micros), 0) AS total FROM scheduler_cost_events");
	return Number(row?.total) || 0;
}

/** 在途预留：只是预算占用，不是费用，也不会写入费用事件。 */
export function heldMicros(tx, { taskId = null, jobId = null } = {}) {
	const where = ["status = 'held'"];
	const params = [];
	if (taskId !== null) {
		where.push("task_id = ?");
		params.push(taskId);
	}
	if (jobId !== null) {
		where.push("job_id = ?");
		params.push(jobId);
	}
	const row = tx.get(`SELECT COALESCE(SUM(amount_micros - settled_micros), 0) AS total FROM scheduler_budget_reservations WHERE ${where.join(" AND ")}`, params);
	return Math.max(0, Number(row?.total) || 0);
}

/** 已承诺金额 = 实际费用 + 在途预留；预算判断只看这个口径。 */
export function committedMicros(tx, { taskId = null } = {}) {
	return spentMicros(tx, { taskId }) + heldMicros(tx, { taskId });
}

/** 在途费用：活跃任务已上报的实际费用，加上尚未消耗的预留。 */
export function inFlightMicros(tx) {
	const row = tx.get(
		"SELECT COALESCE(SUM(e.amount_micros), 0) AS total FROM scheduler_cost_events e WHERE e.task_id IN (SELECT task_id FROM scheduler_jobs WHERE status IN ('dispatching','running','retrying','cancelling'))"
	);
	return Math.max(0, Number(row?.total) || 0) + heldMicros(tx);
}

export function getReservation(tx, jobId) {
	const row = tx.get("SELECT * FROM scheduler_budget_reservations WHERE job_id = ?", [jobId]);
	if (!row) return null;
	return {
		reservation_id: row.reservation_id,
		job_id: row.job_id,
		task_id: row.task_id,
		amount_micros: Number(row.amount_micros),
		settled_micros: Number(row.settled_micros),
		remaining_micros: Math.max(0, Number(row.amount_micros) - Number(row.settled_micros)),
		currency: row.currency,
		status: row.status,
		created_at: row.created_at,
		settled_at: row.settled_at ?? null
	};
}

/**
 * 派发前预留预算。预留金额来自服务端预算上限与剩余额度，不是费用估算；
 * 并发时按剩余槽位分摊，保证预留总和不超过剩余预算。
 */
export function reserveJobBudget(tx, { job, settings, shareSlots = 1, now }) {
	const existing = tx.get("SELECT * FROM scheduler_budget_reservations WHERE job_id = ?", [job.job_id]);
	if (existing) return { blocked: false, ...getReservation(tx, job.job_id), created: false };
	const globalBudget = settings.globalBudgetMicros;
	const globalAvailable = globalBudget === null ? null : Math.max(0, globalBudget - spentMicros(tx) - heldMicros(tx));
	const taskAvailable = job.budget_micros === null ? null : Math.max(0, job.budget_micros - spentMicros(tx, { taskId: job.task_id }) - heldMicros(tx, { taskId: job.task_id }));
	if (globalAvailable === 0 || taskAvailable === 0) {
		return {
			blocked: true,
			reason: taskAvailable === 0 ? "task_budget_exhausted" : "global_budget_exhausted",
			amount_micros: 0
		};
	}
	if (globalAvailable === null && taskAvailable === null) return { blocked: false, amount_micros: 0, created: false, reason: "no_budget_configured" };
	let amount = Math.min(taskAvailable ?? Number.MAX_SAFE_INTEGER, globalAvailable ?? Number.MAX_SAFE_INTEGER);
	if (globalAvailable !== null && shareSlots > 1) amount = Math.min(amount, Math.floor(globalAvailable / shareSlots));
	amount = Math.max(0, Math.floor(amount));
	if (amount === 0) return { blocked: true, reason: "budget_share_exhausted", amount_micros: 0 };
	const reservationId = "sbr_" + randomUUID();
	tx.run(
		"INSERT INTO scheduler_budget_reservations (reservation_id, job_id, task_id, amount_micros, currency, status, settled_micros, created_at, updated_at) VALUES (?, ?, ?, ?, ?, 'held', 0, ?, ?)",
		[reservationId, job.job_id, job.task_id, amount, settings.currency, now, now]
	);
	return { blocked: false, reservation_id: reservationId, job_id: job.job_id, task_id: job.task_id, amount_micros: amount, settled_micros: 0, remaining_micros: amount, currency: settings.currency, status: "held", created: true };
}

/** 真实费用到达后按实际金额消耗预留，避免实际费用与预留重复计入。 */
export function settleReservation(tx, { jobId, amountMicros, now }) {
	if (typeof jobId !== "string" || jobId === "") return null;
	const row = tx.get("SELECT * FROM scheduler_budget_reservations WHERE job_id = ? AND status = 'held'", [jobId]);
	if (!row) return null;
	const remaining = Math.max(0, Number(row.amount_micros) - Number(row.settled_micros));
	const settledMicros = Number(row.settled_micros) + Math.min(remaining, Math.max(0, Number(amountMicros) || 0));
	const status = settledMicros >= Number(row.amount_micros) ? "settled" : "held";
	tx.run(
		"UPDATE scheduler_budget_reservations SET settled_micros = ?, status = ?, settled_at = CASE WHEN ? = 'settled' THEN ? ELSE settled_at END, updated_at = ? WHERE reservation_id = ?",
		[settledMicros, status, status, now, now, row.reservation_id]
	);
	return { reservation_id: row.reservation_id, amount_micros: Number(row.amount_micros), settled_micros: settledMicros, remaining_micros: Math.max(0, Number(row.amount_micros) - settledMicros), status };
}

export function releaseReservation(tx, { jobId, now }) {
	if (typeof jobId !== "string" || jobId === "") return null;
	const result = tx.run("UPDATE scheduler_budget_reservations SET status = 'released', updated_at = ? WHERE job_id = ? AND status = 'held'", [now, jobId]);
	return result.changes === 1;
}

function releaseTerminalReservations(tx, now) {
	tx.run(
		"UPDATE scheduler_budget_reservations SET status = 'released', updated_at = ? WHERE status = 'held' AND job_id IN (SELECT job_id FROM scheduler_jobs WHERE status IN ('succeeded','failed','cancelled','budget_blocked'))",
		[now]
	);
}

export function recordCostEvent(tx, p) {
	if (p.source !== "host") throw new SchedulerError("PAYLOAD_SCHEMA_INVALID", "费用只能来自宿主真实统计（source=host）", { source: p.source ?? null });
	if ((p.basis ?? "actual") !== "actual") throw new SchedulerError("PAYLOAD_SCHEMA_INVALID", "不接受 Token 估算或其他非实际费用", { basis: p.basis ?? null });
	const amount = Number(p.amountMicros);
	if (!Number.isInteger(amount) || amount < 0) throw new SchedulerError("PAYLOAD_SCHEMA_INVALID", "费用金额必须是非负整数微单位", { amountMicros: p.amountMicros });
	const currency = String(p.currency ?? "USD").toUpperCase();
	if (!/^[A-Z]{3}$/.test(currency)) throw new SchedulerError("PAYLOAD_SCHEMA_INVALID", "币种必须是三位大写代码", { currency });
	if (p.jobId !== undefined && p.jobId !== null) {
		const job = tx.get("SELECT task_id FROM scheduler_jobs WHERE job_id = ?", [p.jobId]);
		if (!job) throw new SchedulerError("NOT_FOUND", "调度任务不存在", { jobId: p.jobId });
		if (String(job.task_id) !== String(p.taskId)) throw new SchedulerError("PAYLOAD_SCHEMA_INVALID", "费用事件的任务与调度任务不一致", { taskId: p.taskId, jobId: p.jobId });
	}
	const externalId = p.externalId == null ? "host:" + randomUUID() : String(p.externalId);
	const result = tx.run(
		"INSERT OR IGNORE INTO scheduler_cost_events (job_id, task_id, run_id, amount_micros, currency, source, basis, external_id, reported_at, created_at) VALUES (?, ?, ?, ?, ?, 'host', 'actual', ?, ?, ?)",
		[p.jobId ?? null, p.taskId, p.runId ?? null, amount, currency, externalId, p.reportedAt ?? p.now, p.now]
	);
	const reservation = result.changes === 1 && p.jobId ? settleReservation(tx, { jobId: p.jobId, amountMicros: amount, now: p.now }) : p.jobId ? getReservation(tx, p.jobId) : null;
	return { inserted: result.changes === 1, external_id: externalId, amount_micros: amount, currency, reservation };
}

function notifyOnce(tx, { kind, taskId = null, jobId = null, message, severity = "warning", metadata = null, now, windowMs = 3600000 }) {
	const since = new Date(Date.parse(now) - windowMs).toISOString();
	const existing = tx.get("SELECT id FROM scheduler_notifications WHERE kind = ? AND COALESCE(task_id, '') = COALESCE(?, '') AND COALESCE(job_id, '') = COALESCE(?, '') AND created_at > ? LIMIT 1", [kind, taskId, jobId, since]);
	if (existing) return false;
	tx.run("INSERT INTO scheduler_notifications (kind, task_id, job_id, message, severity, metadata_json, created_at) VALUES (?, ?, ?, ?, ?, ?, ?)", [kind, taskId, jobId, message, severity, metadata === null ? null : JSON.stringify(metadata), now]);
	return true;
}

function reconcile(tx, now) {
	const rows = tx.all(
		"SELECT j.job_id, j.run_id, j.status, r.status AS run_status FROM scheduler_jobs j LEFT JOIN runs r ON r.run_id = j.run_id WHERE j.status IN ('dispatching','running','cancelling')"
	);
	for (const row of rows) {
		if (!row.run_id || !row.run_status) continue;
		const runStatus = row.run_status;
		if (runStatus === "succeeded" || runStatus === "succeeded_with_unverified") {
			tx.run("UPDATE scheduler_jobs SET status = 'succeeded', finished_at = ?, updated_at = ? WHERE job_id = ?", [now, now, row.job_id]);
		} else if (runStatus === "failed" || runStatus === "timed_out") {
			tx.run("UPDATE scheduler_jobs SET status = 'failed', finished_at = ?, updated_at = ? WHERE job_id = ?", [now, now, row.job_id]);
		} else if (runStatus === "cancelled" || runStatus === "interrupted") {
			tx.run("UPDATE scheduler_jobs SET status = 'cancelled', finished_at = ?, updated_at = ? WHERE job_id = ?", [now, now, row.job_id]);
		}
	}
	releaseTerminalReservations(tx, now);
}

function usagePercent(committed, budget) {
	if (budget === null || budget === undefined) return null;
	if (budget <= 0) return committed > 0 ? 100 : 0;
	return Math.min(100, Math.floor(committed * 100 / budget));
}

function budgetState(committed, budget, warnPercent) {
	if (budget === null || budget === undefined) return "unbounded";
	if (committed >= budget) return "exhausted";
	if (usagePercent(committed, budget) >= warnPercent) return "warning";
	return "ok";
}

export function budgetSummary(tx, settings) {
	const actual = spentMicros(tx);
	const reserved = heldMicros(tx);
	const global = {
		budget_micros: settings.globalBudgetMicros,
		actual_micros: actual,
		reserved_micros: reserved,
		committed_micros: actual + reserved,
		remaining_micros: settings.globalBudgetMicros === null ? null : Math.max(0, settings.globalBudgetMicros - actual - reserved),
		usage_percent: usagePercent(actual + reserved, settings.globalBudgetMicros),
		state: budgetState(actual + reserved, settings.globalBudgetMicros, settings.budgetWarnPercent)
	};
	const tasks = new Map();
	for (const row of tx.all("SELECT task_id, budget_micros FROM scheduler_jobs WHERE budget_micros IS NOT NULL ORDER BY order_seq ASC")) {
		tasks.set(row.task_id, Number(row.budget_micros));
	}
	const taskBudgets = [];
	for (const [taskId, budgetMicros] of tasks) {
		const taskActual = spentMicros(tx, { taskId });
		const taskReserved = heldMicros(tx, { taskId });
		taskBudgets.push({
			task_id: taskId,
			budget_micros: budgetMicros,
			actual_micros: taskActual,
			reserved_micros: taskReserved,
			committed_micros: taskActual + taskReserved,
			remaining_micros: Math.max(0, budgetMicros - taskActual - taskReserved),
			usage_percent: usagePercent(taskActual + taskReserved, budgetMicros),
			state: budgetState(taskActual + taskReserved, budgetMicros, settings.budgetWarnPercent)
		});
	}
	return {
		currency: settings.currency,
		warn_percent: settings.budgetWarnPercent,
		interrupt_policy: settings.budgetInterruptPolicy,
		global,
		tasks: taskBudgets
	};
}

/**
 * 调度一拍：返回需要宿主导入执行的动作。
 * 只把可执行任务从 queued 领取为 dispatching；真正的 startRun 由调用方执行。
 */
export function claimSchedulerJobs(tx, { now, limit = 20 } = {}) {
	reconcile(tx, now);
	const settings = readSchedulerSettings(tx);
	for (const row of tx.all("SELECT * FROM scheduler_jobs WHERE status = 'budget_blocked'")) {
		const job = jobFromRow(row);
		const taskBlocked = job.budget_micros !== null && committedMicros(tx, { taskId: job.task_id }) >= job.budget_micros;
		const globalBlocked = settings.globalBudgetMicros !== null && committedMicros(tx) >= settings.globalBudgetMicros;
		if (!taskBlocked && !globalBlocked) {
			tx.run("UPDATE scheduler_jobs SET status = 'queued', cancel_reason = NULL, updated_at = ? WHERE job_id = ? AND status = 'budget_blocked'", [now, job.job_id]);
		}
	}
	const actions = [];
	const blocked = [];
	const notifications = [];

	for (const row of tx.all("SELECT * FROM scheduler_jobs WHERE status = 'running' AND started_at IS NOT NULL AND timeout_ms > 0")) {
		const job = jobFromRow(row);
		const deadline = Date.parse(job.started_at) + job.timeout_ms;
		if (Number.isFinite(deadline) && deadline <= Date.parse(now) && job.status !== "cancelling") {
			tx.run("UPDATE scheduler_jobs SET status = 'cancelling', cancel_reason = 'timeout', updated_at = ? WHERE job_id = ?", [now, job.job_id]);
			tx.run("UPDATE tasks SET retry_limit = retry_count, next_retry_at = NULL, updated_at = ?, revision = revision + 1 WHERE task_id = ?", [now, job.task_id]);
			actions.push({ type: "cancel", job: { ...job, status: "cancelling" }, reason: "scheduler_timeout" });
			if (notifyOnce(tx, { kind: "job_timeout", taskId: job.task_id, jobId: job.job_id, message: `任务「${job.title}」超过 ${Math.round(job.timeout_ms / 1000)} 秒，已请求取消。`, now })) notifications.push("job_timeout");
		}
	}

	for (const row of tx.all("SELECT * FROM scheduler_jobs WHERE status = 'failed' AND allow_auto_retry = 1 AND retry_count < retry_limit ORDER BY priority DESC, order_seq ASC LIMIT ?", [limit])) {
		const job = jobFromRow(row);
		const task = tx.get("SELECT lifecycle_state FROM tasks WHERE task_id = ?", [job.task_id]);
		if (!task || task.lifecycle_state === "cancelled") continue;
		tx.run("UPDATE scheduler_jobs SET status = 'retrying', retry_count = retry_count + 1, updated_at = ? WHERE job_id = ? AND status = 'failed'", [now, job.job_id]);
		actions.push({ type: "retry", job: { ...job, status: "retrying", retry_count: job.retry_count + 1 } });
	}

	const globalActual = spentMicros(tx);
	const globalReserved = heldMicros(tx);
	const globalCommitted = globalActual + globalReserved;
	const globalBudget = settings.globalBudgetMicros;
	const budgetExhausted = globalBudget !== null && globalCommitted >= globalBudget;
	if (budgetExhausted) {
		if (notifyOnce(tx, {
			kind: "budget_global",
			severity: "critical",
			metadata: { actual_micros: globalActual, reserved_micros: globalReserved, budget_micros: globalBudget },
			message: `全局费用预算已用尽（实际 ${globalActual} + 预留 ${globalReserved} / ${globalBudget} ${settings.currency}），停止新的派发。`,
			now
		})) notifications.push("budget_global");
	} else if (globalBudget !== null && usagePercent(globalCommitted, globalBudget) >= settings.budgetWarnPercent) {
		if (notifyOnce(tx, {
			kind: "budget_global_warning",
			severity: "warning",
			metadata: { actual_micros: globalActual, reserved_micros: globalReserved, budget_micros: globalBudget, warn_percent: settings.budgetWarnPercent },
			message: `全局费用预算已达到 ${settings.budgetWarnPercent}% 告警阈值（实际 ${globalActual} + 预留 ${globalReserved} / ${globalBudget} ${settings.currency}）。`,
			now
		})) notifications.push("budget_global_warning");
	}

	for (const row of tx.all("SELECT * FROM scheduler_jobs WHERE status = 'queued' ORDER BY priority DESC, order_seq ASC LIMIT ?", [Math.max(1, limit)])) {
		const job = jobFromRow(row);
		if (job.budget_micros !== null) {
			const taskCommitted = committedMicros(tx, { taskId: job.task_id });
			if (taskCommitted >= job.budget_micros) {
				tx.run("UPDATE scheduler_jobs SET status = 'budget_blocked', cancel_reason = 'budget', updated_at = ? WHERE job_id = ? AND status = 'queued'", [now, job.job_id]);
				if (notifyOnce(tx, { kind: "budget_task", taskId: job.task_id, jobId: job.job_id, severity: "critical", message: `任务「${job.title}」单任务预算已用尽（${taskCommitted}/${job.budget_micros} ${settings.currency}）。`, now })) notifications.push("budget_task");
				continue;
			}
			if (usagePercent(taskCommitted, job.budget_micros) >= settings.budgetWarnPercent) {
				if (notifyOnce(tx, { kind: "budget_task_warning", taskId: job.task_id, jobId: job.job_id, severity: "warning", message: `任务「${job.title}」单任务预算已达到告警阈值（${taskCommitted}/${job.budget_micros} ${settings.currency}）。`, now })) notifications.push("budget_task_warning");
			}
		}
		const deps = dependenciesOf(tx, job.job_id);
		const unmet = deps.filter((taskId) => {
			const task = tx.get("SELECT lifecycle_state FROM tasks WHERE task_id = ?", [taskId]);
			return !task || task.lifecycle_state !== "passed";
		});
		if (unmet.length > 0) {
			blocked.push({ job_id: job.job_id, task_id: job.task_id, reason: "dependency_not_passed", depends_on: unmet });
			continue;
		}
	}

	if (settings.budgetInterruptPolicy === "cancel_running") {
		for (const row of tx.all("SELECT * FROM scheduler_jobs WHERE status IN ('dispatching','running','retrying')")) {
			const job = jobFromRow(row);
			const taskCommitted = committedMicros(tx, { taskId: job.task_id });
			const taskExhausted = job.budget_micros !== null && taskCommitted >= job.budget_micros;
			if (!budgetExhausted && !taskExhausted) continue;
			const reason = taskExhausted ? "budget_task" : "budget_global";
			tx.run("UPDATE scheduler_jobs SET status = 'cancelling', cancel_reason = ?, updated_at = ? WHERE job_id = ? AND status IN ('dispatching','running','retrying')", [reason, now, job.job_id]);
			tx.run("UPDATE tasks SET retry_limit = retry_count, next_retry_at = NULL, updated_at = ?, revision = revision + 1 WHERE task_id = ?", [now, job.task_id]);
			actions.push({ type: "cancel", job: { ...job, status: "cancelling", cancel_reason: reason }, reason: reason === "budget_task" ? "budget_task_exhausted" : "budget_global_exhausted" });
			if (notifyOnce(tx, { kind: "budget_interrupt", taskId: job.task_id, jobId: job.job_id, severity: "critical", metadata: { reason, task_committed_micros: taskCommitted }, message: `任务「${job.title}」因预算用尽被中断。`, now })) notifications.push("budget_interrupt");
		}
	}

	const active = tx.all(`SELECT job_id, workspace_key FROM scheduler_jobs WHERE status IN (${ACTIVE_STATUSES.map(() => "?").join(",")})`, ACTIVE_STATUSES);
	const runningKeys = new Set(active.map((row) => row.workspace_key).filter((key) => typeof key === "string" && key !== ""));
	const slots = Math.max(0, settings.maxConcurrency - active.length);
	const selected = [];
	if (!settings.paused && !budgetExhausted && slots > 0) {
		for (const row of tx.all("SELECT * FROM scheduler_jobs WHERE status = 'queued' ORDER BY priority DESC, order_seq ASC LIMIT ?", [Math.max(1, limit)])) {
			if (selected.length >= slots) break;
			const job = jobFromRow(row);
			const deps = dependenciesOf(tx, job.job_id);
			if (deps.some((taskId) => {
				const task = tx.get("SELECT lifecycle_state FROM tasks WHERE task_id = ?", [taskId]);
				return !task || task.lifecycle_state !== "passed";
			})) continue;
			if (job.workspace_key && (runningKeys.has(job.workspace_key) || selected.some((item) => item.workspace_key === job.workspace_key))) continue;
			const reservation = reserveJobBudget(tx, { job, settings, shareSlots: Math.max(1, slots - selected.length), now });
			if (reservation.blocked) {
				if (reservation.reason === "task_budget_exhausted") {
					tx.run("UPDATE scheduler_jobs SET status = 'budget_blocked', cancel_reason = 'budget', updated_at = ? WHERE job_id = ? AND status = 'queued'", [now, job.job_id]);
					if (notifyOnce(tx, { kind: "budget_task", taskId: job.task_id, jobId: job.job_id, severity: "critical", message: `任务「${job.title}」单任务预算已用尽。`, now })) notifications.push("budget_task");
				}
				if (reservation.reason === "global_budget_exhausted" || reservation.reason === "budget_share_exhausted") break;
				continue;
			}
			const updated = tx.run("UPDATE scheduler_jobs SET status = 'dispatching', started_at = ?, updated_at = ? WHERE job_id = ? AND status = 'queued'", [now, now, job.job_id]);
			if (updated.changes !== 1) {
				if (reservation.created) releaseReservation(tx, { jobId: job.job_id, now });
				continue;
			}
			selected.push({ ...job, status: "dispatching", started_at: now });
			actions.push({ type: "start", job: { ...job, status: "dispatching", started_at: now } });
			if (job.workspace_key) runningKeys.add(job.workspace_key);
		}
	}
	const finalReserved = heldMicros(tx);
	const finalCommitted = spentMicros(tx) + finalReserved;
	return {
		settings,
		actions,
		selected,
		blocked,
		notifications,
		stats: {
			queued: tx.get("SELECT COUNT(*) AS n FROM scheduler_jobs WHERE status = 'queued'")?.n ?? 0,
			active: active.length,
			max_concurrency: settings.maxConcurrency,
			paused: settings.paused,
			global_spent_micros: globalActual,
			global_reserved_micros: finalReserved,
			global_committed_micros: finalCommitted,
			in_flight_micros: inFlightMicros(tx),
			global_budget_micros: globalBudget,
			budget_exhausted: budgetExhausted
		}
	};
}

export function markJobRunning(tx, { jobId, runId, now }) {
	tx.run("UPDATE scheduler_jobs SET status = 'running', run_id = ?, started_at = COALESCE(started_at, ?), updated_at = ? WHERE job_id = ?", [runId, now, now, jobId]);
}

export function markJobFailed(tx, { jobId, error, now }) {
	tx.run("UPDATE scheduler_jobs SET status = 'failed', last_error = ?, finished_at = ?, updated_at = ? WHERE job_id = ?", [String(error ?? "dispatch_failed").slice(0, 500), now, now, jobId]);
	releaseReservation(tx, { jobId, now });
}

export function markJobRetried(tx, { jobId, runId, now }) {
	tx.run("UPDATE scheduler_jobs SET status = 'running', run_id = ?, started_at = ?, updated_at = ? WHERE job_id = ?", [runId, now, now, jobId]);
}

export function cancelJobs(tx, { jobIds = [], taskIds = [], allQueued = false, reason = "cancelled_by_user", actor = null, now }) {
	const conditions = [];
	const params = [];
	if (jobIds.length > 0) {
		conditions.push(`job_id IN (${jobIds.map(() => "?").join(",")})`);
		params.push(...jobIds);
	}
	if (taskIds.length > 0) {
		conditions.push(`task_id IN (${taskIds.map(() => "?").join(",")})`);
		params.push(...taskIds);
	}
	if (allQueued) conditions.push("status = 'queued'");
	if (conditions.length === 0) return { cancelled: [], actions: [] };
	const rows = tx.all(`SELECT * FROM scheduler_jobs WHERE ${conditions.join(" OR ")}`, params);
	const cancelled = [];
	const actions = [];
	for (const row of rows) {
		const job = jobFromRow(row);
		if (["succeeded", "failed", "cancelled"].includes(job.status)) continue;
		tx.run("UPDATE scheduler_jobs SET allow_auto_retry = 0, cancel_reason = ?, updated_at = ? WHERE job_id = ?", [reason, now, job.job_id]);
		tx.run("UPDATE tasks SET retry_limit = retry_count, next_retry_at = NULL, updated_at = ?, revision = revision + 1 WHERE task_id = ?", [now, job.task_id]);
		if (["dispatching", "running", "retrying", "cancelling"].includes(job.status)) {
			tx.run("UPDATE scheduler_jobs SET status = 'cancelling', updated_at = ? WHERE job_id = ?", [now, job.job_id]);
			actions.push({ type: "cancel", job: { ...job, status: "cancelling", allow_auto_retry: false }, reason });
		} else {
			tx.run("UPDATE scheduler_jobs SET status = 'cancelled', finished_at = ?, updated_at = ? WHERE job_id = ?", [now, now, job.job_id]);
			releaseReservation(tx, { jobId: job.job_id, now });
		}
		cancelled.push(job.job_id);
	}
	if (cancelled.length > 0 && actor) notifyOnce(tx, { kind: "batch_cancel", message: `已取消 ${cancelled.length} 个调度任务：${reason}`, now });
	return { cancelled, actions };
}

export function schedulerSnapshot(tx, { now, limit = 50 } = {}) {
	const settings = readSchedulerSettings(tx);
	const globalSpent = spentMicros(tx);
	const budgets = budgetSummary(tx, settings);
	const notifications = tx.all("SELECT id, kind, task_id, job_id, message, severity, metadata_json, created_at, acknowledged_at FROM scheduler_notifications ORDER BY id DESC LIMIT ?", [Math.max(1, Math.min(200, Number(limit) || 50))]).map((row) => ({
		...row,
		metadata: row.metadata_json ? JSON.parse(row.metadata_json) : null
	}));
	return {
		settings,
		jobs: listJobs(tx, { limit }),
		global_spent_micros: globalSpent,
		global_reserved_micros: budgets.global.reserved_micros,
		global_committed_micros: budgets.global.committed_micros,
		global_budget_remaining_micros: settings.globalBudgetMicros === null ? null : Math.max(0, settings.globalBudgetMicros - globalSpent),
		in_flight_micros: inFlightMicros(tx),
		budgets,
		notifications
	};
}

export function acknowledgeNotifications(tx, { ids = [], all = false, now }) {
	if (all) {
		tx.run("UPDATE scheduler_notifications SET acknowledged_at = ? WHERE acknowledged_at IS NULL", [now]);
		return { acknowledged: true };
	}
	for (const id of ids) tx.run("UPDATE scheduler_notifications SET acknowledged_at = ? WHERE id = ?", [now, Number(id)]);
	return { acknowledged: ids.length };
}
