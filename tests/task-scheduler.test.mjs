import test from "node:test";
import assert from "node:assert/strict";
import { DatabaseSync } from "node:sqlite";
import { readdirSync, readFileSync } from "node:fs";
import { join, dirname } from "node:path";
import { fileURLToPath } from "node:url";
import {
	SchedulerError,
	cancelJobs,
	claimSchedulerJobs,
	enqueueJob,
	modelComplexityForClass,
	readSchedulerSettings,
	recordCostEvent,
	updateSchedulerSettings
} from "../lib/task-scheduler.js";

const MIGRATIONS = join(dirname(fileURLToPath(import.meta.url)), "..", "lib", "infra", "sqlite", "migrations");
const NOW = "2026-01-01T00:00:00.000Z";
const LATER = "2026-01-01T02:00:00.000Z";

function freshDb() {
	const db = new DatabaseSync(":memory:");
	for (const file of readdirSync(MIGRATIONS).filter((name) => name.endsWith(".sql")).sort()) {
		db.exec(readFileSync(join(MIGRATIONS, file), "utf8"));
	}
	return db;
}

function makeTx(db) {
	return {
		all: (sql, params = []) => db.prepare(sql).all(...params),
		get: (sql, params = []) => db.prepare(sql).get(...params),
		run: (sql, params = []) => {
			const result = db.prepare(sql).run(...params);
			return { changes: Number(result.changes), lastInsertRowid: Number(result.lastInsertRowid) };
		}
	};
}

function seedTask(tx, taskId, { lifecycle = "draft", status = "reserved", workspace = null } = {}) {
	tx.run(
		"INSERT INTO tasks (task_id, title, status, current_run_id, lifecycle_state, workspace_source, created_at, updated_at) VALUES (?, ?, ?, NULL, ?, ?, ?, ?)",
		[taskId, "任务 " + taskId, status, lifecycle, workspace, NOW, NOW]
	);
}

function enqueue(tx, taskId, overrides = {}) {
	return enqueueJob(tx, {
		taskId,
		workflowId: "wf_test",
		workflowVersion: "1",
		nodeIds: ["n1"],
		now: NOW,
		...overrides
	});
}

test("高优先级先调度，并发上限生效", () => {
	const db = freshDb();
	const tx = makeTx(db);
	updateSchedulerSettings(tx, { maxConcurrency: 2, now: NOW });
	seedTask(tx, "t_low");
	seedTask(tx, "t_high");
	seedTask(tx, "t_mid");
	const low = enqueue(tx, "t_low", { priority: 1 });
	const high = enqueue(tx, "t_high", { priority: 10 });
	const mid = enqueue(tx, "t_mid", { priority: 5 });
	const claim = claimSchedulerJobs(tx, { now: LATER });
	const starts = claim.actions.filter((action) => action.type === "start").map((action) => action.job.job_id);
	assert.deepEqual(starts, [high.job_id, mid.job_id]);
	assert.equal(claim.stats.active, 0);
	const second = claimSchedulerJobs(tx, { now: LATER });
	assert.equal(second.actions.filter((action) => action.type === "start").length, 0);
	assert.equal(tx.get("SELECT status FROM scheduler_jobs WHERE job_id = ?", [low.job_id]).status, "queued");
});

test("前置任务未通过时子任务不调度，通过后放行", () => {
	const db = freshDb();
	const tx = makeTx(db);
	seedTask(tx, "parent", { lifecycle: "review" });
	seedTask(tx, "child");
	const child = enqueue(tx, "child", { dependsOn: ["parent"] });
	const blocked = claimSchedulerJobs(tx, { now: LATER });
	assert.equal(blocked.actions.filter((action) => action.type === "start").length, 0);
	assert.ok(blocked.blocked.some((item) => item.job_id === child.job_id && item.reason === "dependency_not_passed"));
	tx.run("UPDATE tasks SET lifecycle_state = 'passed' WHERE task_id = 'parent'");
	const allowed = claimSchedulerJobs(tx, { now: LATER });
	assert.deepEqual(allowed.actions.filter((action) => action.type === "start").map((action) => action.job.job_id), [child.job_id]);
});

test("相同工作目录的任务不得并发，不同目录可并发", () => {
	const db = freshDb();
	const tx = makeTx(db);
	updateSchedulerSettings(tx, { maxConcurrency: 3, now: NOW });
	seedTask(tx, "a");
	seedTask(tx, "b");
	seedTask(tx, "c");
	const a = enqueue(tx, "a", { priority: 3, workspaceKey: "/repo/shared" });
	const b = enqueue(tx, "b", { priority: 2, workspaceKey: "/repo/shared" });
	const c = enqueue(tx, "c", { priority: 1, workspaceKey: "/repo/other" });
	const claim = claimSchedulerJobs(tx, { now: LATER });
	const starts = claim.actions.filter((action) => action.type === "start").map((action) => action.job.job_id);
	assert.ok(starts.includes(a.job_id));
	assert.ok(starts.includes(c.job_id));
	assert.ok(!starts.includes(b.job_id));
});

test("暂停后不再派发，恢复后继续", () => {
	const db = freshDb();
	const tx = makeTx(db);
	updateSchedulerSettings(tx, { paused: true, now: NOW });
	seedTask(tx, "t1");
	const job = enqueue(tx, "t1");
	assert.equal(claimSchedulerJobs(tx, { now: LATER }).actions.length, 0);
	updateSchedulerSettings(tx, { paused: false, now: NOW });
	assert.deepEqual(claimSchedulerJobs(tx, { now: LATER }).actions.filter((action) => action.type === "start").map((action) => action.job.job_id), [job.job_id]);
});

test("全局预算用尽时停止派发并通知", () => {
	const db = freshDb();
	const tx = makeTx(db);
	updateSchedulerSettings(tx, { maxConcurrency: 2, globalBudgetMicros: 1000, now: NOW });
	seedTask(tx, "t1");
	enqueue(tx, "t1");
	recordCostEvent(tx, { taskId: "t1", amountMicros: 1000, currency: "USD", source: "host", externalId: "e1", now: NOW });
	const claim = claimSchedulerJobs(tx, { now: LATER });
	assert.equal(claim.actions.filter((action) => action.type === "start").length, 0);
	assert.equal(claim.stats.budget_exhausted, true);
	assert.equal(tx.get("SELECT COUNT(*) AS n FROM scheduler_notifications WHERE kind = 'budget_global'").n, 1);
});

test("单任务预算用尽时阻塞该任务并通知", () => {
	const db = freshDb();
	const tx = makeTx(db);
	updateSchedulerSettings(tx, { maxConcurrency: 2, now: NOW });
	seedTask(tx, "t1");
	const job = enqueue(tx, "t1", { budgetMicros: 500 });
	recordCostEvent(tx, { taskId: "t1", amountMicros: 500, currency: "USD", source: "host", externalId: "e1", now: NOW });
	const claim = claimSchedulerJobs(tx, { now: LATER });
	assert.equal(claim.actions.filter((action) => action.type === "start").length, 0);
	assert.equal(tx.get("SELECT status FROM scheduler_jobs WHERE job_id = ?", [job.job_id]).status, "budget_blocked");
	assert.equal(tx.get("SELECT COUNT(*) AS n FROM scheduler_notifications WHERE kind = 'budget_task'").n, 1);
});

test("运行超时会生成取消动作并关闭自动重试", () => {
	const db = freshDb();
	const tx = makeTx(db);
	seedTask(tx, "t1");
	const job = enqueue(tx, "t1", { timeoutMs: 1000 });
	tx.run("UPDATE scheduler_jobs SET status = 'running', started_at = ? WHERE job_id = ?", [NOW, job.job_id]);
	tx.run("UPDATE tasks SET status = 'running', lifecycle_state = 'running', retry_limit = 3, retry_count = 0 WHERE task_id = 't1'");
	const claim = claimSchedulerJobs(tx, { now: LATER });
	const cancel = claim.actions.find((action) => action.type === "cancel");
	assert.ok(cancel);
	assert.equal(cancel.reason, "scheduler_timeout");
	assert.equal(tx.get("SELECT status FROM scheduler_jobs WHERE job_id = ?", [job.job_id]).status, "cancelling");
	assert.equal(tx.get("SELECT retry_limit FROM tasks WHERE task_id = 't1'").retry_limit, 0);
});

test("自动重试只处理失败任务，用户取消的任务不重试", () => {
	const db = freshDb();
	const tx = makeTx(db);
	seedTask(tx, "t1");
	seedTask(tx, "t2");
	const retryable = enqueue(tx, "t1", { retryLimit: 2 });
	const cancelled = enqueue(tx, "t2", { retryLimit: 2 });
	tx.run("UPDATE tasks SET status = 'failed', lifecycle_state = 'failed' WHERE task_id = 't1'");
	tx.run("UPDATE tasks SET status = 'cancelled', lifecycle_state = 'cancelled' WHERE task_id = 't2'");
	tx.run("UPDATE scheduler_jobs SET status = 'failed' WHERE job_id IN (?, ?)", [retryable.job_id, cancelled.job_id]);
	const claim = claimSchedulerJobs(tx, { now: LATER });
	const retries = claim.actions.filter((action) => action.type === "retry");
	assert.deepEqual(retries.map((action) => action.job.job_id), [retryable.job_id]);
	assert.equal(tx.get("SELECT status FROM scheduler_jobs WHERE job_id = ?", [retryable.job_id]).status, "retrying");
	assert.equal(tx.get("SELECT retry_count FROM scheduler_jobs WHERE job_id = ?", [retryable.job_id]).retry_count, 1);
});

test("批量取消：排队任务直接取消，运行中任务生成取消动作，并禁用自动重试", () => {
	const db = freshDb();
	const tx = makeTx(db);
	seedTask(tx, "queued");
	seedTask(tx, "running");
	const queued = enqueue(tx, "queued", { retryLimit: 3 });
	const running = enqueue(tx, "running", { retryLimit: 3 });
	tx.run("UPDATE scheduler_jobs SET status = 'running', run_id = 'r1' WHERE job_id = ?", [running.job_id]);
	tx.run("UPDATE tasks SET status = 'running', lifecycle_state = 'running', retry_limit = 3 WHERE task_id = 'running'");
	const result = cancelJobs(tx, { jobIds: [queued.job_id, running.job_id], reason: "用户批量取消", actor: "local:user", now: LATER });
	assert.deepEqual(result.cancelled.sort(), [queued.job_id, running.job_id].sort());
	assert.equal(tx.get("SELECT status FROM scheduler_jobs WHERE job_id = ?", [queued.job_id]).status, "cancelled");
	assert.equal(tx.get("SELECT status FROM scheduler_jobs WHERE job_id = ?", [running.job_id]).status, "cancelling");
	assert.equal(tx.get("SELECT allow_auto_retry FROM scheduler_jobs WHERE job_id = ?", [queued.job_id]).allow_auto_retry, 0);
	assert.equal(tx.get("SELECT retry_limit FROM tasks WHERE task_id = 'running'").retry_limit, 0);
	assert.deepEqual(result.actions.map((action) => action.job.job_id), [running.job_id]);
});

test("费用事件只接受宿主实际金额，并按外部编号幂等", () => {
	const db = freshDb();
	const tx = makeTx(db);
	seedTask(tx, "t1");
	assert.throws(() => recordCostEvent(tx, { taskId: "t1", amountMicros: 100, currency: "USD", source: "estimate", now: NOW }), (err) => err instanceof SchedulerError && err.code === "PAYLOAD_SCHEMA_INVALID");
	assert.throws(() => recordCostEvent(tx, { taskId: "t1", amountMicros: 100, currency: "USD", source: "host", basis: "tokens", now: NOW }), (err) => err.code === "PAYLOAD_SCHEMA_INVALID");
	const first = recordCostEvent(tx, { taskId: "t1", amountMicros: 250, currency: "USD", source: "host", externalId: "bill-1", now: NOW });
	const second = recordCostEvent(tx, { taskId: "t1", amountMicros: 250, currency: "USD", source: "host", externalId: "bill-1", now: NOW });
	assert.equal(first.inserted, true);
	assert.equal(second.inserted, false);
	assert.equal(tx.get("SELECT SUM(amount_micros) AS total FROM scheduler_cost_events").total, 250);
});

test("模型档位映射到 DSH 复杂度档位，不直接选择模型", () => {
	assert.equal(modelComplexityForClass("simple"), "routine");
	assert.equal(modelComplexityForClass("code"), "advanced");
	assert.equal(modelComplexityForClass("audit"), "expert");
	assert.equal(modelComplexityForClass("unknown", "standard"), "standard");
	assert.equal(modelComplexityForClass(null, "standard"), "standard");
});

test("调度设置范围校验与持久化", () => {
	const db = freshDb();
	const tx = makeTx(db);
	const initial = readSchedulerSettings(tx);
	assert.equal(initial.maxConcurrency, 1);
	assert.equal(initial.paused, false);
	assert.throws(() => updateSchedulerSettings(tx, { currency: "US", now: NOW }), (err) => err.code === "PAYLOAD_SCHEMA_INVALID");
	const next = updateSchedulerSettings(tx, { maxConcurrency: 999, paused: true, globalBudgetMicros: 5000, now: NOW });
	assert.equal(next.maxConcurrency, 128);
	assert.equal(next.paused, true);
	assert.equal(next.globalBudgetMicros, 5000);
	assert.equal(readSchedulerSettings(tx).maxConcurrency, 128);
});
