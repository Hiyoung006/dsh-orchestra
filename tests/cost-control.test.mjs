import test from "node:test";
import assert from "node:assert/strict";
import { DatabaseSync } from "node:sqlite";
import { readdirSync, readFileSync } from "node:fs";
import { join, dirname } from "node:path";
import { fileURLToPath } from "node:url";
import {
	claimSchedulerJobs,
	enqueueJob,
	getReservation,
	markJobFailed,
	markJobRunning,
	readSchedulerSettings,
	recordCostEvent,
	schedulerSnapshot,
	updateSchedulerSettings
} from "../lib/task-scheduler.js";

const MIGRATIONS = join(dirname(fileURLToPath(import.meta.url)), "..", "lib", "infra", "sqlite", "migrations");
const NOW = "2026-01-01T00:00:00.000Z";

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

function seedTask(tx, taskId) {
	tx.run(
		"INSERT INTO tasks (task_id, title, status, current_run_id, lifecycle_state, created_at, updated_at) VALUES (?, ?, 'reserved', NULL, 'draft', ?, ?)",
		[taskId, "任务 " + taskId, NOW, NOW]
	);
}

function enqueue(tx, taskId, overrides = {}) {
	return enqueueJob(tx, {
		taskId,
		workflowId: "wf_cost",
		workflowVersion: "1",
		nodeIds: ["n1"],
		now: NOW,
		...overrides
	});
}

function settings(tx, overrides = {}) {
	return updateSchedulerSettings(tx, { now: NOW, actor: "test", ...overrides });
}

function reservations(tx) {
	return tx.all("SELECT * FROM scheduler_budget_reservations ORDER BY created_at ASC, job_id ASC");
}

function tasksOfCount(tx, count) {
	for (let index = 0; index < count; index += 1) {
		const taskId = `t${index + 1}`;
		seedTask(tx, taskId);
		enqueue(tx, taskId);
	}
}

test("派发前按剩余槽位分摊预留，预留总额不超过全局预算", () => {
	const db = freshDb();
	const tx = makeTx(db);
	settings(tx, { globalBudgetMicros: 1000, maxConcurrency: 2 });
	tasksOfCount(tx, 3);
	const claim = claimSchedulerJobs(tx, { now: NOW });
	assert.equal(claim.selected.length, 2);
	const rows = reservations(tx);
	assert.equal(rows.length, 2);
	assert.equal(rows.reduce((sum, row) => sum + Number(row.amount_micros), 0), 1000);
	assert.equal(rows.every((row) => row.status === "held"), true);
	assert.equal(claim.stats.global_reserved_micros, 1000);
	assert.equal(claim.stats.global_committed_micros, 1000);
	assert.equal(claim.stats.in_flight_micros, 1000);

	settings(tx, { maxConcurrency: 3 });
	const blocked = claimSchedulerJobs(tx, { now: NOW });
	assert.equal(blocked.selected.length, 0);
	assert.equal(blocked.stats.budget_exhausted, true);
	assert.equal(tx.get("SELECT status FROM scheduler_jobs WHERE task_id = 't3'").status, "queued");
});

test("真实费用到达后消耗预留，实际与在途不再重复计算", () => {
	const db = freshDb();
	const tx = makeTx(db);
	settings(tx, { globalBudgetMicros: 1000, maxConcurrency: 2 });
	tasksOfCount(tx, 2);
	const claim = claimSchedulerJobs(tx, { now: NOW });
	const job = claim.selected[0];
	const before = getReservation(tx, job.job_id);
	assert.equal(before.amount_micros, 500);
	const event = recordCostEvent(tx, {
		taskId: job.task_id,
		jobId: job.job_id,
		amountMicros: 300,
		currency: "USD",
		source: "host",
		externalId: "bill-1",
		now: NOW
	});
	assert.equal(event.inserted, true);
	assert.equal(event.reservation.remaining_micros, 200);
	assert.equal(event.reservation.status, "held");

	const after = getReservation(tx, job.job_id);
	assert.equal(after.settled_micros, 300);
	assert.equal(after.remaining_micros, 200);
	const snapshot = schedulerSnapshot(tx, { now: NOW });
	assert.equal(snapshot.global_spent_micros, 300);
	assert.equal(snapshot.global_reserved_micros, 700);
	assert.equal(snapshot.global_committed_micros, 1000);
	assert.equal(snapshot.budgets.global.actual_micros, 300);
	assert.equal(snapshot.budgets.global.reserved_micros, 700);
	assert.equal(snapshot.budgets.global.state, "exhausted");
});

test("预留完全消耗后标记为 settled，任务终态释放剩余预留", () => {
	const db = freshDb();
	const tx = makeTx(db);
	settings(tx, { globalBudgetMicros: 1000, maxConcurrency: 1 });
	seedTask(tx, "t1");
	const job = enqueue(tx, "t1");
	const claim = claimSchedulerJobs(tx, { now: NOW });
	assert.equal(claim.selected.length, 1);
	recordCostEvent(tx, {
		taskId: "t1",
		jobId: job.job_id,
		amountMicros: 1000,
		source: "host",
		externalId: "bill-full",
		now: NOW
	});
	assert.equal(getReservation(tx, job.job_id).status, "settled");

	const db2 = freshDb();
	const tx2 = makeTx(db2);
	settings(tx2, { globalBudgetMicros: 1000, maxConcurrency: 1 });
	seedTask(tx2, "t1");
	const job2 = enqueue(tx2, "t1");
	claimSchedulerJobs(tx2, { now: NOW });
	tx2.run("INSERT INTO runs (run_id, task_id, status, created_at, updated_at) VALUES ('r1', 't1', 'succeeded', ?, ?)", [NOW, NOW]);
	markJobRunning(tx2, { jobId: job2.job_id, runId: "r1", now: NOW });
	claimSchedulerJobs(tx2, { now: NOW });
	assert.equal(tx2.get("SELECT status FROM scheduler_jobs WHERE job_id = ?", [job2.job_id]).status, "succeeded");
	const released = getReservation(tx2, job2.job_id);
	assert.equal(released.status, "released");
	assert.equal(tx2.get("SELECT COALESCE(SUM(amount_micros - settled_micros), 0) AS n FROM scheduler_budget_reservations WHERE status = 'held'").n, 0);
});

test("单任务预算同时约束实际费用与预留", () => {
	const db = freshDb();
	const tx = makeTx(db);
	settings(tx, { globalBudgetMicros: null, maxConcurrency: 2 });
	seedTask(tx, "t1");
	recordCostEvent(tx, { taskId: "t1", amountMicros: 500, source: "host", externalId: "bill-used", now: NOW });
	enqueue(tx, "t1", { budgetMicros: 500 });
	const claim = claimSchedulerJobs(tx, { now: NOW });
	assert.equal(claim.selected.length, 0);
	assert.equal(reservations(tx).length, 0);
	assert.equal(tx.get("SELECT status FROM scheduler_jobs WHERE task_id = 't1'").status, "budget_blocked");
	assert.equal(tx.get("SELECT severity FROM scheduler_notifications WHERE kind = 'budget_task'").severity, "critical");

	const db2 = freshDb();
	const tx2 = makeTx(db2);
	settings(tx2, { globalBudgetMicros: null, maxConcurrency: 1 });
	seedTask(tx2, "t1");
	enqueue(tx2, "t1", { budgetMicros: 500 });
	assert.equal(claimSchedulerJobs(tx2, { now: NOW }).selected.length, 1);
	const held = reservations(tx2);
	assert.equal(held.length, 1);
	assert.equal(Number(held[0].amount_micros), 500);
});

test("达到告警阈值产生 warning，达到上限产生 critical", () => {
	const db = freshDb();
	const tx = makeTx(db);
	settings(tx, { globalBudgetMicros: 1000, budgetWarnPercent: 80, maxConcurrency: 1 });
	recordCostEvent(tx, { taskId: "t0", amountMicros: 850, source: "host", externalId: "bill-850", now: NOW });
	seedTask(tx, "t1");
	enqueue(tx, "t1");
	const warning = claimSchedulerJobs(tx, { now: NOW });
	assert.equal(warning.selected.length, 1);
	assert.ok(warning.notifications.includes("budget_global_warning"));
	const notified = tx.get("SELECT * FROM scheduler_notifications WHERE kind = 'budget_global_warning'");
	assert.equal(notified.severity, "warning");
	assert.equal(JSON.parse(notified.metadata_json).actual_micros, 850);

	recordCostEvent(tx, { taskId: "t0", amountMicros: 150, source: "host", externalId: "bill-1000", now: NOW });
	const exhausted = claimSchedulerJobs(tx, { now: NOW });
	assert.equal(exhausted.stats.budget_exhausted, true);
	assert.equal(tx.get("SELECT severity FROM scheduler_notifications WHERE kind = 'budget_global'").severity, "critical");
});

test("cancel_running 策略在预算用尽时中断运行中的任务", () => {
	const db = freshDb();
	const tx = makeTx(db);
	settings(tx, { globalBudgetMicros: 1000, maxConcurrency: 1, budgetInterruptPolicy: "cancel_running" });
	seedTask(tx, "t1");
	const job = enqueue(tx, "t1");
	claimSchedulerJobs(tx, { now: NOW });
	tx.run("INSERT INTO runs (run_id, task_id, status, created_at, updated_at) VALUES ('r1', 't1', 'running', ?, ?)", [NOW, NOW]);
	markJobRunning(tx, { jobId: job.job_id, runId: "r1", now: NOW });
	recordCostEvent(tx, { taskId: "t1", jobId: job.job_id, amountMicros: 1000, source: "host", externalId: "bill-over", now: NOW });
	const claim = claimSchedulerJobs(tx, { now: NOW });
	const cancel = claim.actions.find((action) => action.type === "cancel");
	assert.ok(cancel);
	assert.equal(cancel.reason, "budget_global_exhausted");
	assert.equal(cancel.job.status, "cancelling");
	assert.ok(claim.notifications.includes("budget_interrupt"));
	assert.equal(tx.get("SELECT severity FROM scheduler_notifications WHERE kind = 'budget_interrupt'").severity, "critical");
	assert.equal(tx.get("SELECT retry_limit = retry_count AS off FROM tasks WHERE task_id = 't1'").off, 1);
});

test("block_new 策略只拦截新派发，不中断运行中任务", () => {
	const db = freshDb();
	const tx = makeTx(db);
	settings(tx, { globalBudgetMicros: 1000, maxConcurrency: 1, budgetInterruptPolicy: "block_new" });
	seedTask(tx, "t1");
	const job = enqueue(tx, "t1");
	claimSchedulerJobs(tx, { now: NOW });
	tx.run("INSERT INTO runs (run_id, task_id, status, created_at, updated_at) VALUES ('r1', 't1', 'running', ?, ?)", [NOW, NOW]);
	markJobRunning(tx, { jobId: job.job_id, runId: "r1", now: NOW });
	recordCostEvent(tx, { taskId: "t1", jobId: job.job_id, amountMicros: 1000, source: "host", externalId: "bill-over", now: NOW });
	const claim = claimSchedulerJobs(tx, { now: NOW });
	assert.equal(claim.actions.some((action) => action.type === "cancel"), false);
	assert.equal(tx.get("SELECT status FROM scheduler_jobs WHERE job_id = ?", [job.job_id]).status, "running");
});

test("没有配置预算时不做预留，避免虚构在途金额", () => {
	const db = freshDb();
	const tx = makeTx(db);
	settings(tx, { globalBudgetMicros: null, maxConcurrency: 1 });
	seedTask(tx, "t1");
	enqueue(tx, "t1");
	const claim = claimSchedulerJobs(tx, { now: NOW });
	assert.equal(claim.selected.length, 1);
	assert.equal(reservations(tx).length, 0);
	assert.equal(claim.stats.global_reserved_micros, 0);
	assert.equal(claim.stats.in_flight_micros, 0);
});

test("拒绝 Token 估算、跨任务费用与非法设置", () => {
	const db = freshDb();
	const tx = makeTx(db);
	seedTask(tx, "t1");
	seedTask(tx, "t2");
	const job = enqueue(tx, "t1");
	assert.throws(() => recordCostEvent(tx, { taskId: "t1", amountMicros: 1, source: "token_estimate", now: NOW }), (error) => error?.code === "PAYLOAD_SCHEMA_INVALID");
	assert.throws(() => recordCostEvent(tx, { taskId: "t1", amountMicros: 1, basis: "estimated", source: "host", now: NOW }), (error) => error?.code === "PAYLOAD_SCHEMA_INVALID");
	assert.throws(() => recordCostEvent(tx, { taskId: "t2", jobId: job.job_id, amountMicros: 1, source: "host", now: NOW }), (error) => error?.code === "PAYLOAD_SCHEMA_INVALID");
	assert.throws(() => recordCostEvent(tx, { taskId: "t1", jobId: "missing", amountMicros: 1, source: "host", now: NOW }), (error) => error?.code === "NOT_FOUND");
	assert.equal(settings(tx, { budgetWarnPercent: 0 }).budgetWarnPercent, 1);
	assert.equal(settings(tx, { budgetWarnPercent: 101 }).budgetWarnPercent, 100);
	assert.throws(() => settings(tx, { budgetInterruptPolicy: "kill_all" }), (error) => error?.code === "PAYLOAD_SCHEMA_INVALID");
	const defaults = readSchedulerSettings(makeTx(freshDb()));
	assert.equal(defaults.budgetWarnPercent, 80);
	assert.equal(defaults.budgetInterruptPolicy, "block_new");
});

test("派发失败或取消时释放预留", () => {
	const db = freshDb();
	const tx = makeTx(db);
	settings(tx, { globalBudgetMicros: 1000, maxConcurrency: 2 });
	tasksOfCount(tx, 2);
	const claim = claimSchedulerJobs(tx, { now: NOW });
	const failed = claim.selected[0];
	markJobFailed(tx, { jobId: failed.job_id, error: "boom", now: NOW });
	assert.equal(getReservation(tx, failed.job_id).status, "released");
	assert.equal(claimSchedulerJobs(tx, { now: NOW }).stats.global_reserved_micros, 500);
});
