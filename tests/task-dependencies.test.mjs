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
	schedulerSnapshot,
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

function seedTask(tx, taskId, { lifecycle = "draft" } = {}) {
	tx.run(
		"INSERT INTO tasks (task_id, title, status, current_run_id, lifecycle_state, created_at, updated_at) VALUES (?, ?, 'reserved', NULL, ?, ?, ?)",
		[taskId, "任务 " + taskId, lifecycle, NOW, NOW]
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

test("依赖链：前置未通过时阻塞，前置通过后按序放行", () => {
	const db = freshDb();
	const tx = makeTx(db);
	updateSchedulerSettings(tx, { maxConcurrency: 3, now: NOW });
	seedTask(tx, "a");
	seedTask(tx, "b");
	seedTask(tx, "c");
	enqueue(tx, "b", { dependsOn: ["a"] });
	enqueue(tx, "c", { dependsOn: ["b"] });

	const blocked = claimSchedulerJobs(tx, { now: LATER });
	assert.equal(blocked.actions.filter((action) => action.type === "start").length, 0);
	assert.deepEqual(blocked.blocked.map((item) => [item.task_id, item.reason, item.depends_on]), [
		["b", "dependency_not_passed", ["a"]],
		["c", "dependency_not_passed", ["b"]]
	]);

	tx.run("UPDATE tasks SET lifecycle_state = 'passed' WHERE task_id = 'a'");
	const first = claimSchedulerJobs(tx, { now: LATER });
	assert.deepEqual(first.actions.filter((action) => action.type === "start").map((action) => action.job.task_id), ["b"]);
	assert.ok(first.blocked.some((item) => item.task_id === "c"));

	tx.run("UPDATE tasks SET lifecycle_state = 'passed' WHERE task_id = 'b'");
	const second = claimSchedulerJobs(tx, { now: LATER });
	assert.deepEqual(second.actions.filter((action) => action.type === "start").map((action) => action.job.task_id), ["c"]);
});

test("依赖/被依赖关系可从调度表反查", () => {
	const db = freshDb();
	const tx = makeTx(db);
	seedTask(tx, "parent");
	seedTask(tx, "child");
	enqueue(tx, "child", { dependsOn: ["parent"] });

	const dependencies = tx.all(
		"SELECT DISTINCT d.depends_on_task_id AS task_id FROM scheduler_job_dependencies d JOIN scheduler_jobs j ON j.job_id = d.job_id WHERE j.task_id = ? ORDER BY task_id",
		["child"]
	).map((row) => row.task_id);
	const dependents = tx.all(
		"SELECT DISTINCT j.task_id AS task_id FROM scheduler_job_dependencies d JOIN scheduler_jobs j ON j.job_id = d.job_id WHERE d.depends_on_task_id = ? ORDER BY task_id",
		["parent"]
	).map((row) => row.task_id);
	assert.deepEqual(dependencies, ["parent"]);
	assert.deepEqual(dependents, ["child"]);
});

test("自依赖与不存在的前置任务被拒绝", () => {
	const db = freshDb();
	const tx = makeTx(db);
	seedTask(tx, "a");
	assert.throws(
		() => enqueue(tx, "a", { dependsOn: ["a"] }),
		(error) => error instanceof SchedulerError && error.code === "PAYLOAD_SCHEMA_INVALID"
	);
	assert.throws(
		() => enqueue(tx, "a", { dependsOn: ["ghost"] }),
		(error) => error instanceof SchedulerError && error.code === "NOT_FOUND"
	);
});

test("环路依赖被拒绝，且不会留下半个调度项", () => {
	const db = freshDb();
	const tx = makeTx(db);
	seedTask(tx, "a");
	seedTask(tx, "b");
	seedTask(tx, "c");
	enqueue(tx, "a");
	enqueue(tx, "b", { dependsOn: ["a"] });
	enqueue(tx, "c", { dependsOn: ["b"] });
	// 模拟历史数据中 a 的调度项被清理但依赖边仍被保留，再尝试把 a 指回 c。
	tx.run("DELETE FROM scheduler_jobs WHERE task_id = 'a'");
	assert.throws(
		() => enqueue(tx, "a", { dependsOn: ["c"] }),
		(error) => error instanceof SchedulerError && error.code === "PAYLOAD_SCHEMA_INVALID" && /环路/.test(error.message)
	);
	assert.equal(tx.get("SELECT COUNT(*) AS n FROM scheduler_jobs WHERE task_id = 'a'").n, 0);
});

test("同一任务重复入队被拒绝，重复调度不会启动两次", () => {
	const db = freshDb();
	const tx = makeTx(db);
	updateSchedulerSettings(tx, { maxConcurrency: 2, now: NOW });
	seedTask(tx, "a");
	const first = enqueue(tx, "a");
	assert.equal(first.status, "queued");
	assert.throws(
		() => enqueue(tx, "a"),
		(error) => error instanceof SchedulerError && error.code === "STALE_GENERATION" && /重复派发/.test(error.message)
	);
	assert.equal(tx.get("SELECT COUNT(*) AS n FROM scheduler_jobs WHERE task_id = 'a'").n, 1);

	const initial = claimSchedulerJobs(tx, { now: LATER });
	assert.equal(initial.actions.filter((action) => action.type === "start").length, 1);
	const replay = claimSchedulerJobs(tx, { now: LATER });
	assert.equal(replay.actions.filter((action) => action.type === "start").length, 0);
});

test("取消排队任务后不再派发，并锁定自动重试额度", () => {
	const db = freshDb();
	const tx = makeTx(db);
	updateSchedulerSettings(tx, { maxConcurrency: 2, now: NOW });
	seedTask(tx, "keep");
	seedTask(tx, "drop");
	const keep = enqueue(tx, "keep");
	const drop = enqueue(tx, "drop", { retryLimit: 3 });
	const result = cancelJobs(tx, { taskIds: ["drop"], reason: "用户取消", actor: "human:test", now: LATER });
	assert.deepEqual(result.cancelled, [drop.job_id]);
	assert.deepEqual(result.actions, []);
	assert.equal(tx.get("SELECT status FROM scheduler_jobs WHERE job_id = ?", [drop.job_id]).status, "cancelled");
	assert.equal(tx.get("SELECT retry_limit FROM tasks WHERE task_id = 'drop'").retry_limit, 0);

	const claim = claimSchedulerJobs(tx, { now: LATER });
	const started = claim.actions.filter((action) => action.type === "start").map((action) => action.job.job_id);
	assert.deepEqual(started, [keep.job_id]);
	const snapshot = schedulerSnapshot(tx, { now: LATER });
	assert.equal(snapshot.jobs.find((job) => job.job_id === drop.job_id).status, "cancelled");
});

test("前端监控索引随迁移创建", () => {
	const db = freshDb();
	const names = new Set(db.prepare("SELECT name FROM sqlite_master WHERE type = 'index'").all().map((row) => row.name));
	assert.ok(names.has("node_run_events_model_used_idx"));
	assert.ok(names.has("tasks_active_lifecycle_idx"));
});
