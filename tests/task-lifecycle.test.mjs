import test from "node:test";
import assert from "node:assert/strict";
import { DatabaseSync } from "node:sqlite";
import { readdirSync, readFileSync } from "node:fs";
import { join, dirname } from "node:path";
import { fileURLToPath } from "node:url";
import {
	TASK_STATUSES,
	TASK_TERMINAL_STATUSES,
	TaskLifecycleError,
	applyTaskLifecycle,
	assertAttemptOwnership,
	assertTaskEventOwnership,
	assertTaskTransition,
	attemptIdFor,
	canRetryTask,
	classifyStartupTask,
	dueRetryTasks,
	isAttemptCurrent,
	listTaskAttempts,
	loadTaskRecoverySnapshot,
	planTaskRetry,
	projectTaskLifecycle,
	recoverTaskLifecycle,
	retryBackoff,
	taskTransitionAllowed,
	taskTransitionGraph
} from "../lib/task-lifecycle.js";

const MIGRATIONS = join(dirname(fileURLToPath(import.meta.url)), "..", "lib", "infra", "sqlite", "migrations");
const NOW = "2026-01-01T00:00:00.000Z";
const ZERO = "0".repeat(64);

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

function seed(db, { taskId = "t1", runId = "r1", nodeRunId = "n1", epoch = 7, lifecycle = "draft", nodeStatus = "queued", attempt = 1 } = {}) {
	const tx = makeTx(db);
	tx.run("INSERT INTO tasks (task_id, title, status, current_run_id, lifecycle_state, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?)", [taskId, "任务", "running", runId, lifecycle, NOW, NOW]);
	tx.run("INSERT INTO runs (run_id, task_id, status, created_at, updated_at, root_run_id, subworkflow_depth) VALUES (?, ?, ?, ?, ?, ?, ?)", [runId, taskId, "running", NOW, NOW, runId, 0]);
	tx.run("INSERT INTO node_runs (node_run_id, run_id, node_id, seq, node_type, attempt, status, revision, created_at, updated_at, owner_epoch) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", [nodeRunId, runId, "a", 1, "agent", attempt, nodeStatus, 0, NOW, NOW, epoch]);
	return tx;
}

function setTask(db, taskId, fields) {
	const keys = Object.keys(fields);
	const sql = `UPDATE tasks SET ${keys.map((key) => `${key} = ?`).join(", ")} WHERE task_id = ?`;
	db.prepare(sql).run(...keys.map((key) => fields[key]), taskId);
}

// ---------------------------------------------------------------------------
// 状态机
// ---------------------------------------------------------------------------

test("13 个任务状态齐全，且状态转换图完整", () => {
	assert.deepEqual([...TASK_STATUSES], ["draft", "blocked", "queued", "dispatching", "running", "review", "retrying", "passed", "rejected", "failed", "timeout", "cancelled", "interrupted"]);
	assert.deepEqual([...TASK_TERMINAL_STATUSES].sort(), ["cancelled", "failed", "interrupted", "passed", "rejected", "timeout"].sort());
	const graph = taskTransitionGraph();
	assert.deepEqual(Object.keys(graph).sort(), [...TASK_STATUSES].sort());
	assert.ok(graph.queued.includes("dispatching"));
	assert.ok(graph.dispatching.includes("running"));
	assert.ok(graph.running.includes("review"));
	assert.ok(graph.review.includes("passed"));
});

test("running 永远不会回到 draft", () => {
	assert.equal(taskTransitionAllowed("running", "draft").allowed, false);
	assert.equal(taskTransitionAllowed("dispatching", "draft").allowed, false);
	assert.equal(taskTransitionAllowed("queued", "draft").allowed, false);
	assert.throws(() => assertTaskTransition("running", "draft"), /不允许/);
});

test("正常链路与异常链路都经过校验", () => {
	for (const [from, to] of [
		["draft", "queued"],
		["queued", "dispatching"],
		["dispatching", "running"],
		["running", "review"],
		["review", "passed"],
		["running", "failed"],
		["failed", "retrying"],
		["retrying", "queued"],
		["blocked", "running"],
		["review", "running"]
	]) {
		assert.equal(taskTransitionAllowed(from, to).allowed, true, `${from} -> ${to}`);
	}
	assert.equal(taskTransitionAllowed("passed", "running").allowed, false);
	assert.equal(taskTransitionAllowed("review", "draft").allowed, false);
	assert.equal(taskTransitionAllowed("passed", "retrying").allowed, false);
	assert.equal(taskTransitionAllowed("passed", "retrying", { force: true, actorKind: "system" }).allowed, false);
	assert.equal(taskTransitionAllowed("passed", "retrying", { force: true, actorKind: "user" }).allowed, true);
	assert.equal(taskTransitionAllowed("rejected", "retrying").allowed, false);
	assert.equal(taskTransitionAllowed("rejected", "retrying", { explicit: true }).allowed, true);
});

test("重试上限与显式重跑规则", () => {
	assert.equal(canRetryTask({ lifecycle: "failed", retryCount: 0, retryLimit: 3, mode: "auto" }).allowed, true);
	assert.equal(canRetryTask({ lifecycle: "failed", retryCount: 3, retryLimit: 3, mode: "auto" }).allowed, false);
	assert.equal(canRetryTask({ lifecycle: "failed", retryCount: 3, retryLimit: 3, mode: "manual" }).allowed, true);
	assert.equal(canRetryTask({ lifecycle: "passed", retryCount: 0, retryLimit: 3, mode: "manual" }).allowed, false);
	assert.equal(canRetryTask({ lifecycle: "passed", retryCount: 0, retryLimit: 3, mode: "manual", force: true, actorKind: "user" }).allowed, true);
});

test("退避按指数增长且有上限", () => {
	const first = retryBackoff({ retryCount: 1, baseDelayMs: 1000, maxDelayMs: 60000, jitter: 0, now: NOW });
	const second = retryBackoff({ retryCount: 2, baseDelayMs: 1000, maxDelayMs: 60000, jitter: 0, now: NOW });
	const tenth = retryBackoff({ retryCount: 10, baseDelayMs: 1000, maxDelayMs: 60000, jitter: 0, now: NOW });
	assert.equal(first.delayMs, 1000);
	assert.equal(second.delayMs, 2000);
	assert.equal(tenth.delayMs, 60000);
	assert.ok(Date.parse(first.nextRetryAt) > Date.parse(NOW));
	const jittered = retryBackoff({ retryCount: 1, baseDelayMs: 1000, maxDelayMs: 60000, jitter: 0.5, random: () => 1, now: NOW });
	assert.equal(jittered.delayMs, 1500);
});

test("attemptId 归属校验拒绝旧执行尝试", () => {
	assert.equal(attemptIdFor("n1", 2), "n1:2");
	assert.equal(isAttemptCurrent("n1:2", "n1:2"), true);
	assert.equal(isAttemptCurrent("n1:2", "n1:1"), false);
	assert.throws(() => assertAttemptOwnership("n1:2", "n1:1"), (error) => error instanceof TaskLifecycleError && error.code === "STALE_GENERATION");
});

// ---------------------------------------------------------------------------
// 启动恢复分类
// ---------------------------------------------------------------------------

const baseSnapshot = {
	lifecycle: "running",
	run: { run_id: "r1", status: "running" },
	nodes: [{ node_run_id: "n1", status: "started", owner_epoch: 7, heartbeat_at: NOW, executor_agent_id: "a1" }],
	decisions: [],
	acceptedReport: false,
	now: "2026-01-01T00:01:00.000Z",
	currentEpoch: 7
};

test("运行中的子会话心跳新鲜时保持 running", () => {
	const out = classifyStartupTask({ ...baseSnapshot, sessionFacts: { n1: { exists: true, running: true, turnEnded: false } } });
	assert.equal(out.state, "running");
});

test("子会话不存在时恢复为 interrupted 而不是 draft", () => {
	const out = classifyStartupTask({ ...baseSnapshot, sessionFacts: { n1: { exists: false, running: false, turnEnded: false } } });
	assert.equal(out.state, "interrupted");
	assert.equal(out.reason, "session_missing");
});

test("owner_epoch 变化说明进程已重启，判定为中断", () => {
	const out = classifyStartupTask({ ...baseSnapshot, nodes: [{ ...baseSnapshot.nodes[0], owner_epoch: 6 }], sessionFacts: { n1: { exists: false, running: false, turnEnded: false } } });
	assert.equal(out.state, "interrupted");
	assert.equal(out.reason, "owner_epoch_changed");
});

test("心跳过期的运行任务判定为中断", () => {
	const out = classifyStartupTask({
		...baseSnapshot,
		nodes: [{ ...baseSnapshot.nodes[0], heartbeat_at: "2026-01-01T00:00:00.000Z" }],
		now: "2026-01-01T00:10:00.000Z"
	});
	assert.equal(out.state, "interrupted");
	assert.equal(out.reason, "heartbeat_stale");
});

test("会话已结束且存在有效结果时进入 review", () => {
	const out = classifyStartupTask({
		...baseSnapshot,
		acceptedReport: true,
		sessionFacts: { n1: { exists: true, running: false, turnEnded: true } }
	});
	assert.equal(out.state, "review");
	assert.equal(out.reason, "result_pending_review");
});

test("会话结束但没有有效结果时判定为中断", () => {
	const out = classifyStartupTask({
		...baseSnapshot,
		sessionFacts: { n1: { exists: true, running: false, turnEnded: true } }
	});
	assert.equal(out.state, "interrupted");
	assert.equal(out.reason, "turn_ended_without_valid_result");
});

test("审批未决与终端 run 的恢复结果", () => {
	assert.equal(classifyStartupTask({ ...baseSnapshot, decisions: [{ kind: "pre_execution", status: "pending" }] }).state, "blocked");
	assert.equal(classifyStartupTask({ ...baseSnapshot, decisions: [{ kind: "post_execution", status: "pending" }] }).state, "review");
	assert.equal(classifyStartupTask({ ...baseSnapshot, run: { run_id: "r1", status: "succeeded" } }).state, "passed");
	assert.equal(classifyStartupTask({ ...baseSnapshot, run: { run_id: "r1", status: "failed" } }).state, "failed");
	assert.equal(classifyStartupTask({ ...baseSnapshot, run: { run_id: "r1", status: "failed" }, nodes: [{ node_run_id: "n1", status: "timeout" }] }).state, "timeout");
	assert.equal(classifyStartupTask({ ...baseSnapshot, run: { run_id: "r1", status: "orphaned" } }).state, "interrupted");
});

test("没有 run 的活动任务不会退回 draft", () => {
	const out = classifyStartupTask({ ...baseSnapshot, run: null });
	assert.equal(out.state, "interrupted");
	assert.notEqual(out.state, "draft");
});

test("待自动重试的任务不会被终端 run 或扫描覆盖", () => {
	const snapshot = {
		lifecycle: "retrying",
		task: { next_retry_at: NOW },
		run: { run_id: "r1", status: "failed" },
		nodes: [{ node_run_id: "n1", status: "failed" }],
		decisions: [],
		now: NOW,
		currentEpoch: 7
	};
	assert.equal(projectTaskLifecycle(snapshot).state, "retrying");
	assert.equal(classifyStartupTask(snapshot).state, "retrying");
});

test("在线投影按节点状态推导 queued/dispatching/running/review", () => {
	assert.equal(projectTaskLifecycle({ lifecycle: "draft", run: { status: "running" }, nodes: [{ status: "queued" }] }).state, "queued");
	assert.equal(projectTaskLifecycle({ lifecycle: "queued", run: { status: "running" }, nodes: [{ status: "leased" }] }).state, "dispatching");
	assert.equal(projectTaskLifecycle({ lifecycle: "dispatching", run: { status: "running" }, nodes: [{ status: "started" }] }).state, "running");
	assert.equal(projectTaskLifecycle({ lifecycle: "running", run: { status: "running" }, nodes: [{ status: "done" }] }).state, "review");
	assert.equal(projectTaskLifecycle({ lifecycle: "running", run: { status: "succeeded" }, nodes: [{ status: "done" }] }).state, "passed");
});

// ---------------------------------------------------------------------------
// 集成：迁移、attempt 触发器、生命周期迁移与恢复
// ---------------------------------------------------------------------------

test("派发/开始/结束自动写入 attempt 历史（含模型信息与尝试归属）", () => {
	const db = freshDb();
	const tx = seed(db);
	const inserted = tx.get("SELECT attempt_id, task_id, run_id, node_run_id, attempt_no, status, retry_count FROM task_attempts");
	assert.equal(inserted.attempt_id, "n1:1");
	assert.equal(inserted.task_id, "t1");
	assert.equal(inserted.run_id, "r1");
	assert.equal(inserted.status, "queued");
	assert.equal(inserted.retry_count, 0);
	assert.equal(tx.get("SELECT last_attempt_id FROM tasks").last_attempt_id, "n1:1");

	tx.run("INSERT INTO node_run_events (node_run_id, seq, type, prev_hash, hash, payload, created_at) VALUES (?, ?, ?, ?, ?, ?, ?)", ["n1", 1, "executor.model_used", ZERO, "h1", JSON.stringify({ attempt: 1, model_ref: "m1", usage: { provider: "p", model: "mm" }, reasoning_effort: "high" }), NOW]);
	const model = tx.get("SELECT model_ref, model_provider, model_name, reasoning_effort FROM task_attempts WHERE attempt_id = 'n1:1'");
	assert.deepEqual({ ...model }, { model_ref: "m1", model_provider: "p", model_name: "mm", reasoning_effort: "high" });

	tx.run("UPDATE node_runs SET status = ?, started_at = ?, updated_at = ? WHERE node_run_id = ?", ["started", "2026-01-01T00:00:01.000Z", "2026-01-01T00:00:01.000Z", "n1"]);
	const running = tx.get("SELECT status, dispatched_at FROM task_attempts WHERE attempt_id = 'n1:1'");
	assert.equal(running.status, "running");
	assert.equal(running.dispatched_at, "2026-01-01T00:00:01.000Z");
	tx.run("UPDATE node_runs SET status = ?, updated_at = ? WHERE node_run_id = ?", ["done", "2026-01-01T00:00:02.000Z", "n1"]);
	const done = tx.get("SELECT status, outcome, completed_at FROM task_attempts WHERE attempt_id = 'n1:1'");
	assert.equal(done.status, "passed");
	assert.equal(done.outcome, "passed");
	assert.equal(done.completed_at, "2026-01-01T00:00:02.000Z");
	assert.deepEqual(listTaskAttempts(tx, "t1").map((row) => row.attempt_id), ["n1:1"]);
});

test("生命周期迁移写审计事件，且非法迁移被拒绝", () => {
	const db = freshDb();
	const tx = seed(db);
	applyTaskLifecycle(tx, { taskId: "t1", next: "queued", now: NOW, reason: "admit", actorKind: "system" });
	applyTaskLifecycle(tx, { taskId: "t1", next: "dispatching", now: NOW, reason: "dispatch", actorKind: "system" });
	applyTaskLifecycle(tx, { taskId: "t1", next: "running", now: NOW, reason: "started", actorKind: "system" });
	assert.equal(tx.get("SELECT lifecycle_state FROM tasks WHERE task_id = 't1'").lifecycle_state, "running");
	assert.equal(tx.all("SELECT from_state, to_state FROM task_lifecycle_events WHERE task_id = 't1' ORDER BY id").length, 3);
	assert.throws(
		() => applyTaskLifecycle(tx, { taskId: "t1", next: "draft", now: NOW, reason: "illegal", actorKind: "system" }),
		(error) => error instanceof TaskLifecycleError && error.code === "STALE_GENERATION"
	);
	assert.equal(tx.get("SELECT lifecycle_state FROM tasks WHERE task_id = 't1'").lifecycle_state, "running");
});

test("启动恢复把上一个进程的运行任务标为 interrupted，并保留 attempt", () => {
	const db = freshDb();
	const tx = seed(db, { lifecycle: "running", nodeStatus: "started", epoch: 6 });
	tx.run("UPDATE node_runs SET heartbeat_at = ? WHERE node_run_id = ?", [NOW, "n1"]);
	const before = tx.get("SELECT attempt_id, status FROM task_attempts");
	const result = recoverTaskLifecycle(tx, "t1", { now: "2026-01-01T00:05:00.000Z", currentEpoch: 7 });
	assert.equal(result.changed, true);
	assert.equal(result.state, "interrupted");
	const task = tx.get("SELECT lifecycle_state, last_attempt_id FROM tasks WHERE task_id = 't1'");
	assert.equal(task.lifecycle_state, "interrupted");
	assert.equal(task.last_attempt_id, before.attempt_id);
	assert.notEqual(task.lifecycle_state, "draft");
	assert.equal(tx.all("SELECT * FROM task_lifecycle_events WHERE task_id = 't1'").length, 1);
});

test("旧执行批次的尝试不能改写当前任务状态", () => {
	const db = freshDb();
	const tx = seed(db);
	tx.run("INSERT INTO runs (run_id, task_id, status, created_at, updated_at, root_run_id, subworkflow_depth) VALUES (?, ?, ?, ?, ?, ?, ?)", ["r0", "t1", "failed", NOW, NOW, "r0", 0]);
	assert.throws(
		() => assertTaskEventOwnership(tx, { taskId: "t1", attemptId: "n9:1", runId: "r0" }),
		(error) => error instanceof TaskLifecycleError && error.code === "STALE_GENERATION"
	);
	assert.doesNotThrow(() => assertTaskEventOwnership(tx, { taskId: "t1", attemptId: "n1:1", runId: "r1" }));
});

test("重试进入 retrying、按退避到期，并受上限/已通过保护", () => {
	const db = freshDb();
	const tx = seed(db, { lifecycle: "failed", nodeStatus: "failed" });
	setTask(db, "t1", { retry_limit: 3, retry_count: 0 });
	const first = planTaskRetry(tx, { taskId: "t1", mode: "auto", now: NOW, actorKind: "system", random: () => 0 });
	assert.equal(first.retryCount, 1);
	assert.equal(first.delayMs, 1000);
	assert.equal(tx.get("SELECT lifecycle_state FROM tasks WHERE task_id = 't1'").lifecycle_state, "retrying");
	assert.equal(dueRetryTasks(tx, NOW).length, 0);
	assert.equal(dueRetryTasks(tx, "2026-01-01T00:00:01.000Z").length, 1);

	setTask(db, "t1", { lifecycle_state: "failed", retry_count: 3 });
	assert.throws(
		() => planTaskRetry(tx, { taskId: "t1", mode: "auto", now: NOW, actorKind: "system" }),
		(error) => error instanceof TaskLifecycleError && /不允许重试/.test(error.message)
	);
	const manualAtLimit = planTaskRetry(tx, { taskId: "t1", mode: "manual", now: NOW, actorKind: "user", random: () => 0 });
	assert.equal(manualAtLimit.retryCount, 4);
	setTask(db, "t1", { lifecycle_state: "passed", retry_count: 0 });
	assert.throws(
		() => planTaskRetry(tx, { taskId: "t1", mode: "manual", now: NOW, actorKind: "user" }),
		(error) => error instanceof TaskLifecycleError && /不允许重试/.test(error.message)
	);
	const forced = planTaskRetry(tx, { taskId: "t1", mode: "manual", now: NOW, actorKind: "user", force: true, random: () => 0 });
	assert.equal(forced.retryCount, 1);
	assert.equal(tx.get("SELECT lifecycle_state FROM tasks WHERE task_id = 't1'").lifecycle_state, "retrying");
});

test("旧库升级：069 迁移按既有事实回填生命周期与 attempt 历史", () => {
	const db = new DatabaseSync(":memory:");
	const files = readdirSync(MIGRATIONS).filter((name) => name.endsWith(".sql")).sort();
	for (const file of files.filter((name) => name < "069")) db.exec(readFileSync(join(MIGRATIONS, file), "utf8"));
	const tx = makeTx(db);
	tx.run("INSERT INTO tasks (task_id, title, status, current_run_id, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?)", ["legacy", "旧任务", "running", "r1", NOW, NOW]);
	tx.run("INSERT INTO runs (run_id, task_id, status, created_at, updated_at, root_run_id, subworkflow_depth) VALUES (?, ?, ?, ?, ?, ?, ?)", ["r1", "legacy", "succeeded", NOW, "2026-01-01T00:00:02.000Z", "r1", 0]);
	tx.run("INSERT INTO node_runs (node_run_id, run_id, node_id, seq, node_type, attempt, status, revision, created_at, updated_at, owner_epoch) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", ["n1", "r1", "a", 1, "agent", 1, "done", 0, NOW, "2026-01-01T00:00:02.000Z", 7]);
	db.exec(readFileSync(join(MIGRATIONS, "069_task_lifecycle.sql"), "utf8"));
	const task = tx.get("SELECT lifecycle_state, retry_limit FROM tasks WHERE task_id = 'legacy'");
	assert.equal(task.lifecycle_state, "passed");
	assert.equal(task.retry_limit, 0);
	const attempt = tx.get("SELECT attempt_id, status, outcome, completed_at FROM task_attempts WHERE task_id = 'legacy'");
	assert.equal(attempt.attempt_id, "n1:1");
	assert.equal(attempt.status, "passed");
	assert.equal(attempt.outcome, "passed");
	assert.equal(attempt.completed_at, "2026-01-01T00:00:02.000Z");
});

test("在线投影会在扫描中推进 queued -> running -> review -> passed", () => {
	const db = freshDb();
	const tx = seed(db);
	const queued = loadTaskRecoverySnapshot(tx, "t1");
	assert.equal(queued.task.lifecycle_state, "draft");
	assert.equal(projectTaskLifecycle({ ...queued, lifecycle: "draft" }).state, "queued");
	applyTaskLifecycle(tx, { taskId: "t1", next: "queued", now: NOW, reason: "projection", actorKind: "system" });
	tx.run("UPDATE node_runs SET status = ? WHERE node_run_id = ?", ["started", "n1"]);
	assert.equal(projectTaskLifecycle({ ...loadTaskRecoverySnapshot(tx, "t1"), lifecycle: "queued" }).state, "running");
	applyTaskLifecycle(tx, { taskId: "t1", next: "dispatching", now: NOW, reason: "projection", actorKind: "system" });
	applyTaskLifecycle(tx, { taskId: "t1", next: "running", now: NOW, reason: "projection", actorKind: "system" });
	tx.run("UPDATE node_runs SET status = 'done' WHERE node_run_id = 'n1'");
	tx.run("UPDATE runs SET status = 'succeeded' WHERE run_id = 'r1'");
	// 运行成功只进入待验收；默认 assisted 模式必须由人工批准后才 passed。
	assert.equal(projectTaskLifecycle({ ...loadTaskRecoverySnapshot(tx, "t1"), lifecycle: "running" }).state, "review");
	applyTaskLifecycle(tx, { taskId: "t1", next: "review", now: NOW, reason: "projection", actorKind: "system" });
	tx.run("UPDATE tasks SET acceptance_status = 'approved' WHERE task_id = 't1'");
	assert.equal(projectTaskLifecycle({ ...loadTaskRecoverySnapshot(tx, "t1"), lifecycle: "review" }).state, "passed");
	applyTaskLifecycle(tx, { taskId: "t1", next: "passed", now: NOW, reason: "projection", actorKind: "user" });
	assert.equal(tx.get("SELECT lifecycle_state FROM tasks WHERE task_id = 't1'").lifecycle_state, "passed");
});
