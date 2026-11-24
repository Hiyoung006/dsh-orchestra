import test from "node:test";
import assert from "node:assert/strict";
import { DatabaseSync } from "node:sqlite";
import { readdirSync, readFileSync } from "node:fs";
import { join, dirname } from "node:path";
import { fileURLToPath } from "node:url";
import {
	AcceptanceError,
	acceptanceAllowsPass,
	acceptanceHistory,
	acceptanceSnapshot,
	assertForceAuthorization,
	assertAgentAcceptanceAction,
	buildReviewItems,
	criteriaFromResults,
	freezeCriteria,
	latestCriteria,
	pendingRemediation,
	planReview,
	recordApproval,
	recordForceAuthorization,
	recordReview,
	requireHumanApprover,
	setAcceptanceMode
} from "../lib/task-acceptance.js";
import { loadTaskRecoverySnapshot, projectTaskLifecycle } from "../lib/task-lifecycle.js";

const MIGRATIONS = join(dirname(fileURLToPath(import.meta.url)), "..", "lib", "infra", "sqlite", "migrations");
const NOW = "2026-01-01T00:00:00.000Z";
const HUMAN = { kind: "human", id: "local-dsh:tester" };

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

function seed(db, { taskId = "t1", runId = "r1", lifecycle = "running", acceptance = "pending", mode = "assisted" } = {}) {
	const tx = makeTx(db);
	tx.run("INSERT INTO tasks (task_id, title, status, current_run_id, lifecycle_state, acceptance_mode, acceptance_status, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)", [taskId, "任务", "succeeded", runId, lifecycle, mode, acceptance, NOW, NOW]);
	tx.run("INSERT INTO runs (run_id, task_id, status, created_at, updated_at, root_run_id, subworkflow_depth) VALUES (?, ?, ?, ?, ?, ?, 0)", [runId, taskId, "succeeded", NOW, NOW, runId]);
	return tx;
}

function artifactResult({ key = "a|out.txt", outcome = "passed", reason = null, machine = true } = {}) {
	return {
		deliverable_key: key,
		title: "out.txt",
		node_id: "a",
		kind: "artifact",
		rule_version: "rulehash",
		rule: { kind: "artifact", file_name: "out.txt", minimum_bytes: 10 },
		machine_checkable: machine,
		parse_error: null,
		outcome,
		reason,
		evidence: { file_name: "out.txt", bytes: outcome === "passed" ? 32 : 3, minimum_bytes: 10 }
	};
}

function toolResult({ key = "a|test", outcome = "passed", reason = null, machine = true } = {}) {
	return {
		deliverable_key: key,
		title: "test",
		node_id: "a",
		kind: "tool_evidence",
		rule_version: "rulehash2",
		rule: { kind: "tool_evidence", tool: "npm test", exit_code: 0 },
		machine_checkable: machine,
		parse_error: null,
		outcome,
		reason,
		evidence: { tool: "npm test", expected_exit_code: 0, exit_codes: outcome === "passed" ? [0] : [1] }
	};
}

function freeze(tx, results, overrides = {}) {
	const criteria = criteriaFromResults(results);
	return freezeCriteria(tx, {
		taskId: "t1",
		planId: "p1",
		contentHash: "content",
		criteria,
		actor: "service:acceptance",
		actorKind: "system",
		now: NOW,
		...overrides
	});
}

// ---------------------------------------------------------------------------
// 标准冻结与版本
// ---------------------------------------------------------------------------

test("验收标准冻结后不可原地修改，内容变化只能追加新版本", () => {
	const db = freshDb();
	const tx = seed(db);
	const first = freeze(tx, [artifactResult(), toolResult()]);
	assert.equal(first.reused, false);
	assert.equal(first.criteria.version, 1);
	assert.equal(tx.get("SELECT COUNT(*) AS n FROM acceptance_criteria").n, 1);

	assert.throws(() => tx.run("UPDATE acceptance_criteria SET criteria_json = '[]' WHERE criteria_id = ?", [first.criteria.criteria_id]), /已冻结/);

	const same = freeze(tx, [artifactResult(), toolResult()]);
	assert.equal(same.reused, true);
	assert.equal(same.criteria.version, 1);
	assert.equal(tx.get("SELECT COUNT(*) AS n FROM acceptance_criteria").n, 1);

	// AI 不能替换已冻结标准。
	assert.throws(() => freeze(tx, [artifactResult({ key: "a|changed" })], { actorKind: "ai", actor: "agent:n1" }), (err) => err instanceof AcceptanceError && err.code === "FORBIDDEN");

	const second = freeze(tx, [artifactResult({ key: "a|changed" })]);
	assert.equal(second.criteria.version, 2);
	assert.equal(latestCriteria(tx, "t1").version, 2);
	assert.equal(tx.get("SELECT COUNT(*) AS n FROM acceptance_criteria WHERE task_id = 't1'").n, 2);
	assert.equal(tx.get("SELECT acceptance_criteria_version FROM tasks WHERE task_id = 't1'").acceptance_criteria_version, 2);
});

// ---------------------------------------------------------------------------
// 人工审批边界
// ---------------------------------------------------------------------------

test("AI 只能写预审意见，最终审批必须由人工完成", () => {
	const db = freshDb();
	const tx = seed(db);
	const frozen = freeze(tx, [artifactResult({ outcome: "failed", reason: "artifact_below_minimum_bytes" })]);
	const items = buildReviewItems([artifactResult({ outcome: "failed", reason: "artifact_below_minimum_bytes" })]);

	const review = recordReview(tx, {
		taskId: "t1",
		runId: "r1",
		criteria: frozen.criteria,
		mode: "assisted",
		reviewer: "service:acceptance",
		reviewerKind: "ai",
		items,
		now: NOW
	});
	assert.equal(review.verdict, "fail");
	assert.ok(review.remediation.length > 0);
	assert.equal(tx.get("SELECT acceptance_status FROM tasks WHERE task_id = 't1'").acceptance_status, "ai_reviewed");
	assert.equal(tx.get("SELECT COUNT(*) AS n FROM acceptance_reviews").n, 1);
	assert.equal(tx.get("SELECT COUNT(*) AS n FROM acceptance_approvals").n, 0);

	assert.throws(() => requireHumanApprover({ kind: "agent", id: "agent:n1" }), (err) => err.code === "FORBIDDEN");
	assert.throws(() => requireHumanApprover({ kind: "human", id: "system:auto" }), (err) => err.code === "FORBIDDEN");
	assert.throws(() => recordApproval(tx, {
		taskId: "t1",
		runId: "r1",
		criteria: frozen.criteria,
		reviewId: review.review_id,
		decision: "approved",
		approver: { kind: "agent", id: "agent:n1" },
		approverKind: "human",
		items,
		mode: "assisted",
		now: NOW
	}), (err) => err.code === "FORBIDDEN");
	assert.equal(tx.get("SELECT COUNT(*) AS n FROM acceptance_approvals").n, 0);

	const approval = recordApproval(tx, {
		taskId: "t1",
		runId: "r1",
		criteria: frozen.criteria,
		reviewId: review.review_id,
		decision: "approved",
		approver: HUMAN,
		approverKind: "human",
		reason: "人工确认交付可用",
		items,
		mode: "assisted",
		now: NOW
	});
	assert.equal(approval.status, "approved");
	const task = tx.get("SELECT acceptance_status, acceptance_approved_by, acceptance_approved_at, acceptance_reason FROM tasks WHERE task_id = 't1'");
	assert.equal(task.acceptance_status, "approved");
	assert.equal(task.acceptance_approved_by, HUMAN.id);
	assert.equal(task.acceptance_approved_at, NOW);
	assert.equal(task.acceptance_reason, "人工确认交付可用");
	assert.equal(tx.get("SELECT COUNT(*) AS n FROM acceptance_approvals").n, 1);
});

test("用户驳回后保存理由与整改意见，且重试可读取", () => {
	const db = freshDb();
	const tx = seed(db);
	const results = [artifactResult({ outcome: "failed", reason: "artifact_below_minimum_bytes" }), toolResult({ outcome: "unverifiable", reason: "tool_evidence_missing" })];
	const frozen = freeze(tx, results);
	const items = buildReviewItems(results);
	recordReview(tx, {
		taskId: "t1",
		runId: "r1",
		criteria: frozen.criteria,
		mode: "assisted",
		reviewer: "service:acceptance",
		reviewerKind: "ai",
		items,
		now: NOW
	});
	assert.throws(() => recordApproval(tx, {
		taskId: "t1",
		runId: "r1",
		criteria: frozen.criteria,
		decision: "rejected",
		approver: HUMAN,
		approverKind: "human",
		items,
		mode: "assisted",
		now: NOW
	}), /理由/);
	const approval = recordApproval(tx, {
		taskId: "t1",
		runId: "r1",
		criteria: frozen.criteria,
		decision: "rejected",
		approver: HUMAN,
		approverKind: "human",
		reason: "交付文件内容不完整",
		items,
		mode: "assisted",
		now: NOW
	});
	assert.equal(approval.status, "rejected");
	assert.ok(approval.remediation.length >= 2);
	assert.deepEqual(pendingRemediation(tx, "t1"), approval.remediation);
	const snapshot = acceptanceSnapshot(tx, "t1");
	assert.equal(snapshot.status, "rejected");
	assert.equal(snapshot.approval.reason, "交付文件内容不完整");
	assert.equal(snapshot.review.verdict, "fail");
	assert.ok(snapshot.history.some((item) => item.kind === "approval.rejected"));
	assert.ok(snapshot.history.some((item) => item.kind === "review.recorded"));
	assert.ok(snapshot.history.some((item) => item.kind === "criteria.frozen"));
});

test("manual 模式的预审不会自动改变待审批状态", () => {
	const db = freshDb();
	const tx = seed(db, { mode: "manual" });
	const frozen = freeze(tx, [artifactResult()]);
	const items = buildReviewItems([artifactResult()]);
	recordReview(tx, {
		taskId: "t1",
		runId: "r1",
		criteria: frozen.criteria,
		mode: "manual",
		reviewer: "service:acceptance",
		reviewerKind: "ai",
		items,
		now: NOW
	});
	assert.equal(tx.get("SELECT acceptance_status FROM tasks WHERE task_id = 't1'").acceptance_status, "pending");
	assert.equal(tx.get("SELECT COUNT(*) AS n FROM acceptance_approvals").n, 0);
	const approval = recordApproval(tx, {
		taskId: "t1",
		runId: "r1",
		criteria: frozen.criteria,
		decision: "approved",
		approver: HUMAN,
		approverKind: "human",
		items,
		mode: "manual",
		now: NOW
	});
	assert.equal(approval.status, "approved");
});

// ---------------------------------------------------------------------------
// automatic 模式
// ---------------------------------------------------------------------------

test("automatic 只允许全部机器标准通过的自动审批", () => {
	const passed = [artifactResult(), toolResult()];
	const criteria = criteriaFromResults(passed);
	assert.equal(planReview({ mode: "automatic", criteria, items: buildReviewItems(passed) }).autoApprovable, true);
	assert.equal(planReview({ mode: "assisted", criteria, items: buildReviewItems(passed) }).autoApprovable, false);
	assert.equal(planReview({ mode: "automatic", criteria, items: buildReviewItems([artifactResult({ outcome: "unverifiable", reason: "artifact_not_registered" })]) }).autoApprovable, false);
	assert.equal(planReview({ mode: "automatic", criteria: criteriaFromResults([artifactResult({ machine: false })]), items: buildReviewItems([artifactResult({ machine: false })]) }).autoApprovable, false);

	const db = freshDb();
	const tx = seed(db, { mode: "automatic" });
	const frozen = freeze(tx, passed);
	const items = buildReviewItems(passed);
	const review = recordReview(tx, {
		taskId: "t1",
		runId: "r1",
		criteria: frozen.criteria,
		mode: "automatic",
		reviewer: "service:acceptance",
		reviewerKind: "ai",
		items,
		now: NOW
	});
	const auto = recordApproval(tx, {
		taskId: "t1",
		runId: "r1",
		criteria: frozen.criteria,
		reviewId: review.review_id,
		decision: "approved",
		approver: "system:auto_acceptance",
		approverKind: "system_auto",
		reason: "全部机器化验收标准均已通过",
		items,
		mode: "automatic",
		now: NOW
	});
	assert.equal(auto.status, "approved");
	assert.equal(auto.approver_kind, "system_auto");

	const db2 = freshDb();
	const tx2 = seed(db2, { taskId: "t2", runId: "r2", mode: "automatic" });
	const frozen2 = freeze(tx2, [artifactResult({ outcome: "failed", reason: "artifact_below_minimum_bytes" })], { taskId: "t2" });
	const items2 = buildReviewItems([artifactResult({ outcome: "failed", reason: "artifact_below_minimum_bytes" })]);
	recordReview(tx2, { taskId: "t2", runId: "r2", criteria: frozen2.criteria, mode: "automatic", reviewer: "service:acceptance", reviewerKind: "ai", items: items2, now: NOW });
	assert.throws(() => recordApproval(tx2, {
		taskId: "t2",
		runId: "r2",
		criteria: frozen2.criteria,
		decision: "approved",
		approver: "system:auto_acceptance",
		approverKind: "system_auto",
		items: items2,
		mode: "automatic",
		now: NOW
	}), (err) => err.code === "CONFIRMATION_REQUIRED");
	assert.equal(tx2.get("SELECT COUNT(*) AS n FROM acceptance_approvals").n, 0);
});

// ---------------------------------------------------------------------------
// 证据映射与生命周期
// ---------------------------------------------------------------------------

test("每项验收标准必须且只能对应一条带证据的记录", () => {
	const db = freshDb();
	const tx = seed(db);
	const frozen = freeze(tx, [artifactResult(), toolResult()]);
	assert.throws(() => recordReview(tx, {
		taskId: "t1",
		runId: "r1",
		criteria: frozen.criteria,
		mode: "assisted",
		reviewer: "service:acceptance",
		reviewerKind: "ai",
		items: [{ key: "a|out.txt", outcome: "passed", evidence: {} }],
		now: NOW
	}), (err) => err.code === "ACCEPTANCE_EVIDENCE_MISSING");
	const missingEvidence = buildReviewItems([artifactResult(), toolResult()]);
	delete missingEvidence[0].evidence;
	assert.throws(() => recordReview(tx, {
		taskId: "t1",
		runId: "r1",
		criteria: frozen.criteria,
		mode: "assisted",
		reviewer: "service:acceptance",
		reviewerKind: "ai",
		items: missingEvidence,
		now: NOW
	}), (err) => err.code === "ACCEPTANCE_EVIDENCE_MISSING");
});

test("驳回必须填写理由，并保存可执行的整改意见", () => {
	const db = freshDb();
	const tx = seed(db);
	const frozen = freeze(tx, [artifactResult()]);
	const items = buildReviewItems([artifactResult({ outcome: "failed", reason: "artifact_below_minimum_bytes" })]);
	assert.throws(
		() => recordApproval(tx, {
			taskId: "t1",
			runId: "r1",
			criteria: { version: frozen.criteria.version },
			decision: "rejected",
			approver: HUMAN,
			items,
			mode: "assisted",
			reason: "   ",
			now: NOW
		}),
		(error) => error instanceof AcceptanceError && /理由/.test(error.message)
	);
	const rejected = recordApproval(tx, {
		taskId: "t1",
		runId: "r1",
		criteria: { version: frozen.criteria.version },
		decision: "rejected",
		approver: HUMAN,
		items,
		mode: "assisted",
		reason: "交付文件没有实际内容",
		now: NOW
	});
	assert.equal(rejected.status, "rejected");
	assert.equal(rejected.remediation.length, 1);
	assert.match(rejected.remediation[0].action, /最低字节/);
	const pending = pendingRemediation(tx, "t1");
	assert.equal(pending.length, 1);
	assert.match(pending[0].action, /最低字节/);
});

test("已批准的任务不能再次改判为驳回", () => {
	const db = freshDb();
	const tx = seed(db);
	const frozen = freeze(tx, [artifactResult()]);
	recordApproval(tx, {
		taskId: "t1",
		runId: "r1",
		criteria: { version: frozen.criteria.version },
		decision: "approved",
		approver: HUMAN,
		items: buildReviewItems([artifactResult()]),
		mode: "assisted",
		reason: "人工核对通过",
		now: NOW
	});
	assert.throws(
		() => recordApproval(tx, {
			taskId: "t1",
			runId: "r1",
			criteria: { version: frozen.criteria.version },
			decision: "rejected",
			approver: HUMAN,
			items: buildReviewItems([artifactResult({ outcome: "failed", reason: "artifact_below_minimum_bytes" })]),
			mode: "assisted",
			reason: "反悔",
			now: NOW
		}),
		(error) => error instanceof AcceptanceError && error.code === "STALE_GENERATION"
	);
	assert.equal(acceptanceSnapshot(tx, "t1").status, "approved");
});

test("运行成功但未审批时只进入 review，审批后才 passed", () => {
	const db = freshDb();
	const tx = seed(db, { lifecycle: "running" });
	const snapshot = loadTaskRecoverySnapshot(tx, "t1");
	assert.equal(projectTaskLifecycle({ ...snapshot, lifecycle: "running" }).state, "review");
	assert.equal(acceptanceAllowsPass({ status: "pending" }), false);
	assert.equal(acceptanceAllowsPass({ status: "approved" }), true);
	assert.equal(acceptanceAllowsPass(undefined), true);
	tx.run("UPDATE tasks SET acceptance_status = 'approved' WHERE task_id = 't1'");
	assert.equal(projectTaskLifecycle({ ...loadTaskRecoverySnapshot(tx, "t1"), lifecycle: "review" }).state, "passed");
});

test("验收模式切换与历史记录可追溯", () => {
	const db = freshDb();
	const tx = seed(db);
	const changed = setAcceptanceMode(tx, { taskId: "t1", mode: "manual", actor: HUMAN.id, actorKind: "user", now: NOW });
	assert.equal(changed.changed, true);
	assert.equal(tx.get("SELECT acceptance_mode FROM tasks WHERE task_id = 't1'").acceptance_mode, "manual");
	const history = acceptanceHistory(tx, "t1");
	assert.ok(history.events.some((item) => item.kind === "mode.changed"));
});

// ---------------------------------------------------------------------------
// force 授权
// ---------------------------------------------------------------------------

test("orchestra_accept 的工具动作白名单拒绝任何审批", () => {
	assert.equal(assertAgentAcceptanceAction("status"), "status");
	assert.equal(assertAgentAcceptanceAction("pre_review"), "pre_review");
	for (const action of ["approve", "reject"]) {
		assert.throws(() => assertAgentAcceptanceAction(action), (err) => err instanceof AcceptanceError && err.code === "FORBIDDEN");
	}
	assert.throws(() => assertAgentAcceptanceAction("delete"), (err) => err.code === "PAYLOAD_SCHEMA_INVALID");
});

test("force 派发必须同时具备授权、理由与人工操作者，并留痕", () => {
	assert.throws(() => assertForceAuthorization({ force: true, authorize: false, reason: "紧急修复", actor: HUMAN }), (err) => err.code === "CONFIRMATION_REQUIRED");
	assert.throws(() => assertForceAuthorization({ force: true, authorize: true, reason: "", actor: HUMAN }), (err) => err.code === "PAYLOAD_SCHEMA_INVALID");
	assert.throws(() => assertForceAuthorization({ force: true, authorize: true, reason: "紧急修复", actor: { kind: "agent", id: "agent:n1" } }), (err) => err.code === "FORBIDDEN");
	const auth = assertForceAuthorization({ force: true, authorize: true, reason: "紧急修复", actor: HUMAN });
	assert.deepEqual(auth, { authorized_by: HUMAN.id, reason: "紧急修复" });
	assert.equal(assertForceAuthorization({ force: false }), null);

	const db = freshDb();
	const tx = seed(db);
	recordForceAuthorization(tx, { taskId: "t1", runId: "r1", operationId: "op1", kind: "retry", authorizedBy: auth.authorized_by, reason: auth.reason, detail: { forced: true }, now: NOW });
	const row = tx.get("SELECT task_id, run_id, kind, authorized_by, reason FROM dispatch_authorizations");
	assert.equal(row.kind, "retry");
	assert.equal(row.authorized_by, HUMAN.id);
	assert.equal(row.reason, "紧急修复");
});
