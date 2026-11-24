import test from "node:test";
import assert from "node:assert/strict";
import { DatabaseSync } from "node:sqlite";
import { readdirSync, readFileSync } from "node:fs";
import { join, dirname } from "node:path";
import { fileURLToPath } from "node:url";
import { createHash } from "node:crypto";
import {
	REVIEW_AGENT_ID,
	latestReviewReport,
	persistReviewReport,
	reportRemediation,
	reviewAllowsAcceptance,
	runIndependentReview
} from "../lib/review-agent.js";

const MIGRATIONS = join(dirname(fileURLToPath(import.meta.url)), "..", "lib", "infra", "sqlite", "migrations");
const NOW = "2026-01-01T00:00:00.000Z";
const CONTENT = "console.log('ok');";
const DIGEST = createHash("sha256").update(CONTENT).digest("hex");

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

function seed(db, { taskId = "t1", runId = "r1", artifactDigest = DIGEST, withTool = true } = {}) {
	const tx = makeTx(db);
	tx.run("INSERT INTO tasks (task_id, title, status, current_run_id, lifecycle_state, created_at, updated_at) VALUES (?, '任务', 'succeeded', ?, 'review', ?, ?)", [taskId, runId, NOW, NOW]);
	tx.run("INSERT INTO runs (run_id, task_id, status, created_at, updated_at) VALUES (?, ?, 'succeeded', ?, ?)", [runId, taskId, NOW, NOW]);
	tx.run("INSERT INTO node_runs (node_run_id, run_id, node_id, seq, node_type, attempt, status, created_at, updated_at) VALUES ('n1', ?, 'a', 1, 'agent', 1, 'done', ?, ?)", [runId, NOW, NOW]);
	tx.run("INSERT INTO artifacts (artifact_id, run_id, uri, sha256, created_at) VALUES ('art1', ?, 'artifact://out.txt', ?, ?)", [runId, artifactDigest, NOW]);
	tx.run("INSERT INTO artifact_contents (artifact_id, file_name, node_run_id, bytes) VALUES ('art1', 'out.txt', 'n1', ?)", [Buffer.from(CONTENT)]);
	if (withTool) {
		tx.run("INSERT INTO tool_executions (node_run_id, attempt, tool_seq, tool, execution_id, exit_code, started_at, finished_at) VALUES ('n1', 1, 1, 'npm test', 'tex1', 0, ?, ?)", [NOW, NOW]);
	}
	return tx;
}

function criteria() {
	return [
		{ key: "a|out.txt", title: "out.txt", kind: "artifact", rule: { kind: "artifact", file_name: "out.txt", minimum_bytes: 5 }, machine_checkable: true, parse_error: null },
		{ key: "a|test", title: "test", kind: "tool_evidence", rule: { kind: "tool_evidence", tool: "npm test", exit_code: 0 }, machine_checkable: true, parse_error: null }
	];
}

function items({ artifactOutcome = "passed", toolOutcome = "passed", artifactEvidence = { artifact_id: "art1", file_name: "out.txt", sha256: DIGEST }, toolEvidence = { tool: "npm test", matched_execution_id: "tex1", expected_exit_code: 0 } } = {}) {
	return [
		{ key: "a|out.txt", title: "out.txt", kind: "artifact", outcome: artifactOutcome, reason: artifactOutcome === "failed" ? "artifact_below_minimum_bytes" : null, evidence: artifactEvidence },
		{ key: "a|test", title: "test", kind: "tool_evidence", outcome: toolOutcome, reason: toolOutcome === "unverifiable" ? "tool_evidence_missing" : null, evidence: toolEvidence }
	];
}

const WORKSPACE = { real_path: "/tmp/dsh-workspace", allowed_roots: JSON.stringify(["/tmp/dsh-workspace"]) };

test("独立审查通过：交付物内容哈希匹配、工具证据真实存在、修改范围合法", () => {
	const db = freshDb();
	const tx = seed(db);
	const report = runIndependentReview(tx, {
		taskId: "t1",
		runId: "r1",
		criteria: criteria(),
		items: items(),
		workspace: WORKSPACE,
		diff: { kind: "git_worktree", changedFiles: ["src/a.js"], patch: "+const value = 1;\n" },
		now: NOW,
		criteriaVersion: 1
	});
	assert.equal(report.reviewStatus, "pass");
	assert.equal(report.score, 100);
	assert.equal(report.passedCriteria.length, 2);
	assert.equal(report.failedCriteria.length, 0);
	assert.equal(report.missingEvidence.length, 0);
	assert.equal(report.securityIssues.length, 0);
	assert.equal(report.checks.filesExist, true);
	assert.equal(report.checks.testsExecuted, true);
	assert.equal(report.checks.taskBoundary, "ok");
	assert.equal(report.reviewer, REVIEW_AGENT_ID);
	assert.equal(report.reviewer_kind, "independent_agent");
	assert.equal(reviewAllowsAcceptance(report), true);
});

test("任一验收项失败则整体 FAIL，且给出具体整改意见", () => {
	const db = freshDb();
	const tx = seed(db);
	const report = runIndependentReview(tx, {
		taskId: "t1",
		runId: "r1",
		criteria: criteria(),
		items: items({ artifactOutcome: "failed" }),
		workspace: WORKSPACE,
		diff: { kind: "git_worktree", changedFiles: ["src/a.js"], patch: "+const value = 1;\n" },
		now: NOW
	});
	assert.equal(report.reviewStatus, "fail");
	assert.equal(report.failedCriteria.length, 1);
	assert.ok(report.suggestions.length > 0);
	assert.ok(reportRemediation(report).length > 0);
	assert.equal(reviewAllowsAcceptance(report), false);
});

test("没有证据或证据不可读取时只能要求补充证据，不能通过", () => {
	const db = freshDb();
	const tx = seed(db);
	const missing = runIndependentReview(tx, {
		taskId: "t1",
		runId: "r1",
		criteria: criteria(),
		items: items({ artifactEvidence: { artifact_id: "missing", file_name: "out.txt" } }),
		workspace: WORKSPACE,
		diff: { kind: "git_worktree", changedFiles: ["src/a.js"], patch: "" },
		now: NOW
	});
	assert.equal(missing.reviewStatus, "need_evidence");
	assert.ok(missing.missingEvidence.length > 0);
	assert.equal(reviewAllowsAcceptance(missing), false);

	const empty = runIndependentReview(tx, {
		taskId: "t1",
		runId: "r1",
		criteria: [],
		items: [],
		workspace: WORKSPACE,
		diff: null,
		now: NOW
	});
	assert.equal(empty.reviewStatus, "need_evidence");
	assert.equal(reviewAllowsAcceptance(empty), false);
});

test("交付物哈希不一致按失败处理，不能拿分数放行", () => {
	const db = freshDb();
	const tx = seed(db, { artifactDigest: "0".repeat(64) });
	const report = runIndependentReview(tx, {
		taskId: "t1",
		runId: "r1",
		criteria: criteria(),
		items: items(),
		workspace: WORKSPACE,
		diff: { kind: "git_worktree", changedFiles: ["src/a.js"], patch: "" },
		now: NOW
	});
	assert.equal(report.reviewStatus, "fail");
	assert.ok(report.failedCriteria.some((item) => item.reason === "artifact_hash_mismatch"));
	assert.equal(reviewAllowsAcceptance(report), false);
});

test("安全检查命中高风险问题时直接 FAIL，即使其余项目全部通过", () => {
	const db = freshDb();
	const tx = seed(db);
	const report = runIndependentReview(tx, {
		taskId: "t1",
		runId: "r1",
		criteria: criteria(),
		items: items(),
		workspace: WORKSPACE,
		diff: { kind: "git_worktree", changedFiles: ["src/a.js"], patch: "+const key = '-----BEGIN RSA PRIVATE KEY-----';\n" },
		now: NOW
	});
	assert.equal(report.reviewStatus, "fail");
	assert.ok(report.securityIssues.some((item) => item.severity === "high"));
	assert.equal(report.passedCriteria.length, 2);
	assert.equal(reviewAllowsAcceptance(report), false);
});

test("越界修改判定为任务边界违规", () => {
	const db = freshDb();
	const tx = seed(db);
	for (const changed of [["../outside.txt"], ["/etc/passwd"]]) {
		const report = runIndependentReview(tx, {
			taskId: "t1",
			runId: "r1",
			criteria: criteria(),
			items: items(),
			workspace: WORKSPACE,
			diff: { kind: "git_worktree", changedFiles: changed, patch: "" },
			now: NOW
		});
		assert.equal(report.reviewStatus, "fail", changed.join(","));
		assert.equal(report.checks.taskBoundary, "violated");
		assert.equal(reviewAllowsAcceptance(report), false);
	}
});

test("审查报告可持久化并按任务回溯", () => {
	const db = freshDb();
	const tx = seed(db);
	const report = runIndependentReview(tx, {
		taskId: "t1",
		runId: "r1",
		criteria: criteria(),
		items: items(),
		workspace: WORKSPACE,
		diff: { kind: "git_worktree", changedFiles: ["src/a.js"], patch: "" },
		now: NOW,
		criteriaVersion: 3
	});
	persistReviewReport(tx, report);
	const loaded = latestReviewReport(tx, "t1");
	assert.equal(loaded.review_id, report.review_id);
	assert.equal(loaded.reviewStatus, "pass");
	assert.equal(loaded.criteria_version, 3);
});
