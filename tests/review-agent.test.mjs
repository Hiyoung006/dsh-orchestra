import test from "node:test";
import assert from "node:assert/strict";
import { DatabaseSync } from "node:sqlite";
import { readdirSync, readFileSync } from "node:fs";
import { join, dirname } from "node:path";
import { fileURLToPath } from "node:url";
import { createHash } from "node:crypto";
import {
	LLM_REVIEW_SECTIONS,
	REVIEW_AGENT_ID,
	ReviewIsolationError,
	analyzeAst,
	buildLlmEvidence,
	detectSecrets,
	latestReviewReport,
	mergeLlmReview,
	persistReviewReport,
	reportRemediation,
	reviewAllowsAcceptance,
	runIndependentLlmReview,
	runIndependentReview,
	runIndependentReviewWithLlm
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

function addArtifact(tx, { id, name, content }) {
	const bytes = Buffer.isBuffer(content) ? content : Buffer.from(String(content));
	const digest = createHash("sha256").update(bytes).digest("hex");
	tx.run("INSERT INTO artifacts (artifact_id, run_id, uri, sha256, created_at) VALUES (?, 'r1', ?, ?, ?)", [id, `artifact://${name}`, digest, NOW]);
	tx.run("INSERT INTO artifact_contents (artifact_id, file_name, node_run_id, bytes) VALUES (?, ?, 'n1', ?)", [id, name, bytes]);
}

function baseReview(tx, overrides = {}) {
	return runIndependentReview(tx, {
		taskId: "t1",
		runId: "r1",
		criteria: criteria(),
		items: items(),
		workspace: WORKSPACE,
		diff: { kind: "git_worktree", changedFiles: ["src/a.js"], patch: "+const value = 1;\n" },
		now: NOW,
		...overrides
	});
}

test("diff 存在时仍扫描全部交付物，并记录双扫描范围", () => {
	const db = freshDb();
	const tx = seed(db);
	addArtifact(tx, { id: "art_secret", name: "secret.txt", content: "const key = 'AKIAABCDEFGHIJKLMNOP';\n" });
	const report = baseReview(tx);
	assert.equal(report.scan.diff.scanned, true);
	assert.equal(report.scan.scope.workspace_diff, true);
	assert.ok(report.scan.artifacts.some((item) => item.file_name === "secret.txt" && item.scanned));
	assert.ok(report.securityIssues.some((issue) => issue.rule === "aws_access_key" && issue.source === "artifact:secret.txt"));
	assert.equal(report.reviewStatus, "fail");
});

test("扫描范围记录文件大小、二进制和跳过原因", () => {
	const db = freshDb();
	const tx = seed(db);
	addArtifact(tx, { id: "art_big", name: "big.txt", content: "a".repeat(300000) });
	addArtifact(tx, { id: "art_bin", name: "bin.dat", content: Buffer.from([1, 0, 2]) });
	const report = baseReview(tx);
	const big = report.scan.artifacts.find((item) => item.file_name === "big.txt");
	const binary = report.scan.artifacts.find((item) => item.file_name === "bin.dat");
	assert.equal(big.scanned, false);
	assert.equal(big.skipped_reason, "artifact_too_large");
	assert.equal(binary.scanned, false);
	assert.equal(binary.skipped_reason, "binary_content");
	assert.ok(report.scan.scope.artifacts_total >= 3);
	assert.equal(report.scan.limits.maxArtifactBytes, 262144);
	assert.equal(report.reviewStatus, "pass");
});

test("AST 接口：未配置时明确不可用，配置 parser 后可发现危险调用", () => {
	const unavailable = analyzeAst({ files: [{ path: "a.js", text: "eval('x')" }] });
	assert.equal(unavailable.available, false);
	assert.equal(unavailable.reason, "ast_parser_not_configured");

	const parse = () => ({
		type: "Program",
		body: [{
			type: "ExpressionStatement",
			expression: {
				type: "CallExpression",
				callee: { type: "Identifier", name: "eval" },
				arguments: []
			}
		}]
	});
	const found = analyzeAst({ files: [{ path: "a.js", text: "eval('x')" }], parse });
	assert.equal(found.available, true);
	assert.ok(found.findings.some((item) => item.rule === "ast_dynamic_code_execution"));

	const db = freshDb();
	const tx = seed(db);
	addArtifact(tx, { id: "art_js", name: "a.js", content: "eval('x')" });
	const report = baseReview(tx, { ast: { parse } });
	assert.equal(report.reviewStatus, "fail");
	assert.ok(report.staticAnalysis.findings.some((item) => item.rule === "ast_dynamic_code_execution"));
	assert.equal(typeof report.checks.staticAnalysis, "number");
});

test("依赖漏洞接口：provider 可注入，高风险漏洞导致失败", () => {
	const db = freshDb();
	const tx = seed(db);
	addArtifact(tx, { id: "art_manifest", name: "package.json", content: "{\"dependencies\":{\"left-pad\":\"1.0.0\"}}" });
	const unavailable = baseReview(tx);
	assert.equal(unavailable.dependencies.available, false);
	assert.equal(unavailable.dependencies.reason, "advisory_provider_not_configured");

	const report = baseReview(tx, {
		dependency: {
			advisoryProvider: ({ path }) => path.endsWith("package.json") ? [{
				id: "CVE-TEST-1",
				package: "left-pad",
				version: "1.0.0",
				severity: "high",
				summary: "测试漏洞"
			}] : []
		}
	});
	assert.equal(report.dependencies.available, true);
	assert.equal(report.dependencies.findings.length, 1);
	assert.equal(report.reviewStatus, "fail");
});

test("秘密信息检测接口只返回脱敏片段", () => {
	const findings = detectSecrets("const token = 'AKIAABCDEFGHIJKLMNOP';");
	assert.ok(findings.length > 0);
	assert.ok(findings.every((item) => item.evidence_type === "deterministic"));
	assert.ok(findings.every((item) => !item.excerpt.includes("AKIAABCDEFGHIJKLMNOP")));
	assert.ok(findings.some((item) => item.excerpt.includes("[redacted]")));
});

test("独立 LLM Review：字段完整，模型判断与推测分开，高分不能自动通过", async () => {
	const db = freshDb();
	const tx = seed(db);
	const report = baseReview(tx, { llm: { enabled: true, model: "review-model" } });
	assert.equal(report.llm.configured, true);
	assert.equal(reviewAllowsAcceptance(report), false);

	const llm = await runIndependentLlmReview({
		report,
		config: {
			enabled: true,
			model: "review-model",
			complete: async () => ({
				correctness: { status: "pass", summary: "ok" },
				security: { status: "pass", findings: [{ title: "无明显问题", severity: "info" }] },
				maintainability: { status: "pass" },
				test_coverage: { status: "pass" },
				requirement_compliance: { status: "pass" },
				score: 100,
				missing_evidence: ["缺少性能证据"],
				false_positive_candidates: ["疑似误报"],
				remediation: ["补充边界测试"],
				speculation: [{ claim: "可能存在的隐患" }]
			})
		}
	});
	assert.equal(llm.available, true);
	for (const key of LLM_REVIEW_SECTIONS) assert.ok(llm[key]);
	assert.equal(llm.score, 100);
	assert.equal(llm.score_used_for_approval, false);
	assert.equal(llm.approval_eligible, false);

	const merged = mergeLlmReview(report, llm);
	assert.equal(merged.reviewStatus, "pass");
	assert.equal(reviewAllowsAcceptance(merged), false);
	assert.ok(merged.resultKinds.model_judgment.length > 0);
	assert.equal(merged.resultKinds.speculation[0].evidence_type, "speculation");
	assert.ok(reportRemediation(merged).some((item) => item.evidence_type === "model_judgment"));
});

test("LLM 阻断项只能收紧结论，不能把失败升级为通过", async () => {
	const db = freshDb();
	const tx = seed(db);
	const report = baseReview(tx, { llm: { enabled: true, model: "review-model" } });
	const llm = await runIndependentLlmReview({
		report,
		config: {
			enabled: true,
			complete: async () => ({
				correctness: { status: "pass" },
				security: { status: "fail", findings: [{ title: "认证绕过", severity: "high" }] },
				maintainability: { status: "pass" },
				test_coverage: { status: "pass" },
				requirement_compliance: { status: "pass" },
				score: 99
			})
		}
	});
	const merged = mergeLlmReview(report, llm);
	assert.equal(merged.reviewStatus, "need_evidence");
	assert.equal(merged.checks.aiReview, "blocking");
	assert.equal(reviewAllowsAcceptance(merged), false);

	const failed = mergeLlmReview({ ...report, reviewStatus: "fail" }, llm);
	assert.equal(failed.reviewStatus, "fail");
});

test("审查器只读：可写工作区被拒绝，LLM 证据不含工作区路径", () => {
	const db = freshDb();
	const tx = seed(db);
	assert.throws(() => baseReview(tx, { workspaceAccess: "writable" }), ReviewIsolationError);
	assert.throws(() => baseReview(tx, { workspace: { ...WORKSPACE, writable: true } }), ReviewIsolationError);

	const report = baseReview(tx);
	assert.equal(report.reviewIsolation.mode, "read_only_evidence");
	assert.equal(report.reviewIsolation.workspace, "not_mounted");
	assert.equal(report.reviewIsolation.shared_with_executor, false);
	assert.equal(report.reviewIsolation.llm_workspace_access, false);

	const evidence = buildLlmEvidence(report);
	assert.equal(evidence.constraints.read_only, true);
	assert.equal(evidence.constraints.executor_workspace_shared, false);
	assert.equal("workspace" in evidence, false);
	assert.ok(report.resultKinds.real_evidence.length > 0);
	assert.deepEqual(report.resultKinds.model_judgment, []);
});

test("LLM 开关：异步入口合并报告，但永远保留人工审批", async () => {
	const db = freshDb();
	const tx = seed(db);
	const report = await runIndependentReviewWithLlm(tx, {
		taskId: "t1",
		runId: "r1",
		criteria: criteria(),
		items: items(),
		workspace: WORKSPACE,
		diff: { kind: "git_worktree", changedFiles: ["src/a.js"], patch: "+const value = 1;\n" },
		now: NOW,
		llm: {
			enabled: true,
			model: "review-model",
			complete: async () => ({
				correctness: { status: "pass" },
				security: { status: "pass" },
				maintainability: { status: "pass" },
				test_coverage: { status: "pass" },
				requirement_compliance: { status: "pass" },
				score: 100
			})
		}
	});
	assert.equal(report.llmReview.available, true);
	assert.equal(report.llmReview.score, 100);
	assert.equal(report.reviewStatus, "pass");
	assert.equal(reviewAllowsAcceptance(report), false);
	assert.equal(report.checks.aiReview, "completed");
});
