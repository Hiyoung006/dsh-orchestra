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
	ReviewTriggerError,
	analyzeAst,
	buildLlmEvidence,
	createOsvAdvisoryProvider,
	createTypeScriptParserProvider,
	detectSecrets,
	latestReviewReport,
	mergeLlmReview,
	mergeScanResults,
	persistReviewReport,
	reportRemediation,
	reviewAllowsAcceptance,
	runIndependentLlmReview,
	runIndependentReview,
	runIndependentReviewWithLlm,
	runManualLlmReview,
	runSastScan,
	scanDependencyVulnerabilities,
	scanDependencyVulnerabilitiesAsync
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

test("SAST 接口：内置规则与外部 provider 并行，异步 provider 明确跳过", () => {
	const files = [
		{ path: "src/evil.js", text: "const key = 'AKIAABCDEFGHIJKLMNOP'; eval('x')" },
		{ path: "src/clean.js", text: "const ok = 1;" },
		{ path: "src/no-text.js" }
	];
	const builtin = runSastScan({ files });
	assert.equal(builtin.available, true);
	assert.equal(builtin.engine, "builtin_rules");
	assert.ok(builtin.secretFindings.some((item) => item.rule === "aws_access_key"));
	assert.ok(builtin.secretFindings.some((item) => item.excerpt.includes("[redacted]")));
	assert.ok(builtin.scanned.includes("src/evil.js"));
	assert.ok(builtin.skipped.some((item) => item.reason === "text_unreadable"));

	const external = runSastScan({
		files,
		provider: ({ path }) => path.endsWith("evil.js") ? [{ id: "SAST-TEST-1", severity: "high", summary: "外部扫描命中" }] : []
	});
	assert.equal(external.engine, "builtin_rules+host_provider");
	assert.ok(external.findings.some((item) => item.rule === "SAST-TEST-1"));

	const asynchronous = runSastScan({ files: [{ path: "a.js", text: "ok" }], provider: async () => [] });
	assert.ok(asynchronous.skipped.some((item) => item.reason === "async_provider_requires_async_review"));
});

test("审查报告分别记录 SAST、AST、依赖与证据来源", () => {
	const db = freshDb();
	const tx = seed(db);
	addArtifact(tx, { id: "art_secret", name: "secret.js", content: "const key = 'AKIAABCDEFGHIJKLMNOP';" });
	const report = baseReview(tx, {
		sast: {
			provider: ({ path }) => path.endsWith("secret.js") ? [{ id: "SAST-EXT-1", severity: "high", summary: "外部规则命中" }] : []
		}
	});
	assert.equal(report.sast.available, true);
	assert.equal(report.sast.engine, "builtin_rules+host_provider");
	assert.ok(report.sast.findings.some((item) => item.rule === "aws_access_key"));
	assert.ok(report.sast.external.findings.some((item) => item.rule === "SAST-EXT-1"));
	assert.equal(report.checks.sast, report.securityIssues.length);
	assert.equal(report.reviewStatus, "fail");
	assert.equal(report.resultKinds.real_evidence.every((item) => item.evidence_type === "deterministic"), true);
});

test("人工触发 LLM Review：必须具名、记录来源、不能自动批准", async () => {
	const db = freshDb();
	const tx = seed(db);
	const report = baseReview(tx);

	await assert.rejects(
		runManualLlmReview({ report, config: { enabled: true } }),
		(error) => error instanceof ReviewTriggerError && error.code === "LLM_REVIEW_MANUAL_TRIGGER_REQUIRED"
	);
	await assert.rejects(
		runManualLlmReview({ report: null, requestedBy: "local-dsh:tester" }),
		(error) => error instanceof ReviewTriggerError && error.code === "REVIEW_REPORT_REQUIRED"
	);

	const manual = await runManualLlmReview({
		report,
		requestedBy: "local-dsh:tester",
		now: NOW,
		config: {
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
	assert.notEqual(manual.review_id, report.review_id);
	assert.equal(manual.parent_review_id, report.review_id);
	assert.equal(manual.trigger.kind, "manual");
	assert.equal(manual.trigger.requested_by, "local-dsh:tester");
	assert.equal(manual.trigger.requested_at, NOW);
	assert.equal(manual.trigger.automatic_approval, false);
	assert.equal(manual.llm_trigger, "manual");
	assert.equal(manual.approval_eligible, false);
	assert.equal(manual.reviewStatus, "pass");
	assert.equal(reviewAllowsAcceptance(manual), false);

	persistReviewReport(tx, manual);
	assert.equal(latestReviewReport(tx, "t1").review_id, manual.review_id);
});

test("人工 LLM Review 不可用或不可解析时保持原结论并阻断自动通过", async () => {
	const db = freshDb();
	const tx = seed(db);
	const report = baseReview(tx);

	const failed = await runManualLlmReview({
		report,
		requestedBy: "local-dsh:tester",
		now: NOW,
		config: { enabled: true, complete: async () => { throw new Error("reviewer down"); } }
	});
	assert.equal(failed.reviewStatus, "pass");
	assert.equal(failed.llmReview.available, false);
	assert.equal(failed.checks.aiReview, "unavailable");
	assert.equal(reviewAllowsAcceptance(failed), false);

	const failedReport = mergeLlmReview({ ...report, reviewStatus: "fail" }, { available: true, blocking: false, engine: "independent_llm" });
	assert.equal(failedReport.reviewStatus, "fail");
	assert.equal(reviewAllowsAcceptance(failedReport), false);
});

test("AST 正式 Parser：TypeScript 解析源码，解析失败不能当作通过", () => {
	const parser = createTypeScriptParserProvider();
	assert.equal(parser.available, true);
	assert.equal(parser.engine, "typescript");
	assert.equal(typeof parser.parse, "function");

	const found = analyzeAst({ files: [{ path: "src/app.ts", text: "const n: number = 1;\neval('1');\n" }], parse: parser.parse });
	assert.equal(found.status, "ok");
	assert.equal(found.engine, "typescript");
	assert.ok(found.findings.some((item) => item.rule === "ast_dynamic_code_execution"));

	const broken = analyzeAst({ files: [{ path: "src/broken.ts", text: "function (" }], parse: parser.parse });
	assert.equal(broken.status, "failed");
	assert.equal(broken.available, true);
	assert.equal(broken.findings.length, 0);
});

test("依赖扫描：锁文件解析组件与版本，未配置/失败/成功严格区分", async () => {
	const lock = JSON.stringify({
		lockfileVersion: 3,
		packages: {
			"": { name: "app", version: "1.0.0" },
			"node_modules/lodash": { version: "4.17.20" }
		}
	});
	const files = [{ path: "package-lock.json", text: lock }];

	const unconfigured = await scanDependencyVulnerabilitiesAsync({ files });
	assert.equal(unconfigured.status, "unconfigured");
	assert.equal(unconfigured.available, false);
	assert.equal(unconfigured.reason, "advisory_provider_not_configured");

	const failed = await scanDependencyVulnerabilitiesAsync({ files, advisoryProvider: async () => { throw new Error("offline"); } });
	assert.equal(failed.status, "failed");
	assert.equal(failed.available, false);

	const ok = await scanDependencyVulnerabilitiesAsync({
		files,
		advisoryProvider: async ({ components }) => components.filter((item) => item.name === "lodash").map((item) => ({
			vulnerability_id: "GHSA-test",
			component: item.name,
			version: item.version,
			severity: "high",
			fixed_version: "4.17.21"
		}))
	});
	assert.equal(ok.status, "ok");
	assert.equal(ok.available, true);
	assert.equal(ok.components.find((item) => item.name === "lodash")?.version, "4.17.20");
	assert.equal(ok.findings[0].vulnerability_id, "GHSA-test");
	assert.equal(ok.findings[0].fixed_version, "4.17.21");

	const sync = scanDependencyVulnerabilities({ files, advisoryProvider: () => [] });
	assert.equal(sync.status, "ok");
	assert.equal(sync.counts.components, 1);
	assert.equal(sync.components[0].name, "lodash");
});

test("OSV Advisory Provider：映射漏洞编号、严重程度与修复建议", async () => {
	const requested = [];
	const provider = createOsvAdvisoryProvider({
		baseUrl: "https://osv.test",
		timeoutMs: 2000,
		fetch: async (url) => {
			requested.push(url);
			if (url.endsWith("/v1/querybatch")) return { ok: true, status: 200, json: async () => ({ results: [{ vulns: [{ id: "GHSA-test" }] }] }) };
			if (url.includes("/v1/vulns/GHSA-test")) return {
				ok: true,
				status: 200,
				json: async () => ({
					id: "GHSA-test",
					summary: "prototype pollution",
					database_specific: { severity: "HIGH" },
					affected: [{ package: { name: "lodash" }, ranges: [{ events: [{ introduced: "0" }, { fixed: "4.17.21" }] }] }]
				})
			};
			return { ok: false, status: 404, json: async () => ({}) };
		}
	});
	const findings = await provider({ components: [{ name: "lodash", version: "4.17.20" }] });
	assert.equal(findings.length, 1);
	assert.equal(findings[0].vulnerability_id, "GHSA-test");
	assert.equal(findings[0].severity, "high");
	assert.equal(findings[0].fixed_version, "4.17.21");
	assert.ok(findings[0].remediation.includes("4.17.21"));
	assert.ok(requested.some((url) => url.endsWith("/v1/querybatch")));
});

test("扫描结果并入报告：只收紧结论，未执行或失败不能算通过", () => {
	const report = { review_id: "rv_base", reviewStatus: "pass", score: 90, checks: {}, suggestions: [] };
	const unconfigured = mergeScanResults(report, {
		staticAnalysis: { status: "unconfigured", findings: [] },
		dependencies: { status: "unconfigured", findings: [] },
		requestedBy: "local-dsh:tester",
		now: NOW
	});
	assert.equal(unconfigured.checks.staticAnalysis, "unconfigured");
	assert.equal(unconfigured.checks.dependencyScan, "unconfigured");
	assert.equal(unconfigured.approval_eligible, false);
	assert.equal(unconfigured.parent_review_id, "rv_base");

	const failed = mergeScanResults(report, {
		staticAnalysis: { status: "failed", findings: [] },
		dependencies: { status: "ok", findings: [] },
		requestedBy: "local-dsh:tester",
		now: NOW
	});
	assert.equal(failed.reviewStatus, "need_evidence");

	const blocked = mergeScanResults(report, {
		staticAnalysis: { status: "ok", findings: [{ rule: "ast_x", severity: "high", detail: "危险调用" }] },
		dependencies: { status: "ok", findings: [] },
		requestedBy: "local-dsh:tester",
		now: NOW
	});
	assert.equal(blocked.reviewStatus, "fail");
	assert.equal(blocked.score <= 50, true);
	assert.equal(blocked.approval_eligible, false);
});
