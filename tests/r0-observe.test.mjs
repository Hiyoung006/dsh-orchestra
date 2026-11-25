import test from "node:test";
import assert from "node:assert/strict";
import { createHash, generateKeyPairSync } from "node:crypto";
import { chmodSync, mkdirSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { DatabaseSync } from "node:sqlite";

import {
	assertCandidate,
	assertTarball,
	parseFlags,
	parseWindow,
	signObservationReport,
	verifyObservation,
	verifySignedReport
} from "../scripts/r0-observe.mjs";

const COMMIT = "a".repeat(40);
const START = Date.parse("2026-01-01T00:00:00.000Z");
const SAMPLES = 3;
const INTERVAL = 1000;

function sha256(data) {
	return createHash("sha256").update(data).digest("hex");
}

function fixture(mutate = () => {}) {
	const dir = mkdtempSync(join(tmpdir(), "r0-observe-"));
	const workspace = join(dir, "workspace");
	mkdirSync(workspace);
	const sample = join(workspace, "sample.txt");
	writeFileSync(sample, "R0 synthetic read-only sample\n");
	chmodSync(sample, 0o444);
	const ledgerPath = join(dir, "ledger.db");
	const db = new DatabaseSync(ledgerPath);
	db.exec("CREATE TABLE schema_migrations (version INTEGER PRIMARY KEY, name TEXT NOT NULL, checksum TEXT NOT NULL, applied_at TEXT NOT NULL)");
	db.exec("CREATE TABLE events (seq INTEGER PRIMARY KEY AUTOINCREMENT, event_id TEXT NOT NULL)");
	for (let version = 1; version <= 75; version += 1) db.prepare("INSERT INTO schema_migrations (version, name, checksum, applied_at) VALUES (?, ?, ?, ?)").run(version, `m${version}`, "x", "2026-01-01T00:00:00.000Z");
	db.prepare("INSERT INTO events (event_id) VALUES (?)").run("e1");
	db.close();
	const written = {
		"launch.json": {
			schema_version: 1,
			candidate: COMMIT,
			version: "0.1.0-beta.3",
			mode: "dry-run",
			started_at: new Date(START).toISOString()
		},
		"soak-config.json": { schema_version: 1, duration_ms: SAMPLES * INTERVAL, sample_interval_ms: INTERVAL, mode: "dry-run", read_only: true },
		"worker.json": {
			pid: 1,
			started_at: new Date(START).toISOString(),
			sampling_started_at: new Date(START).toISOString(),
			node: process.version
		},
		"end.json": {
			status: "completed",
			started_at: new Date(START).toISOString(),
			ended_at: new Date(START + SAMPLES * INTERVAL).toISOString(),
			samples: SAMPLES
		}
	};
	const samples = Array.from({ length: SAMPLES }, (_, slot) => ({
		slot,
		at: new Date(START + slot * INTERVAL).toISOString(),
		monotonic_ms: 100 + slot * INTERVAL,
		rss_bytes: 1024,
		ledger: { schema_version: 75, migrations: 75, event_seq: 1, event_count: 1 },
		trial: { candidate: COMMIT, circle: "R0", revision: 1, halted: false },
		workspace_sha256: sha256("R0 synthetic read-only sample\n")
	}));
	writeFileSync(join(dir, "samples.ndjson"), `${samples.map((entry) => JSON.stringify(entry)).join("\n")}\n`);
	for (const [name, value] of Object.entries(written)) writeFileSync(join(dir, name), `${JSON.stringify(value, null, 2)}\n`);
	mutate({ dir, samples, written });
	return dir;
}

function withFixture(mutate, run) {
	const dir = fixture(mutate);
	try {
		run(dir);
	} finally {
		rmSync(dir, { recursive: true, force: true });
	}
}

/** 构造满足 72 小时硬条件的观察目录（72 个 1 小时采样，不实际等待）。 */
function soakFixture(mutate = () => {}) {
	const dir = fixture();
	const hours = 72;
	const interval = 60 * 60 * 1000;
	const started = START;
	const written = {
		"launch.json": {
			schema_version: 1,
			candidate: COMMIT,
			version: "0.1.0-beta.3",
			mode: "soak",
			started_at: new Date(started).toISOString()
		},
		"soak-config.json": { schema_version: 1, duration_ms: hours * interval, sample_interval_ms: interval, mode: "soak", read_only: true },
		"worker.json": {
			pid: 1,
			started_at: new Date(started).toISOString(),
			sampling_started_at: new Date(started).toISOString(),
			node: process.version,
			recovery: { attempted: true, ok: true, detail: "任务恢复已执行" }
		},
		"end.json": {
			status: "completed",
			started_at: new Date(started).toISOString(),
			ended_at: new Date(started + hours * interval).toISOString(),
			samples: hours,
			recovery: { attempted: true, ok: true, detail: "任务恢复已执行" },
			tasks: { counts: { succeeded: 1 }, unfinished: [] },
			exceptions: { count: 0, items: [] }
		}
	};
	const samples = Array.from({ length: hours }, (_, slot) => ({
		slot,
		at: new Date(started + slot * interval).toISOString(),
		monotonic_ms: 100 + slot * interval,
		rss_bytes: 1024,
		resource: { rss_bytes: 1024, heap_used_bytes: 512, pid: 1, cpu_user_ms: slot, cpu_system_ms: 0 },
		disk: { block_size: 4096, blocks: 1000, blocks_free: 500, blocks_available: 500 },
		tasks: { counts: { succeeded: 1 }, unfinished: [] },
		exceptions: { count: 0 },
		ledger: { schema_version: 75, migrations: 75, event_seq: 1, event_count: 1 },
		trial: { candidate: COMMIT, circle: "R0", revision: 1, halted: false },
		workspace_sha256: sha256("R0 synthetic read-only sample\n")
	}));
	writeFileSync(join(dir, "samples.ndjson"), `${samples.map((entry) => JSON.stringify(entry)).join("\n")}\n`);
	for (const [name, value] of Object.entries(written)) writeFileSync(join(dir, name), `${JSON.stringify(value, null, 2)}\n`);
	mutate({ dir, samples, written });
	return dir;
}

function writeSigningKey(dir) {
	const { privateKey } = generateKeyPairSync("ed25519");
	const keyPath = join(dir, "signing-key.pem");
	writeFileSync(keyPath, privateKey.export({ type: "pkcs8", format: "pem" }));
	return keyPath;
}

test("R0 harness：有效观察目录通过离线核验", () => {
	withFixture(undefined, (dir) => {
		const summary = verifyObservation(dir);
		assert.equal(summary.candidate, COMMIT);
		assert.equal(summary.status, "completed");
		assert.equal(summary.samples, SAMPLES);
		assert.equal(summary.expected_slots, SAMPLES);
		assert.equal(summary.schema_version, 75);
	});
});

test("R0 harness：单调计时回退会被拒绝", () => {
	withFixture(({ samples, dir }) => {
		samples[2].monotonic_ms = samples[1].monotonic_ms;
		writeFileSync(join(dir, "samples.ndjson"), `${samples.map((entry) => JSON.stringify(entry)).join("\n")}\n`);
	}, (dir) => {
		assert.throws(() => verifyObservation(dir), /单调计时/u);
	});
});

test("R0 harness：采样缺口与时间回退会被拒绝", () => {
	withFixture(({ samples, dir }) => {
		samples[1].at = new Date(START + 10_000).toISOString();
		samples[1].monotonic_ms = 10_100;
		writeFileSync(join(dir, "samples.ndjson"), `${samples.map((entry) => JSON.stringify(entry)).join("\n")}\n`);
	}, (dir) => {
		assert.throws(() => verifyObservation(dir), /采样间隔超限/u);
	});
	withFixture(({ samples, dir }) => {
		samples[2].at = samples[1].at;
		writeFileSync(join(dir, "samples.ndjson"), `${samples.map((entry) => JSON.stringify(entry)).join("\n")}\n`);
	}, (dir) => {
		assert.throws(() => verifyObservation(dir), /采样时间未递增/u);
	});
});

test("R0 harness：工作区变化、候选变更与失败终态会被拒绝", () => {
	withFixture(({ samples, dir }) => {
		samples[1].workspace_sha256 = "0".repeat(64);
		writeFileSync(join(dir, "samples.ndjson"), `${samples.map((entry) => JSON.stringify(entry)).join("\n")}\n`);
	}, (dir) => {
		assert.throws(() => verifyObservation(dir), /合成工作区内容发生变化/u);
	});
	withFixture(({ samples, dir }) => {
		samples[1].trial.circle = "R1";
		writeFileSync(join(dir, "samples.ndjson"), `${samples.map((entry) => JSON.stringify(entry)).join("\n")}\n`);
	}, (dir) => {
		assert.throws(() => verifyObservation(dir), /采样离开 R0/u);
	});
	withFixture(({ written, dir }) => {
		written["end.json"].status = "failed";
		writeFileSync(join(dir, "end.json"), `${JSON.stringify(written["end.json"], null, 2)}\n`);
	}, (dir) => {
		assert.throws(() => verifyObservation(dir), /观察未正常结束/u);
	});
});

test("R0 harness：完成观察缺少采样槽会被拒绝", () => {
	withFixture(({ samples, written, dir }) => {
		writeFileSync(join(dir, "samples.ndjson"), `${JSON.stringify(samples[0])}\n`);
		written["end.json"].samples = 1;
		writeFileSync(join(dir, "end.json"), `${JSON.stringify(written["end.json"], null, 2)}\n`);
	}, (dir) => {
		assert.throws(() => verifyObservation(dir), /采样不足/u);
	});
});

test("R0 harness：参数解析固定默认 72 小时并识别短时 dry-run", () => {
	const soak = parseWindow({});
	assert.equal(soak.durationMs, 72 * 60 * 60 * 1000);
	assert.equal(soak.sampleMs, 60 * 1000);
	assert.equal(soak.mode, "soak");
	const dry = parseWindow({ seconds: "30", "sample-seconds": "5" });
	assert.equal(dry.durationMs, 30_000);
	assert.equal(dry.sampleMs, 5_000);
	assert.equal(dry.mode, "dry-run");
	assert.throws(() => parseWindow({ seconds: "1", "sample-seconds": "2" }), /不能短于采样间隔/u);
	assert.throws(() => parseWindow({ "sample-seconds": "0.1" }), /之间/u);
	const parsed = parseFlags(["launch", "dir", "--seconds", "5", "--dry-run"]);
	assert.deepEqual(parsed.positional, ["launch", "dir"]);
	assert.equal(parsed.flags.seconds, "5");
	assert.equal(parsed.flags["dry-run"], true);
});

test("R0 harness：候选提交与包摘要必须匹配", () => {
	assert.throws(() => assertCandidate("abc"), /40 位/u);
	assertCandidate(COMMIT);
	const dir = mkdtempSync(join(tmpdir(), "r0-package-"));
	try {
		const tarball = join(dir, "pkg.tgz");
		writeFileSync(tarball, "candidate");
		const digest = sha256("candidate");
		assert.equal(assertTarball(tarball, digest).actual, digest);
		assert.throws(() => assertTarball(tarball, "0".repeat(64)), /SHA256 不匹配/u);
		assert.throws(() => assertTarball(join(dir, "missing.tgz"), digest), /候选包不存在/u);
	} finally {
		rmSync(dir, { recursive: true, force: true });
	}
});

test("R0 harness：严格核验拒绝 dry-run 与不足 72 小时的观察", () => {
	withFixture(undefined, (dir) => {
		assert.throws(() => verifyObservation(dir, { requireSoak: true }), /不满足 72 小时验收条件/u);
	});
	withFixture(({ written, dir }) => {
		written["launch.json"].mode = "soak";
		written["soak-config.json"].mode = "soak";
		writeFileSync(join(dir, "launch.json"), `${JSON.stringify(written["launch.json"], null, 2)}\n`);
		writeFileSync(join(dir, "soak-config.json"), `${JSON.stringify(written["soak-config.json"], null, 2)}\n`);
	}, (dir) => {
		assert.throws(() => verifyObservation(dir, { requireSoak: true }), /duration/u);
	});
});

test("R0 harness：72 小时观察生成签名验收报告并可离线复核", () => {
	const dir = soakFixture();
	try {
		const keyPath = writeSigningKey(dir);
		const report = signObservationReport(dir, { keyPath });
		assert.equal(report.verification.accepted, true);
		assert.ok(report.verification.checks.every((check) => check.ok));
		const payload = verifySignedReport(join(dir, "report.json"));
		assert.equal(payload.verification.accepted, true);
		assert.equal(payload.observation.launch.mode, "soak");
		assert.equal(payload.artifacts.samples_sha256.length, 64);

		const tampered = JSON.parse(readFileSync(join(dir, "report.json"), "utf8"));
		tampered.verification.accepted = true;
		tampered.verification.summary.duration_ms = 1;
		writeFileSync(join(dir, "report.json"), JSON.stringify(tampered));
		assert.throws(() => verifySignedReport(join(dir, "report.json")), /签名校验失败/u);
	} finally {
		rmSync(dir, { recursive: true, force: true });
	}
});

test("R0 harness：未运行 72 小时或出现异常时签名报告不得标记通过", () => {
	withFixture(undefined, (dir) => {
		const keyPath = writeSigningKey(dir);
		const report = signObservationReport(dir, { keyPath });
		assert.equal(report.verification.accepted, false);
		assert.ok(report.verification.checks.some((check) => check.id === "soak_mode" && !check.ok));
		assert.equal(verifySignedReport(join(dir, "report.json")).verification.accepted, false);
	});
	const dir = soakFixture(({ dir: target, samples }) => {
		samples[10].exceptions = { count: 1, last_message: "boom" };
		writeFileSync(join(target, "samples.ndjson"), `${samples.map((entry) => JSON.stringify(entry)).join("\n")}\n`);
	});
	try {
		const keyPath = writeSigningKey(dir);
		const report = signObservationReport(dir, { keyPath });
		assert.equal(report.verification.accepted, false);
		assert.ok(report.verification.checks.some((check) => check.id === "exceptions" && !check.ok));
	} finally {
		rmSync(dir, { recursive: true, force: true });
	}
});
