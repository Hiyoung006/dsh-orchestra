import assert from "node:assert/strict";
import { chmodSync, existsSync, mkdtempSync, readFileSync, readdirSync, writeFileSync, writeSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { spawn, spawnSync } from "node:child_process";
import { DatabaseSync } from "node:sqlite";
import { test } from "node:test";
import { fileURLToPath } from "node:url";

import {
	EXPORT_KIND,
	acquireFileLockSync,
	migrateJsonDocument,
	readJsonSync,
	readOrUpgradeJsonSync,
	writeFileAtomicSync,
	writeJsonAtomicSync
} from "../lib/persistence/json-store.js";
import {
	assertDatabaseIntegritySync,
	acquireMaintenanceLockSync,
	assertNoMaintenanceLockSync,
	backupDirectoryFor,
	createDatabaseSnapshotSync,
	exportDatabaseJsonSync,
	importDatabaseJsonSync,
	inspectMigrationsSync,
	prepareServiceStorageSync,
	restoreSnapshotManifestSync
} from "../lib/persistence/sqlite-backup.js";

const here = fileURLToPath(new URL(".", import.meta.url));

function tempDir(prefix = "dsh-persist-") {
	return mkdtempSync(join(tmpdir(), prefix));
}

function makeDb(file, { withData = true } = {}) {
	const db = new DatabaseSync(file);
	db.exec("PRAGMA journal_mode=WAL");
	db.exec("CREATE TABLE IF NOT EXISTS service_meta (key TEXT PRIMARY KEY, value TEXT NOT NULL, updated_at TEXT NOT NULL)");
	db.exec("CREATE TABLE IF NOT EXISTS schema_migrations (version INTEGER PRIMARY KEY, name TEXT NOT NULL, checksum TEXT NOT NULL, applied_at TEXT NOT NULL)");
	db.exec("CREATE TABLE IF NOT EXISTS notes (id INTEGER PRIMARY KEY, text TEXT NOT NULL, payload BLOB)");
	if (withData) db.prepare("INSERT INTO notes (id, text, payload) VALUES (?,?,?)").run(1, "hello", Buffer.from([1, 2, 3]));
	db.close();
}

const TEST_STEPS = [{
	version: 1,
	name: "test",
	checksum: "checksum-v1",
	statements: [
		"CREATE TABLE IF NOT EXISTS notes (id INTEGER PRIMARY KEY, text TEXT NOT NULL, payload BLOB)"
	]
}];

test("正常读取和写入", () => {
	const dir = tempDir();
	const file = join(dir, "orchestra.json");
	writeJsonAtomicSync(file, {
		schemaVersion: 2,
		kind: EXPORT_KIND,
		dbSchemaVersion: 1,
		tableColumns: { notes: ["id", "text"] },
		tables: { notes: [{ id: 1, text: "a" }] }
	});
	const read = readJsonSync(file);
	assert.equal(read.schemaVersion, 2);
	assert.equal(read.tables.notes[0].text, "a");
	const upgraded = readOrUpgradeJsonSync(file, { backupDir: join(dir, "backups") });
	assert.equal(upgraded.to, 2);
	assert.deepEqual(upgraded.applied, []);
});

test("JSON 文件损坏：拒绝覆盖、隔离副本、原文件不变", () => {
	const dir = tempDir();
	const file = join(dir, "orchestra.json");
	const broken = '{"schemaVersion":2,"tables":{';
	writeFileSync(file, broken);
	assert.throws(
		() => readOrUpgradeJsonSync(file, { backupDir: join(dir, "backups") }),
		(error) => error.code === "STORE_CORRUPT"
	);
	assert.equal(readFileSync(file, "utf8"), broken);
	const corruptDir = join(dir, "backups", "corrupt");
	const quarantined = readdirSync(corruptDir);
	assert.ok(quarantined.some((name) => name.endsWith("orchestra.json")));
	assert.ok(quarantined.some((name) => name.endsWith(".error.txt")));
});

test("磁盘写入失败：目标文件保持原值、临时文件被清理", () => {
	const dir = tempDir();
	const file = join(dir, "orchestra.json");
	writeJsonAtomicSync(file, { ok: true });
	const before = readFileSync(file, "utf8");
	const enospc = new Error("no space left on device");
	enospc.code = "ENOSPC";
	assert.throws(
		() => writeJsonAtomicSync(file, { ok: false }, {
			writeImpl(fd, buffer) {
				// 先写一半，再模拟磁盘满
				writeFileSync(fd, buffer.subarray(0, Math.max(1, Math.floor(buffer.length / 2))));
				throw enospc;
			}
		}),
		(error) => error.code === "ENOSPC"
	);
	assert.equal(readFileSync(file, "utf8"), before);
	assert.ok(!readdirSync(dir).some((name) => name.endsWith(".tmp")));
});

test("写入瞬时失败会重试，最终成功", () => {
	const dir = tempDir();
	const file = join(dir, "orchestra.json");
	let attempts = 0;
	writeJsonAtomicSync(file, { ok: true }, {
		writeImpl(fd, buffer) {
			attempts += 1;
			if (attempts === 1) {
				const busy = new Error("resource temporarily unavailable");
				busy.code = "EBUSY";
				throw busy;
			}
			writeSync(fd, buffer);
		}
	});
	assert.equal(attempts, 2);
	assert.deepEqual(readJsonSync(file), { ok: true });
	assert.ok(!readdirSync(dir).some((name) => name.endsWith(".tmp")));
});

test("文件权限不足：明确失败且不产生半成品", { skip: process.getuid?.() === 0 }, () => {
	const dir = tempDir();
	const file = join(dir, "orchestra.json");
	writeJsonAtomicSync(file, { ok: true });
	const before = readFileSync(file, "utf8");
	try {
		chmodSync(dir, 0o500);
		assert.throws(
			() => writeJsonAtomicSync(file, { ok: false }),
			(error) => error.code === "EACCES" || error.code === "EPERM"
		);
	} finally {
		chmodSync(dir, 0o700);
	}
	assert.equal(readFileSync(file, "utf8"), before);
	assert.ok(!readdirSync(dir).some((name) => name.endsWith(".tmp")));
});

test("两个进程同时写入：文件锁串行化，不丢更新", async () => {
	const dir = tempDir();
	const file = join(dir, "counter.json");
	writeJsonAtomicSync(file, { counter: 0, writers: [] });
	const worker = join(here, "fixtures", "lock-worker.mjs");
	const run = (id) => new Promise((resolve, reject) => {
		const child = spawn(process.execPath, [worker, file, id], { stdio: ["ignore", "pipe", "pipe"] });
		let stderr = "";
		child.stderr.on("data", (chunk) => {
			stderr += chunk;
		});
		child.on("error", reject);
		child.on("exit", (code) => code === 0 ? resolve() : reject(new Error(`worker ${id} exit ${code}: ${stderr}`)));
	});
	await Promise.all([run("a"), run("b")]);
	const doc = readJsonSync(file);
	assert.equal(doc.counter, 2);
	assert.deepEqual([...doc.writers].sort(), ["a", "b"]);
});

test("写入过程中被中断：残留锁可接管，目标文件不被破坏", async () => {
	const dir = tempDir();
	const file = join(dir, "counter.json");
	writeJsonAtomicSync(file, { counter: 7 });
	const worker = join(here, "fixtures", "interrupt-worker.mjs");
	const result = spawnSync(process.execPath, [worker, file]);
	assert.equal(result.status, 9);
	assert.deepEqual(readJsonSync(file), { counter: 7 });
	const lock = acquireFileLockSync(file, { staleMs: 50, timeoutMs: 2000 });
	try {
		const doc = readJsonSync(file);
		doc.counter += 1;
		writeJsonAtomicSync(file, doc);
	} finally {
		lock.release();
	}
	assert.deepEqual(readJsonSync(file), { counter: 8 });
});

test("旧版本数据升级：保留数据并生成滚动备份", () => {
	const dir = tempDir();
	const file = join(dir, "orchestra.json");
	writeFileSync(file, JSON.stringify({
		schemaVersion: 1,
		generatedAt: "2026-01-01T00:00:00.000Z",
		data: { tables: { notes: [{ id: 1, text: "legacy" }] } }
	}));
	const result = readOrUpgradeJsonSync(file, { backupDir: join(dir, "backups"), keep: 3 });
	assert.equal(result.from, 1);
	assert.equal(result.to, 2);
	assert.deepEqual(result.applied, [2]);
	const upgraded = readJsonSync(file);
	assert.equal(upgraded.schemaVersion, 2);
	assert.equal(upgraded.tables.notes[0].text, "legacy");
	assert.equal(upgraded.kind, EXPORT_KIND);
	const backups = readdirSync(join(dir, "backups")).filter((name) => name.endsWith(".json"));
	assert.equal(backups.length, 1);
	assert.equal(readJsonSync(join(dir, "backups", backups[0])).schemaVersion, 1);
});

test("版本高于当前支持：拒绝读取，不覆盖", () => {
	const dir = tempDir();
	const file = join(dir, "orchestra.json");
	const future = JSON.stringify({ schemaVersion: 99, kind: EXPORT_KIND, tables: {} });
	writeFileSync(file, future);
	assert.throws(() => migrateJsonDocument(JSON.parse(future)), (error) => error.code === "STORE_SCHEMA_UNSUPPORTED");
	assert.throws(() => readOrUpgradeJsonSync(file, { backupDir: join(dir, "backups") }), (error) => error.code === "STORE_SCHEMA_UNSUPPORTED");
	assert.equal(readFileSync(file, "utf8"), future);
});

test("SQLite 快照滚动保留 N 份，并带 schemaVersion", () => {
	const dir = tempDir();
	const dbFile = join(dir, "orchestra.sqlite");
	makeDb(dbFile);
	const backupDir = join(dir, "backups", "snapshots");
	for (let i = 0; i < 4; i += 1) createDatabaseSnapshotSync(dbFile, backupDir, { keep: 2, dbSchemaVersion: 1, label: `n${i}` });
	const files = readdirSync(backupDir).filter((name) => name.endsWith(".sqlite"));
	const manifests = readdirSync(backupDir).filter((name) => name.endsWith(".json"));
	assert.equal(files.length, 2);
	assert.equal(manifests.length, 2);
	const manifest = readJsonSync(join(backupDir, manifests[0]));
	assert.equal(manifest.schemaVersion, 2);
	assert.equal(manifest.kind, "dsh-orchestra-snapshot");
	assert.match(manifest.snapshot.sha256, /^[a-f0-9]{64}$/);
});

test("SQLite 逻辑导出/恢复：BLOB 与旧数据保留", () => {
	const dir = tempDir();
	const dbFile = join(dir, "orchestra.sqlite");
	makeDb(dbFile);
	const out = join(dir, "orchestra.json");
	const exported = exportDatabaseJsonSync(dbFile, out, { keep: 3 });
	assert.equal(exported.tables >= 1, true);
	const doc = readJsonSync(out);
	assert.equal(doc.schemaVersion, 2);
	assert.equal(doc.kind, EXPORT_KIND);
	assert.equal(doc.tables.notes[0].payload.__dsh_blob_base64__, Buffer.from([1, 2, 3]).toString("base64"));

	// 改动数据库，再从 JSON 恢复
	const db = new DatabaseSync(dbFile);
	db.prepare("INSERT INTO notes (id, text) VALUES (?,?)").run(2, "mutated");
	db.close();
	importDatabaseJsonSync(out, dbFile, { steps: TEST_STEPS, keep: 3 });
	const check = new DatabaseSync(dbFile);
	const rows = check.prepare("SELECT id, text, payload FROM notes ORDER BY id").all();
	check.close();
	assert.equal(rows.length, 1);
	assert.equal(rows[0].text, "hello");
	assert.deepEqual([...rows[0].payload], [1, 2, 3]);
	assert.ok(existsSync(join(backupDirectoryFor(dbFile), "snapshots")));
	assert.ok(readdirSync(join(backupDirectoryFor(dbFile), "corrupt")).length > 0);
});

test("迁移失败：原始库回滚，失败库与快照都保留", () => {
	const dir = tempDir();
	const dbFile = join(dir, "orchestra.sqlite");
	makeDb(dbFile);
	const db0 = new DatabaseSync(dbFile);
	db0.prepare("INSERT INTO schema_migrations (version, name, checksum, applied_at) VALUES (?,?,?,?)")
		.run(1, "test", "checksum-v1", new Date().toISOString());
	db0.close();
	const steps = [
		TEST_STEPS[0],
		{ version: 2, name: "next", checksum: "checksum-v2", statements: ["CREATE TABLE IF NOT EXISTS added (id INTEGER)"] }
	];
	const guard = prepareServiceStorageSync({ dbFile, steps, backupRoot: join(dir, "backups"), keep: 5 });
	assert.ok(guard.snapshot);
	// 模拟迁移执行了一半后失败
	const db1 = new DatabaseSync(dbFile);
	db1.exec("CREATE TABLE added (id INTEGER)");
	db1.prepare("INSERT INTO notes (id, text) VALUES (?,?)").run(2, "partial");
	db1.close();
	guard.rollback(new Error("migration boom"));
	const check = new DatabaseSync(dbFile);
	const tables = check.prepare("SELECT name FROM sqlite_master WHERE type='table'").all().map((row) => row.name);
	const rows = check.prepare("SELECT id FROM notes ORDER BY id").all();
	check.close();
	assert.ok(!tables.includes("added"));
	assert.deepEqual(rows.map((row) => row.id), [1]);
	const corrupt = readdirSync(join(dir, "backups", "corrupt"));
	assert.ok(corrupt.length > 0);
});

test("SQLite 完整性校验与快照恢复接口", () => {
	const dir = tempDir();
	const dbFile = join(dir, "orchestra.sqlite");
	makeDb(dbFile);
	assert.equal(assertDatabaseIntegritySync(dbFile), true);
	const snapshot = createDatabaseSnapshotSync(dbFile, join(dir, "backups", "snapshots"), { keep: 3, dbSchemaVersion: 1 });
	const db = new DatabaseSync(dbFile);
	db.prepare("INSERT INTO notes (id, text) VALUES (?,?)").run(2, "after-snapshot");
	db.close();
	restoreSnapshotManifestSync(snapshot.manifestPath, dbFile, join(dir, "backups"), { reason: "test" });
	const check = new DatabaseSync(dbFile);
	const rows = check.prepare("SELECT id FROM notes ORDER BY id").all();
	check.close();
	assert.deepEqual(rows.map((row) => row.id), [1]);
});

test("迁移状态检查：识别待执行与更新版本", () => {
	const dir = tempDir();
	const dbFile = join(dir, "orchestra.sqlite");
	makeDb(dbFile);
	const db = new DatabaseSync(dbFile);
	db.prepare("INSERT INTO schema_migrations (version, name, checksum, applied_at) VALUES (?,?,?,?)")
		.run(1, "test", "checksum-v1", new Date().toISOString());
	db.close();
	const state = inspectMigrationsSync(dbFile, TEST_STEPS);
	assert.equal(state.maxVersion, 1);
	assert.equal(state.pending.length, 0);
	assert.equal(state.newerThanCode, false);
	const newer = inspectMigrationsSync(dbFile, [{ version: 1, checksum: "checksum-v1" }]);
	assert.equal(newer.newerThanCode, false);
});

test("迁移记录不连续：拒绝升级且不产生快照", () => {
	const dir = tempDir();
	const dbFile = join(dir, "orchestra.sqlite");
	makeDb(dbFile);
	const db = new DatabaseSync(dbFile);
	const insert = db.prepare("INSERT INTO schema_migrations (version, name, checksum, applied_at) VALUES (?,?,?,?)");
	insert.run(1, "a", "c1", new Date().toISOString());
	insert.run(3, "c", "c3", new Date().toISOString());
	db.close();
	const steps = [
		{ version: 1, name: "a", checksum: "c1" },
		{ version: 2, name: "b", checksum: "c2" },
		{ version: 3, name: "c", checksum: "c3" }
	];
	assert.throws(
		() => prepareServiceStorageSync({ dbFile, steps, backupRoot: join(dir, "backups") }),
		(error) => error.code === "SCHEMA_VERSION_UNSUPPORTED" && error.details.gaps.includes(2)
	);
	assert.ok(!existsSync(join(dir, "backups", "snapshots")));
});

test("恢复被中断：缺失库时拒绝静默新建", () => {
	const dir = tempDir();
	const dbFile = join(dir, "orchestra.sqlite");
	const marker = `${dbFile}.restore-in-progress.json`;
	writeJsonAtomicSync(marker, { kind: "snapshot-restore", startedAt: new Date().toISOString() });
	assert.throws(
		() => prepareServiceStorageSync({ dbFile, steps: [], backupRoot: join(dir, "backups") }),
		(error) => error.code === "DB_RESTORE_INCOMPLETE"
	);
	assert.ok(!existsSync(dbFile));
	makeDb(dbFile);
	assert.doesNotThrow(() => prepareServiceStorageSync({ dbFile, steps: [], backupRoot: join(dir, "backups") }));
	assert.ok(!existsSync(marker));
});

test("维护锁：放弃的锁可自动接管，活进程的锁会阻止启动", () => {
	const dir = tempDir();
	const dbFile = join(dir, "orchestra.sqlite");
	makeDb(dbFile);
	const maintenance = `${dbFile}.maintenance`;
	writeFileSync(maintenance, JSON.stringify({ pid: 999999, at: new Date().toISOString() }));
	assert.doesNotThrow(() => assertNoMaintenanceLockSync(dbFile));
	assert.ok(!existsSync(maintenance));
	const lock = acquireMaintenanceLockSync(dbFile);
	try {
		assert.throws(() => assertNoMaintenanceLockSync(dbFile), (error) => error.code === "STORE_MAINTENANCE");
	} finally {
		lock.release();
	}
	assert.ok(!existsSync(maintenance));
});

test("CLI 接口：status / backup / export / verify 可运行", () => {
	const dir = tempDir();
	const dbFile = join(dir, "orchestra.sqlite");
	makeDb(dbFile);
	const cli = join(here, "..", "scripts", "orchestra-persistence.mjs");
	const status = spawnSync(process.execPath, [cli, "status", "--db", dbFile], { encoding: "utf8" });
	assert.equal(status.status, 0, status.stderr);
	assert.equal(JSON.parse(status.stdout).dbSchemaVersion, 0);
	const backup = spawnSync(process.execPath, [cli, "backup", "--db", dbFile, "--keep", "2"], { encoding: "utf8" });
	assert.equal(backup.status, 0, backup.stderr);
	assert.ok(JSON.parse(backup.stdout).manifestPath.includes("orchestra-"));
	const exported = spawnSync(process.execPath, [cli, "export", "--db", dbFile, "--out", join(dir, "orchestra.json")], { encoding: "utf8" });
	assert.equal(exported.status, 0, exported.stderr);
	assert.ok(existsSync(join(dir, "orchestra.json")));
	const verify = spawnSync(process.execPath, [cli, "verify", "--db", dbFile], { encoding: "utf8" });
	assert.equal(verify.status, 0, verify.stderr);
});
