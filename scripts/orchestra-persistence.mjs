#!/usr/bin/env node
// dsh-orchestra 持久化维护接口。
// 用法：
//   node scripts/orchestra-persistence.mjs list   [--dir <personal 数据目录>]
//   node scripts/orchestra-persistence.mjs status --db <db.sqlite>
//   node scripts/orchestra-persistence.mjs verify --db <db.sqlite> [--quick]
//   node scripts/orchestra-persistence.mjs backup --db <db.sqlite> [--out <目录>] [--keep N]
//   node scripts/orchestra-persistence.mjs export --db <db.sqlite> [--out <orchestra.json>] [--keep N]
//   node scripts/orchestra-persistence.mjs restore --db <db.sqlite> --from <manifest.json|orchestra.json> [--keep N]
//   node scripts/orchestra-persistence.mjs migrate-json --file <orchestra.json>
//   node scripts/orchestra-persistence.mjs snapshots --dir <备份目录>
//
// 安全约定：所有 DB 维护操作要求 DSH 已停止；工具会检查 <db>.lock 并创建 <db>.maintenance。
// 任何失败都保留原始库；恢复前自动做 pre-restore 快照。

import { existsSync, readdirSync, statSync } from "node:fs";
import { homedir } from "node:os";
import { join, resolve } from "node:path";
import {
	EXPORT_KIND,
	PersistenceError,
	acquireFileLockSync,
	readJsonSync,
	readOrUpgradeJsonSync
} from "../lib/persistence/json-store.js";
import {
	acquireMaintenanceLockSync,
	assertDatabaseIntegritySync,
	backupDirectoryFor,
	createDatabaseSnapshotSync,
	exportDatabaseJsonSync,
	importDatabaseJsonSync,
	inspectMigrationsSync,
	loadMigrationStepsSync,
	readSnapshotManifestSync,
	restoreSnapshotManifestSync
} from "../lib/persistence/sqlite-backup.js";

function parse(argv) {
	const [command = "help", ...rest] = argv;
	const flags = {};
	for (let i = 0; i < rest.length; i += 1) {
		const token = rest[i];
		if (!token.startsWith("--")) throw new Error(`无法识别的参数：${token}`);
		const equal = token.indexOf("=");
		if (equal >= 0) {
			flags[token.slice(2, equal)] = token.slice(equal + 1);
			continue;
		}
		const key = token.slice(2);
		const next = rest[i + 1];
		if (next !== undefined && !next.startsWith("--")) {
			flags[key] = next;
			i += 1;
		} else flags[key] = true;
	}
	return { command, flags };
}

function requireFlag(flags, name) {
	if (!flags[name] || flags[name] === true) throw new Error(`缺少参数 --${name}`);
	return String(flags[name]);
}

function numberFlag(flags, name, fallback) {
	if (flags[name] === undefined) return fallback;
	const value = Number(flags[name]);
	if (!Number.isFinite(value) || value <= 0) throw new Error(`--${name} 必须是正数`);
	return value;
}

function personalDataDir(flags) {
	return resolve(String(flags.dir ?? process.env.DSH_ORCHESTRA_DATA_DIR ?? join(process.env.DSH_HOME || join(homedir(), ".dsh"), "orchestra", "personal")));
}

function print(value) {
	process.stdout.write(JSON.stringify(value, null, 2) + "\n");
}

function withDatabaseLock(dbFile, fn) {
	const lock = acquireMaintenanceLockSync(dbFile);
	try {
		return fn();
	} finally {
		lock.release();
	}
}

function main() {
	const { command, flags } = parse(process.argv.slice(2));
	switch (command) {
		case "list": {
			const dir = personalDataDir(flags);
			if (!existsSync(dir)) return print({ dir, databases: [] });
			const databases = readdirSync(dir)
				.filter((name) => name.endsWith(".sqlite"))
				.map((name) => {
					const file = join(dir, name);
					const stat = statSync(file);
					return { file, bytes: stat.size, modifiedAt: stat.mtime.toISOString() };
				});
			return print({ dir, databases });
		}
		case "status": {
			const dbFile = resolve(requireFlag(flags, "db"));
			const steps = loadMigrationStepsSync();
			const state = inspectMigrationsSync(dbFile, steps);
			const backupDir = backupDirectoryFor(dbFile);
			const snapshots = existsSync(join(backupDir, "snapshots"))
				? readdirSync(join(backupDir, "snapshots")).filter((name) => name.endsWith(".sqlite")).length
				: 0;
			const jsonVersions = existsSync(join(backupDir, "json"))
				? readdirSync(join(backupDir, "json")).filter((name) => name.endsWith(".json")).length
				: 0;
			return print({
				dbFile,
				exists: state.exists,
				integrity: state.exists ? "ok" : "missing",
				dbSchemaVersion: state.maxVersion,
				latestMigration: state.latest,
				pendingMigrations: state.pending.map((step) => step.version),
				rollingSnapshots: snapshots,
				rollingJsonVersions: jsonVersions,
				backupDir
			});
		}
		case "verify": {
			const dbFile = resolve(requireFlag(flags, "db"));
			assertDatabaseIntegritySync(dbFile, { quick: flags.quick === true });
			return print({ ok: true, dbFile, mode: flags.quick === true ? "quick_check" : "integrity_check" });
		}
		case "backup": {
			const dbFile = resolve(requireFlag(flags, "db"));
			const out = resolve(String(flags.out ?? join(backupDirectoryFor(dbFile), "snapshots")));
			const keep = numberFlag(flags, "keep", 10);
			return withDatabaseLock(dbFile, () => {
				const result = createDatabaseSnapshotSync(dbFile, out, { keep, label: "manual" });
				return print(result);
			});
		}
		case "export": {
			const dbFile = resolve(requireFlag(flags, "db"));
			const out = resolve(String(flags.out ?? join(backupDirectoryFor(dbFile), "orchestra.json")));
			const keep = numberFlag(flags, "keep", 10);
			return withDatabaseLock(dbFile, () => print(exportDatabaseJsonSync(dbFile, out, { keep })));
		}
		case "restore": {
			const dbFile = resolve(requireFlag(flags, "db"));
			const from = resolve(requireFlag(flags, "from"));
			const keep = numberFlag(flags, "keep", 10);
			return withDatabaseLock(dbFile, () => {
				const backupDir = backupDirectoryFor(dbFile);
				const document = readJsonSync(from);
				if (document?.kind === EXPORT_KIND || document?.tables) {
					return print({ ok: true, kind: EXPORT_KIND, ...importDatabaseJsonSync(from, dbFile, { keep }) });
				}
				const manifest = readSnapshotManifestSync(from, { backupDir });
				const result = restoreSnapshotManifestSync(from, dbFile, backupDir, { reason: "cli-restore" });
				return print({ ok: true, kind: manifest.kind, dbFile, result });
			});
		}
		case "restore-json": {
			const dbFile = resolve(requireFlag(flags, "db"));
			const from = resolve(requireFlag(flags, "from"));
			const keep = numberFlag(flags, "keep", 10);
			return withDatabaseLock(dbFile, () => print(importDatabaseJsonSync(from, dbFile, { keep })));
		}
		case "migrate-json": {
			const file = resolve(requireFlag(flags, "file"));
			const lock = acquireFileLockSync(file);
			try {
				const result = readOrUpgradeJsonSync(file, { backupDir: join(file, "..", "backups", "json") });
				return print({ ok: true, file, from: result.from, to: result.to, applied: result.applied });
			} finally {
				lock.release();
			}
		}
		case "snapshots": {
			const dir = resolve(requireFlag(flags, "dir"));
			if (!existsSync(dir)) return print({ dir, snapshots: [] });
			const snapshots = readdirSync(dir)
				.filter((name) => name.endsWith(".json"))
				.map((name) => {
					const file = join(dir, name);
					try {
						return { manifest: file, ...readSnapshotManifestSync(file) };
					} catch (error) {
						return { manifest: file, error: error.message };
					}
				});
			return print({ dir, snapshots });
		}
		default:
			process.stderr.write("未知命令。支持：list/status/verify/backup/export/restore/restore-json/migrate-json/snapshots\n");
			return process.exitCode = 2;
	}
}

try {
	main();
} catch (error) {
	if (error instanceof PersistenceError) {
		process.stderr.write(`[${error.code}] ${error.message}\n`);
		if (Object.keys(error.details ?? {}).length) process.stderr.write(JSON.stringify(error.details, null, 2) + "\n");
		process.exitCode = error.code === "STORE_LOCKED" || error.code === "STORE_MAINTENANCE" ? 3 : 1;
	} else {
		process.stderr.write(`${error?.stack ?? error}\n`);
		process.exitCode = 1;
	}
}
