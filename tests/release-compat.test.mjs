import test from "node:test";
import assert from "node:assert/strict";
import { readFileSync, readdirSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

import { version } from "../lib/index.js";

const ROOT = join(dirname(fileURLToPath(import.meta.url)), "..");

test("发布元数据：版本、引擎范围与打包清单一致", () => {
	const pkg = JSON.parse(readFileSync(join(ROOT, "package.json"), "utf8"));
	assert.equal(pkg.version, version);
	assert.equal(pkg.engines.dsh, ">=0.2.2-0 <0.2.3-0");
	assert.equal(pkg.engines.node, "^22.19.0 || >=24.0.0");
	assert.equal(pkg.dependencies["proper-lockfile"], "4.1.2");
	assert.equal(pkg.dependencies.zod, "4.4.3");
	for (const [name, range] of Object.entries(pkg.peerDependencies)) {
		if (name.startsWith("@deepseek-ai/dsh-")) assert.equal(range, ">=0.2.2-0 <0.2.3-0", `宿主依赖范围被改动: ${name}`);
	}
	assert.equal(pkg.peerDependencies["@deepseek-ai/cordis"], "^4.1.0");
	assert.ok(pkg.files.includes("CHANGELOG.md"));
	assert.ok(pkg.files.includes("docs/upgrade-downgrade.md"));
	assert.ok(pkg.files.includes("scripts/orchestra-persistence.mjs"));
});

test("插件 Bundle 包装与 Profile 配置可解析", () => {
	const client = readFileSync(join(ROOT, "lib", "client.js"), "utf8");
	assert.ok(client.startsWith('window.__ModuleLoader__.load({'));
	assert.match(client, /id: "dsh-orchestra"/);
	assert.match(client, /factory: \(require\) =>/);
	const config = JSON.parse(readFileSync(join(ROOT, "orchestra.config.json"), "utf8"));
	assert.equal(typeof config.phase, "string");
	assert.match(config.phase, /^S[1-6]$/);
	const patch = readFileSync(join(ROOT, "cordis.patch.yml"), "utf8");
	assert.match(patch, /id: dsh-orchestra/);
});

test("数据库迁移链连续，最后一项为 074", () => {
	const files = readdirSync(join(ROOT, "lib", "infra", "sqlite", "migrations"))
		.filter((name) => name.endsWith(".sql"))
		.sort();
	assert.equal(files.length, 74);
	files.forEach((file, index) => {
		assert.equal(Number(file.slice(0, 3)), index + 1, `迁移编号不连续: ${file}`);
	});
	assert.equal(files.at(-1), "074_frontend_monitoring.sql");
});
