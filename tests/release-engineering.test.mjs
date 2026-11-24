import test from "node:test";
import assert from "node:assert/strict";
import { existsSync, readFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

const ROOT = join(dirname(fileURLToPath(import.meta.url)), "..");
const read = (path) => readFileSync(join(ROOT, path), "utf8");
const pkg = JSON.parse(read("package.json"));

test("发布工程：锁文件固定，且不自动安装 DSH 宿主 peer", () => {
	assert.ok(existsSync(join(ROOT, "pnpm-lock.yaml")));
	const lock = read("pnpm-lock.yaml");
	assert.match(lock, /lockfileVersion: '9\.0'/u);
	assert.match(lock, /autoInstallPeers: false/u);
	assert.equal(lock.includes("@deepseek-ai"), false);
	const workspace = read("pnpm-workspace.yaml");
	assert.match(workspace, /autoInstallPeers: false/u);
	assert.match(read(".npmrc"), /engine-strict=true/u);
});

test("发布工程：脚本不再依赖缺失的 TypeScript 源码", () => {
	assert.match(pkg.scripts.build, /build-package\.mjs/u);
	assert.match(pkg.scripts.typecheck, /type-surface-check\.mjs/u);
	assert.doesNotMatch(pkg.scripts.build, /tsdown|tsc/u);
	assert.doesNotMatch(pkg.scripts.typecheck, /tsdown|tsc/u);
	assert.equal(pkg.scripts.gate, "pnpm check");
	assert.ok(existsSync(join(ROOT, "scripts", "build-package.mjs")));
	assert.ok(existsSync(join(ROOT, "scripts", "verify-package.mjs")));
	assert.ok(existsSync(join(ROOT, "scripts", "type-surface-check.mjs")));
});

test("发布工程：CI 覆盖六个必需作业", () => {
	const ci = read(".github/workflows/ci.yml");
	for (const name of ["Type Check", "Unit Test", "Integration Test", "Security Test", "Build", "Package Integrity"]) {
		assert.ok(ci.includes(`name: ${name}`), `CI 缺少作业 ${name}`);
	}
	assert.match(ci, /pnpm install --frozen-lockfile/u);
	assert.match(ci, /pnpm(?: run)? audit/u);
	assert.match(ci, /pnpm verify:package/u);
	assert.match(ci, /verify-package\.mjs/u);
});
