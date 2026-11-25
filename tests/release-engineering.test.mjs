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

test("发布工程：CI 覆盖七个必需作业", () => {
	const ci = read(".github/workflows/ci.yml");
	for (const name of ["Type Check", "Unit Test", "Integration Test", "Dependency Review", "Security Test", "Build", "Package Integrity"]) {
		assert.ok(ci.includes(`name: ${name}`), `CI 缺少作业 ${name}`);
	}
	assert.match(ci, /pnpm install --frozen-lockfile/u);
	assert.match(ci, /pnpm(?: run)? audit/u);
	assert.match(ci, /pnpm verify:package/u);
	assert.match(ci, /verify-package\.mjs/u);
	assert.match(ci, /dependency-review-action@v4/u);
	assert.match(ci, /fail-on-severity: high/u);
	assert.match(ci, /sha256sum -c SHA256SUMS/u);
});

test("发布工程：标签发布流水线生成来源证明并上传完整产物", () => {
	const release = read(".github/workflows/release.yml");
	assert.match(release, /tags: \["v\*"\]/u);
	assert.match(release, /pnpm install --frozen-lockfile/u);
	assert.match(release, /pnpm check/u);
	assert.match(release, /attest-build-provenance@v2/u);
	assert.match(release, /id-token: write/u);
	assert.match(release, /attestations: write/u);
	assert.match(release, /gh release create/u);
	for (const asset of ["SHA256SUMS", "build-info.json", "sbom.cdx.json", "package-files.json"]) {
		assert.ok(release.includes(asset), `发布流水线缺少产物 ${asset}`);
	}
	assert.doesNotMatch(release, /@main\b/u);
});

test("发布工程：完整性校验覆盖文件级清单、锁文件与 SBOM", () => {
	const verify = read("scripts/verify-package.mjs");
	const build = read("scripts/build-package.mjs");
	assert.match(verify, /package-files\.json/u);
	assert.match(verify, /lockfile_sha256/u);
	assert.match(verify, /CycloneDX/u);
	assert.match(verify, /GITHUB_SHA/u);
	assert.match(verify, /dist 含未登记文件/u);
	assert.match(build, /package_files:/u);
	assert.match(build, /schema_version: 2/u);
});
