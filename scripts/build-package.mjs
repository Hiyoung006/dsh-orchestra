// 可复现打包：生成 tarball、SHA256、SBOM 和关联 Git commit 的 build-info。
import { createHash } from "node:crypto";
import { spawnSync } from "node:child_process";
import { existsSync, mkdirSync, readFileSync, readdirSync, rmSync, writeFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

const ROOT = join(dirname(fileURLToPath(import.meta.url)), "..");
const DIST = join(ROOT, "dist");
const REPEAT = join(DIST, "repeat");
const pkg = JSON.parse(readFileSync(join(ROOT, "package.json"), "utf8"));

function run(command, args, options = {}) {
	const result = spawnSync(command, args, { cwd: ROOT, encoding: "utf8", ...options });
	if (result.status !== 0) throw new Error(`${command} ${args.join(" ")} 失败：${String(result.stderr || result.stdout).trim()}`);
	return String(result.stdout ?? "").trim();
}

function sha256File(path) {
	return createHash("sha256").update(readFileSync(path)).digest("hex");
}

function tarballFiles(path) {
	return run("tar", ["-tzf", path]).split("\n").filter(Boolean).sort();
}

function tarballManifest(path, files) {
	const manifest = [];
	for (const file of files) {
		if (file.endsWith("/")) continue;
		const content = spawnSync("tar", ["-xOzf", path, file], { cwd: ROOT, maxBuffer: 64 * 1024 * 1024 });
		if (content.status !== 0) throw new Error(`无法读取 tarball 成员 ${file}`);
		manifest.push({ path: file, bytes: content.stdout.length, sha256: createHash("sha256").update(content.stdout).digest("hex") });
	}
	return manifest;
}

function manifestDigest(manifest) {
	const digest = createHash("sha256");
	for (const entry of manifest) digest.update(`${entry.path}\0${entry.bytes}\0${entry.sha256}\n`);
	return digest.digest("hex");
}

function collectComponents(projects) {
	const components = new Map();
	const visit = (dependencies) => {
		if (!dependencies || typeof dependencies !== "object") return;
		for (const [name, value] of Object.entries(dependencies)) {
			const version = typeof value === "string" ? value : value?.version;
			if (typeof version === "string") components.set(`${name}@${version}`, { name, version });
			visit(value?.dependencies);
		}
	};
	for (const project of Array.isArray(projects) ? projects : []) visit(project.dependencies);
	return [...components.values()].sort((a, b) => `${a.name}@${a.version}`.localeCompare(`${b.name}@${b.version}`));
}

function writeSbom(lockfileSha256) {
	const raw = run("pnpm", ["list", "--json", "--prod", "--depth", "Infinity"]);
	const components = collectComponents(JSON.parse(raw));
	const bom = {
		bomFormat: "CycloneDX",
		specVersion: "1.5",
		version: 1,
		metadata: {
			component: {
				type: "application",
				name: pkg.name,
				version: pkg.version
			},
			properties: [
				{ name: "dsh-orchestra:lockfile_sha256", value: lockfileSha256 }
			]
		},
		components: components.map((component) => ({
			type: "library",
			name: component.name,
			version: component.version,
			purl: `pkg:npm/${encodeURIComponent(component.name)}@${component.version}`
		}))
	};
	writeFileSync(join(DIST, "sbom.cdx.json"), `${JSON.stringify(bom, null, 2)}\n`);
	return components.length;
}

const dirty = run("git", ["status", "--porcelain"]).length > 0;
if (dirty && process.env.CI !== "true") console.warn("警告：工作区不干净，build-info 会标记 dirty=true。");

rmSync(DIST, { recursive: true, force: true });
mkdirSync(REPEAT, { recursive: true });
const commit = process.env.GITHUB_SHA || run("git", ["rev-parse", "HEAD"]);
const lockfilePath = join(ROOT, "pnpm-lock.yaml");
if (!existsSync(lockfilePath)) throw new Error("缺少 pnpm-lock.yaml，无法生成可复现包");
const lockfileSha256 = sha256File(lockfilePath);

const packed = run("pnpm", ["pack", "--pack-destination", DIST]);
const tarballName = packed.split("\n").pop().split("/").pop();
const tarballPath = join(DIST, tarballName);
if (!existsSync(tarballPath)) throw new Error(`pnpm pack 未生成 ${tarballName}`);
run("pnpm", ["pack", "--pack-destination", REPEAT]);
const repeated = readdirSync(REPEAT).find((name) => name.endsWith(".tgz"));
if (!repeated) throw new Error("第二次 pack 未生成 tarball");
const repeatedPath = join(REPEAT, repeated);

const files = tarballFiles(tarballPath);
const repeatedFiles = tarballFiles(repeatedPath);
const manifest = tarballManifest(tarballPath, files);
const repeatedManifest = tarballManifest(repeatedPath, repeatedFiles);
const identical = JSON.stringify(files) === JSON.stringify(repeatedFiles) && manifestDigest(manifest) === manifestDigest(repeatedManifest);
if (!identical) throw new Error("两次打包的文件清单或内容不一致，构建不可复现");

const required = ["package/package.json", "package/lib/index.js", "package/lib/client.js", "package/cordis.patch.yml", "package/README.md", "package/LICENSE", "package/CHANGELOG.md"];
for (const entry of required) if (!files.includes(entry)) throw new Error(`发布包缺少必要文件：${entry}`);
const forbidden = [/node_modules\//u, /\.sqlite/u, /\.env/u, /^package\/tests\//u, /^package\/src\//u, /^package\/\.git/u, /^package\/\.github\//u, /\.log$/u];
for (const entry of files) {
	if (forbidden.some((pattern) => pattern.test(entry))) throw new Error(`发布包含有禁止文件：${entry}`);
}

const tarballSha256 = sha256File(tarballPath);
const packageFiles = {
	schema_version: 1,
	tarball: tarballName,
	tarball_sha256: tarballSha256,
	files: manifest
};
writeFileSync(join(DIST, "package-files.json"), `${JSON.stringify(packageFiles, null, 2)}\n`);
const sbomComponents = writeSbom(lockfileSha256);
const buildInfo = {
	schema_version: 2,
	name: pkg.name,
	version: pkg.version,
	commit,
	commit_short: commit.slice(0, 12),
	dirty,
	node: process.version,
	package_manager: pkg.packageManager ?? null,
	lockfile_sha256: lockfileSha256,
	tarball: tarballName,
	tarball_sha256: tarballSha256,
	sbom: "sbom.cdx.json",
	sbom_components: sbomComponents,
	package_files: {
		manifest: "package-files.json",
		count: manifest.length,
		sha256: manifestDigest(manifest)
	},
	reproducibility: {
		packs: 2,
		files: files.length,
		identical
	},
	generated_at: new Date().toISOString()
};
writeFileSync(join(DIST, "build-info.json"), `${JSON.stringify(buildInfo, null, 2)}\n`);
writeFileSync(join(DIST, "SHA256SUMS"), `${tarballSha256}  ${tarballName}\n`);
rmSync(REPEAT, { recursive: true, force: true });

console.log(`打包完成：${tarballName}`);
console.log(`SHA256：${tarballSha256}`);
console.log(`Commit：${buildInfo.commit_short}${dirty ? "（dirty）" : ""}`);
console.log(`文件数：${files.length}，SBOM 组件：${sbomComponents}`);
