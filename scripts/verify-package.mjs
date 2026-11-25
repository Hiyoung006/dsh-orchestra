// 发布包完整性校验：SHA256、文件级清单、SBOM、锁文件与 build-info 关联。
import { createHash } from "node:crypto";
import { spawnSync } from "node:child_process";
import { existsSync, readFileSync, readdirSync } from "node:fs";
import { basename, dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

const ROOT = join(dirname(fileURLToPath(import.meta.url)), "..");
const DIST = process.env.ORCHESTRA_DIST ? join(ROOT, process.env.ORCHESTRA_DIST) : join(ROOT, "dist");
const infoPath = join(DIST, "build-info.json");
if (!existsSync(infoPath)) throw new Error("缺少 dist/build-info.json，请先运行 pnpm build");
const info = JSON.parse(readFileSync(infoPath, "utf8"));
if (info.schema_version !== 2) throw new Error(`不支持的 build-info 版本：${info.schema_version}`);

function sha256(data) {
	return createHash("sha256").update(data).digest("hex");
}

function manifestDigest(manifest) {
	const digest = createHash("sha256");
	for (const entry of manifest) digest.update(`${entry.path}\0${entry.bytes}\0${entry.sha256}\n`);
	return digest.digest("hex");
}

function readTarballMember(path, member) {
	const result = spawnSync("tar", ["-xOzf", path, member], { maxBuffer: 64 * 1024 * 1024 });
	if (result.status !== 0) throw new Error(`无法读取发布包成员 ${member}`);
	return result.stdout;
}

if (basename(info.tarball) !== info.tarball || !info.tarball.endsWith(".tgz")) throw new Error(`发布包文件名不合法：${info.tarball}`);
const tarballPath = join(DIST, info.tarball);
if (!existsSync(tarballPath)) throw new Error(`缺少发布包 ${info.tarball}`);

const digest = sha256(readFileSync(tarballPath));
if (digest !== info.tarball_sha256) throw new Error(`SHA256 不匹配：${digest} != ${info.tarball_sha256}`);

const sums = readFileSync(join(DIST, "SHA256SUMS"), "utf8").trim();
const expected = `${info.tarball_sha256}  ${info.tarball}`;
if (sums !== expected) throw new Error("SHA256SUMS 与 build-info 不一致");

const allowed = new Set([info.tarball, "build-info.json", "SHA256SUMS", info.sbom, info.package_files?.manifest]);
for (const entry of readdirSync(DIST)) {
	if (!allowed.has(entry)) throw new Error(`dist 含未登记文件：${entry}`);
}

const list = spawnSync("tar", ["-tzf", tarballPath], { encoding: "utf8" });
if (list.status !== 0) throw new Error("无法读取发布包文件清单");
const files = list.stdout.split("\n").filter(Boolean);
const required = ["package/package.json", "package/lib/index.js", "package/lib/client.js", "package/lib/index.d.ts", "package/cordis.patch.yml", "package/README.md", "package/LICENSE", "package/CHANGELOG.md"];
for (const entry of required) if (!files.includes(entry)) throw new Error(`发布包缺少 ${entry}`);
const forbidden = [/node_modules\//u, /\.sqlite/u, /\.env/u, /^package\/tests\//u, /^package\/src\//u, /^package\/\.github\//u, /\.log$/u];
for (const entry of files) if (forbidden.some((pattern) => pattern.test(entry))) throw new Error(`发布包含有禁止文件：${entry}`);

const packed = JSON.parse(readTarballMember(tarballPath, "package/package.json").toString("utf8"));
if (packed.name !== info.name || packed.version !== info.version) throw new Error("发布包 package.json 与 build-info 不一致");

if (!info.package_files || !Number.isInteger(info.package_files.count) || typeof info.package_files.sha256 !== "string") throw new Error("build-info 缺少文件级清单摘要");
const manifestPath = join(DIST, info.package_files.manifest);
if (!existsSync(manifestPath)) throw new Error("缺少文件级清单 package-files.json");
const manifestDoc = JSON.parse(readFileSync(manifestPath, "utf8"));
if (manifestDoc.tarball !== info.tarball || manifestDoc.tarball_sha256 !== info.tarball_sha256) throw new Error("文件级清单与 build-info 的包标识不一致");
const manifest = manifestDoc.files;
if (!Array.isArray(manifest) || manifest.length !== info.package_files.count) throw new Error("文件级清单条目数与 build-info 不一致");
if (manifestDigest(manifest) !== info.package_files.sha256) throw new Error("文件级清单摘要与 build-info 不一致");
const packagedFiles = files.filter((entry) => !entry.endsWith("/"));
if (packagedFiles.length !== manifest.length) throw new Error("文件级清单未覆盖发布包全部文件");
const byPath = new Map();
for (const entry of manifest) {
	if (byPath.has(entry.path)) throw new Error(`文件级清单重复条目：${entry.path}`);
	byPath.set(entry.path, entry);
}
for (const entry of packagedFiles) {
	const recorded = byPath.get(entry);
	if (!recorded) throw new Error(`文件级清单缺少 ${entry}`);
	const content = readTarballMember(tarballPath, entry);
	if (content.length !== recorded.bytes) throw new Error(`文件大小不匹配：${entry}`);
	if (sha256(content) !== recorded.sha256) throw new Error(`文件摘要不匹配：${entry}`);
}

const lockfilePath = join(ROOT, "pnpm-lock.yaml");
if (!existsSync(lockfilePath)) throw new Error("缺少 pnpm-lock.yaml，无法核对锁文件");
if (sha256(readFileSync(lockfilePath)) !== info.lockfile_sha256) throw new Error("锁文件摘要与 build-info 不一致");

const sbomPath = join(DIST, info.sbom);
if (!existsSync(sbomPath)) throw new Error("缺少 SBOM 文件");
const sbom = JSON.parse(readFileSync(sbomPath, "utf8"));
if (sbom.bomFormat !== "CycloneDX" || sbom.specVersion !== "1.5") throw new Error("SBOM 不是 CycloneDX 1.5");
if (sbom.metadata?.component?.name !== info.name || sbom.metadata?.component?.version !== info.version) throw new Error("SBOM 组件与 build-info 不一致");
if (!Array.isArray(sbom.components) || sbom.components.length !== info.sbom_components) throw new Error("SBOM 组件数与 build-info 不一致");
const lockProperty = (sbom.metadata?.properties ?? []).find((property) => property.name === "dsh-orchestra:lockfile_sha256");
if (lockProperty?.value !== info.lockfile_sha256) throw new Error("SBOM 锁文件摘要与 build-info 不一致");

if (!/^[a-f0-9]{40}$/u.test(info.commit)) throw new Error("build-info 缺少合法 Git commit");
if (process.env.GITHUB_SHA && info.commit !== process.env.GITHUB_SHA) throw new Error(`build-info commit ${info.commit} 与 CI 提交 ${process.env.GITHUB_SHA} 不一致`);
if (!Number.isInteger(info.sbom_components) || info.sbom_components < 1) throw new Error("SBOM 组件数为空");
if (info.reproducibility?.packs !== 2 || info.reproducibility?.identical !== true) throw new Error("缺少双次打包可复现证据");
if (info.reproducibility.files !== files.length) throw new Error("可复现记录的文件数与发布包不一致");

console.log(`包完整性校验通过：${info.tarball}`);
console.log(`SHA256：${info.tarball_sha256}`);
console.log(`文件清单：${manifest.length} 项，摘要 ${info.package_files.sha256.slice(0, 12)}…`);
console.log(`Commit：${info.commit_short}，文件数：${files.length}，SBOM 组件：${info.sbom_components}`);
