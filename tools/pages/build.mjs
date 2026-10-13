// Builds the GitHub Pages site: the landing page from `site/`, and one HTML
// page plus the unchanged Markdown source for every file under `docs/`.
//
// Usage: node tools/pages/build.mjs <output-dir>
//
// `docs/<path>.md` becomes `docs/<path>.html` next to a copy of the source at
// `docs/<path>.md`. A `README.md` becomes `index.html` and is also copied to
// `index.md`. `llms.txt` lists every Markdown URL, and `llms-full.txt` holds
// all documents in one file.

import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { Marked } from "marked";
import { gfmHeadingId } from "marked-gfm-heading-id";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "../..");
const DOCS = path.join(ROOT, "docs");
const SITE = path.join(ROOT, "site");
const REPO_URL = "https://github.com/cafeal/beave-rs";
const SITE_URL = (process.env.SITE_URL || "https://cafeal.github.io/beave-rs/").replace(/\/?$/, "/");
const NAME = "beave-rs";
const DESCRIPTION = "A lightweight Rust message processing framework built around typed handlers.";

const out = process.argv[2];
if (!out) {
  console.error("usage: node tools/pages/build.mjs <output-dir>");
  process.exit(2);
}
const OUT = path.resolve(out);

// Repository-relative paths use forward slashes regardless of platform.
const posix = (p) => p.split(path.sep).join("/");

function walk(dir) {
  return fs.readdirSync(dir, { withFileTypes: true }).flatMap((entry) => {
    const full = path.join(dir, entry.name);
    return entry.isDirectory() ? walk(full) : [full];
  });
}

const escapeHtml = (text) =>
  text.replace(/[&<>"]/g, (c) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;" })[c]);

// Source path relative to the repository, e.g. `docs/adapters/kafka.md`.
function pageOf(source) {
  const markdown = fs.readFileSync(source, "utf8");
  const repoPath = posix(path.relative(ROOT, source));
  const sitePath = repoPath.replace(/(^|\/)README\.md$/, "$1index.md").replace(/\.md$/, "");
  const title = markdown.match(/^#\s+(.+)$/m)?.[1].trim() ?? path.basename(source, ".md");
  return { source, repoPath, sitePath, title, markdown };
}

const pages = walk(DOCS)
  .filter((file) => file.endsWith(".md"))
  .map(pageOf);
const byRepoPath = new Map(pages.map((page) => [page.repoPath, page]));

// Resolves a relative link in `fromRepoPath` to a repository path, or returns
// null for absolute URLs, fragments, and site-absolute paths.
function resolveLink(fromRepoPath, href) {
  if (/^[a-z][a-z0-9+.-]*:/i.test(href) || href.startsWith("#") || href.startsWith("/")) return null;
  const [target, fragment] = splitFragment(href);
  const repoPath = path.posix.normalize(path.posix.join(path.posix.dirname(fromRepoPath), target));
  return { repoPath, fragment };
}

function splitFragment(href) {
  const index = href.indexOf("#");
  return index === -1 ? [href, ""] : [href.slice(0, index), href.slice(index)];
}

function relativeUrl(fromSitePath, toSitePath) {
  const relative = path.posix.relative(path.posix.dirname(fromSitePath), toSitePath);
  return relative === "" ? "./" : relative;
}

// Links to files outside `docs/` point at GitHub, where the rest of the
// repository lives.
function githubUrl(repoPath, fragment) {
  const kind = fs.existsSync(path.join(ROOT, repoPath)) && fs.statSync(path.join(ROOT, repoPath)).isDirectory() ? "tree" : "blob";
  return `${REPO_URL}/${kind}/main/${repoPath}${fragment}`;
}

function htmlHref(page, href) {
  const link = resolveLink(page.repoPath, href);
  if (!link) return href;
  const target = byRepoPath.get(link.repoPath);
  if (target) return relativeUrl(page.sitePath, `${target.sitePath}.html`) + link.fragment;
  if (link.repoPath.startsWith("docs/")) return href;
  return githubUrl(link.repoPath, link.fragment);
}

// The Markdown copy keeps the source text. Only links that leave `docs/` are
// rewritten, because the site does not serve those files.
function markdownCopy(page) {
  return page.markdown.replace(/(\]\()([^)\s]+)(\))/g, (match, open, href, close) => {
    const link = resolveLink(page.repoPath, href);
    if (!link || link.repoPath.startsWith("docs/")) return match;
    return open + githubUrl(link.repoPath, link.fragment) + close;
  });
}

function render(page) {
  const marked = new Marked(gfmHeadingId());
  marked.use({
    walkTokens(token) {
      if (token.type === "link" || token.type === "image") token.href = htmlHref(page, token.href);
    },
  });
  return marked.parse(page.markdown);
}

// Navigation order follows the links in `docs/README.md`, then the links in
// the pages it reaches. A page is listed under the page whose name matches its
// directory, such as `adapters/kafka.md` under `adapters.md`.
function navigationOrder() {
  const index = byRepoPath.get("docs/README.md");
  const ordered = [index];
  for (let i = 0; i < ordered.length; i++) {
    const from = ordered[i];
    for (const [, href] of from.markdown.matchAll(/\]\(([^)\s#]+)/g)) {
      const page = byRepoPath.get(resolveLink(from.repoPath, href)?.repoPath);
      if (page && !ordered.includes(page)) ordered.push(page);
    }
  }
  for (const page of [...pages].sort((a, b) => a.repoPath.localeCompare(b.repoPath))) {
    if (!ordered.includes(page)) ordered.push(page);
  }
  const parentOf = (page) => {
    const dir = path.posix.dirname(page.sitePath);
    const owner = path.posix.basename(page.sitePath) === "index" ? path.posix.dirname(dir) : dir;
    if (owner === "docs") return null;
    return ordered.find((p) => p !== page && (p.sitePath === owner || p.sitePath === `${owner}/index`)) ?? null;
  };
  const tree = (parent) =>
    ordered
      .filter((page) => page !== index && parentOf(page) === parent)
      .map((page) => ({ page, children: tree(page) }));
  return { index, ordered, tree: tree(null) };
}

function navHtml(current, nav) {
  const item = ({ page, children }) => {
    const href = relativeUrl(current.sitePath, `${page.sitePath}.html`);
    const attrs = page === current ? ' aria-current="page"' : "";
    const sub = children.length ? `<ul>${children.map(item).join("")}</ul>` : "";
    return `<li><a href="${href}"${attrs}>${escapeHtml(page.title)}</a>${sub}</li>`;
  };
  const indexHref = relativeUrl(current.sitePath, `${nav.index.sitePath}.html`);
  const indexAttrs = current === nav.index ? ' aria-current="page"' : "";
  return `<ul><li><a href="${indexHref}"${indexAttrs}>${escapeHtml(nav.index.title)}</a></li>${nav.tree.map(item).join("")}</ul>`;
}

function pageHtml(page, nav) {
  const depth = page.sitePath.split("/").length - 1;
  const root = "../".repeat(depth);
  const markdownHref = `${path.posix.basename(page.sitePath)}.md`;
  return `<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>${escapeHtml(page.title)} - ${NAME}</title>
  <meta name="color-scheme" content="light dark">
  <link rel="stylesheet" href="${root}style.css">
  <link rel="alternate" type="text/markdown" href="${markdownHref}">
</head>
<body>
  <header class="top">
    <div class="wrap wide top-inner">
      <a class="name" href="${root}">${NAME}</a>
      <nav>
        <a href="${relativeUrl(page.sitePath, "docs/index.html")}">Guides</a>
        <a href="https://docs.rs/beavers">API</a>
        <a href="https://crates.io/crates/beavers">crates.io</a>
        <a href="${REPO_URL}">GitHub</a>
      </nav>
    </div>
  </header>

  <div class="wrap wide docs">
    <nav class="toc" aria-label="Guides">
${navHtml(page, nav)}
    </nav>
    <main class="doc">
${render(page)}
      <p class="source"><a href="${markdownHref}">Markdown</a> · <a href="${REPO_URL}/blob/main/${page.repoPath}">Source on GitHub</a></p>
    </main>
  </div>
</body>
</html>
`;
}

// Purpose column of the table in `docs/README.md`, keyed by repository path.
function descriptions(index) {
  const result = new Map();
  for (const [, href, purpose] of index.markdown.matchAll(/^\|\s*\[[^\]]*\]\(([^)]+)\)\s*\|\s*(.+?)\s*\|\s*$/gm)) {
    const link = resolveLink(index.repoPath, href);
    if (link) result.set(link.repoPath, purpose);
  }
  return result;
}

function llmsTxt(nav) {
  const purpose = descriptions(nav.index);
  const lines = [`# ${NAME}`, "", `> ${DESCRIPTION}`, ""];
  lines.push(
    "Every guide is available as Markdown at the page URL with `.html` replaced by `.md`.",
    `All guides in one file: ${SITE_URL}llms-full.txt`,
    "",
    "## Guides",
    "",
  );
  for (const page of nav.ordered) {
    const note = purpose.get(page.repoPath);
    lines.push(`- [${page.title}](${SITE_URL}${page.sitePath}.md)${note ? `: ${note}` : ""}`);
  }
  lines.push(
    "",
    "## Optional",
    "",
    `- [API reference](https://docs.rs/beavers): Rust API documentation on docs.rs`,
    `- [Repository README](${REPO_URL}/blob/main/README.md): Project overview and quick start`,
    "",
  );
  return lines.join("\n");
}

function llmsFullTxt(nav) {
  return nav.ordered
    .map((page) => `<!-- ${SITE_URL}${page.sitePath}.md -->\n\n${markdownCopy(page).trimEnd()}\n`)
    .join("\n");
}

function write(relative, contents) {
  const target = path.join(OUT, relative);
  fs.mkdirSync(path.dirname(target), { recursive: true });
  fs.writeFileSync(target, contents);
}

fs.rmSync(OUT, { recursive: true, force: true });
fs.cpSync(SITE, OUT, { recursive: true });
fs.copyFileSync(path.join(DOCS, "assets/overview.svg"), path.join(OUT, "overview.svg"));
fs.cpSync(path.join(DOCS, "assets"), path.join(OUT, "docs/assets"), { recursive: true });
write(".nojekyll", "");

const nav = navigationOrder();
for (const page of pages) {
  write(`${page.sitePath}.html`, pageHtml(page, nav));
  write(`${page.sitePath}.md`, markdownCopy(page));
  // Relative links inside the Markdown copies still name `README.md`.
  if (page.repoPath.endsWith("README.md")) write(page.repoPath, markdownCopy(page));
}
write("llms.txt", llmsTxt(nav));
write("llms-full.txt", llmsFullTxt(nav));

console.log(`Built ${pages.length} guides into ${path.relative(process.cwd(), OUT) || "."}`);
