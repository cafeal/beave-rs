// Builds the GitHub Pages site: the landing page from `site/index.html` and
// the guides in `docs/`, all with the same header and stylesheet.
//
// Usage: node tools/pages/build.mjs <output-dir>
//
// `docs/<path>.md` becomes `docs/<path>.html`, and a `docs/**/README.md`
// becomes `index.html` in its directory. Each guide's Markdown is published at
// the same path with `.md` instead of `.html`; the landing page's Markdown,
// `index.md`, is the repository README. `llms.txt` lists every Markdown URL, and
// `llms-full.txt` holds all of them in one file.

import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import hljs from "highlight.js";
import { Marked } from "marked";
import { gfmHeadingId } from "marked-gfm-heading-id";
import { markedHighlight } from "marked-highlight";

const HERE = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(HERE, "../..");
const DOCS = path.join(ROOT, "docs");
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

function pageOf(source) {
  const markdown = fs.readFileSync(source, "utf8");
  // Repository path such as `docs/adapters/kafka.md`, and the site path of the
  // page without its extension, such as `docs/adapters/kafka`.
  const repoPath = posix(path.relative(ROOT, source));
  const sitePath = repoPath.replace(/(^|\/)README\.md$/, "$1index.md").replace(/\.md$/, "");
  const title = markdown.match(/^#\s+(.+)$/m)?.[1].trim() ?? path.basename(source, ".md");
  return { repoPath, sitePath, title, markdown };
}

const pages = walk(DOCS)
  .filter((file) => file.endsWith(".md"))
  .map(pageOf);
const byRepoPath = new Map(pages.map((page) => [page.repoPath, page]));
const guides = byRepoPath.get("docs/README.md");
// The README is published only as the landing page's Markdown, `index.md`.
const readme = pageOf(path.join(ROOT, "README.md"));

// Resolves a relative link in `fromRepoPath` to a repository path, or returns
// null for absolute URLs, fragments, and site-absolute paths.
function resolveLink(fromRepoPath, href) {
  if (/^[a-z][a-z0-9+.-]*:/i.test(href) || href.startsWith("#") || href.startsWith("/")) return null;
  const index = href.indexOf("#");
  const [target, fragment] = index === -1 ? [href, ""] : [href.slice(0, index), href.slice(index)];
  const repoPath = path.posix.normalize(path.posix.join(path.posix.dirname(fromRepoPath), target));
  return { repoPath, fragment };
}

function relativeUrl(fromSitePath, toSitePath) {
  const relative = path.posix.relative(path.posix.dirname(fromSitePath), toSitePath);
  return relative === "" ? "./" : relative;
}

// The site serves the guides and `docs/`. Links to anything else point at the
// repository on GitHub.
const published = (repoPath) => byRepoPath.has(repoPath) || repoPath.startsWith("docs/");

function githubUrl(repoPath, fragment) {
  const full = path.join(ROOT, repoPath);
  const kind = fs.existsSync(full) && fs.statSync(full).isDirectory() ? "tree" : "blob";
  return `${REPO_URL}/${kind}/main/${repoPath}${fragment}`;
}

function htmlHref(page, href) {
  const link = resolveLink(page.repoPath, href);
  if (!link) return href;
  const target = byRepoPath.get(link.repoPath);
  if (target) return relativeUrl(page.sitePath, `${target.sitePath}.html`) + link.fragment;
  if (published(link.repoPath)) return href;
  return githubUrl(link.repoPath, link.fragment);
}

// The Markdown copy keeps the source text. Only links to files the site does
// not serve are rewritten.
function markdownCopy(page) {
  return page.markdown.replace(/(\]\()([^)\s]+)(\))/g, (match, open, href, close) => {
    const link = resolveLink(page.repoPath, href);
    if (!link || published(link.repoPath)) return match;
    return open + githubUrl(link.repoPath, link.fragment) + close;
  });
}

function highlight(code, info) {
  // Rustdoc attributes such as `rust,ignore` follow the language name.
  const language = info.split(/[\s,]/)[0];
  return hljs.getLanguage(language) ? hljs.highlight(code, { language }).value : escapeHtml(code);
}

function render(page) {
  const marked = new Marked(
    gfmHeadingId(),
    markedHighlight({ emptyLangClass: "hljs", langPrefix: "hljs language-", highlight }),
  );
  marked.use({
    walkTokens(token) {
      if (token.type === "link" || token.type === "image") token.href = htmlHref(page, token.href);
    },
  });
  return marked.parse(page.markdown);
}

// Navigation starts with the guide index, then follows the links in it and
// the pages it reaches. Top-level pages are grouped by the `##` section of the
// guide index that first links them. A page is listed under the page whose
// name matches its directory, such as `adapters/kafka.md` under `adapters.md`.
function navigation() {
  const ordered = [guides];
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
    if (owner === "docs" || owner === ".") return null;
    return ordered.find((p) => p !== page && (p.sitePath === owner || p.sitePath === `${owner}/index`)) ?? null;
  };
  const tree = (parent) =>
    ordered
      .filter((page) => page !== guides && parentOf(page) === parent)
      .map((page) => ({ page, children: tree(page) }));
  const sections = [...guides.markdown.matchAll(/^## (.+)\n([\s\S]*?)(?=^## |(?![\s\S]))/gm)].map(([, title, text]) => ({
    title,
    links: [...text.matchAll(/\]\(([^)\s#]+)/g)].map(([, href]) => resolveLink(guides.repoPath, href)?.repoPath),
  }));
  const sectionOf = (page) => Math.max(0, sections.findIndex((section) => section.links.includes(page.repoPath)));
  const groups = sections.map(({ title }, index) => ({
    title,
    tree: tree(null).filter(({ page }) => sectionOf(page) === index),
  }));
  return { ordered, groups };
}

const flatten = (tree) => tree.flatMap(({ page, children }) => [page, ...flatten(children)]);

// Navigation labels drop what the surrounding list already says: the project
// name, and "adapter", "codec", or "wrapper" under the entries that list them.
function navLabel(page, parent) {
  const label = page.title.replace(/^beave-rs\s+—\s+/, "");
  return parent ? label.replace(/\s+(adapter|codecs?|wrapper)$/i, "") : label;
}

function navHtml(current, nav) {
  const item = ({ page, children }, parent) => {
    const href = relativeUrl(current.sitePath, `${page.sitePath}.html`);
    const attrs = page === current ? ' aria-current="page"' : "";
    const sub = children.length ? `<ul>${children.map((child) => item(child, page)).join("")}</ul>` : "";
    return `<li><a href="${href}"${attrs}>${escapeHtml(navLabel(page, parent))}</a>${sub}</li>`;
  };
  const groups = nav.groups
    .map(({ title, tree }) => `<p class="toc-group">${escapeHtml(title)}</p><ul>${tree.map((entry) => item(entry, null)).join("")}</ul>`)
    .join("");
  // Narrow screens show the list collapsed above the page; see style.css.
  return `<details open><summary>Guides</summary><ul>${item({ page: guides, children: [] }, null)}</ul>${groups}</details>`;
}

// Wraps a page body in the shared head and header. `sitePath` is the page's
// path without its extension.
function shell({ sitePath, title, markdownHref, body }) {
  const root = "../".repeat(sitePath.split("/").length - 1);
  return `<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>${escapeHtml(title)}</title>
  <meta name="description" content="${DESCRIPTION}">
  <meta name="color-scheme" content="light dark">
  <link rel="stylesheet" href="${root}style.css">
  <link rel="alternate" type="text/markdown" href="${markdownHref}">
</head>
<body>
  <header class="top">
    <div class="wrap top-inner">
      <a class="name" href="${root || "./"}">${NAME}</a>
      <nav>
        <a href="${relativeUrl(sitePath, `${guides.sitePath}.html`)}"${sitePath.startsWith("docs/") ? ' aria-current="true"' : ""}>Guides</a>
        <a href="https://docs.rs/beavers">docs.rs</a>
        <a href="https://crates.io/crates/beavers">crates.io</a>
        <a href="${REPO_URL}">GitHub</a>
      </nav>
    </div>
  </header>
${body}
  <footer class="foot">
    <div class="wrap">MIT license · <a href="${REPO_URL}">GitHub</a> · <a href="https://crates.io/crates/beavers">crates.io</a> · <a href="https://docs.rs/beavers">docs.rs</a> · <a href="${root}llms.txt">llms.txt</a></div>
  </footer>
</body>
</html>
`;
}

function guideHtml(page, nav) {
  const markdownHref = `${path.posix.basename(page.sitePath)}.md`;
  return shell({
    sitePath: page.sitePath,
    title: `${page.title} - ${NAME}`,
    markdownHref,
    body: `
  <div class="wrap layout">
    <nav class="toc" aria-label="Guides">
${navHtml(page, nav)}
    </nav>
    <main class="doc">
${render(page)}
      <p class="source"><a href="${markdownHref}">Markdown</a> · <a href="${REPO_URL}/blob/main/${page.repoPath}">Source on GitHub</a></p>
    </main>
  </div>
  <script>
    if (matchMedia("(max-width: 860px)").matches) document.querySelector(".toc details").open = false;
  </script>
`,
  });
}

const unescapeHtml = (text) =>
  text.replace(/&(lt|gt|quot|amp);/g, (_, name) => ({ lt: "<", gt: ">", quot: '"', amp: "&" })[name]);

// The landing page body is hand-written HTML; only its code blocks are
// highlighted here, the same way as in the guides.
function landingHtml() {
  const source = fs.readFileSync(path.join(ROOT, "site/index.html"), "utf8");
  const body = source.replace(
    /<pre><code class="language-([\w-]+)">([\s\S]*?)<\/code><\/pre>/g,
    (_, language, code) => `<pre><code class="hljs language-${language}">${highlight(unescapeHtml(code), language)}</code></pre>`,
  );
  return shell({ sitePath: "index", title: NAME, markdownHref: "index.md", body: `<main class="landing">\n${body}</main>` });
}

// Purpose column of the table in `docs/README.md`, keyed by repository path.
function descriptions() {
  const result = new Map();
  for (const [, href, purpose] of guides.markdown.matchAll(/^\|\s*\[[^\]]*\]\(([^)]+)\)\s*\|\s*(.+?)\s*\|\s*$/gm)) {
    const link = resolveLink(guides.repoPath, href);
    if (link) result.set(link.repoPath, purpose);
  }
  return result;
}

function llmsTxt(nav) {
  const purpose = descriptions();
  const lines = [
    `# ${NAME}`,
    "",
    `> ${DESCRIPTION}`,
    "",
    "Every page is available as Markdown at the page URL with `.html` replaced by `.md`.",
    `All pages in one file: ${SITE_URL}llms-full.txt`,
    "",
    "## Overview",
    "",
    `- [README](${SITE_URL}index.md): Project overview, installation, quick start, and Cargo features`,
    `- [${guides.title}](${SITE_URL}${guides.sitePath}.md): Index of the guides`,
  ];
  for (const { title, tree } of nav.groups) {
    lines.push("", `## ${title}`, "");
    for (const page of flatten(tree)) {
      const note = purpose.get(page.repoPath);
      lines.push(`- [${page.title}](${SITE_URL}${page.sitePath}.md)${note ? `: ${note}` : ""}`);
    }
  }
  lines.push("", "## Optional", "", "- [API reference](https://docs.rs/beavers): Rust API documentation on docs.rs", "");
  return lines.join("\n");
}

function llmsFullTxt(nav) {
  return [{ ...readme, sitePath: "index" }, ...nav.ordered]
    .map((page) => `<!-- ${SITE_URL}${page.sitePath}.md -->\n\n${markdownCopy(page).trimEnd()}\n`)
    .join("\n");
}

function write(relative, contents) {
  const target = path.join(OUT, relative);
  fs.mkdirSync(path.dirname(target), { recursive: true });
  fs.writeFileSync(target, contents);
}

fs.rmSync(OUT, { recursive: true, force: true });
fs.cpSync(path.join(DOCS, "assets"), path.join(OUT, "docs/assets"), { recursive: true });
fs.copyFileSync(path.join(HERE, "style.css"), path.join(OUT, "style.css"));
write(".nojekyll", "");

const nav = navigation();
write("index.html", landingHtml());
write("index.md", markdownCopy(readme));
for (const page of pages) {
  write(`${page.sitePath}.html`, guideHtml(page, nav));
  write(`${page.sitePath}.md`, markdownCopy(page));
  // Relative links inside the Markdown copies still name `README.md`.
  if (page.repoPath.endsWith("README.md")) write(page.repoPath, markdownCopy(page));
}
write("llms.txt", llmsTxt(nav));
write("llms-full.txt", llmsFullTxt(nav));

console.log(`Built ${pages.length} pages into ${path.relative(process.cwd(), OUT) || "."}`);
