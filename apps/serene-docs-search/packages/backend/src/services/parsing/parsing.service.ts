import path from "node:path";
import type { ContentConfig } from "@serenedb/docs-search-core";
import type { Section } from "@repositories/sections";
import type { SourceFile } from "@services/sources";
import { mapUrl, pageKey, resolveUrlMapping, withAnchor } from "@utils/urlmap";
import { parseHtml } from "./html";
import { parseMarkdown } from "./markdown";
import { parseNotebook, parseRst, parseText } from "./misc";
import { objectFromTitle, type DocObject } from "./objects";
import { contentHash, humanize, sectionId, type RawSection } from "./section";

/**
 * Outbound links of a page as page keys (see pageKey) — same site only,
 * self-links removed. Markdown sources link files
 * ("../functions/text.md#x"), which map like the files do; a relative
 * href on an index page resolves against its directory, as a browser
 * reading ".../sql/" would.
 */
function resolveLinks(
    hrefs: string[],
    filePath: string,
    pageUrl: string,
    content: ContentConfig,
): string[] {
    const site = /^https?:\/\//.test(pageUrl) ? undefined : "http://site.invalid";
    const indexNames = resolveUrlMapping(filePath, content.urlMapping).indexFiles ?? ["index", "README"];
    const isIndex = indexNames.some((n) => n.toLowerCase() === basename(filePath).toLowerCase());
    let base: URL;
    try {
        base = new URL(isIndex && !pageUrl.endsWith("/") ? `${pageUrl}/` : pageUrl, site);
    } catch {
        return [];
    }
    const self = pageKey(pageUrl);
    const out = new Set<string>();
    for (const href of hrefs) {
        if (!href || href.startsWith("#") || /^(mailto|javascript|tel|data):/i.test(href)) continue;
        let target: string | null = null;
        const file = /^([^#?]*\.mdx?)(?:[#?].*)?$/i.exec(href);
        if (file && !/^[a-z]+:/i.test(href)) {
            const dir = path.posix.dirname(filePath.replace(/\\/g, "/"));
            const resolved = path.posix.normalize(
                file[1].startsWith("/") ? file[1].slice(1) : path.posix.join(dir, file[1]),
            );
            target = pageKey(mapUrl(resolved, content.urlMapping));
        } else {
            try {
                const u = new URL(href, base);
                if (u.origin !== base.origin) continue;
                target = pageKey(site ? u.pathname : `${u.origin}${u.pathname}`);
            } catch {
                continue;
            }
        }
        if (target && target !== self) out.add(target);
    }
    return [...out];
}

/** A parent's indexed text stops growing here (huge reference pages). */
const MAX_BODY_CHARS = 120_000;

/**
 * The section's own text followed by every section nested under it, with
 * their headings: a page's top section carries the whole page and an h2
 * carries its h3s. Query words spread over several subsections then still
 * meet in one indexed unit — the page or the chapter that covers them all —
 * the way serened's embedded docs index nests its rows. Leaves are just
 * their own content.
 */
function subtreeText(sections: RawSection[], at: number): string {
    const parts = [sections[at].content];
    let size = parts[0].length;
    for (let j = at + 1; j < sections.length && sections[j].level > sections[at].level; j++) {
        const part = `${sections[j].title}\n${sections[j].content}`;
        if (size + part.length > MAX_BODY_CHARS) break;
        parts.push(part);
        size += part.length + 2;
    }
    return parts.join("\n\n").trim();
}

function basename(p: string): string {
    const b = path.basename(p);
    const dot = b.lastIndexOf(".");
    return dot > 0 ? b.slice(0, dot) : b;
}

/** "docs/replication/read-replicas.md" -> ["Docs", "Replication", "Read replicas"] */
function crumbSegments(filePath: string, content: ContentConfig, docTitle: string): string[] {
    let p = filePath.replace(/\\/g, "/").replace(/^\.?\//, "");
    const prefix = content.urlMapping?.stripPrefix?.replace(/^\/+|\/+$/g, "");
    if (prefix && p.startsWith(prefix + "/")) p = p.slice(prefix.length + 1);
    const dirs = p.split("/").slice(0, -1).filter(Boolean);
    return [...dirs.map(humanize), docTitle];
}

async function parsePdf(file: SourceFile): Promise<RawSection[]> {
    try {
        // lazy: pdf-parse is optional and its root entry has import-time side effects
        const mod = (await import("pdf-parse/lib/pdf-parse.js" as string)) as {
            default?: (b: Buffer) => Promise<{ text: string }>;
        };
        const pdfParse = mod.default ?? (mod as unknown as (b: Buffer) => Promise<{ text: string }>);
        const buf = Buffer.from(file.content, file.encoding === "base64" ? "base64" : "utf8");
        const res = await pdfParse(buf);
        const text = res.text.replace(/\n{3,}/g, "\n\n").trim();
        return text ? [{ title: "", kind: "text", level: 0, content: text }] : [];
    } catch (err) {
        console.warn(`pdf parse failed for ${file.path}:`, (err as Error).message);
        return [];
    }
}

export const ParsingService = {
    /** Turns one fetched file into indexable sections with final URLs. */
    parseFile: async (file: SourceFile, content: ContentConfig): Promise<Section[]> => {
        let docTitle: string | null = null;
        let raw: RawSection[] = [];
        let links: string[] = [];

        switch (file.extension) {
            case ".md":
            case ".mdx": {
                const res = parseMarkdown(file.content, {
                    mode: content.markdown?.mode ?? "split",
                    depth: content.markdown?.depth,
                });
                docTitle = res.docTitle;
                raw = res.sections;
                links = res.links;
                break;
            }
            case ".html":
            case ".htm": {
                const res = parseHtml(file.content, content.html);
                docTitle = res.docTitle;
                raw = res.sections;
                links = res.links;
                break;
            }
            case ".rst":
                raw = parseRst(file.content);
                docTitle = raw[0]?.title || null;
                break;
            case ".txt":
                raw = parseText(file.content);
                break;
            case ".ipynb":
                raw = parseNotebook(file.content);
                docTitle = raw[0]?.title || null;
                break;
            case ".pdf": {
                raw = await parsePdf(file);
                docTitle = raw[0]?.title || null;
                break;
            }
            default:
                return [];
        }

        const fallbackTitle = docTitle ?? humanize(basename(file.path));
        const effectiveUrlMapping = resolveUrlMapping(file.path, content.urlMapping);
        const baseUrl = file.url ?? mapUrl(file.path, content.urlMapping);
        const crumbBase = crumbSegments(
            file.path,
            { ...content, urlMapping: effectiveUrlMapping },
            fallbackTitle,
        );

        const kept = raw.filter((s) => s.title || s.content);
        // no section above all the others (a page that starts at "## Setup"
        // with its title in front matter): a page-level unit carries the
        // whole page, as the h1 section does elsewhere
        if (kept.length > 1 && kept.slice(1).some((s) => s.level <= kept[0].level)) {
            kept.unshift({ title: fallbackTitle, kind: "heading", level: 0, content: "" });
        }
        const outLinks = resolveLinks(links, file.path, baseUrl, content);
        const titles = kept.map((s) => s.title || fallbackTitle);
        const crumb = crumbBase.join(" › ");
        const group = crumbBase.length > 1 ? crumbBase[crumbBase.length - 2] : fallbackTitle;
        // heading chain above each section: the page title, then the
        // headings it is nested under ("Full-Text Search › Prefix and
        // wildcard"). Indexed, so a generic subsection ("Examples",
        // "Parameters") is still found by what it is an example OF.
        const ancestors: Array<{ level: number; title: string }> = [];
        const trails = kept.map((s, i) => {
            while (ancestors.length && ancestors[ancestors.length - 1].level >= s.level) {
                ancestors.pop();
            }
            const chain = ancestors.map((a) => a.title);
            if (titles[i] !== fallbackTitle && chain[0] !== fallbackTitle) chain.unshift(fallbackTitle);
            ancestors.push({ level: s.level, title: titles[i] });
            // an h2 that repeats the page title adds nothing to the trail
            const trail = chain.filter((t, k) => t !== chain[k - 1]);
            while (trail.length && trail[trail.length - 1] === titles[i]) trail.pop();
            return trail.join(" › ");
        });

        return kept.map((s, i) => {
            const title = titles[i];
            const text = s.content;
            const url = withAnchor(baseUrl, s.anchor);
            const code = s.code ?? "";
            const trail = trails[i];
            const body = subtreeText(kept, i);
            const objects = [objectFromTitle(title, s.level), ...(s.objects ?? [])].filter(
                (o): o is DocObject => o != null,
            );
            return {
                id: sectionId(file.path, s.anchor, i),
                path: file.path,
                url,
                anchor: s.anchor,
                title,
                crumb,
                group,
                kind: s.kind,
                level: s.level,
                content: text,
                code,
                trail,
                body,
                objects,
                // the page's outbound links ride on its first section only
                links: i === 0 ? outLinks : undefined,
                // Snapshot equality must include generated navigation and
                // display metadata. Otherwise changing urlMapping leaves
                // unchanged files with stale relative URLs forever.
                hash: contentHash(
                    [
                        "section-v4",
                        file.path,
                        url,
                        s.anchor ?? "",
                        title,
                        crumb,
                        group,
                        s.kind,
                        String(s.level),
                        text,
                        code,
                        trail,
                        body,
                        // a links-only edit must still rebuild the link graph
                        i === 0 ? outLinks.join(" ") : "",
                    ].join("\0"),
                ),
            };
        });
    },
};
