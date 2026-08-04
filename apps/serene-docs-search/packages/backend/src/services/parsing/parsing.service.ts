import path from "node:path";
import type { ContentConfig } from "@serenedb/docs-search-core";
import type { Section } from "@repositories/sections";
import type { SourceFile } from "@services/sources";
import { mapUrl, resolveUrlMapping, withAnchor } from "@utils/urlmap";
import { parseHtml } from "./html";
import { parseMarkdown } from "./markdown";
import { parseNotebook, parseRst, parseText } from "./misc";
import { contentHash, humanize, sectionId, type RawSection } from "./section";

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

        switch (file.extension) {
            case ".md":
            case ".mdx": {
                const res = parseMarkdown(file.content, {
                    mode: content.markdown?.mode ?? "split",
                    depth: content.markdown?.depth,
                });
                docTitle = res.docTitle;
                raw = res.sections;
                break;
            }
            case ".html":
            case ".htm": {
                const res = parseHtml(file.content, content.html);
                docTitle = res.docTitle;
                raw = res.sections;
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

        return raw
            .filter((s) => s.title || s.content)
            .map((s, i) => {
                const title = s.title || fallbackTitle;
                const text = s.content;
                const url = withAnchor(baseUrl, s.anchor);
                const crumb = crumbBase.join(" › ");
                const group = crumbBase.length > 1 ? crumbBase[crumbBase.length - 2] : fallbackTitle;
                const code = s.code ?? "";
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
                    // Snapshot equality must include generated navigation and
                    // display metadata. Otherwise changing urlMapping leaves
                    // unchanged files with stale relative URLs forever.
                    hash: contentHash(
                        [
                            "section-v2",
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
                        ].join("\0"),
                    ),
                };
            });
    },
};
