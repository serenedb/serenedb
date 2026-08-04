import { classifyKind, slugify, type RawSection } from "./section";

export interface MarkdownParseResult {
    /** Frontmatter title / first h1 / null. */
    docTitle: string | null;
    frontmatter: Record<string, string>;
    sections: RawSection[];
}

/**
 * Splits a markdown/MDX document into heading-anchored sections, the way
 * DocSearch-style indexes want them: every heading down to `depth` (default
 * h4 — API references document functions there) opens a section whose body
 * runs until the next such heading. Content before the first heading belongs
 * to a level-0 "document" section.
 */
export function parseMarkdown(
    raw: string,
    opts: { mode: "split" | "whole"; depth?: number },
): MarkdownParseResult {
    const { frontmatter, body } = extractFrontmatter(raw);
    const cleaned = stripMdx(body);
    const lines = cleaned.split("\n");

    const docTitle = frontmatter["title"] || findFirstH1(lines) || null;

    if (opts.mode === "whole") {
        const text = collapse(lines.join("\n"));
        return {
            docTitle,
            frontmatter,
            sections: text
                ? [{ title: docTitle ?? "", kind: "text", level: 0, content: text }]
                : [],
        };
    }

    const depth = Math.min(Math.max(Math.trunc(opts.depth ?? 4), 1), 6);
    const headingRe = new RegExp(`^(#{1,${depth}})\\s+(.+?)\\s*#*\\s*$`);
    const sections: RawSection[] = [];
    const slugCounts = new Map<string, number>();
    let currentTitle: string | null = null;
    let currentLevel = 0;
    let currentAnchor: string | undefined;
    let buf: string[] = [];
    let codeBuf: string[] = [];
    let inFence = false;

    const flush = () => {
        const content = collapse(buf.join("\n"));
        const code = codeBuf.join("\n").trim();
        buf = [];
        codeBuf = [];
        if (currentTitle == null && !content) return;
        const title = currentTitle ?? docTitle ?? "";
        if (!title && !content) return;
        sections.push({
            anchor: currentAnchor,
            title,
            kind: classifyKind(title, content, currentLevel),
            level: currentLevel,
            content,
            code: code || undefined,
        });
    };

    for (const line of lines) {
        const marker = /^\s*(```|~~~)/.test(line);
        if (marker) inFence = !inFence;
        const h = !inFence && !marker ? headingRe.exec(line) : null;
        if (h) {
            flush();
            currentLevel = h[1].length;
            const { text, explicitId } = headingParts(h[2]);
            currentTitle = text;
            currentAnchor = explicitId ?? uniqueSlug(slugify(text), slugCounts);
        } else {
            buf.push(line);
            // fenced lines also feed the code column (symbol-aware ngram search)
            if (inFence && !marker) codeBuf.push(line);
        }
    }
    flush();
    return { docTitle, frontmatter, sections };
}

/** {#custom-id} suffixes win over generated slugs (Docusaurus convention). */
function headingParts(heading: string): { text: string; explicitId?: string } {
    const m = /^(.*?)\s*\{#([^}]+)\}\s*$/.exec(heading);
    if (m) return { text: stripInline(m[1]), explicitId: m[2] };
    return { text: stripInline(heading) };
}

function uniqueSlug(slug: string, counts: Map<string, number>): string {
    const n = counts.get(slug) ?? 0;
    counts.set(slug, n + 1);
    return n === 0 ? slug : `${slug}-${n}`;
}

export function extractFrontmatter(raw: string): {
    frontmatter: Record<string, string>;
    body: string;
} {
    const m = /^---\r?\n([\s\S]*?)\r?\n---\r?\n?/.exec(raw);
    if (!m) return { frontmatter: {}, body: raw };
    const frontmatter: Record<string, string> = {};
    for (const line of m[1].split("\n")) {
        const kv = /^([A-Za-z0-9_-]+):\s*(.*)$/.exec(line);
        if (kv) frontmatter[kv[1]] = kv[2].trim().replace(/^["']|["']$/g, "");
    }
    return { frontmatter, body: raw.slice(m[0].length) };
}

function findFirstH1(lines: string[]): string | null {
    let inFence = false;
    for (const line of lines) {
        if (/^\s*(```|~~~)/.test(line)) inFence = !inFence;
        if (inFence) continue;
        const m = /^#\s+(.+?)\s*#*\s*$/.exec(line);
        if (m) return stripInline(m[1]);
    }
    return null;
}

/** Drop MDX imports/exports and JSX component tags, keep their text children. */
export function stripMdx(body: string): string {
    return (
        body
            .replace(/^import\s[^\n]*$/gm, "")
            .replace(/^export\s[^\n]*$/gm, "")
            // self-closing components: <Tabs ... />
            .replace(/<[A-Z][A-Za-z0-9.]*[^>]*\/>/g, "")
            // paired component tags — keep inner content
            .replace(/<\/?[A-Z][A-Za-z0-9.]*[^>]*>/g, "")
            // html comments
            .replace(/<!--[\s\S]*?-->/g, "")
    );
}

/** Markdown inline syntax -> plain text (links, emphasis, inline code). */
export function stripInline(text: string): string {
    return text
        .replace(/!\[([^\]]*)\]\([^)]*\)/g, "$1")
        .replace(/\[([^\]]*)\]\([^)]*\)/g, "$1")
        .replace(/`([^`]*)`/g, "$1")
        .replace(/\*\*([^*]*)\*\*/g, "$1")
        .replace(/\*([^*]*)\*/g, "$1")
        .replace(/__([^_]*)__/g, "$1")
        .trim();
}

/** Collapse a section body to clean indexable text (keeps code blocks). */
export function collapse(text: string): string {
    const noFenceMarkers = text.replace(/^\s*(```|~~~)[^\n]*$/gm, "");
    return noFenceMarkers
        .split("\n")
        .map((l) => stripInline(l.replace(/^\s*[-*+]\s+/, "").replace(/^\s*\d+\.\s+/, "")))
        .join("\n")
        .replace(/\n{3,}/g, "\n\n")
        .trim();
}
