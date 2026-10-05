import * as cheerio from "cheerio";
import type { Cheerio } from "cheerio";
import type { AnyNode, Element as DomElement, Text as DomText } from "domhandler";
import { objectsFromTable, type DocObject } from "./objects";
import { classifyKind, slugify, type RawSection } from "./section";

export interface HtmlParseOptions {
    /** CSS selectors scoping extraction, e.g. "article, main .content". */
    selectors?: string;
    /** Tags that become indexed content: h1 h2 h3 p li pre code table. */
    tags?: string[];
    /** Elements matching these selectors are removed before extraction. */
    excludeSelectors?: string;
}

export interface HtmlParseResult {
    docTitle: string | null;
    sections: RawSection[];
    /** href of every link in the scoped content (page popularity input). */
    links: string[];
}

const DEFAULT_TAGS = ["h1", "h2", "h3", "h4", "p", "li", "pre", "code", "table"];

/**
 * Walks the scoped content containers in document order; the heading tags in
 * `tags` (any of h1–h6) open sections (anchor = element id or generated
 * slug), the other enabled tags contribute their text to the open section.
 */
export function parseHtml(html: string, opts: HtmlParseOptions = {}): HtmlParseResult {
    const $ = cheerio.load(html);
    const tags = (opts.tags?.length ? opts.tags : DEFAULT_TAGS).map((t) => t.toLowerCase());
    const fromTags = tags.filter((t) => /^h[1-6]$/.test(t));
    const headingTags = fromTags.length ? fromTags : ["h1", "h2", "h3"];
    const heading = new Set(headingTags);
    const contentTags = tags.filter((t) => !/^h[1-6]$/.test(t));

    $("script, style, noscript, template").remove();

    let scope: Cheerio<AnyNode> = $("body");
    const scoped = opts.selectors ? $(opts.selectors) : null;
    if (scoped && scoped.length > 0) {
        scope = scoped;
        // inside a content container keep <header> (docusaurus wraps the h1
        // in one) but drop breadcrumbs and article footers
        scope.find("nav, footer").remove();
    } else {
        // walking the whole body — strip the site chrome first
        $("nav, header, footer").remove();
    }

    if (opts.excludeSelectors) {
        try {
            scope.find(opts.excludeSelectors).remove();
        } catch {
            // fail the sync with a message that names the config knob,
            // not css-select's internal parse error
            throw new Error(`invalid html.excludeSelectors: "${opts.excludeSelectors}"`);
        }
    }

    const docTitle =
        cleanText($("h1").first().text()) || cleanText($("title").first().text()) || null;
    // links of the indexed text only: chrome inside the scope (a post's
    // tag chips in its <header>, a TOC <aside>) would make tag listings
    // and the TOC targets look popular
    const linkContainers = contentTags.filter((t) => t !== "code" && t !== "pre").join(", ");
    const links = scope
        .find("a[href]")
        .toArray()
        .filter(
            (a) =>
                $(a).closest("header, nav, footer, aside").length === 0 &&
                (!linkContainers || $(a).closest(linkContainers).length > 0),
        )
        .map((a) => $(a).attr("href") ?? "")
        .filter(Boolean);

    const sections: RawSection[] = [];
    const slugCounts = new Map<string, number>();
    let currentTitle: string | null = null;
    let currentLevel = 0;
    let currentAnchor: string | undefined;
    let buf: string[] = [];
    let codeBuf: string[] = [];
    let objBuf: DocObject[] = [];

    const flush = () => {
        const content = buf.join("\n").replace(/\n{3,}/g, "\n\n").trim();
        const code = codeBuf.join("\n").trim();
        const objects = objBuf;
        buf = [];
        codeBuf = [];
        objBuf = [];
        // a heading-less preamble with almost no text is page chrome that
        // slipped past the removal rules — as a section it would duplicate
        // the page title and trip the duplicate-title demotion downstream
        if (currentTitle == null && content.length < 30) return;
        const title = currentTitle ?? docTitle ?? "";
        if (!title && !content) return;
        sections.push({
            anchor: currentAnchor,
            title,
            kind: classifyKind(title, content, currentLevel),
            level: currentLevel,
            content,
            code: code || undefined,
            objects: objects.length ? objects : undefined,
        });
    };

    // headings per id'd element, counted once (a per-heading subtree scan
    // of a big container is quadratic): an id'd ancestor names a heading
    // only when it wraps that heading alone
    const headingsUnder = new Map<AnyNode, number>();
    scope.find(headingTags.join(", ")).each((_, h) => {
        const owner = $(h).parent().closest("[id]").get(0);
        if (owner) headingsUnder.set(owner, (headingsUnder.get(owner) ?? 0) + 1);
    });

    const selector = [...new Set([...headingTags, ...contentTags])].join(", ");
    const seen = new Set<AnyNode>();
    scope.each((_, scopeEl) => {
        $(scopeEl)
            .find(selector)
            .addBack(selector)
            .each((__, el) => {
                if (seen.has(el)) return;
                seen.add(el);
                const tag = (el as unknown as { tagName?: string }).tagName?.toLowerCase() ?? "";
                const text =
                    tag === "pre" || tag === "code"
                        ? codeText(el)
                        : tag === "table"
                          ? tableText($, el)
                          : cleanText($(el).text());
                if (!text) return;
                if (heading.has(tag)) {
                    flush();
                    currentLevel = Number(tag[1]);
                    currentTitle = cleanText(text);
                    // h1 is the page itself — no anchor unless it carries an
                    // explicit id. An id'd ancestor only counts when it wraps
                    // this heading alone: a layout container's id (Docusaurus'
                    // __docusaurus_skipToContent_fallback <main>) would send
                    // every id-less heading to the top of the page
                    const wrapper = currentLevel > 1 ? $(el).parent().closest("[id]") : null;
                    const owner = wrapper?.get(0);
                    const id =
                        $(el).attr("id") ||
                        $(el).find("[id]").first().attr("id") ||
                        (owner && headingsUnder.get(owner) === 1 ? wrapper!.attr("id") : undefined);
                    currentAnchor =
                        id ||
                        (currentLevel > 1 ? uniqueSlug(slugify(currentTitle), slugCounts) : undefined);
                } else if (contentTags.includes(tag)) {
                    if (tag === "li" && $(el).parents("li").length > 0) return;
                    if (tag === "code") {
                        // <pre> already carried both content and code text
                        if (contentTags.includes("pre") && $(el).parents("pre").length > 0) return;
                        codeBuf.push(text);
                        // inline code inside a counted container (p/li/table) is
                        // already part of that container's content text
                        const containers = contentTags.filter((t) => t !== "code" && t !== "pre");
                        if (containers.length && $(el).parents(containers.join(", ")).length > 0)
                            return;
                        buf.push(text);
                        return;
                    }
                    buf.push(text);
                    if (tag === "pre") codeBuf.push(text);
                    if (tag === "table") objBuf.push(...tableObjects($, el));
                }
            });
    });
    flush();
    return { docTitle, sections, links };
}

function uniqueSlug(slug: string, counts: Map<string, number>): string {
    const n = counts.get(slug) ?? 0;
    counts.set(slug, n + 1);
    return n === 0 ? slug : `${slug}-${n}`;
}

/**
 * A table rendered row by row, cells joined with a separator — plain .text()
 * glues adjacent cells into unreadable runs ("FeatureSupport StateDetails").
 */
function tableText($: cheerio.CheerioAPI, el: AnyNode): string {
    const rows: string[] = [];
    $(el)
        .find("tr")
        .each((_, tr) => {
            const cells: string[] = [];
            $(tr)
                .find("th, td")
                .each((__, cell) => {
                    const t = cleanText($(cell).text());
                    if (t) cells.push(t);
                });
            if (cells.length) rows.push(cells.join(" · "));
        });
    return rows.join("\n");
}

/** Reference-table rows that name objects (first header Function / Name / …). */
function tableObjects($: cheerio.CheerioAPI, el: AnyNode): DocObject[] {
    const rows = $(el)
        .find("tr")
        .toArray()
        .map((tr) =>
            $(tr)
                .find("th, td")
                .toArray()
                .map((cell) => ({ th: (cell as DomElement).tagName === "th", text: cleanText($(cell).text()) })),
        )
        .filter((cells) => cells.length > 0);
    if (rows.length < 2 || !rows[0].every((c) => c.th)) return [];
    return objectsFromTable(
        rows[0].map((c) => c.text),
        rows.slice(1).map((cells) => cells.map((c) => c.text)),
    );
}

function cleanText(text: string): string {
    // zero-width chars come from heading anchor-link markup
    return text.replace(/[\u200B-\u200D\uFEFF]/g, "").replace(/\s+/g, " ").trim();
}

/**
 * Text of a <pre>/<code> element with line structure preserved. Syntax
 * highlighters (prism/docusaurus) render each code line as a span with no
 * newline text nodes between lines — plain .text() would glue the last token
 * of one line to the first token of the next.
 */
function codeText(el: AnyNode): string {
    const parts: string[] = [];
    const walk = (node: AnyNode): void => {
        if (node.type === "text") {
            parts.push((node as DomText).data);
            return;
        }
        if (node.type !== "tag") return;
        const e = node as DomElement;
        if (e.tagName === "br") {
            parts.push("\n");
            return;
        }
        e.children.forEach(walk);
        const cls = e.attribs?.["class"] ?? "";
        if (e.tagName === "div" || /\btoken-line\b/.test(cls)) parts.push("\n");
    };
    walk(el);
    return parts
        .join("")
        .replace(/[ \t]+\n/g, "\n")
        .replace(/\n{3,}/g, "\n\n")
        .trim();
}
