import * as cheerio from "cheerio";
import picomatch from "picomatch";
import type { SiteSource } from "@serenedb/docs-search-core";
import type { FetchContext, FetchResult, SourceFile } from "./sources.types";

const MAX_PAGES = 2000;
const FETCH_TIMEOUT = 20_000;
const CONCURRENCY = 4;

interface QueueItem {
    url: string;
    depth: number;
}

/**
 * Same-origin breadth-first crawler. Seeds from the start URL (plus
 * sitemap.xml when enabled), follows <a href> up to the configured depth
 * and yields raw HTML pages for the html parser.
 */
export async function fetchSite(source: SiteSource, ctx: FetchContext): Promise<FetchResult> {
    const start = new URL(source.url);
    const maxDepth = source.depth === "all" ? Infinity : (source.depth ?? 2);
    const seen = new Set<string>();
    const queue: QueueItem[] = [];
    const files: SourceFile[] = [];
    // exclude globs match the URL path: "/blog/**", "**/changelog"
    const isExcluded = ctx.exclude.length
        ? picomatch(ctx.exclude, { dot: true })
        : () => false;

    const push = (url: string, depth: number) => {
        const norm = normalize(url, start);
        if (!norm || seen.has(norm) || seen.size >= MAX_PAGES) return;
        const path = new URL(norm).pathname;
        if (isExcluded(path) || isExcluded(path.replace(/^\//, ""))) return;
        seen.add(norm);
        queue.push({ url: norm, depth });
    };

    push(start.href, 0);
    if (source.sitemap !== false) {
        for (const loc of await sitemapUrls(start)) push(loc, 0);
    }

    while (queue.length > 0) {
        const batch = queue.splice(0, CONCURRENCY);
        await Promise.all(
            batch.map(async ({ url, depth }) => {
                const html = await fetchPage(url);
                if (html == null) return;
                files.push({ path: pathOf(url), url, content: html, extension: ".html" });
                ctx.onProgress?.(files.length, url);
                if (depth < maxDepth) {
                    for (const href of extractLinks(html, url)) push(href, depth + 1);
                }
            }),
        );
    }
    return { files, ref: `crawl:${new Date().toISOString()}` };
}

function pathOf(url: string): string {
    const u = new URL(url);
    return u.pathname === "/" ? "/index" : u.pathname;
}

/** Same origin, strip hash/query, skip binary-looking paths. */
function normalize(href: string, start: URL): string | null {
    let u: URL;
    try {
        u = new URL(href, start);
    } catch {
        return null;
    }
    if (u.origin !== start.origin) return null;
    if (!u.pathname.startsWith(rootPath(start))) return null;
    if (/\.(png|jpe?g|gif|svg|ico|css|js|mjs|json|xml|pdf|zip|gz|woff2?|ttf|mp4|webm)$/i.test(u.pathname)) {
        return null;
    }
    u.hash = "";
    u.search = "";
    return u.href;
}

/** Crawl stays under the start URL's directory. */
function rootPath(start: URL): string {
    const p = start.pathname;
    if (p.endsWith("/")) return p;
    const cut = p.lastIndexOf("/");
    return cut <= 0 ? "/" : p.slice(0, cut + 1);
}

async function fetchPage(url: string): Promise<string | null> {
    try {
        const res = await fetch(url, {
            signal: AbortSignal.timeout(FETCH_TIMEOUT),
            headers: { "User-Agent": "SereneDocsSearch/0.6 (+https://serenedb.com)" },
            redirect: "follow",
        });
        if (!res.ok) return null;
        const type = res.headers.get("content-type") ?? "";
        if (!type.includes("text/html")) return null;
        return await res.text();
    } catch {
        return null;
    }
}

function extractLinks(html: string, baseUrl: string): string[] {
    const $ = cheerio.load(html);
    const out: string[] = [];
    $("a[href]").each((_, el) => {
        const href = $(el).attr("href");
        if (href && !href.startsWith("mailto:") && !href.startsWith("javascript:")) {
            try {
                out.push(new URL(href, baseUrl).href);
            } catch {
                /* unparseable href */
            }
        }
    });
    return out;
}

async function sitemapUrls(start: URL): Promise<string[]> {
    try {
        const res = await fetch(new URL("/sitemap.xml", start.origin), {
            signal: AbortSignal.timeout(FETCH_TIMEOUT),
        });
        if (!res.ok) return [];
        const xml = await res.text();
        const locs = [...xml.matchAll(/<loc>\s*([^<\s]+)\s*<\/loc>/g)].map((m) => m[1]);
        return locs.slice(0, MAX_PAGES);
    } catch {
        return [];
    }
}
