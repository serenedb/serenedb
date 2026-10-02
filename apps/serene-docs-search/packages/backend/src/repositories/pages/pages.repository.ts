import { getDbContext } from "@database";
import type { Section } from "@repositories/sections";
import { pageKey } from "@utils/urlmap";

const INSERT_BATCH = 500;

/** Version marker of the link-graph build (meta table), like the catalog's. */
export const PAGES_SIGNATURE = "v2";

/**
 * Page popularity: how many other pages of the corpus link to each page.
 * The docs' own cross-references are the cheapest relevance prior there
 * is — the reference page for "hybrid search" is linked from every
 * tutorial, a client library's page of the same title from its siblings
 * only. Replaced whole (small: one row per linked page) whenever the
 * corpus changed or PAGES_SIGNATURE moved.
 */
export const PagesRepository = {
    replaceAll: async (inlinks: Map<string, number>): Promise<void> => {
        const ctx = getDbContext();
        // a lookup racing the rebuild must not cache the half-built table
        ctx.pageRankGeneration++;
        ctx.pageRank = undefined;
        await ctx.pool.query(`DROP TABLE IF EXISTS ${ctx.pagesTable}`);
        await ctx.pool.query(`CREATE TABLE ${ctx.pagesTable} (url VARCHAR, inlinks INTEGER)`);
        const entries = [...inlinks];
        for (let i = 0; i < entries.length; i += INSERT_BATCH) {
            const batch = entries.slice(i, i + INSERT_BATCH);
            const values = batch.map((_, j) => `($${j * 2 + 1}, $${j * 2 + 2})`).join(", ");
            await ctx.pool.query(
                `INSERT INTO ${ctx.pagesTable} (url, inlinks) VALUES ${values}`,
                batch.flat(),
            );
        }
        ctx.pageRankGeneration++;
        ctx.pageRank = inlinks;
    },

    /** page key → inlink count, cached on the context until the next rebuild. */
    inlinks: async (): Promise<Map<string, number>> => {
        const ctx = getDbContext();
        if (ctx.pageRank) return ctx.pageRank;
        const generation = ctx.pageRankGeneration;
        const r = await ctx.pool.query(`SELECT url, inlinks FROM ${ctx.pagesTable}`);
        const map = new Map(r.rows.map((row) => [String(row.url), Number(row.inlinks)]));
        if (ctx.pageRankGeneration === generation) ctx.pageRank = map;
        return map;
    },
};

/** Distinct linking pages per page key (self-links never count). */
export function inlinkCounts(sections: Section[]): Map<string, number> {
    const from = new Map<string, Set<string>>();
    for (const s of sections) {
        if (!s.links?.length) continue;
        const source = pageKey(s.url);
        for (const target of s.links) {
            if (target === source) continue;
            (from.get(target) ?? from.set(target, new Set()).get(target)!).add(source);
        }
    }
    return new Map([...from].map(([url, sources]) => [url, sources.size]));
}
