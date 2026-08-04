import { getDbContext } from "@database";
import type { AnalyticsReport } from "./analytics.types";

/** Typesense-style feedback loop: settled queries + result clicks. */
export const AnalyticsRepository = {
    /** Settled query (the widget reports after the user stops typing). */
    recordQuery: async (q: string, hits: number): Promise<void> => {
        const ctx = getDbContext();
        await ctx.pool.query(
            `INSERT INTO ${ctx.queriesTable} (q, count, hits, last_at) VALUES ($1, 1, $2, $3)
             ON CONFLICT (q) DO UPDATE SET
                count = ${ctx.queriesTable}.count + 1,
                hits = EXCLUDED.hits,
                last_at = EXCLUDED.last_at`,
            [q.slice(0, 200), hits, new Date().toISOString()],
        );
    },

    /** Result click — feeds the popularity counter. */
    recordClick: async (id: string, url: string, title: string): Promise<void> => {
        const ctx = getDbContext();
        await ctx.pool.query(
            `INSERT INTO ${ctx.clicksTable} (id, url, title, clicks) VALUES ($1, $2, $3, 1)
             ON CONFLICT (id) DO UPDATE SET
                clicks = ${ctx.clicksTable}.clicks + 1,
                url = EXCLUDED.url, title = EXCLUDED.title`,
            [id, url.slice(0, 500), title.slice(0, 300)],
        );
    },

    report: async (): Promise<AnalyticsReport> => {
        const ctx = getDbContext();
        const [top, none, clicked] = await Promise.all([
            ctx.pool.query(
                `SELECT q, count, hits FROM ${ctx.queriesTable} ORDER BY count DESC, q LIMIT 50`,
            ),
            ctx.pool.query(
                `SELECT q, count FROM ${ctx.queriesTable} WHERE hits = 0 ORDER BY count DESC, q LIMIT 50`,
            ),
            ctx.pool.query(
                `SELECT url, title, clicks FROM ${ctx.clicksTable} ORDER BY clicks DESC, url LIMIT 50`,
            ),
        ]);
        return {
            topQueries: top.rows.map((r) => ({ q: String(r.q), count: Number(r.count), hits: Number(r.hits) })),
            noHitQueries: none.rows.map((r) => ({ q: String(r.q), count: Number(r.count) })),
            topClicked: clicked.rows.map((r) => ({ url: String(r.url), title: String(r.title), clicks: Number(r.clicks) })),
        };
    },
};
