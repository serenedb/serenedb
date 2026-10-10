import type { SearchResultItem } from "@serenedb/docs-search-core";
import { getDbContext } from "@database";
import { makeSnippet, SNIPPET_SOURCE_CHARS } from "@utils/snippet";
import { toItem } from "../rows";
import type { IndexStats, Section } from "./sections.types";

/** Section row CRUD: the sync pipeline's diff/upsert side and point lookups. */
export const SectionsRepository = {
    existingHashes: async (): Promise<Map<string, string>> => {
        const ctx = getDbContext();
        const r = await ctx.pool.query(`SELECT id, hash FROM ${ctx.table}`);
        return new Map(r.rows.map((row) => [String(row.id), String(row.hash)]));
    },

    upsertSections: async (sections: Section[]): Promise<void> => {
        const ctx = getDbContext();
        const cols =
            "(id, path, url, anchor, title, crumb, grp, kind, level, content, code, trail, body, hash)";
        for (let i = 0; i < sections.length; i += 200) {
            const batch = sections.slice(i, i + 200);
            const values: string[] = [];
            const params: unknown[] = [];
            batch.forEach((s, j) => {
                const base = j * 14;
                values.push(
                    `(${Array.from({ length: 14 }, (_, k) => `$${base + k + 1}`).join(", ")})`,
                );
                params.push(
                    s.id,
                    s.path,
                    s.url,
                    s.anchor ?? null,
                    s.title,
                    s.crumb,
                    s.group,
                    s.kind,
                    s.level,
                    s.content,
                    s.code,
                    s.trail,
                    s.body,
                    s.hash,
                );
            });
            await ctx.pool.query(
                `INSERT INTO ${ctx.table} ${cols} VALUES ${values.join(", ")}
                 ON CONFLICT (id) DO UPDATE SET
                    path = EXCLUDED.path, url = EXCLUDED.url, anchor = EXCLUDED.anchor,
                    title = EXCLUDED.title, crumb = EXCLUDED.crumb, grp = EXCLUDED.grp,
                    kind = EXCLUDED.kind, level = EXCLUDED.level,
                    content = EXCLUDED.content, code = EXCLUDED.code,
                    trail = EXCLUDED.trail, body = EXCLUDED.body, hash = EXCLUDED.hash` +
                    // re-embed only when the embedded text (title + own
                    // content) changed — an edit deep in a page changes its
                    // ancestors' body and hash, not what they embed
                    (ctx.hybrid
                        ? `, embedding = CASE WHEN ${ctx.table}.title = EXCLUDED.title
                                AND ${ctx.table}.content = EXCLUDED.content
                                THEN ${ctx.table}.embedding ELSE NULL END`
                        : ""),
                params,
            );
        }
    },

    deleteSections: async (ids: string[]): Promise<void> => {
        const ctx = getDbContext();
        for (let i = 0; i < ids.length; i += 500) {
            const batch = ids.slice(i, i + 500);
            const params = batch.map((_, j) => `$${j + 1}`).join(", ");
            await ctx.pool.query(
                `DELETE FROM ${ctx.table} WHERE id IN (${params})`,
                batch,
            );
        }
    },

    truncate: async (): Promise<void> => {
        const ctx = getDbContext();
        await ctx.pool.query(`DELETE FROM ${ctx.table}`);
    },

    /** Make pending rows searchable + refresh BM25 term stats. */
    refreshIndex: async (): Promise<void> => {
        const ctx = getDbContext();
        await ctx.pool.query(`VACUUM (REFRESH_TABLE) ${ctx.table}`);
        try {
            await ctx.pool.query(`VACUUM (RECOMPUTE_STATS_TABLE) ${ctx.table}`);
        } catch {
            /* stats refresh is best-effort */
        }
    },

    stats: async (): Promise<IndexStats> => {
        const ctx = getDbContext();
        try {
            const r = await ctx.pool.query(
                `SELECT count(*) AS sections, count(DISTINCT path) AS documents FROM ${ctx.table}`,
            );
            return {
                sections: Number(r.rows[0]?.sections ?? 0),
                documents: Number(r.rows[0]?.documents ?? 0),
            };
        } catch {
            return { sections: 0, documents: 0 };
        }
    },

    /**
     * Section text for AI answer context — the body, i.e. the section with
     * its subsections: that is what matched, and a page's top section has
     * little text of its own (callers truncate).
     */
    contentsFor: async (ids: string[]): Promise<Map<string, string>> => {
        const ctx = getDbContext();
        if (ids.length === 0) return new Map();
        const params = ids.map((_, j) => `$${j + 1}`).join(", ");
        const r = await ctx.pool.query(
            `SELECT id, body AS content FROM ${ctx.table} WHERE id IN (${params})`,
            ids,
        );
        return new Map(
            r.rows.map((row) => [String(row.id), String(row.content ?? "")]),
        );
    },

    /**
     * All sections of the page containing the given section (by id or url),
     * page intro first, then subsections. Reading a whole page is what the
     * ask-agent and MCP need — code examples live in sibling subsections.
     */
    pageSections: async (ref: {
        id?: string;
        url?: string;
    }): Promise<
        {
            title: string;
            anchor: string | null;
            level: number;
            content: string;
        }[]
    > => {
        const ctx = getDbContext();
        let page = ref.url?.split("#")[0];
        if (!page && ref.id) {
            const r = await ctx.pool.query(
                `SELECT url FROM ${ctx.table} WHERE id = $1`,
                [ref.id],
            );
            page = r.rows[0] ? String(r.rows[0].url).split("#")[0] : undefined;
        }
        if (!page) return [];
        const r = await ctx.pool.query(
            `SELECT title, anchor, level, content FROM ${ctx.table}
             WHERE url = $1 OR url LIKE $2
             ORDER BY level, anchor`,
            [page, `${page}#%`],
        );
        return r.rows.map((row) => ({
            title: String(row.title ?? ""),
            anchor: row.anchor == null ? null : String(row.anchor),
            level: Number(row.level ?? 1),
            content: String(row.content ?? ""),
        }));
    },

    /** Result items for these section ids, in the order given (missing ids skipped). */
    itemsByIds: async (ids: string[], queryTokens: string[]): Promise<SearchResultItem[]> => {
        const ctx = getDbContext();
        if (ids.length === 0) return [];
        const params = ids.map((_, j) => `$${j + 1}`).join(", ");
        const r = await ctx.pool.query(
            `SELECT id, path, url, anchor, title, crumb, grp, kind, level,
                    substr(body, 1, ${SNIPPET_SOURCE_CHARS}) AS content_head
             FROM ${ctx.table}
             WHERE id IN (${params})`,
            ids,
        );
        const byId = new Map(
            r.rows.map((row) => [
                String(row.id),
                toItem(row, { snippet: makeSnippet(String(row.content_head ?? ""), queryTokens) }),
            ]),
        );
        return ids.flatMap((id) => byId.get(id) ?? []);
    },

    /** Look up one section by its exact URL or the page it anchors ("/docs/x" matches "/docs/x#…"). */
    sectionByUrl: async (url: string): Promise<SearchResultItem | null> => {
        const ctx = getDbContext();
        const r = await ctx.pool.query(
            `SELECT id, path, url, anchor, title, crumb, grp, kind,
                    substr(content, 1, ${SNIPPET_SOURCE_CHARS}) AS content_head
             FROM ${ctx.table}
             WHERE url = $1 OR url LIKE $2
             ORDER BY level, id
             LIMIT 1`,
            [url, `${url}#%`],
        );
        if (r.rows.length === 0) return null;
        const row = r.rows[0];
        return toItem(row, {
            snippet: makeSnippet(String(row.content_head ?? ""), []),
        });
    },
};
