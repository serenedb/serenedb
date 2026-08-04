import type { SearchResultItem } from "@serenedb/docs-search-core";

/** Lowercase alphanumeric tokens with a crude plural fold ("functions" ≡ "function"). */
const titleTokens = (text: string): string[] => {
    // "_" splits here too — must mirror the index analyzer so that
    // "ts_starts_with" and "starts with" produce comparable token lists
    return text
        .toLowerCase()
        .split(/[^\p{L}\p{N}]+/u)
        .filter(Boolean)
        .map(pluralFold);
};

const pluralFold = (t: string): string => {
    if (t.length > 4 && /(?:[sxz]|ch|sh)es$/.test(t)) return t.slice(0, -2);
    if (t.length > 3 && t.endsWith("s") && !t.endsWith("ss")) return t.slice(0, -1);
    return t;
};

export const RankingService = {
    /**
     * Per-page diversity (Algolia "distinct" / Typesense group_by): keeps the
     * first `cap` sections per page, preserving order. Without it one page can
     * flood the whole list — h4 splitting made that real: "order by" returned
     * ten window-function signatures from a single page, burying the actual
     * ORDER BY clause page.
     */
    capPerPage: (results: SearchResultItem[], cap: number): SearchResultItem[] => {
        const seen = new Map<string, number>();
        return results.filter((item) => {
            const page = item.url.split("#")[0];
            const n = seen.get(page) ?? 0;
            seen.set(page, n + 1);
            return n < cap;
        });
    },

    /** Case-insensitive match with "*" wildcards ("install*", "*replica*"). */
    pinMatches: (pattern: string, query: string): boolean => {
        const re = new RegExp(
            `^${pattern
                .trim()
                .toLowerCase()
                .split("*")
                .map((part) => part.replace(/[.*+?^${}()|[\]\\]/g, "\\$&"))
                .join(".*")}$`,
        );
        return re.test(query);
    },

    /**
     * Title-exactness tiers on top of relevance scores. BM25 has no notion of
     * "the title IS the query" and RRF flattens score gaps into rank gaps, so an
     * exact-title hit can drift below a superset title ("Date Part Extraction
     * Functions" above "Date Part Functions"). Tiers fix that deterministically:
     *   2 — title ≡ query (token sequence, plural-insensitive; the trailing query
     *       token matches as a prefix, so "date part functio" still pins
     *       "Date Part Functions" while the user is mid-word)
     *   1 — every query term matches a title word
     *   0 — everything else
     * Within a tier, repeated titles ("See also", "Next steps" — boilerplate
     * sections that recur on every page and score high on term density) sink
     * below distinct ones; otherwise the relevance order is untouched.
     */
    rerankByTitle: (q: string, results: SearchResultItem[]): SearchResultItem[] => {
        const queryTokens = titleTokens(q);
        if (queryTokens.length === 0) return results;

        // raw-prefix tie-break: tokenization splits "ts_co" into [ts, co], so
        // "ts_offsets(column…)" covers the same tokens ("co" ⊂ column) and ties
        // with "ts_compound(…)" — but only the latter literally continues what
        // the user typed. The dictionary can't see joined identifiers (terms
        // are stored split), so the signal lives here.
        const rawQ = q.toLowerCase().trim().replace(/\s+/g, " ");
        const rawPrefix = (title: string): number =>
            rawQ.length >= 2 &&
            title.toLowerCase().trim().replace(/\s+/g, " ").startsWith(rawQ)
                ? 1
                : 0;

        const tier = (item: SearchResultItem): number => {
            const words = titleTokens(item.title);
            if (
                words.length === queryTokens.length &&
                words.every((w, i) =>
                    i === queryTokens.length - 1 ? w.startsWith(queryTokens[i]) : w === queryTokens[i],
                )
            ) {
                return 2;
            }
            const covered = queryTokens.every((t) =>
                words.some((w) => w.startsWith(t) || (t.startsWith(w) && w.length >= 3)),
            );
            return covered ? 1 : 0;
        };

        const titleCounts = new Map<string, number>();
        for (const item of results) {
            const key = titleTokens(item.title).join(" ");
            titleCounts.set(key, (titleCounts.get(key) ?? 0) + 1);
        }
        const seen = new Map<string, number>();
        const decorated = results.map((item, i) => {
            const key = titleTokens(item.title).join(" ");
            const occurrence = seen.get(key) ?? 0;
            seen.set(key, occurrence + 1);
            // every copy of a recurring title is boilerplate, including the first
            const dup = (titleCounts.get(key) ?? 1) > 1 ? 1 + occurrence : 0;
            return { item, i, tier: tier(item), pre: rawPrefix(item.title), dup };
        });

        return decorated
            .sort((a, b) => b.tier - a.tier || b.pre - a.pre || a.dup - b.dup || a.i - b.i)
            .map((x) => x.item);
    },
};
