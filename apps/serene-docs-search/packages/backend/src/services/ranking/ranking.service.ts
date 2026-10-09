import type { SearchResultItem } from "@serenedb/docs-search-core";
import { pageKey } from "@utils/urlmap";

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

/** A heading repeated on this many pages of one result list is boilerplate. */
const BOILERPLATE_PAGES = 3;

/** Fewer inlinks than this is "not linked" — noise, not a popularity signal. */
const MIN_INLINKS = 3;

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
     * Within a tier, boilerplate titles ("See also", "Next steps" — sections
     * that recur on every page and score high on term density) sink below
     * distinct ones; otherwise the relevance order is untouched. A title
     * counts as boilerplate when sections of at least BOILERPLATE_PAGES
     * different pages carry it — two pages' "Aggregate Functions" are two
     * real answers, not boilerplate.
     *
     * Several titles that all equal the query ("Hybrid Search" in the
     * reference, in a client library, in its FAQ) are told apart first by
     * being the page's own title (the page about it beats a section about
     * it elsewhere), then by how many pages link to theirs: the page the
     * docs themselves point readers to comes first. Pages with fewer than
     * three inlinks count as unlinked; above that, half-log2 buckets
     * (4 and 5 links tie, 12 beats 7) keep small differences from
     * reordering equal titles — the relevance order decides inside a
     * bucket, though one more link can still cross a bucket edge.
     */
    rerankByTitle: (
        q: string,
        results: SearchResultItem[],
        inlinks?: ReadonlyMap<string, number>,
    ): SearchResultItem[] => {
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

        const titlePages = new Map<string, Set<string>>();
        for (const item of results) {
            const key = titleTokens(item.title).join(" ");
            (titlePages.get(key) ?? titlePages.set(key, new Set()).get(key)!).add(pageKey(item.url));
        }
        const seen = new Map<string, number>();
        const decorated = results.map((item, i) => {
            const key = titleTokens(item.title).join(" ");
            const occurrence = seen.get(key) ?? 0;
            seen.set(key, occurrence + 1);
            // every copy of a boilerplate title sinks, including the first
            const dup = (titlePages.get(key)?.size ?? 1) >= BOILERPLATE_PAGES ? 1 + occurrence : 0;
            const t = tier(item);
            const page = t === 2 && item.level != null && item.level <= 1 ? 1 : 0;
            const links = t === 2 ? (inlinks?.get(pageKey(item.url)) ?? 0) : 0;
            const pop = links < MIN_INLINKS ? 0 : Math.floor(2 * Math.log2(links));
            return { item, i, tier: t, pre: rawPrefix(item.title), page, pop, dup };
        });

        return decorated
            .sort(
                (a, b) =>
                    b.tier - a.tier ||
                    b.pre - a.pre ||
                    b.page - a.page ||
                    b.pop - a.pop ||
                    a.dup - b.dup ||
                    a.i - b.i,
            )
            .map((x) => x.item);
    },
};
