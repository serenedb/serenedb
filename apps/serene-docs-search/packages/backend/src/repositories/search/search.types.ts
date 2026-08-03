import type { SearchResultItem } from "@serenedb/docs-search-core";

export interface FulltextResult {
    items: SearchResultItem[];
    /** Results came from the typo-tolerant fallback. */
    fuzzy: boolean;
    /**
     * Index into `items` where partial matches (documents missing some query
     * terms) begin. Full matches always come first — Meilisearch's "words"
     * rule as a two-bucket ladder. Equal to items.length when every result
     * matched all terms.
     */
    partialFrom: number;
}
