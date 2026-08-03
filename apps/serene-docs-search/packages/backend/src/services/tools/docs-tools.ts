import type { SearchResultItem } from "@serenedb/docs-search-core";
import { SearchRepository } from "@repositories/search";
import { SectionsRepository } from "@repositories/sections";
import { RankingService } from "@services/ranking";

const MAX_TOOL_RESULTS = 5;
const MAX_SECTION_CHARS = 8000;
const PER_PAGE_CAP = 3;

export interface DocsSearchHit {
    id: string;
    title: string;
    crumb: string;
    url: string;
    snippet: string;
}

/**
 * The docs-search tool surface shared by the agentic Ask AI loop and the MCP
 * server: the same retrieval the widget uses, packaged as callable tools.
 */
export const DocsTools = {
    /**
     * The same ranking pipeline the widget search uses (fused RRF, title
     * rerank, per-page cap) — raw vector kNN alone buries exact-title pages.
     */
    search: async (query: string, hybrid: boolean, limit = MAX_TOOL_RESULTS): Promise<DocsSearchHit[]> => {
        const capped = Math.max(1, Math.min(limit, 10));
        let hits: SearchResultItem[] = [];
        if (hybrid) {
            try {
                hits = (await SearchRepository.searchHybrid(query, capped * 3)).items;
            } catch {
                /* fall back to fulltext */
            }
        }
        if (hits.length === 0) {
            hits = (await SearchRepository.searchFulltext(query, capped * 3)).items;
        }
        hits = RankingService.rerankByTitle(query, hits);
        hits = RankingService.capPerPage(hits, PER_PAGE_CAP).slice(0, capped);
        return hits.map((h) => ({
            id: h.id,
            title: h.title,
            crumb: h.crumb,
            url: h.url,
            snippet: (h.snippet ?? "").replace(/<\/?mark>/g, ""),
        }));
    },

    /**
     * Full text of the PAGE containing the referenced section — the answer
     * to a section hit usually lives in its sibling subsections (that's
     * where docs keep the code examples).
     */
    read: async (ref: { id?: string; url?: string }): Promise<string | null> => {
        const rows = await SectionsRepository.pageSections(ref);
        if (rows.length === 0) return null;
        const text = rows
            .map((r) => {
                const hashes = "#".repeat(Math.min(Math.max(r.level, 1), 6));
                return `${hashes} ${r.title}\n${r.content}`;
            })
            .join("\n\n");
        return text.slice(0, MAX_SECTION_CHARS);
    },
};
