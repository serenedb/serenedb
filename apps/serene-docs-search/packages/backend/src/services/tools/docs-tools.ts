import { SectionsRepository } from "@repositories/sections";
import { SearchService } from "@services/search";

const MAX_TOOL_RESULTS = 5;
const MAX_SECTION_CHARS = 8000;

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
     * Exactly the widget's search (SearchService: fused RRF, known objects
     * first, title rerank, per-page cap) — raw vector kNN alone buries
     * exact-title pages, and a second copy of the pipeline drifts.
     */
    search: async (query: string, hybrid: boolean, limit = MAX_TOOL_RESULTS): Promise<DocsSearchHit[]> => {
        const capped = Math.max(1, Math.min(limit, 10));
        const { results } = await SearchService.search(query, hybrid ? "hybrid" : "fulltext", capped);
        return results.map((h) => ({
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
