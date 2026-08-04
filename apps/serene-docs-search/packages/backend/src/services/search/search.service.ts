import type {
    SearchResponse,
    SearchResultItem,
    SereneSearchConfig,
} from "@serenedb/docs-search-core";
import { SearchRepository, type FulltextResult } from "@repositories/search";
import { SectionsRepository } from "@repositories/sections";
import NotConfiguredError from "@utils/errors/notConfiguredError";
import { app } from "../../app";
import { RankingService } from "@services/ranking";
import { SpellingService } from "@services/spelling";

const DEFAULT_LIMIT = 12;
/** Max sections of one page in the final list (multiple anchors stay useful). */
const PER_PAGE_CAP = 3;

const runQuery = async (
    config: SereneSearchConfig,
    q: string,
    mode: "fulltext" | "hybrid" | undefined,
    max: number,
): Promise<{ outcome: FulltextResult; effectiveMode: "fulltext" | "hybrid" }> => {
    const wantHybrid =
        (mode ?? config.search.type) === "hybrid" && config.search.type === "hybrid";
    if (wantHybrid) {
        try {
            // fused inside SereneDB — see SearchRepository.searchHybrid (cookbook RRF)
            return { outcome: await SearchRepository.searchHybrid(q, max), effectiveMode: "hybrid" };
        } catch (err) {
            console.warn("hybrid fusion failed, lexical only:", (err as Error).message);
        }
    }
    return { outcome: await SearchRepository.searchFulltext(q, max), effectiveMode: "fulltext" };
};

/**
 * Curation (Typesense pinned_hits / Algolia Rules): when the query matches
 * a configured pattern, the target section is forced to the top — fetched
 * from the table if the organic results missed it entirely.
 */
const applyPins = async (
    config: SereneSearchConfig,
    q: string,
    results: SearchResultItem[],
    limit: number,
): Promise<SearchResultItem[]> => {
    const pins = config.search.pins;
    if (!pins?.length) return results;
    const query = q.trim().toLowerCase();
    const pinnedTop: SearchResultItem[] = [];
    let rest = results;
    for (const pin of pins) {
        if (!RankingService.pinMatches(pin.match, query)) continue;
        const inRest = rest.find((r) => r.url === pin.url || r.url.startsWith(`${pin.url}#`));
        let item = inRest;
        if (!item) {
            try {
                item = (await SectionsRepository.sectionByUrl(pin.url)) ?? undefined;
            } catch {
                /* pin target missing — skip silently */
            }
        }
        if (!item || pinnedTop.some((p) => p.id === item!.id)) continue;
        rest = rest.filter((r) => r.id !== item!.id);
        pinnedTop.push({ ...item, pinned: true });
    }
    if (pinnedTop.length === 0) return results;
    return [...pinnedTop, ...rest].slice(0, Math.max(limit, pinnedTop.length));
};

/**
 * Search orchestration: mode selection (hybrid with lexical fallback), then
 * the post-ranking passes — "did you mean", title-exactness rerank per
 * words-bucket, curation pins. See docs/search-pipeline.md, steps 5–6.
 */
export const SearchService = {
    search: async (
        q: string,
        mode: "fulltext" | "hybrid" | undefined,
        limit: number | undefined,
    ): Promise<SearchResponse> => {
        const config = app.config;
        if (!config) throw new NotConfiguredError();
        const started = Date.now();
        const max = Math.min(Math.max(limit ?? DEFAULT_LIMIT, 1), 50);
        // over-fetch so the per-page collapse below can still fill a
        // page-diverse top `max`
        const fetchLimit = Math.min(50, max * 3);
        // the correction depends only on q, never on the outcome (see
        // SpellingService), so it runs concurrently with the search itself
        const [{ outcome, effectiveMode }, correctedQuery] = await Promise.all([
            runQuery(config, q, mode, fetchLimit),
            SpellingService.correct(q),
        ]);
        const { items, fuzzy, partialFrom } = outcome;
        // rerank per words-bucket: partial matches never jump above full ones
        const rankQ = correctedQuery ?? q;
        let results = [
            ...RankingService.rerankByTitle(rankQ, items.slice(0, partialFrom)),
            ...RankingService.rerankByTitle(rankQ, items.slice(partialFrom)),
        ];
        results = RankingService.capPerPage(results, PER_PAGE_CAP).slice(0, max);
        results = await applyPins(config, q, results, max);
        return {
            query: q,
            mode: effectiveMode,
            results,
            total: results.length,
            tookMs: Date.now() - started,
            fuzzy: fuzzy || undefined,
            partial: (results.length > 0 && partialFrom === 0) || undefined,
            correctedQuery,
        };
    },
};
