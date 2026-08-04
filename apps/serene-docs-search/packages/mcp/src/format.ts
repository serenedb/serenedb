import {
    formatDocsHits,
    formatDocsSection,
    type DocsHit,
    type SearchResultItem,
    type SectionResponse,
} from "@serenedb/docs-search-core";

/** Absolutize a site-relative doc url when a site base is configured. */
export function absolutize(url: string, siteUrl?: string): string {
    if (!siteUrl || /^https?:\/\//.test(url)) return url;
    return siteUrl.replace(/\/+$/, "") + url;
}

/** Search hits as a compact text block an agent can read and cite. */
export function formatHits(hits: SearchResultItem[], siteUrl?: string): string {
    const dtos: DocsHit[] = hits.map((h) => ({
        title: h.title,
        crumb: h.crumb,
        url: absolutize(h.url, siteUrl),
        snippet: (h.snippet ?? "").replace(/<\/?mark>/g, ""),
    }));
    return formatDocsHits(dtos);
}

/** One full section/page as text. */
export function formatSection(section: SectionResponse, siteUrl?: string): string {
    const crumb = section.crumb ? ` — ${section.crumb}` : "";
    return formatDocsSection(
        `${section.title}${crumb}\nurl: ${absolutize(section.url, siteUrl)}`,
        `\n${section.content}`,
    );
}
