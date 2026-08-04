import type {
    SearchResultItem,
    SearchSectionConfig,
} from "@serenedb/docs-search-core";

export interface SectionResultGroup {
    id?: string;
    label: string;
    items: SearchResultItem[];
    /** This section matches the page where the widget is currently open. */
    active?: boolean;
}

export interface GroupedSearchResults {
    /** Stable selection/render order after contextual section prioritization. */
    results: SearchResultItem[];
    groups: SectionResultGroup[];
    /** False preserves the legacy unsectioned built-in UI. */
    sectioned: boolean;
    activeSectionId?: string;
}

interface MatchTarget {
    urls: string[];
    paths: string[];
}

/**
 * Group results without touching relevance order inside a section. The first
 * matching configured section owns a result; the active section is rendered
 * first, followed by the remaining configured order and unmatched results.
 */
export function groupResultsBySections(
    results: SearchResultItem[],
    sections: SearchSectionConfig[] | undefined,
    contextUrl?: string,
): GroupedSearchResults {
    const configured = (sections ?? []).filter(isUsableSection);
    if (configured.length === 0) {
        return {
            results,
            groups: legacyGroups(results),
            sectioned: false,
        };
    }

    const context = contextUrl ? targetFromUrl(contextUrl, contextUrl) : null;
    const active = context
        ? configured.find((section) => sectionMatches(section, context))
        : undefined;
    const buckets = new Map(configured.map((section) => [section.id, [] as SearchResultItem[]]));
    const unmatched: SearchResultItem[] = [];

    for (const item of results) {
        const target = targetFromResult(item);
        const section = configured.find((candidate) => sectionMatches(candidate, target));
        if (section) buckets.get(section.id)!.push(item);
        else unmatched.push(item);
    }

    const orderedSections = active
        ? [active, ...configured.filter((section) => section.id !== active.id)]
        : configured;
    const groups: SectionResultGroup[] = orderedSections.flatMap((section) => {
        const items = buckets.get(section.id)!;
        return items.length
            ? [
                  {
                      id: section.id,
                      label: section.label,
                      items,
                      active: section.id === active?.id || undefined,
                  },
              ]
            : [];
    });
    if (unmatched.length) {
        groups.push({ id: "__other", label: "Other results", items: unmatched });
    }

    return {
        results: groups.flatMap((group) => group.items),
        groups,
        // A heading adds no information when every visible hit belongs to the
        // same bucket. Reveal section labels only for genuinely mixed results.
        sectioned: groups.length > 1,
        activeSectionId: active?.id,
    };
}

function isUsableSection(section: SearchSectionConfig): boolean {
    return Boolean(
        section.id.trim() &&
            section.label.trim() &&
            (section.match.urls?.some(Boolean) || section.match.paths?.some(Boolean)),
    );
}

function legacyGroups(results: SearchResultItem[]): SectionResultGroup[] {
    const groups = new Map<string, SearchResultItem[]>();
    for (const item of results) {
        const label = item.group || "Results";
        const list = groups.get(label);
        if (list) list.push(item);
        else groups.set(label, [item]);
    }
    return [...groups].map(([label, items]) => ({ label, items }));
}

function sectionMatches(section: SearchSectionConfig, target: MatchTarget): boolean {
    return (
        matchesAny(target.urls, section.match.urls) ||
        matchesAny(target.paths, section.match.paths)
    );
}

function matchesAny(candidates: string[], patterns: string[] | undefined): boolean {
    return Boolean(
        patterns?.some((pattern) =>
            candidates.some((candidate) => globMatches(candidate, pattern.trim())),
        ),
    );
}

function targetFromResult(item: SearchResultItem): MatchTarget {
    // Never resolve a relative result URL against the page where the modal is
    // open. A blog hit such as `/blog/post` opened from docs.example.com would
    // otherwise become `https://docs.example.com/blog/post` and be claimed by
    // the Docs URL rule before its indexed `blog/...` source path is checked.
    // The context URL is only for choosing the active/prioritized section.
    const target = targetFromUrl(item.url);
    const paths = new Set(target.paths);
    addPathVariants(paths, item.path);
    return { ...target, paths: [...paths] };
}

function targetFromUrl(value: string, base?: string): MatchTarget {
    const urls = new Set<string>();
    const paths = new Set<string>();
    if (value) urls.add(value);

    try {
        const parsed = new URL(value, base || "http://serene.local/");
        urls.add(parsed.href);
        parsed.hash = "";
        parsed.search = "";
        urls.add(parsed.href);
        addPathVariants(paths, parsed.pathname);
    } catch {
        addPathVariants(paths, value.split(/[?#]/, 1)[0]);
    }

    return { urls: [...urls], paths: [...paths] };
}

function addPathVariants(paths: Set<string>, value: string): void {
    const normalized = value.replace(/\\/g, "/").replace(/[?#].*$/, "");
    if (!normalized) return;
    paths.add(normalized);
    paths.add(normalized.startsWith("/") ? normalized.slice(1) : `/${normalized}`);
}

/** Small browser-safe glob matcher: `*` stays within a segment, `**` crosses it. */
function globMatches(value: string, pattern: string): boolean {
    if (!pattern) return false;
    let source = "";
    for (let i = 0; i < pattern.length; i++) {
        const char = pattern[i];
        if (char === "/" && pattern.slice(i) === "/**") {
            source += "(?:/.*)?";
            i += 2;
        } else if (char === "*" && pattern[i + 1] === "*") {
            source += ".*";
            i++;
        } else if (char === "*") {
            source += "[^/]*";
        } else if (char === "?") {
            source += "[^/]";
        } else {
            source += char.replace(/[\\^$.*+?()[\]{}|]/g, "\\$&");
        }
    }
    return new RegExp(`^${source}$`).test(value);
}
