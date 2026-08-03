import type { UrlMapping, UrlMappingOptions } from "@serenedb/docs-search-core";
import picomatch from "picomatch";

const DEFAULT_INDEX_FILES = ["index", "README"];

/**
 * Maps a source file path to the URL it is served under.
 * "docs/replication/read-replicas.md" with baseUrl "/docs" and
 * stripPrefix "docs/" -> "/docs/replication/read-replicas".
 */
export function mapUrl(filePath: string, mapping: UrlMapping | undefined): string {
    const m = resolveUrlMapping(filePath, mapping);
    let p = filePath.replace(/\\/g, "/").replace(/^\.?\//, "");

    if (m.stripPrefix) {
        const prefix = m.stripPrefix.replace(/^\/+|\/+$/g, "") + "/";
        if (p.startsWith(prefix)) p = p.slice(prefix.length);
    }
    if (m.stripExtensions !== false) {
        p = p.replace(/\.[A-Za-z0-9]+$/, "");
    }
    const indexNames = m.indexFiles ?? DEFAULT_INDEX_FILES;
    const segments = p.split("/");
    const last = segments[segments.length - 1];
    if (indexNames.some((n) => n.toLowerCase() === last.toLowerCase())) {
        segments.pop();
        p = segments.join("/");
    }

    const base = (m.baseUrl ?? "").replace(/\/+$/, "");
    if (!p) return base || "/";
    return `${base}/${p}`.replace(/\/{2,}/g, "/").replace(/^(https?:)\//, "$1//");
}

/** Resolve the first per-path override while inheriting shared defaults. */
export function resolveUrlMapping(
    filePath: string,
    mapping: UrlMapping | undefined,
): UrlMappingOptions {
    if (!mapping) return {};
    const { rules, ...shared } = mapping;
    if (!rules?.length) return shared;

    const normalized = filePath.replace(/\\/g, "/").replace(/^\.?\//, "");
    const rule = rules.find((candidate) =>
        picomatch.isMatch(normalized, candidate.match, { dot: true }),
    );
    if (!rule) return shared;
    const { match: _match, ...overrides } = rule;
    return { ...shared, ...overrides };
}

export function withAnchor(url: string, anchor: string | undefined): string {
    return anchor ? `${url}#${anchor}` : url;
}
