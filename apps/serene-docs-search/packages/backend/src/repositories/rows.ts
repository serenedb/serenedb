import type { SearchResultItem } from "@serenedb/docs-search-core";

export function toItem(
    row: Record<string, unknown>,
    scores: { score?: number; vecScore?: number; snippet?: string; aiSuggested?: boolean },
): SearchResultItem {
    return {
        id: String(row.id),
        path: String(row.path ?? ""),
        url: String(row.url ?? ""),
        anchor: row.anchor == null ? undefined : String(row.anchor),
        title: String(row.title ?? ""),
        crumb: String(row.crumb ?? ""),
        group: String(row.grp ?? ""),
        kind: (row.kind as SearchResultItem["kind"]) ?? "text",
        ...scores,
    };
}

/** pg returns FLOAT[] as "{0.1,0.2}" or an array depending on type mapping. */
export function parseVector(v: unknown): number[] {
    if (Array.isArray(v)) return v.map(Number);
    if (typeof v === "string") {
        return v.replace(/^[{[]|[}\]]$/g, "").split(",").map(Number);
    }
    throw new Error("Unexpected vector representation from server");
}

export function toVectorLiteral(vec: number[]): string {
    return "[" + vec.join(", ") + "]";
}
