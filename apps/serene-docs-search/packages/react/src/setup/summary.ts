import type { AiProvider, SereneSearchConfig } from "@serenedb/docs-search-core";

/** One-line config digests for the wizard's "Config preview" and the indexing log. */

export function summarizeSource(config: SereneSearchConfig): string {
    const s = config.source;
    switch (s.type) {
        case "git": {
            const repo = (s.url || "…").replace(/^https?:\/\//, "").replace(/\.git$/, "");
            const subdir = Array.isArray(s.subdir) ? s.subdir.join(", ") : s.subdir;
            return `${repo} @ ${s.commit || s.branch || "main"}${subdir ? ` · ${subdir}` : ""}`;
        }
        case "folder":
            return `${s.path || "…"} (mounted)`;
        case "site":
            return `${s.url || "…"} · depth ${s.depth ?? 2}`;
        case "bucket":
            return s.uri || "…";
    }
}

export function summarizeInclude(config: SereneSearchConfig): string {
    const exts = config.content.extensions.join(" ") || "—";
    const md =
        config.content.extensions.some((e) => e === ".md" || e === ".mdx")
            ? ` · ${
                  config.content.markdown?.mode === "whole"
                      ? "whole files"
                      : `split at h1–h${config.content.markdown?.depth ?? 4}`
              }`
            : "";
    return exts + md;
}

export function summarizeSearch(config: SereneSearchConfig): string {
    const parts: string[] = [config.search.type];
    const p = (x?: AiProvider) => x && `${x.kind === "ollama" ? "ollama" : "api"}/${x.model ?? "?"}`;
    if (config.search.type === "hybrid" && config.ai?.embeddings) {
        parts.push(`embed ${p(config.ai.embeddings)}`);
    }
    if (config.ai?.enabled) parts.push(`answers ${p(config.ai.answers)}`);
    if (config.search.sections?.length) {
        parts.push(`${config.search.sections.length} sections`);
    }
    if (config.mcp?.enabled) parts.push("mcp");
    return parts.join(" · ");
}

export function summarizeSync(config: SereneSearchConfig): string {
    const sy = config.sync;
    const base =
        sy.mode === "poll"
            ? `poll every ${sy.interval ?? "1h"}`
            : sy.mode === "commits"
              ? "on git commits"
              : "webhook /v1/reindex";
    return base + (sy.snapshots !== false ? " · snapshots" : "");
}
