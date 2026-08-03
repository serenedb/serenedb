import type { AiProvider, SereneSearchConfig } from "@serenedb/docs-search-core";

function providerValid(p: AiProvider | undefined): boolean {
    if (!p?.model?.trim()) return false;
    if (p.kind === "openai" && !p.baseUrl?.trim()) return false;
    return true;
}

function sectionsValid(config: SereneSearchConfig): boolean {
    const sections = config.search.sections;
    if (!sections?.length) return true;
    const ids = new Set<string>();
    return sections.every((section) => {
        const id = section.id.trim();
        if (!id || !section.label.trim() || ids.has(id)) return false;
        ids.add(id);
        return Boolean(section.match.urls?.some(Boolean) || section.match.paths?.some(Boolean));
    });
}

function urlMappingValid(config: SereneSearchConfig): boolean {
    const rules = config.content.urlMapping?.rules;
    return !rules?.length || rules.every((rule) => Boolean(rule.match.trim()));
}

/** Can the user proceed past the current step? */
export function stepValid(config: SereneSearchConfig, step: number): boolean {
    if (step === 1) {
        const s = config.source;
        if (s.type === "git") return Boolean(s.url.trim());
        if (s.type === "folder") return Boolean(s.path.trim());
        if (s.type === "site") return /^https?:\/\/.+/.test(s.url.trim());
        return /^s3:\/\/.+/.test(s.uri.trim());
    }
    if (step === 2) {
        return config.content.extensions.length > 0 && urlMappingValid(config);
    }
    if (step === 3) {
        if (!sectionsValid(config)) return false;
        if (config.search.type === "hybrid" && !providerValid(config.ai?.embeddings)) return false;
        if (config.ai?.enabled && !providerValid(config.ai?.answers)) return false;
        return true;
    }
    return true;
}
