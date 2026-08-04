export function tokenize(q: string): string[] {
    // "_" splits too — the analyzer breaks code identifiers the same way,
    // so "starts_with" and "ts_starts_with" meet on [starts, with] terms
    return q
        .toLowerCase()
        .split(/[^\p{L}\p{N}]+/u)
        .filter(Boolean)
        .slice(0, 12);
}

/** Meilisearch-style query syntax: "quoted phrases" and -negated words. */
export interface ParsedQuery {
    tokens: string[];
    phrases: string[];
    negatives: string[];
}

export function parseQuery(q: string): ParsedQuery {
    const phrases: string[] = [];
    const negatives: string[] = [];
    let rest = q.replace(/"([^"]+)"/g, (_, phrase: string) => {
        if (phrase.trim()) phrases.push(phrase.trim());
        return " ";
    });
    rest = rest.replace(/(^|\s)-([\p{L}\p{N}_]{2,})/gu, (_, pre: string, word: string) => {
        negatives.push(word.toLowerCase());
        return pre;
    });
    // phrase words still count as required terms for ranking/snippets
    const tokens = tokenize([rest, ...phrases].join(" "));
    return { tokens, phrases: phrases.slice(0, 4), negatives: negatives.slice(0, 8) };
}
