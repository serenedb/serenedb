/* NB: snippets are built in JS from a content prefix. ts_highlight would be
   the natural tool, but combining it with BM25() and a composite tsquery
   (boost / || / two-column OR) triggers unbounded memory growth and OOM in
   SereneDB 26.07.1 — re-verify on newer engines before switching back. */
export const SNIPPET_SOURCE_CHARS = 1500;
const SNIPPET_WINDOW = 150;

/**
 * Fragment around the first query-term occurrence, with matching words wrapped
 * in <mark>. A word matches when it starts with the term or vice versa — that
 * covers inflections both ways ("run" ⇄ "running", "database" ⇄ "databases").
 * No hit -> the section's leading text, unmarked.
 */
export function makeSnippet(contentHead: string, tokens: string[]): string | undefined {
    const text = contentHead.replace(/\s+/g, " ").trim();
    if (!text) return undefined;

    const terms = tokens.filter((t) => t.length >= 2).map((t) => t.toLowerCase());
    const wordMatches = (word: string): boolean => {
        const w = word.toLowerCase();
        // code identifiers match by their "_"-separated parts too:
        // query "starts" highlights the whole ts_starts_with token
        const candidates = w.includes("_") ? [w, ...w.split("_").filter(Boolean)] : [w];
        return terms.some((t) =>
            candidates.some((c) => c.startsWith(t) || (t.startsWith(c) && c.length >= 3)),
        );
    };

    let firstHit = -1;
    const wordRe = /[\p{L}\p{N}_]+/gu;
    let m: RegExpExecArray | null;
    while ((m = wordRe.exec(text))) {
        if (wordMatches(m[0])) {
            firstHit = m.index;
            break;
        }
    }

    let start = 0;
    if (firstHit > SNIPPET_WINDOW / 2) {
        start = text.lastIndexOf(" ", firstHit - Math.floor(SNIPPET_WINDOW / 3));
        if (start < 0) start = firstHit - Math.floor(SNIPPET_WINDOW / 3);
        start += 1;
    }
    let end = Math.min(text.length, start + SNIPPET_WINDOW);
    const lastSpace = text.lastIndexOf(" ", end);
    if (end < text.length && lastSpace > start + SNIPPET_WINDOW / 2) end = lastSpace;

    const fragment = text
        .slice(start, end)
        .trim()
        .replace(/[\p{L}\p{N}_]+/gu, (word) => (wordMatches(word) ? `<mark>${word}</mark>` : word));
    return (start > 0 ? "…" : "") + fragment + (end < text.length ? "…" : "");
}
