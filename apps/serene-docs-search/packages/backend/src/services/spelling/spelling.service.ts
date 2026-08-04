import { VocabRepository } from "@repositories/vocab";
import { parseQuery } from "@utils/query";

/** The three vocab lookups spelling needs (VocabRepository satisfies this). */
export interface VocabLookup {
    existing(terms: string[]): Promise<Set<string>>;
    hasPrefix(prefix: string): Promise<boolean>;
    suggest(term: string, maxDist: number): Promise<{ term: string; sim: number; freq: number } | null>;
}

let warnedOnce = false;

/**
 * The correction pass behind SpellingService.correct, parameterized by the
 * vocab lookups so the logic is unit-testable without a database.
 *
 * Returns the query with misspelled words replaced in place — quotes,
 * minuses, word order and the casing of untouched words all survive, so
 * "\"read replika\" -deprecated" corrects the phrase word without
 * resurrecting the negated one. Undefined when nothing needed fixing.
 */
export const correctSpelling = async (
    vocab: VocabLookup,
    q: string,
): Promise<string | undefined> => {
    // parseQuery caps tokens at 12 and already excludes negated words
    const { tokens } = parseQuery(q);
    const correctable = (t: string): boolean => t.length >= 5 && !/^\d+$/.test(t);
    const candidates = [...new Set(tokens.filter(correctable))];
    if (candidates.length === 0) return undefined;
    try {
        const known = await vocab.existing(candidates);
        let missing = candidates.filter((t) => !known.has(t));
        if (missing.length === 0) return undefined;

        // as-you-type guard: a trailing token still being typed matches
        // documents as a prefix ("stemmin" → stemming), so correcting it
        // mid-word would fight the user; a trailing space, quote or "?"
        // means the word is finished and the guard does not apply
        const trailing = /[\p{L}\p{N}]$/u.test(q)
            ? q.toLowerCase().match(/[\p{L}\p{N}]+$/u)?.[0]
            : undefined;
        if (trailing && missing.includes(trailing) && (await vocab.hasPrefix(trailing))) {
            missing = missing.filter((t) => t !== trailing);
            if (missing.length === 0) return undefined;
        }

        // distance budget mirrors termClause: 1 edit from 5 chars, 2 from 9
        const fixes = new Map<string, string>();
        await Promise.all(
            missing.map(async (tok) => {
                const s = await vocab.suggest(tok, tok.length >= 9 ? 2 : 1);
                if (s && s.term !== tok) fixes.set(tok, s.term);
            }),
        );
        if (fixes.size === 0) return undefined;
        return q.replace(/[\p{L}\p{N}]+/gu, (w) => fixes.get(w.toLowerCase()) ?? w);
    } catch (err) {
        if ((err as { statusCode?: number }).statusCode === 409) return undefined; // not configured
        if ((err as { code?: string }).code === "42P01") return undefined; // vocab not built yet
        if (!warnedOnce) {
            warnedOnce = true;
            console.warn("spell correction unavailable:", (err as Error).message);
        }
        return undefined;
    }
};

/**
 * DB-side "did you mean" over the corpus vocabulary table. The decision
 * depends ONLY on the query and the vocabulary — never on which search mode
 * ran or what it returned — so the widget's fulltext and hybrid passes always
 * carry the same correction (a divergent pair flickers the banner), and
 * corrections fire even when stemming/prefix recall already found results for
 * the typo ("embeding" matches docs via the stem "embed", but the word the
 * user meant is still "embedding").
 */
export const SpellingService = {
    correct: (q: string): Promise<string | undefined> => correctSpelling(VocabRepository, q),
};
