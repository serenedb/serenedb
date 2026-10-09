/** Distinct terms a query is matched on; longer pastes keep their phrase. */
export const MAX_TERMS = 12;
/** Words of the whole-query phrase clause (pasted sentences); the rest is cut. */
export const MAX_PHRASE_TOKENS = 32;

/**
 * Function words of questions ("how do I …", "what is the …"). They are
 * dropped from the terms a query must match and kept in its phrase clause,
 * so "how do I create an inverted index" matches like "create inverted
 * index" while the literal sentence still ranks first. Query side only —
 * the index keeps them, because docs are full of keywords that look like
 * stopwords (NOT NULL, GROUP BY, ON CONFLICT). Same list as serened's
 * embedded docs search (server/docs/docs_search.cpp, kStopwords).
 */
export const QUERY_STOPWORDS: ReadonlySet<string> = new Set(
    (
        "a an and are as at be by can could do does for from get how i if in into is it its " +
        "let make me my no not of on or our should so than that the their then there this to " +
        "use using we what when where which why will with would yes you your"
    ).split(" "),
);

/**
 * Control characters out of a query (as spaces). A NUL can't travel in a
 * SQL statement at all — the server rejects the whole message — and the
 * query text is inlined into the lexical SQL (see the search repository).
 */
export function cleanQuery(q: string): string {
    return q.replace(/[\u0000-\u0008\u000B\u000C\u000E-\u001F\u007F]/g, " ");
}

function allTokens(q: string): string[] {
    // "_" splits too — the analyzer breaks code identifiers the same way,
    // so "starts_with" and "ts_starts_with" meet on [starts, with] terms
    return q
        .toLowerCase()
        .split(/[^\p{L}\p{N}]+/u)
        .filter(Boolean);
}

export function tokenize(q: string): string[] {
    return allTokens(q).slice(0, MAX_TERMS);
}

/** Meilisearch-style query syntax: "quoted phrases" and -negated words. */
export interface ParsedQuery {
    /** Tokens in order, stopwords included (snippets, spelling, title tiers). */
    tokens: string[];
    /**
     * What the query is matched on: distinct tokens minus question
     * stopwords and single characters — all of them again when that would
     * leave nothing ("on", "is not null" keeps "null"; "the" keeps "the").
     */
    terms: string[];
    /** Ordered tokens of the whole-query phrase, stopwords included. */
    sequence: string[];
    /**
     * The query as written, quotes unwrapped and negations dropped — what
     * phrase clauses hand to the column analyzer, so "transaction's",
     * "e.g." or "write.parquet.row" are tokenized exactly like the text.
     */
    text: string;
    /** The last token while it is still being typed (no space after it). */
    trailing?: string;
    phrases: string[];
    negatives: string[];
}

export function parseQuery(raw: string): ParsedQuery {
    const q = cleanQuery(raw);
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
    // the query as written, quotes dropped: quoted words stay in place, so
    // the whole-query phrase and its word pairs keep the user's word order
    // (phrase words also count as required terms for ranking/snippets)
    const inOrder = q
        .replace(/"([^"]*)"/g, " $1 ")
        .replace(/(^|\s)-([\p{L}\p{N}_]{2,})/gu, "$1")
        .replace(/\s+/g, " ")
        .trim();
    const sequence = allTokens(inOrder).slice(0, MAX_PHRASE_TOKENS);
    // the phrase text spans exactly those tokens — cut after the last one,
    // never inside a word
    let phraseEnd = inOrder.length;
    if (sequence.length === MAX_PHRASE_TOKENS) {
        let n = 0;
        for (const m of inOrder.matchAll(/[\p{L}\p{N}]+/gu)) {
            if (++n === MAX_PHRASE_TOKENS) {
                phraseEnd = (m.index ?? 0) + m[0].length;
                break;
            }
        }
    }
    const distinct = [...new Set(sequence)];
    const content = distinct.filter((t) => t.length >= 2 && !QUERY_STOPWORDS.has(t));
    const terms = (content.length > 0 ? content : distinct).slice(0, MAX_TERMS);
    // mid-word: the raw query ends inside a plain word — not after a space,
    // a closing quote or "?", and not inside a -negation — so that word
    // may still grow and matches as a prefix
    const lastWord = /(?:^|\s)(-?)([^\s"]*[\p{L}\p{N}_])$/u.exec(q);
    const tail = lastWord && !lastWord[1] ? allTokens(lastWord[2]).at(-1) : undefined;
    const trailing = tail && tail === sequence.at(-1) ? tail : undefined;
    return {
        tokens: sequence.slice(0, MAX_TERMS),
        terms,
        sequence,
        text: inOrder.slice(0, phraseEnd),
        trailing,
        phrases: phrases.slice(0, 4),
        negatives: negatives.slice(0, 8),
    };
}
