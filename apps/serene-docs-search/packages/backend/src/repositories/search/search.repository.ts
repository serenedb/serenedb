import type { SearchResultItem } from "@serenedb/docs-search-core";
import { getDbContext, type DbContext } from "@database";
import { EmbeddingRepository } from "@repositories/embedding";
import { VocabRepository } from "@repositories/vocab";
import { cleanQuery, parseQuery, tokenize, type ParsedQuery } from "@utils/query";
import { makeSnippet, SNIPPET_SOURCE_CHARS } from "@utils/snippet";
import { lit } from "@utils/sql";
import { toItem, toVectorLiteral } from "../rows";
import type { FulltextResult } from "./search.types";

/*
 * Query text is inlined as escaped literals rather than bind parameters:
 * on SereneDB 26.07.1 a parameterized composite tsquery combined with
 * BM25() kills the connection, and a parameterized ts_starts_with()
 * silently matches nothing. Terms come from tokenize() ([letters digits]);
 * phrase clauses and the code ngram take the raw query text, which
 * cleanQuery() rids of control characters (a NUL can't travel in a
 * statement) and lit() escapes for quotes.
 */

/** ts_ngram similarity floor: tolerates typos/whitespace noise, cuts drift. */
const NGRAM_THRESHOLD = 0.45;
const NGRAM_BOOST = 4;

/**
 * Per-field weights of the scored clauses (one BM25 sum). Matching reads
 * three fields: the heading, the heading trail above it and the body (the
 * section with everything nested under it); surface forms of title and
 * body score again through the exact dictionary (Meilisearch's exactness).
 * A word in the heading counts about twice a word in the body — enough to
 * lift "the section about it", not enough that one generic heading word
 * ("SereneDB", "Connect") outweighs the rare words of a question.
 */
const W = {
    title: 2,
    titleExact: 2,
    /** every term in the heading — "the title IS the query" */
    titleAll: 3,
    trail: 1,
    body: 1,
    bodyExact: 1,
    /** the whole query as written, stopwords included */
    phraseTitle: 8,
    phraseBody: 4,
    /** two adjacent query words adjacent in the text too */
    bigram: 2,
};

/**
 * BM25 with mild length normalization (the dictionaries store norms):
 * b = 0.4 keeps a page's top section — which carries the whole page —
 * from outscoring the subsection that actually answers, without
 * flattening the coordination that whole pages give a many-word query.
 */
const BM25_B = 0.4;
const bm25 = (ctx: DbContext): string => `BM25(${ctx.index}.tableoid, 1.2, ${BM25_B})`;

/**
 * A prefix match scores a flat 1 per expanded term (constant scorer), so
 * only the word being typed gets full weight; on finished words a prefix
 * only adds recall for truncated compounds ("meta functions" →
 * "Metadata Functions"), the way serened weights its stem prefixes.
 */
const PREFIX_WEIGHT = { trailing: 1, inner: 0.2 };
/** Inner words take a prefix only in short queries — pastes don't need it. */
const INNER_PREFIX_MAX_TERMS = 4;
/** Typo alternatives (fuzzy/relaxed passes) only for short queries. */
const TYPO_MAX_TERMS = 5;
/** Adjacent word pairs of the query that get a proximity boost. */
const MAX_BIGRAMS = 8;

const MATCH_FIELDS = ["title", "trail", "body"];
/**
 * The exact-dictionary copies match too: a typo or a prefix of an inflected
 * word ("trasactions", "configurat") is within reach of the surface form
 * but not of its stem.
 */
const EXACT_FIELDS = ["lower(title)", "lower(body)"];

/**
 * Pasted code rather than prose: operators and brackets. The word tokenizer
 * throws these symbols away, so such queries get an extra ngram clause over
 * the code column (grams keep symbols and spacing). Quotes deliberately don't
 * count — "what's stemming" is prose and '"exact phrase"' is query syntax;
 * real snippets carry parens/=/@ anyway. Bare "-" stays out too (negation).
 */
const looksLikeCode = (q: string): boolean => /[(){}[\]`;@=<>|\\~^]|::|->/.test(q);

/** lower(code) @@ ts_ngram(...) — the code column is indexed lowercased. */
const ngramClause = (q: string, boosted: boolean): string => {
    // trigrams: a two-character operator ("@@", "->") has none of its own,
    // but code spaces its operators — " @@ " has three
    const text = q.trim().toLowerCase();
    const target = `ts_ngram(${lit(text.length < 3 ? ` ${text} ` : text)}, ${NGRAM_THRESHOLD})`;
    return `lower(code) @@ ${boosted ? `(${target} ^ ${NGRAM_BOOST})` : target}`;
};

type Pass = "strict" | "fuzzy" | "relaxed";

interface Term {
    text: string;
    prefix: number | null;
    typo: number;
    /** An index stopword being typed: matchable only as a prefix. */
    prefixOnly?: boolean;
}

/**
 * Edit budget per term, Meilisearch thresholds: 1 edit from 5 chars, 2
 * from 9, never on pure numbers. (No first-letter anchor: the 4th
 * ts_levenshtein argument is a prefix PREPENDED to the term, not an anchor
 * on it, so it can't express "same first letter".)
 */
const typoBudget = (t: string): number =>
    /^\d+$/.test(t) ? 0 : t.length >= 9 ? 2 : t.length >= 5 ? 1 : 0;

/** The words a pass matches on, each with its prefix/typo alternatives. */
const planTerms = (
    ctx: DbContext,
    parsed: ParsedQuery,
    pass: Pass,
    misspelled?: ReadonlySet<string>,
): Term[] => {
    // index stopwords analyze to nothing — a clause made of one would be
    // empty; typed alone, one still matches as a prefix ("an" → analyze)
    const words = parsed.terms.filter((t) => !ctx.stopwords.includes(t));
    if (words.length === 0 && parsed.trailing && parsed.terms.includes(parsed.trailing)) {
        return [{ text: parsed.trailing, prefix: 1, typo: 0, prefixOnly: true }];
    }
    const short = words.length <= INNER_PREFIX_MAX_TERMS;
    const typos = pass !== "strict" && words.length <= TYPO_MAX_TERMS;
    return words.map((t) => ({
        text: t,
        prefix:
            t === parsed.trailing
                ? PREFIX_WEIGHT.trailing
                : short && t.length >= 3
                  ? PREFIX_WEIGHT.inner
                  : null,
        typo: typos && (!misspelled || misspelled.has(t)) ? typoBudget(t) : 0,
    }));
};

/**
 * A word the corpus spells at most this often is still a typo candidate:
 * the docs quote misspellings themselves ("vacum", "serach" in a
 * spell-correction recipe), so "seen once" doesn't mean "spelled right".
 */
const RARE_TERM_FREQ = 3;

/**
 * Terms worth typo alternatives: the ones the corpus never (or barely)
 * spells that way. A real word missing from the strict match ("how do I
 * connect from a node.js app") is not a typo, and expanding every term by
 * 1–2 edits only drags in look-alikes ("app" ~ "apply"). Undefined when
 * the vocabulary isn't available yet: then every term may be a typo.
 */
const misspelledTerms = async (parsed: ParsedQuery): Promise<Set<string> | undefined> => {
    const candidates = parsed.terms.filter((t) => typoBudget(t) > 0);
    if (candidates.length === 0) return new Set();
    try {
        const freq = await VocabRepository.frequencies(candidates);
        return new Set(candidates.filter((t) => (freq.get(t) ?? 0) <= RARE_TERM_FREQ));
    } catch {
        return undefined;
    }
};

/**
 * The fuzzy pass has something to try when a term may be misspelled, or
 * when adjacent words could be one ("up sert" → upsert): split_join
 * variants are not typos and need no vocabulary verdict.
 */
const fuzzyWorthRunning = (parsed: ParsedQuery, misspelled?: ReadonlySet<string>): boolean =>
    misspelled === undefined ||
    misspelled.size > 0 ||
    (parsed.terms.length >= 2 && parsed.terms.length <= 5);

/**
 * A word still being typed that the terms leave out — a stopword or a
 * single letter ("group b", "create index o", "an" on the way to
 * "analyze") — may be the start of anything, so it only lifts headings it
 * begins a word of; it filters nothing. ts_starts_with reads the index
 * dictionary's terms unanalyzed, so even an index stopword ("an", "the")
 * works as a prefix.
 */
const optionalPrefix = (parsed: ParsedQuery, terms: Term[]): string | null => {
    const t = parsed.trailing;
    if (!t || terms.some((x) => x.text === t)) return null;
    return `title @@ (ts_starts_with(${lit(t)}) ^ ${PREFIX_WEIGHT.trailing})`;
};

/** word / word-or-prefix / word-or-prefix-or-typo for one term. */
const termQuery = (t: Term): string => {
    if (t.prefixOnly) return `ts_starts_with(${lit(t.text)})`;
    const alternatives = [`plainto_tsquery(${lit(t.text)})`];
    if (t.prefix != null) {
        const p = `ts_starts_with(${lit(t.text)})`;
        alternatives.push(t.prefix === 1 ? p : `(${p} ^ ${t.prefix})`);
    }
    if (t.typo > 0) alternatives.push(`ts_levenshtein(${lit(t.text)}, ${t.typo}, true)`);
    return alternatives.length === 1 ? alternatives[0] : `(${alternatives.join(" || ")})`;
};

const anyOf = (parts: string[]): string =>
    parts.length === 1 ? parts[0] : `(${parts.join(" || ")})`;
const allOf = (parts: string[]): string =>
    parts.length === 1 ? parts[0] : `(${parts.join(" && ")})`;
/** SQL-level conjunction of predicates (tsquery-level is allOf). */
const andAll = (preds: string[]): string =>
    preds.length === 1 ? preds[0] : `(${preds.join(" AND ")})`;
/** A term matched in any of the searched fields. */
const inAnyField = (ctx: DbContext, q: string): string => {
    const fields = ctx.exactnessEnabled ? [...MATCH_FIELDS, ...EXACT_FIELDS] : MATCH_FIELDS;
    return `(${fields.map((f) => `${f} @@ ${q}`).join(" OR ")})`;
};

/**
 * Phrase clauses error out when every word is an index stopword ("the",
 * "a") — the analyzer leaves no term to place.
 */
const phraseable = (ctx: DbContext, words: string[]): boolean =>
    words.some((w) => !ctx.stopwords.includes(w));

/**
 * The lexical WHERE of one pass: an unscored filter deciding WHICH rows
 * qualify, AND a disjunction of scored clauses deciding their order.
 *
 * The filter is per term, across fields: a term may sit in the heading,
 * the heading trail or the body — "date_trunc precision" matches a section
 * titled date_trunc whose text explains precision. strict/fuzzy require
 * every term (fuzzy with typo and split-join variants: adjacent words
 * glued, Typesense's split_join_tokens), relaxed any term.
 *
 * The scored clauses sum per field and per term, so the heading, the
 * exact word form and the literal phrase each lift a row on top of plain
 * body matches. Every filtered row matches at least the per-term clauses,
 * so the conjunction never drops a qualified row.
 */
const buildWhere = (
    ctx: DbContext,
    parsed: ParsedQuery,
    rawQ: string,
    pass: Pass,
    misspelled?: ReadonlySet<string>,
): string | null => {
    const terms = planTerms(ctx, parsed, pass, misspelled);
    if (terms.length === 0) return null;
    const queries = terms.map(termQuery);

    // split_join (Typesense): adjacent words glued, so "group by" still
    // finds "groupby" — alternatives of the typo-tolerant passes
    const joined: Term[] = [];
    if (pass !== "strict" && terms.length >= 2 && terms.length <= 5) {
        for (let i = 0; i < terms.length - 1; i++) {
            joined.push({ ...terms[i], text: terms[i].text + terms[i + 1].text });
        }
    }

    let filter: string;
    if (pass === "relaxed") {
        filter = inAnyField(ctx, anyOf([...queries, ...joined.map(termQuery)]));
    } else {
        const variants = [andAll(queries.map((q) => inAnyField(ctx, q)))];
        joined.forEach((pair, i) => {
            const glued = [...terms.slice(0, i), pair, ...terms.slice(i + 2)];
            variants.push(andAll(glued.map((t) => inAnyField(ctx, termQuery(t)))));
        });
        filter = variants.length === 1 ? variants[0] : `(${variants.join(" OR ")})`;
        // quoted phrases are mandatory, in the heading or the body
        for (const p of parsed.phrases) {
            if (!phraseable(ctx, tokenize(p))) continue;
            const ph = `phraseto_tsquery(${lit(p)})`;
            filter = `${filter} AND (title @@ ${ph} OR body @@ ${ph})`;
        }
    }
    const code = looksLikeCode(rawQ);
    if (code) filter = `(${filter}) OR ${ngramClause(rawQ, false)}`;

    // the same alternatives on every field: each column analyzes
    // plainto_tsquery with its own dictionary (stems vs surface forms)
    const any = anyOf([...queries, ...joined.map(termQuery)]);
    const scoring = [
        `title @@ (${any} ^ ${W.title})`,
        `trail @@ (${any} ^ ${W.trail})`,
        `body @@ (${any} ^ ${W.body})`,
        ...(ctx.exactnessEnabled
            ? [
                  `lower(title) @@ (${any} ^ ${W.titleExact})`,
                  `lower(body) @@ (${any} ^ ${W.bodyExact})`,
              ]
            : []),
    ];
    if (terms.length >= 2) scoring.push(`title @@ (${allOf(queries)} ^ ${W.titleAll})`);
    // proximity (Meilisearch's rule 3): the query as written, then its
    // word pairs, outscore the same words scattered. A pair counts when it
    // holds a query term and no index stopword — "insertion order",
    // "create role", "group by", never "do i" or "a select"
    const seq = parsed.sequence;
    if (seq.length >= 2 && phraseable(ctx, seq)) {
        const full = `phraseto_tsquery(${lit(parsed.text)})`;
        scoring.push(`title @@ (${full} ^ ${W.phraseTitle})`, `body @@ (${full} ^ ${W.phraseBody})`);
        if (seq.length >= 3) {
            const isTerm = new Set(terms.map((t) => t.text));
            const pairs = new Set<string>();
            for (let i = 0; i + 1 < seq.length && pairs.size < MAX_BIGRAMS; i++) {
                const pair = [seq[i], seq[i + 1]];
                if (!pair.some((w) => isTerm.has(w))) continue;
                if (pair.some((w) => ctx.stopwords.includes(w))) continue;
                pairs.add(pair.join(" "));
            }
            for (const pair of pairs) {
                scoring.push(`body @@ (phraseto_tsquery(${lit(pair)}) ^ ${W.bigram})`);
            }
        }
    }
    for (const p of parsed.phrases) {
        if (!phraseable(ctx, tokenize(p))) continue;
        scoring.push(`title @@ (phraseto_tsquery(${lit(p)}) ^ ${W.title})`);
    }
    const typing = optionalPrefix(parsed, terms);
    if (typing) scoring.push(typing);
    // snippet paste: symbol-aware contiguous match over the code column
    if (code) scoring.push(ngramClause(rawQ, true));

    let where = `(${filter})::score(NULL) AND (${scoring.join(" OR ")})`;
    if (parsed.negatives.length > 0) {
        const neg = parsed.negatives.map((n) => `plainto_tsquery(${lit(n)})`).join(" || ");
        where += ` AND NOT (title @@ (${neg}) OR body @@ (${neg}))`;
    }
    return where;
};

const ftQuery = async (
    ctx: DbContext,
    whereFragment: string,
    limit: number,
    queryTokens: string[],
): Promise<SearchResultItem[]> => {
    const r = await ctx.pool.query(
        `SELECT id, path, url, anchor, title, crumb, grp, kind, level,
                ${bm25(ctx)} AS score,
                substr(body, 1, ${SNIPPET_SOURCE_CHARS}) AS content_head
         FROM ${ctx.index}
         WHERE ${whereFragment}
         ORDER BY score DESC, id
         LIMIT $1`,
        [limit],
    );
    return r.rows.map((row) =>
        toItem(row, {
            score: Number(row.score),
            snippet: makeSnippet(String(row.content_head ?? ""), queryTokens),
        }),
    );
};

/**
 * Typesense-style distance_threshold: semantic candidates further than
 * this are noise, not suggestions. Off unless configured (the useful
 * value depends on the embeddings model).
 */
const distanceCutSql = (ctx: DbContext, vecExpr: string): string => {
    const t = ctx.vectorDistanceThreshold;
    if (t == null || !Number.isFinite(t) || t <= 0) return "";
    return ` AND cosine_distance(embedding, ${vecExpr}) <= ${Math.min(t, 2)}`;
};

/**
 * The fused statement. Everything is inlined (sanitized literals): bind
 * parameters next to BM25() kill the connection on SereneDB 26.07.1.
 */
const rrfQuery = async (
    ctx: DbContext,
    lexWhere: string | null,
    vec: number[],
    dim: number,
    limit: number,
    queryTokens: string[],
): Promise<SearchResultItem[]> => {
    const max = Math.max(1, Math.min(Math.trunc(limit), 50));
    const vecLit = `${toVectorLiteral(vec)}::FLOAT[${dim}]`;
    const branches: string[] = [];
    if (lexWhere) {
        branches.push(`
              SELECT id, ROW_NUMBER() OVER (ORDER BY s DESC) AS rank,
                     1.0 AS w, 1 AS is_lex
              FROM (
                SELECT id, ${bm25(ctx)} AS s
                FROM ${ctx.index}
                WHERE ${lexWhere}
                ORDER BY s DESC LIMIT ${ctx.rrf.window}
              ) lex`);
    }
    branches.push(`
              SELECT id, ROW_NUMBER() OVER (ORDER BY dist) AS rank,
                     ${ctx.rrf.vectorWeight} AS w, 0 AS is_lex
              FROM (
                SELECT id, cosine_distance(embedding, ${vecLit}) AS dist
                FROM ${ctx.index}
                WHERE embedding IS NOT NULL${distanceCutSql(ctx, vecLit)}
                ORDER BY dist LIMIT ${ctx.rrf.window}
              ) vec`);

    const r = await ctx.pool.query(`
        WITH fused AS (${branches.join("\n              UNION ALL\n")}),
        ranked AS (
          SELECT id, SUM(w / (${ctx.rrf.k} + rank)) AS rrf, MAX(is_lex) AS lex_hit
          FROM fused
          GROUP BY id
          ORDER BY rrf DESC, id
          LIMIT ${max}
        )
        SELECT r.rrf, r.lex_hit, t.id, t.path, t.url, t.anchor, t.title,
               t.crumb, t.grp, t.kind, t.level,
               substr(t.body, 1, ${SNIPPET_SOURCE_CHARS}) AS content_head
        FROM ranked r
        JOIN ${ctx.table} t ON t.id = r.id
        ORDER BY r.rrf DESC, t.id`);
    return r.rows.map((row) =>
        toItem(row, {
            score: Number(row.rrf),
            snippet: makeSnippet(String(row.content_head ?? ""), queryTokens),
            aiSuggested: Number(row.lex_hit) === 0 ? true : undefined,
        }),
    );
};

/**
 * Runs a lexical query; a statement the engine rejects for its shape (an
 * analyzer edge the guards missed — SQLSTATE class 22, e.g. "ts_phrase
 * text arguments produced no searchable terms") degrades to "no lexical
 * matches" instead of a 500. Anything else — connection loss, pool or
 * statement timeouts, a missing table — still fails the request.
 */
const tolerant = async <T>(run: () => Promise<T>, empty: T, q: string): Promise<T> => {
    try {
        return await run();
    } catch (err) {
        if (!/^22/.test(String((err as { code?: string }).code ?? ""))) throw err;
        console.warn(`lexical query rejected for ${JSON.stringify(q)}:`, (err as Error).message);
        return empty;
    }
};

/** From this many terms, the hybrid lexical branch ranks by coverage (OR). */
const HYBRID_OR_MIN_TERMS = 4;

/** A query this long, with this many terms, reads as pasted text when found verbatim. */
const VERBATIM_MIN_TOKENS = 5;
const VERBATIM_MIN_TERMS = 3;
const MAX_VERBATIM = 2;

/**
 * Sections containing the whole query word for word — a sentence or a
 * description pasted from the docs. The fulltext ranking already puts
 * them first (the phrase clause); in hybrid fusion the vector branch's
 * paraphrases outvote them, so they are fetched apart.
 */
const verbatimMatches = async (
    ctx: DbContext,
    parsed: ParsedQuery,
    q: string,
): Promise<SearchResultItem[]> => {
    const seq = parsed.sequence;
    if (
        seq.length < VERBATIM_MIN_TOKENS ||
        parsed.terms.length < VERBATIM_MIN_TERMS ||
        !phraseable(ctx, seq)
    ) {
        return [];
    }
    const phrase = `phraseto_tsquery(${lit(parsed.text)})`;
    let where = `(title @@ ${phrase} OR body @@ ${phrase})`;
    if (parsed.negatives.length > 0) {
        const neg = parsed.negatives.map((n) => `plainto_tsquery(${lit(n)})`).join(" || ");
        where += ` AND NOT (title @@ (${neg}) OR body @@ (${neg}))`;
    }
    return tolerant(() => ftQuery(ctx, where, MAX_VERBATIM, parsed.tokens), [], q);
};

/** Does any row satisfy this lexical WHERE? (cheap existence probe) */
const anyRow = async (ctx: DbContext, where: string | null, q: string): Promise<boolean> => {
    if (!where) return false;
    return tolerant(
        async () => (await ctx.pool.query(`SELECT id FROM ${ctx.index} WHERE ${where} LIMIT 1`)).rows.length > 0,
        false,
        q,
    );
};

/** Exposed for unit tests: the WHERE a pass would run. */
export const buildLexicalWhere = buildWhere;

/** The fulltext / hybrid query paths against the inverted index. */
export const SearchRepository = {
    /**
     * BM25 full-text pass over heading, heading trail and body. The tsquery
     * goes through the column dictionary (plainto_tsquery / ts_levenshtein
     * analyze their input), so stemming, stopwords and synonyms all apply to
     * the query too.
     *
     *   1. strict  — every term must match (in any field); the trailing term
     *                also matches as a prefix (search-as-you-type)
     *   2. fuzzy   — same shape, but each term tolerates 1–2 edits
     *                (Damerau-Levenshtein over the index dictionary)
     *   3. relaxed — any term may match (word/prefix/typo-tolerant):
     *                documents missing some terms, BM25 favours those matching
     *                more. They fill the tail below any full matches — the
     *                Meilisearch "words" rule as buckets.
     */
    searchFulltext: async (raw: string, limit: number): Promise<FulltextResult> => {
        const ctx = getDbContext();
        const q = cleanQuery(raw);
        const parsed = parseQuery(q);
        const { tokens } = parsed;
        const run = (where: string | null, n: number) =>
            where ? tolerant(() => ftQuery(ctx, where, n, tokens), [], q) : Promise.resolve([]);

        const strictWhere = buildWhere(ctx, parsed, q, "strict");
        if (!strictWhere) {
            // pure-symbol snippet ("@@", "->"): no word tokens, ngram only
            if (!looksLikeCode(q)) return { items: [], fuzzy: false, partialFrom: 0 };
            const codeItems = await run(ngramClause(q, false), limit);
            return { items: codeItems, fuzzy: false, partialFrom: codeItems.length };
        }

        let items = await run(strictWhere, limit);
        let fuzzy = false;
        const needsRelaxed = () => items.length < limit && parsed.terms.length > 1;
        const misspelled =
            items.length === 0 || needsRelaxed() ? await misspelledTerms(parsed) : undefined;
        if (items.length === 0 && fuzzyWorthRunning(parsed, misspelled)) {
            items = await run(buildWhere(ctx, parsed, q, "fuzzy", misspelled), limit);
            fuzzy = items.length > 0;
        }

        let partialFrom = items.length;
        if (needsRelaxed()) {
            // partial bucket: phrases stop being mandatory, negations stay
            const relaxed = await run(buildWhere(ctx, parsed, q, "relaxed", misspelled), limit * 2);
            const seen = new Set(items.map((it) => it.id));
            const extra = relaxed.filter((it) => !seen.has(it.id)).slice(0, limit - items.length);
            items = items.concat(extra);
        }
        return { items, fuzzy, partialFrom };
    },

    /**
     * Hybrid pass fused inside SereneDB with weighted Reciprocal Rank Fusion
     * (docs/cookbook/search/reciprocal-rank-fusion): a BM25 branch and a
     * vector-kNN branch are ranked per-branch with ROW_NUMBER, then merged by
     * SUM(w / (k + rank)) in one statement. Falls back to the typo-tolerant
     * lexical expression when the strict one contributes nothing.
     */
    searchHybrid: async (raw: string, limit: number): Promise<FulltextResult> => {
        const ctx = getDbContext();
        const q = cleanQuery(raw);
        if (!ctx.hybrid) throw new Error("hybrid search is not enabled");
        const dim = await EmbeddingRepository.ensureDim();
        const vec = await EmbeddingRepository.embedQuery(q, dim);

        const parsed = parseQuery(q);
        const { tokens } = parsed;
        const fuse = (where: string | null) =>
            tolerant(
                () => rrfQuery(ctx, where, vec, dim, limit, tokens),
                null as SearchResultItem[] | null,
                q,
            ).then((items) => items ?? rrfQuery(ctx, null, vec, dim, limit, tokens));

        // a question rarely has a row holding every one of its words except
        // the long pages that hold every word — the lexical branch then
        // ranks by coverage (OR, like serened's docs search) and the
        // vector branch carries the paraphrase. Pasted text found verbatim
        // and quoted phrases (mandatory) keep the all-terms branch.
        const verbatim = await verbatimMatches(ctx, parsed, q);
        const coverage =
            parsed.terms.length >= HYBRID_OR_MIN_TERMS &&
            verbatim.length === 0 &&
            parsed.phrases.length === 0;
        const withVerbatim = (list: SearchResultItem[]) =>
            verbatim.length
                ? [...verbatim, ...list.filter((it) => !verbatim.some((v) => v.id === it.id))].slice(0, limit)
                : list;
        const contributed = (list: SearchResultItem[]) => list.some((it) => !it.aiSuggested);

        if (coverage) {
            const misspelled = await misspelledTerms(parsed);
            // partial = no row holds every term (the widget says so)
            const [items, complete] = await Promise.all([
                fuse(buildWhere(ctx, parsed, q, "relaxed", misspelled)),
                anyRow(ctx, buildWhere(ctx, parsed, q, "strict"), q),
            ]);
            return { items, fuzzy: false, partialFrom: complete ? items.length : 0 };
        }

        const strictWhere =
            buildWhere(ctx, parsed, q, "strict") ?? (looksLikeCode(q) ? ngramClause(q, false) : null);
        const items = withVerbatim(await fuse(strictWhere));
        if (!contributed(items) && parsed.terms.length > 0) {
            const misspelled = await misspelledTerms(parsed);
            if (fuzzyWorthRunning(parsed, misspelled)) {
                const fuzzyItems = await fuse(buildWhere(ctx, parsed, q, "fuzzy", misspelled));
                if (contributed(fuzzyItems)) {
                    return { items: fuzzyItems, fuzzy: true, partialFrom: fuzzyItems.length };
                }
            }
            // last resort: partial lexical matches (OR) fused with the vector
            // branch — mirrors the fulltext path's "words" bucket
            if (parsed.terms.length > 1) {
                const relaxedItems = await fuse(buildWhere(ctx, parsed, q, "relaxed", misspelled));
                if (contributed(relaxedItems)) {
                    return { items: relaxedItems, fuzzy: false, partialFrom: 0 };
                }
            }
        }
        return { items, fuzzy: false, partialFrom: items.length };
    },

    /** Vector kNN pass; returns cosine similarity as vecScore. */
    searchSemantic: async (raw: string, limit: number): Promise<SearchResultItem[]> => {
        const ctx = getDbContext();
        const q = cleanQuery(raw);
        if (!ctx.hybrid) return [];
        const dim = await EmbeddingRepository.ensureDim();
        const vec = await EmbeddingRepository.embedQuery(q, dim);
        const r = await ctx.pool.query(
            `SELECT id, path, url, anchor, title, crumb, grp, kind, level,
                    cosine_distance(embedding, $1::FLOAT[${dim}]) AS dist,
                    substr(body, 1, ${SNIPPET_SOURCE_CHARS}) AS content_head
             FROM ${ctx.index}
             WHERE embedding IS NOT NULL${distanceCutSql(ctx, `$1::FLOAT[${dim}]`)}
             ORDER BY dist
             LIMIT $2`,
            [toVectorLiteral(vec), limit],
        );
        const tokens = tokenize(q);
        return r.rows.map((row) =>
            toItem(row, {
                vecScore: Math.max(0, 1 - Number(row.dist)),
                snippet: makeSnippet(String(row.content_head ?? ""), tokens),
            }),
        );
    },
};
