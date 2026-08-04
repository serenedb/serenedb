import type { SearchResultItem } from "@serenedb/docs-search-core";
import { getDbContext, type DbContext } from "@database";
import { EmbeddingRepository } from "@repositories/embedding";
import { parseQuery, tokenize, type ParsedQuery } from "@utils/query";
import { makeSnippet, SNIPPET_SOURCE_CHARS } from "@utils/snippet";
import { lit } from "@utils/sql";
import { toItem, toVectorLiteral } from "../rows";
import type { FulltextResult } from "./search.types";

/*
 * Tokens are inlined as escaped literals rather than bind parameters:
 * on SereneDB 26.07.1 a parameterized composite tsquery combined with
 * BM25() kills the connection, and a parameterized ts_starts_with()
 * silently matches nothing. tokenize() only emits [letters digits _]
 * and lit() escapes quotes, so inlining is safe.
 */

/** ts_ngram similarity floor: tolerates typos/whitespace noise, cuts drift. */
const NGRAM_THRESHOLD = 0.45;
const NGRAM_BOOST = 4;

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
    const target = `ts_ngram(${lit(q.trim().toLowerCase())}, ${NGRAM_THRESHOLD})`;
    return `lower(code) @@ ${boosted ? `(${target} ^ ${NGRAM_BOOST})` : target}`;
};

/**
 * Every term must match, and every term matches as complete-word-or-prefix:
 * the trailing one because the user is mid-word (as-you-type), the inner
 * ones because docs queries are full of truncated compounds — "meta
 * functions" has to find "Metadata Functions". BM25 plus the title tiers
 * keep the extra prefix recall from polluting the top of the list.
 */
const buildStrictQuery = (ctx: DbContext, tokens: string[]): string => {
    const clauses: string[] = [];
    tokens.forEach((t, i) => {
        const isLast = i === tokens.length - 1;
        // inner stopwords contribute nothing — the analyzer drops them anyway
        if (!isLast && ctx.stopwords.includes(t)) return;
        const word = `plainto_tsquery(${lit(t)})`;
        if (isLast || t.length >= 3) {
            clauses.push(`(${word} || ts_starts_with(${lit(t)}))`);
        } else {
            clauses.push(word);
        }
    });
    if (clauses.length === 0) return `plainto_tsquery(${lit(tokens.join(" "))})`;
    return clauses.length === 1 ? clauses[0] : `(${clauses.join(" && ")})`;
};

/**
 * word / word-or-prefix / word-or-prefix-or-typo clause for one term.
 * Typo thresholds follow Meilisearch: 1 edit from 5 chars, 2 from 9,
 * never on pure numbers. (No first-letter anchor: ts_levenshtein's 4th
 * "prefix" argument empirically rejects legitimate distance-1 matches
 * on 26.07.1 — e.g. 'vacum'+prefix 'v' stops matching 'vacuum'.)
 */
const termClause = (t: string, withTypo: boolean): string => {
    const alternatives = [`plainto_tsquery(${lit(t)})`];
    if (t.length >= 3) alternatives.push(`ts_starts_with(${lit(t)})`);
    if (withTypo && !/^\d+$/.test(t)) {
        const dist = t.length >= 9 ? 2 : t.length >= 5 ? 1 : 0;
        if (dist > 0) {
            alternatives.push(`ts_levenshtein(${lit(t)}, ${dist}, true)`);
        }
    }
    return alternatives.length === 1 ? alternatives[0] : `(${alternatives.join(" || ")})`;
};

/**
 * Every term: exact-or-typo. Edit distance scales with term length.
 * Typesense's split_join_tokens (fallback mode): variants with adjacent
 * tokens joined are OR-ed in, so "group by" still finds "groupby".
 */
const buildFuzzyQuery = (tokens: string[]): string | null => {
    if (tokens.length === 0) return null;
    const variant = (list: string[]) => {
        const parts = list.map((t) => termClause(t, true));
        return parts.length === 1 ? parts[0] : `(${parts.join(" && ")})`;
    };
    const variants = [variant(tokens)];
    if (tokens.length >= 2 && tokens.length <= 5) {
        for (let i = 0; i < tokens.length - 1; i++) {
            const joined = [
                ...tokens.slice(0, i),
                tokens[i] + tokens[i + 1],
                ...tokens.slice(i + 2),
            ];
            variants.push(variant(joined));
        }
    }
    return variants.length === 1 ? variants[0] : `(${variants.join(" || ")})`;
};

/**
 * ANY term may match (word, prefix or typo) — the last-resort recall pass.
 * BM25's per-term sums rank documents matching more terms higher. Joined
 * adjacent pairs ride along for the split_join fallback.
 */
const buildRelaxedQuery = (tokens: string[]): string => {
    const parts = tokens.map((t) => termClause(t, true));
    if (tokens.length >= 2 && tokens.length <= 5) {
        for (let i = 0; i < tokens.length - 1; i++) {
            parts.push(termClause(tokens[i] + tokens[i + 1], true));
        }
    }
    return parts.length === 1 ? parts[0] : `(${parts.join(" || ")})`;
};

/**
 * Full lexical WHERE fragment. Boost order per clause (Meilisearch's
 * attribute + exactness rules folded into one BM25 score):
 *   exact title ^6  >  stemmed title ^3  >  exact content ^2  >  content
 * Quoted phrases are mandatory (&&); negated words exclude the document.
 */
const buildWhere = (
    ctx: DbContext,
    expr: string,
    parsed: ParsedQuery,
    rawQ: string,
    withPhrases = true,
): string => {
    let pos = expr;
    if (withPhrases && parsed.phrases.length > 0) {
        const ph = parsed.phrases.map((f) => `phraseto_tsquery(${lit(f)})`);
        pos = `(${[pos, ...ph].join(" && ")})`;
    }
    const clauses = [
        ...(ctx.exactnessEnabled
            ? [`lower(title) @@ (${pos} ^ 6)`, `lower(content) @@ (${pos} ^ 2)`]
            : []),
        `title @@ (${pos} ^ 3)`,
        `content @@ ${pos}`,
    ];
    /*
     * Proximity boosts (Meilisearch's rule 3): words adjacent and in
     * order score far above the same words scattered. The full-query
     * phrase only matches documents that already satisfy the strict
     * expression on that column, so it joins the OR list bare; bigrams
     * would widen the filter and let partial matches into the strict
     * bucket, so they stay guarded by `pos`.
     */
    if (withPhrases && parsed.tokens.length >= 2) {
        const full = `phraseto_tsquery(${lit(parsed.tokens.join(" "))})`;
        clauses.unshift(`title @@ (${full} ^ 8)`, `content @@ (${full} ^ 4)`);
        if (parsed.tokens.length >= 3) {
            for (let i = 0; i < parsed.tokens.length - 1 && i < 3; i++) {
                const bi = `phraseto_tsquery(${lit(`${parsed.tokens[i]} ${parsed.tokens[i + 1]}`)})`;
                clauses.push(`content @@ ((${pos}) && (${bi} ^ 2))`);
            }
        }
    }
    // snippet paste: symbol-aware contiguous match over the code column
    if (looksLikeCode(rawQ)) {
        clauses.push(ngramClause(rawQ, true));
    }
    let where = `(${clauses.join(" OR ")})`;
    if (parsed.negatives.length > 0) {
        const neg = parsed.negatives.map((n) => `plainto_tsquery(${lit(n)})`).join(" || ");
        where += ` AND NOT (title @@ (${neg}) OR content @@ (${neg}))`;
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
        `SELECT id, path, url, anchor, title, crumb, grp, kind,
                BM25(${ctx.index}.tableoid) AS score,
                substr(content, 1, ${SNIPPET_SOURCE_CHARS}) AS content_head
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
                SELECT id, BM25(${ctx.index}.tableoid) AS s
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
               t.crumb, t.grp, t.kind,
               substr(t.content, 1, ${SNIPPET_SOURCE_CHARS}) AS content_head
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

/** The fulltext / hybrid query paths against the inverted index. */
export const SearchRepository = {
    /**
     * BM25 full-text pass over title AND body. The tsquery goes through the
     * column dictionary (plainto_tsquery / ts_levenshtein analyze their input),
     * so stemming, stopwords and synonyms all apply to the query too.
     *
     *   1. strict  — every term must match; the trailing term also matches as
     *                a prefix (search-as-you-type)
     *   2. fuzzy   — same shape, but each term tolerates 1–2 edits
     *                (Damerau-Levenshtein over the index dictionary)
     *   3. relaxed — OR over the terms (each still word/prefix/typo-tolerant):
     *                documents missing some terms, BM25 favours those matching
     *                more. They fill the tail below any full matches — the
     *                Meilisearch "words" rule as buckets.
     */
    searchFulltext: async (q: string, limit: number): Promise<FulltextResult> => {
        const ctx = getDbContext();
        const parsed = parseQuery(q);
        const { tokens } = parsed;
        if (tokens.length === 0) {
            // pure-symbol snippet ("@@", "->"): no word tokens, ngram only
            if (!looksLikeCode(q)) return { items: [], fuzzy: false, partialFrom: 0 };
            const codeItems = await ftQuery(ctx, ngramClause(q, false), limit, tokens);
            return { items: codeItems, fuzzy: false, partialFrom: codeItems.length };
        }

        let items = await ftQuery(
            ctx,
            buildWhere(ctx, buildStrictQuery(ctx, tokens), parsed, q),
            limit,
            tokens,
        );
        let fuzzy = false;
        if (items.length === 0) {
            const fuzzyExpr = buildFuzzyQuery(tokens);
            if (fuzzyExpr) {
                items = await ftQuery(ctx, buildWhere(ctx, fuzzyExpr, parsed, q), limit, tokens);
                fuzzy = items.length > 0;
            }
        }

        let partialFrom = items.length;
        if (items.length < limit && tokens.length > 1) {
            // partial bucket: phrases stop being mandatory, negations stay
            const relaxed = await ftQuery(
                ctx,
                buildWhere(ctx, buildRelaxedQuery(tokens), parsed, q, false),
                limit * 2,
                tokens,
            );
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
    searchHybrid: async (q: string, limit: number): Promise<FulltextResult> => {
        const ctx = getDbContext();
        if (!ctx.hybrid) throw new Error("hybrid search is not enabled");
        const dim = await EmbeddingRepository.ensureDim();
        const vec = await EmbeddingRepository.embedQuery(q, dim);

        const parsed = parseQuery(q);
        const { tokens } = parsed;
        const strictWhere =
            tokens.length > 0
                ? buildWhere(ctx, buildStrictQuery(ctx, tokens), parsed, q)
                : looksLikeCode(q)
                  ? ngramClause(q, false)
                  : null;
        const items = await rrfQuery(ctx, strictWhere, vec, dim, limit, tokens);
        const contributed = (list: SearchResultItem[]) => list.some((it) => !it.aiSuggested);
        if (!contributed(items) && tokens.length > 0) {
            const fuzzyExpr = buildFuzzyQuery(tokens);
            if (fuzzyExpr) {
                const fuzzyItems = await rrfQuery(
                    ctx,
                    buildWhere(ctx, fuzzyExpr, parsed, q),
                    vec,
                    dim,
                    limit,
                    tokens,
                );
                if (contributed(fuzzyItems)) {
                    return { items: fuzzyItems, fuzzy: true, partialFrom: fuzzyItems.length };
                }
            }
            // last resort: partial lexical matches (OR) fused with the vector
            // branch — mirrors the fulltext path's "words" bucket
            if (tokens.length > 1) {
                const relaxedItems = await rrfQuery(
                    ctx,
                    buildWhere(ctx, buildRelaxedQuery(tokens), parsed, q, false),
                    vec,
                    dim,
                    limit,
                    tokens,
                );
                if (contributed(relaxedItems)) {
                    return { items: relaxedItems, fuzzy: false, partialFrom: 0 };
                }
            }
        }
        return { items, fuzzy: false, partialFrom: items.length };
    },

    /** Vector kNN pass; returns cosine similarity as vecScore. */
    searchSemantic: async (q: string, limit: number): Promise<SearchResultItem[]> => {
        const ctx = getDbContext();
        if (!ctx.hybrid) return [];
        const dim = await EmbeddingRepository.ensureDim();
        const vec = await EmbeddingRepository.embedQuery(q, dim);
        const r = await ctx.pool.query(
            `SELECT id, path, url, anchor, title, crumb, grp, kind,
                    cosine_distance(embedding, $1::FLOAT[${dim}]) AS dist,
                    substr(content, 1, ${SNIPPET_SOURCE_CHARS}) AS content_head
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
