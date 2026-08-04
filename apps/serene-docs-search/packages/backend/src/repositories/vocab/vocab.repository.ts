import { getDbContext } from "@database";
import type { Section } from "@repositories/sections";
import { lit } from "@utils/sql";
import type { VocabSuggestion } from "./vocab.types";

const MIN_TERM_LEN = 2;
/** Hashes/base64 blobs in code samples are never correction targets. */
const MAX_TERM_LEN = 32;
const INSERT_BATCH = 500;

/**
 * Version marker for the vocab build (stored in the meta table AFTER a
 * successful rebuild, cleared before one starts). Bump when the tokenizer or
 * the term-length window changes so quiet corpora still get re-vocabbed.
 */
export const VOCAB_SIGNATURE = `v1 terms${MIN_TERM_LEN}-${MAX_TERM_LEN}`;

/**
 * Surface-form corpus vocabulary — the data shape that makes the engine's
 * spell-correction recipe (cookbook/search/spell-correction) work on a docs
 * corpus: one term per row plus its corpus frequency, rebuilt at sync time.
 *
 * The term column is indexed as a PLAIN KEYWORD column (no text-search
 * dictionary) on purpose: over an analyzed column `@@ ts_levenshtein` is
 * classified as a document filter and ts_dict_score flattens to 1.0 for
 * every term; over a keyword column the fuzzy matcher drives the scan and
 * the score is the real similarity (1 − distance/len). Verified on 26.07.1.
 */
export const VocabRepository = {
    /** Cheap idempotent DDL, safe to run on every config apply. */
    ensureSchema: async (): Promise<void> => {
        const ctx = getDbContext();
        await ctx.pool.query(
            `CREATE TABLE IF NOT EXISTS ${ctx.vocabTable} (term VARCHAR PRIMARY KEY, freq INTEGER)`,
        );
        try {
            await ctx.pool.query(
                `CREATE INDEX ${ctx.vocabIndex} ON ${ctx.vocabTable} USING inverted (term)`,
            );
        } catch (err) {
            // rerun-on-boot: an existing index is expected; anything else
            // would silently kill did-you-mean, so it must at least be loud
            if (!/already exists|duplicate/i.test((err as Error).message)) {
                console.warn(
                    `vocab index creation failed (${ctx.vocabIndex}):`,
                    (err as Error).message,
                );
            }
        }
    },

    replaceAll: async (freqs: Map<string, number>): Promise<void> => {
        const ctx = getDbContext();
        await ctx.pool.query(`DELETE FROM ${ctx.vocabTable}`);
        const entries = [...freqs];
        for (let i = 0; i < entries.length; i += INSERT_BATCH) {
            const batch = entries.slice(i, i + INSERT_BATCH);
            const values = batch
                .map((_, j) => `($${j * 2 + 1}, $${j * 2 + 2})`)
                .join(", ");
            await ctx.pool.query(
                `INSERT INTO ${ctx.vocabTable} (term, freq) VALUES ${values}`,
                batch.flat(),
            );
        }
        await ctx.pool.query(`VACUUM (REFRESH_TABLE) ${ctx.vocabTable}`);
    },

    /** Is any vocabulary term a completion of this prefix? (as-you-type guard) */
    hasPrefix: async (prefix: string): Promise<boolean> => {
        const ctx = getDbContext();
        const r = await ctx.pool.query(
            `SELECT term FROM ${ctx.vocabTable} WHERE term LIKE $1 LIMIT 1`,
            [`${prefix}%`],
        );
        return r.rows.length > 0;
    },

    /** Which of these terms exist in the vocabulary (the no-correction check). */
    existing: async (terms: string[]): Promise<Set<string>> => {
        const ctx = getDbContext();
        if (terms.length === 0) return new Set();
        const params = terms.map((_, j) => `$${j + 1}`).join(", ");
        const r = await ctx.pool.query(
            `SELECT term FROM ${ctx.vocabTable} WHERE term IN (${params})`,
            terms,
        );
        return new Set(r.rows.map((row) => String(row.term)));
    },

    /**
     * Best correction candidate: the engine enumerates dictionary terms within
     * the edit distance (ts_levenshtein over the keyword index), scores each by
     * similarity, corpus frequency breaks ties ('serch' → search 563, not the
     * doc-example typos 'saerch'/'serach' at freq 1).
     */
    suggest: async (term: string, maxDist: number): Promise<VocabSuggestion | null> => {
        const ctx = getDbContext();
        // the term is inlined: parameterized ts_* functions misbehave on
        // 26.07.1, and suggest() only ever receives [letters, digits] tokens
        const r = await ctx.pool.query(`
            WITH sugg AS (
              SELECT unnest(ts_dict_agg(term)) AS w, unnest(ts_dict_score(term)) AS sim
              FROM ${ctx.vocabIndex}
              WHERE term @@ ts_levenshtein(${lit(term)}, ${maxDist}, true)
            )
            SELECT s.w, s.sim, v.freq
            FROM sugg s JOIN ${ctx.vocabTable} v ON v.term = s.w
            ORDER BY s.sim DESC, v.freq DESC, s.w
            LIMIT 1`);
        if (r.rows.length === 0) return null;
        const row = r.rows[0];
        return { term: String(row.w), sim: Number(row.sim), freq: Number(row.freq) };
    },
};

/**
 * Corpus term frequencies, tokenized the same way queries are ("_" splits
 * too, so code identifiers contribute their parts as candidate words).
 */
export function vocabFrequencies(sections: Section[]): Map<string, number> {
    const freqs = new Map<string, number>();
    for (const s of sections) {
        for (const w of `${s.title} ${s.content}`.toLowerCase().split(/[^\p{L}\p{N}]+/u)) {
            if (w.length < MIN_TERM_LEN || w.length > MAX_TERM_LEN) continue;
            freqs.set(w, (freqs.get(w) ?? 0) + 1);
        }
    }
    return freqs;
}
