import type { SereneSearchConfig } from "@serenedb/docs-search-core";
import { getDbContext, type DbContext } from "@database";
import { EmbeddingRepository } from "@repositories/embedding";
import { MetaRepository } from "@repositories/meta";
import { lit } from "@utils/sql";

/**
 * When the effective schema signature changed (search type / embedding model /
 * analyzer options), ensureSchema drops and recreates — a full re-sync follows.
 */
const signature = (ctx: DbContext): string => {
    return JSON.stringify({
        v: 6,
        exactness: ctx.exactnessEnabled,
        hybrid: ctx.hybrid,
        model: ctx.hybrid ? ctx.embeddings?.model : null,
        stemming: ctx.stemming,
        synonyms: ctx.synonyms ?? null,
        stopwords: ctx.stopwords,
    });
};

/**
 * The analyzers, applied identically at index and query time (that's what
 * makes stemming/synonyms/stopwords "meet in the middle"):
 *
 *   main  — tokenize+fold -> split code identifiers on "_" (so
 *           "starts_with" finds ts_starts_with) -> synonyms (optional)
 *           -> stem (optional)
 *   exact — same minus synonyms/stemming: surface word forms, used by the
 *           exactness boost clauses (Meilisearch's "exactness" rule)
 */
const dictionaryDdl = (
    ctx: DbContext,
    name: string,
    exact: boolean,
): string => {
    const stops = ctx.stopwords
        .map((w) => `"${w.replace(/["']/g, "")}"`)
        .join(",");
    const steps = [
        `step1_template = 'text',
             step1_locale = 'en_US.UTF-8',
             step1_case = 'lower',
             step1_accent = true,
             step1_stemming = false,
             step1_stopwords = ${lit(stops)}`,
        `step2_template = 'delimiter', step2_delimiter = '_'`,
    ];
    if (!exact && ctx.synonyms) {
        steps.push(`step${steps.length + 1}_template = 'solr_synonyms',
             step${steps.length + 1}_synonyms = ${lit(ctx.synonyms)}`);
    }
    if (!exact && ctx.stemming) {
        steps.push(
            `step${steps.length + 1}_template = 'stem', step${steps.length + 1}_locale = 'en'`,
        );
    }
    return `
            CREATE TEXT SEARCH DICTIONARY ${name} (
                template = 'pipeline',
                ${steps.join(",\n                ")},
                frequency = true,
                position = true
            )`;
};

const tableExists = async (ctx: DbContext, name: string): Promise<boolean> => {
    const r = await ctx.pool.query(
        `SELECT 1 FROM information_schema.tables WHERE table_name = $1`,
        [name],
    );
    return (r.rowCount ?? 0) > 0;
};

/**
 * Owns the schema: the sections table, the text-search dictionaries and the
 * one inverted index covering BM25 text and the ivf vector.
 */
export const SchemaRepository = {
    /**
     * Meta + analytics tables: cheap idempotent DDL, safe to run on every
     * config apply (unlike ensureSchema, which needs the embeddings model).
     */
    ensureAuxTables: async (): Promise<void> => {
        const ctx = getDbContext();
        await ctx.pool.query(
            `CREATE TABLE IF NOT EXISTS ${ctx.metaTable} (key VARCHAR PRIMARY KEY, value VARCHAR)`,
        );
        await ctx.pool.query(
            `CREATE TABLE IF NOT EXISTS ${ctx.queriesTable} (
                q VARCHAR PRIMARY KEY, count INTEGER, hits INTEGER, last_at VARCHAR)`,
        );
        await ctx.pool.query(
            `CREATE TABLE IF NOT EXISTS ${ctx.clicksTable} (
                id VARCHAR PRIMARY KEY, url VARCHAR, title VARCHAR, clicks INTEGER)`,
        );
    },

    /** Cheap check (no ai_embed): would ensureSchema drop and recreate? */
    needsRebuild: async (): Promise<boolean> => {
        const ctx = getDbContext();
        try {
            if (!(await tableExists(ctx, ctx.table))) return true;
            return (
                (await MetaRepository.get("schema_signature")) !==
                signature(ctx)
            );
        } catch {
            return true;
        }
    },

    ensureSchema: async (
        _config: SereneSearchConfig,
    ): Promise<{ rebuilt: boolean }> => {
        const ctx = getDbContext();
        const sig = signature(ctx);

        await SchemaRepository.ensureAuxTables();
        const existing = await MetaRepository.get("schema_signature");
        const exists = await tableExists(ctx, ctx.table);
        if (exists && existing === sig) {
            await EmbeddingRepository.ensureSecret();
            return { rebuilt: false };
        }

        if (exists) {
            await ctx.pool.query(`DROP TABLE ${ctx.table}`);
        }
        await EmbeddingRepository.ensureSecret();

        let dim: number | null = null;
        if (ctx.hybrid) {
            dim = await EmbeddingRepository.refreshDim();
        }

        // the schema signature changed, so the analyzers must be rebuilt too
        for (const name of [ctx.dict, ctx.exactDict, ctx.ngramDict]) {
            try {
                await ctx.pool.query(`DROP TEXT SEARCH DICTIONARY ${name}`);
            } catch {
                /* didn't exist yet */
            }
        }
        await ctx.pool.query(dictionaryDdl(ctx, ctx.dict, false));
        if (ctx.exactnessEnabled) {
            await ctx.pool.query(dictionaryDdl(ctx, ctx.exactDict, true));
        }
        // trigrams over raw characters — symbols and spaces stay in the grams,
        // which is what makes pasted code ("body @@ plainto_tsquery('…')")
        // findable; ts_ngram needs frequency+position on the field
        await ctx.pool.query(`
            CREATE TEXT SEARCH DICTIONARY ${ctx.ngramDict} (
                template = 'ngram', mingram = 3, maxgram = 3,
                frequency = true, position = true, norm = true
            )`);

        const embeddingCol = ctx.hybrid
            ? `,\n                embedding FLOAT[${dim}]`
            : "";
        await ctx.pool.query(`
            CREATE TABLE ${ctx.table} (
                id VARCHAR PRIMARY KEY,
                path VARCHAR,
                url VARCHAR,
                anchor VARCHAR,
                title VARCHAR,
                crumb VARCHAR,
                grp VARCHAR,
                kind VARCHAR,
                level INTEGER,
                content VARCHAR,
                code VARCHAR,
                hash VARCHAR${embeddingCol}
            )`);

        // vector ANN opclass is `ivf` (SereneDB v26.07.x; `hnsw` was rejected
        // as an unknown opclass — the built-in vector opclass is ivf here)
        const vectorCol = ctx.hybrid
            ? `, embedding ivf (metric = 'cosine')`
            : "";
        // exactness: the same columns again as expression fields analyzed
        // without stemming/synonyms, so surface forms can be boosted
        const exactCols = ctx.exactnessEnabled
            ? `, lower(title) ${ctx.exactDict}, lower(content) ${ctx.exactDict}`
            : "";
        await ctx.pool.query(`
            CREATE INDEX ${ctx.index} ON ${ctx.table}
            USING inverted (id, title ${ctx.dict}, content ${ctx.dict}${exactCols}, lower(code) ${ctx.ngramDict}${vectorCol})
            INCLUDE (path, url, anchor, crumb, grp, kind, level)
            WITH (optimize_top_k = 'bm25(1.2, 0.75)')`);

        if (dim != null) await MetaRepository.set("embedding_dim", String(dim));
        await MetaRepository.set("schema_signature", sig);
        return { rebuilt: true };
    },
};
