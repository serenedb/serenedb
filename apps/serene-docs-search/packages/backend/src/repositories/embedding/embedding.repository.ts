import { getDbContext, type DbContext } from "@database";
import { MetaRepository } from "@repositories/meta";
import { resolveProvider } from "@utils/providers";
import { lit } from "@utils/sql";
import { parseVector } from "../rows";

const SECRET_NAME = "serene_docs_ai";
const EMBED_TRUNCATE = 6000;

const embedSql = (ctx: DbContext, textExpr: string): string => {
    const model = lit(ctx.embeddings?.model ?? "text-embedding-3-small");
    return `ai_embed(${textExpr}, ${model}, '${SECRET_NAME}')`;
};

const probeDim = async (ctx: DbContext): Promise<number> => {
    await EmbeddingRepository.waitForModels();
    const r = await ctx.pool.query(
        `SELECT array_length(${embedSql(ctx, "'dimension probe'")}, 1) AS dim`,
    );
    const dim = Number(r.rows[0]?.dim);
    if (!Number.isFinite(dim) || dim <= 0) {
        throw new Error("Could not determine embedding dimension from the provider");
    }
    return dim;
};

/**
 * Everything ai_embed: the provider secret, filling the embedding column and
 * query-text vectors. The vector dimension is probed once per schema rebuild
 * and cached (on the context + in the meta table).
 */
export const EmbeddingRepository = {
    /**
     * OpenAI-compatible provider for ai_embed, refreshed on every boot.
     * MUST be PERSISTENT: a plain (temporary) secret is scoped to the ONE
     * pooled connection that created it, so ai_embed on any other pool
     * connection fails "secret not found" and the whole hybrid path silently
     * degrades to fulltext; it is also dropped when the engine restarts. A
     * persistent secret is instance-global and stored in the data dir (the
     * serenedb volume), so it survives both the pool and engine restarts.
     */
    ensureSecret: async (): Promise<void> => {
        const ctx = getDbContext();
        if (!ctx.hybrid || !ctx.embeddings?.baseUrl) return;
        const params = [`TYPE openai`];
        if (ctx.embeddings.apiKey) params.push(`api_key ${lit(ctx.embeddings.apiKey)}`);
        const provider = resolveProvider(ctx.embeddings.baseUrl);
        if (provider) {
            params.push(`base_url ${lit(provider.baseUrl)}`);
            params.push(`embeddings_path ${lit(provider.embeddingsPath)}`);
        }
        // PERSISTENT: instance-global + stored in the data dir (survives the
        // pool AND engine restarts), unlike a plain secret which lands in
        // `memory` storage — session-scoped, so ai_embed on other pool
        // connections can't see it and it's lost on restart. NB: a legacy
        // `memory` secret of the same name (from the pre-fix build) collides
        // here ("Ambiguity detected …") and is only clearable by an engine
        // restart — memory secrets are session-owned, not droppable across
        // connections. `CREATE OR REPLACE PERSISTENT` is storage-qualified, so
        // it never itself trips the ambiguity.
        await ctx.pool.query(
            `CREATE OR REPLACE PERSISTENT SECRET ${SECRET_NAME} (${params.join(", ")})`,
        );
    },

    /** ai_embed needs the model present — ollama pulls may still be running. */
    waitForModels: async (): Promise<void> => {
        const err = await getDbContext().modelsReady;
        if (err) throw new Error(`embeddings model unavailable: ${err.message}`);
    },

    /** Re-probe the provider on schema rebuild and pin the dimension. */
    refreshDim: async (): Promise<number> => {
        const ctx = getDbContext();
        ctx.dim = await probeDim(ctx);
        return ctx.dim;
    },

    ensureDim: async (): Promise<number> => {
        const ctx = getDbContext();
        if (ctx.dim == null) {
            const stored = Number(await MetaRepository.get("embedding_dim"));
            ctx.dim = Number.isFinite(stored) && stored > 0 ? stored : await probeDim(ctx);
        }
        return ctx.dim;
    },

    /**
     * Embeds rows whose embedding is NULL, in small batches so progress is
     * observable and one provider hiccup doesn't fail the whole sync.
     */
    embedMissing: async (onProgress?: (done: number, total: number) => void): Promise<number> => {
        const ctx = getDbContext();
        if (!ctx.hybrid) return 0;
        await EmbeddingRepository.waitForModels();
        const dim = await EmbeddingRepository.ensureDim();
        const totalRes = await ctx.pool.query(
            `SELECT count(*) AS n FROM ${ctx.table} WHERE embedding IS NULL`,
        );
        const total = Number(totalRes.rows[0]?.n ?? 0);
        let done = 0;
        for (;;) {
            const ids = await ctx.pool.query(
                `SELECT id FROM ${ctx.table} WHERE embedding IS NULL LIMIT 16`,
            );
            if (ids.rows.length === 0) break;
            const params = ids.rows.map((_, j) => `$${j + 1}`).join(", ");
            await ctx.pool.query(
                `UPDATE ${ctx.table}
                 SET embedding = ${embedSql(ctx, `substr(title || '. ' || content, 1, ${EMBED_TRUNCATE})`)}::FLOAT[${dim}]
                 WHERE id IN (${params})`,
                ids.rows.map((r) => r.id),
            );
            done += ids.rows.length;
            onProgress?.(Math.min(done, total), total);
        }
        return done;
    },

    /** Query-text embedding through the per-config LRU. */
    embedQuery: async (q: string, dim: number): Promise<number[]> => {
        const ctx = getDbContext();
        const key = q.trim().toLowerCase();
        const hit = ctx.embedCache.get(key);
        if (hit) {
            // refresh recency — Map insertion order approximates the LRU
            ctx.embedCache.delete(key);
            ctx.embedCache.set(key, hit);
            return hit;
        }
        await EmbeddingRepository.waitForModels();
        const r = await ctx.pool.query(
            `SELECT ${embedSql(ctx, "$1")}::FLOAT[${dim}] AS v`,
            [q],
        );
        const vec = parseVector(r.rows[0]?.v);
        ctx.embedCache.set(key, vec);
        if (ctx.embedCache.size > 500) {
            const first = ctx.embedCache.keys().next().value;
            if (first !== undefined) ctx.embedCache.delete(first);
        }
        return vec;
    },
};
