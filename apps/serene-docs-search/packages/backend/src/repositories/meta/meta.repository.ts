import { getDbContext } from "@database";

/** Key/value store next to the sections table: schema signature, sync markers. */
export const MetaRepository = {
    get: async (key: string): Promise<string | null> => {
        const ctx = getDbContext();
        try {
            const r = await ctx.pool.query(
                `SELECT value FROM ${ctx.metaTable} WHERE key = $1`,
                [key],
            );
            return r.rows[0] ? String(r.rows[0].value) : null;
        } catch {
            return null;
        }
    },

    set: async (key: string, value: string): Promise<void> => {
        const ctx = getDbContext();
        await ctx.pool.query(
            `INSERT INTO ${ctx.metaTable} (key, value) VALUES ($1, $2)
             ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value`,
            [key, value],
        );
    },
};
