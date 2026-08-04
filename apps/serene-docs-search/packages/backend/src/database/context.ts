import pg from "pg";
import type { AiProvider } from "@serenedb/docs-search-core";
import { ident } from "@utils/sql";
import type { RuntimeEnv } from "../config";

const { Pool } = pg;

export interface DbRuntimeOptions {
    table: string;
    hybrid: boolean;
    /** Embeddings provider (hybrid installs). */
    embeddings?: AiProvider;
    /** Resolves to an Error when ollama model preparation failed, null when ready. */
    modelsReady?: Promise<Error | null>;
    /** Cosine-distance cut-off for semantic candidates (model-dependent). */
    vectorDistanceThreshold?: number;
    /** Hybrid fusion tuning. */
    rrf?: { vectorWeight?: number; k?: number; window?: number };
    /** Snowball stemming in the analyzer (default true). */
    stemming?: boolean;
    /** Solr-format synonym map (multiline). */
    synonyms?: string;
    /** Stopword list; defaults to a small English set. */
    stopwords?: string[];
}

/** Per-branch RRF defaults: docs search is lexical-first, semantic supplements. */
const RRF_DEFAULTS = { vectorWeight: 0.7, k: 60, window: 50 };

/*
 * Deliberately tiny: technical docs are full of "stopwords" that are really
 * keywords — NOT NULL, GROUP BY, ON CONFLICT, IS NULL, FROM, OR… Dropping
 * them at analysis time makes those queries indistinguishable. Articles and
 * demonstratives are the only safe cut; BM25's idf already dampens the rest.
 */
const DEFAULT_STOPWORDS = ["the", "a", "an", "this", "these", "those"];

/**
 * SereneDB access context: the connection pool, the derived object names,
 * the resolved runtime options and the per-config caches every repository
 * works against. Recreated whenever a new config is applied — repositories
 * reach the active instance through getDbContext().
 *
 * NB: full-text predicates must SELECT FROM THE INDEX name, and scorers take
 * the index's tableoid — see docs/sql/indexes/inverted.
 */
export class DbContext {
    readonly pool: pg.Pool;
    readonly table: string;
    readonly hybrid: boolean;
    readonly embeddings?: AiProvider;
    readonly vectorDistanceThreshold?: number;
    readonly rrf: { vectorWeight: number; k: number; window: number };
    /** Resolves to an Error when ollama model preparation failed, null when ready. */
    readonly modelsReady: Promise<Error | null>;
    readonly stemming: boolean;
    readonly synonyms?: string;
    readonly stopwords: string[];
    /** Embedding dimension, pinned by probe per schema build. */
    dim: number | null = null;
    /** Query-embedding LRU so typing doesn't hammer the provider. */
    readonly embedCache = new Map<string, number[]>();

    constructor(env: RuntimeEnv, opts: DbRuntimeOptions) {
        this.table = ident(opts.table);
        this.hybrid = opts.hybrid;
        this.embeddings = opts.embeddings;
        this.vectorDistanceThreshold = opts.vectorDistanceThreshold;
        this.rrf = { ...RRF_DEFAULTS, ...opts.rrf };
        this.modelsReady = opts.modelsReady ?? Promise.resolve(null);
        this.stemming = opts.stemming !== false;
        this.synonyms = opts.synonyms?.trim() || undefined;
        this.stopwords = opts.stopwords ?? DEFAULT_STOPWORDS;
        this.pool = new Pool({
            host: env.serenedb.host,
            port: env.serenedb.port,
            database: env.serenedb.database,
            user: env.serenedb.user,
            password: env.serenedb.password,
            // Concurrency hardening: a bigger pool (query-embedding holds a
            // connection during the ollama round-trip), a bounded wait for a
            // free connection (else pg queues forever → "requests hang until
            // timeout" under load), and a server-side statement ceiling so a
            // wedged ai_embed can't leak a connection permanently.
            max: env.serenedb.poolMax,
            connectionTimeoutMillis: env.serenedb.connectionTimeoutMillis,
            statement_timeout: env.serenedb.statementTimeoutMillis,
            idleTimeoutMillis: 30_000,
        });
    }

    get dict(): string {
        return `${this.table}_dict`;
    }
    get exactDict(): string {
        return `${this.table}_dict_x`;
    }
    get ngramDict(): string {
        return `${this.table}_dict_ng`;
    }
    get index(): string {
        return `${this.table}_idx`;
    }
    get metaTable(): string {
        return `${this.table}_meta`;
    }
    get queriesTable(): string {
        return `${this.table}_queries`;
    }
    get clicksTable(): string {
        return `${this.table}_clicks`;
    }
    get vocabTable(): string {
        return `${this.table}_vocab`;
    }
    get vocabIndex(): string {
        return `${this.table}_vocab_idx`;
    }

    /** Exactness clauses only pay off when the main analyzer rewrites terms. */
    get exactnessEnabled(): boolean {
        return this.stemming || Boolean(this.synonyms);
    }

    async close(): Promise<void> {
        await this.pool.end();
    }

    async serverVersion(): Promise<string | null> {
        try {
            const r = await this.pool.query("SELECT version()");
            return String(r.rows[0]?.version ?? "");
        } catch {
            return null;
        }
    }
}
