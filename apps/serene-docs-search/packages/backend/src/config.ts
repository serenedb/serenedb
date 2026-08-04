import { readFileSync, existsSync, writeFileSync, mkdirSync } from "node:fs";
import path from "node:path";
import type { SereneSearchConfig } from "@serenedb/docs-search-core";

export interface RuntimeEnv {
    configPath: string;
    /** Where a config pushed via PUT /v1/config is persisted. */
    statePath: string;
    port: number;
    token?: string;
    serenedb: {
        host: string;
        port: number;
        database?: string;
        user?: string;
        password?: string;
        /** pg pool size. Every hybrid search embeds its query via a DB
         * round-trip that holds a connection, so this caps real concurrency. */
        poolMax: number;
        /** Wait-for-a-free-connection budget; without it pg waits FOREVER, so
         * a saturated pool makes requests hang until the client/proxy times
         * out instead of failing fast. */
        connectionTimeoutMillis: number;
        /** Server-side per-statement ceiling so a stuck ai_embed (ollama wedged)
         * can't hold a pool connection indefinitely and starve everyone else. */
        statementTimeoutMillis: number;
    };
}

export function readEnv(): RuntimeEnv {
    return {
        configPath: process.env.SERENE_SEARCH_CONFIG || "/etc/serene/config.json",
        statePath: process.env.SERENE_SEARCH_STATE || "/var/lib/serene-search",
        port: intEnv("PORT", 7700),
        token: process.env.SERENE_SEARCH_TOKEN || undefined,
        serenedb: {
            host: process.env.SERENEDB_HOST || "serenedb",
            port: intEnv("SERENEDB_PORT", 7890),
            // SereneDB bootstraps with the postgres/postgres role+db
            database: process.env.SERENEDB_DATABASE || "postgres",
            user: process.env.SERENEDB_USER || "postgres",
            password: process.env.SERENEDB_PASSWORD || undefined,
            poolMax: intEnv("SERENEDB_POOL_MAX", 20),
            connectionTimeoutMillis: intEnv("SERENEDB_POOL_CONNECT_TIMEOUT_MS", 10_000),
            statementTimeoutMillis: intEnv("SERENEDB_STATEMENT_TIMEOUT_MS", 30_000),
        },
    };
}

function intEnv(name: string, fallback: number): number {
    const v = Number(process.env[name]);
    return Number.isFinite(v) && v > 0 ? v : fallback;
}

/**
 * Loads the config: an explicitly mounted file wins; otherwise the last
 * config pushed from the widget (persisted under statePath). Returns null
 * when the backend is still unconfigured.
 */
export function loadConfig(env: RuntimeEnv): SereneSearchConfig | null {
    for (const p of [env.configPath, pushedConfigPath(env)]) {
        if (!existsSync(p)) continue;
        const raw = JSON.parse(readFileSync(p, "utf8")) as SereneSearchConfig;
        return normalizeConfig(expandEnvVars(raw));
    }
    return null;
}

/**
 * Accepts configs written before the AI section split into per-role
 * providers (flat { baseUrl, apiKey, model, embeddingsModel }) and lifts
 * them into the { answers, embeddings } shape.
 */
export function normalizeConfig(config: SereneSearchConfig): SereneSearchConfig {
    const ai = config.ai as
        | (SereneSearchConfig["ai"] & {
              baseUrl?: string;
              apiKey?: string;
              model?: string;
              embeddingsModel?: string;
          })
        | undefined;
    if (!ai || ai.answers || ai.embeddings || !(ai.baseUrl || ai.model || ai.embeddingsModel)) {
        return config;
    }
    const base = { kind: "openai" as const, baseUrl: ai.baseUrl, apiKey: ai.apiKey };
    return {
        ...config,
        ai: {
            enabled: Boolean(ai.enabled),
            answers: ai.enabled ? { ...base, model: ai.model } : undefined,
            embeddings: ai.embeddingsModel ? { ...base, model: ai.embeddingsModel } : undefined,
            systemPrompt: ai.systemPrompt,
        },
    };
}

export function savePushedConfig(env: RuntimeEnv, config: SereneSearchConfig): void {
    mkdirSync(env.statePath, { recursive: true });
    writeFileSync(pushedConfigPath(env), JSON.stringify(config, null, 2));
}

function pushedConfigPath(env: RuntimeEnv): string {
    return path.join(env.statePath, "config.json");
}

/** Expands "${VAR}" strings anywhere in the config from process.env. */
function expandEnvVars<T>(value: T): T {
    if (typeof value === "string") {
        const m = /^\$\{([A-Z0-9_]+)\}$/i.exec(value);
        if (m) return (process.env[m[1]] ?? "") as unknown as T;
        return value;
    }
    if (Array.isArray(value)) return value.map((v) => expandEnvVars(v)) as unknown as T;
    if (value && typeof value === "object") {
        const out: Record<string, unknown> = {};
        for (const [k, v] of Object.entries(value)) out[k] = expandEnvVars(v);
        return out as T;
    }
    return value;
}
