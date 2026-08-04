import type { SereneSearchConfig } from "@serenedb/docs-search-core";
import { DEFAULT_TABLE } from "@serenedb/docs-search-core";
import { currentDbContext, DbContext, setDbContext } from "@database";
import { MetaRepository } from "@repositories/meta";
import { SchemaRepository } from "@repositories/schema";
import { SectionsRepository } from "@repositories/sections";
import { VocabRepository } from "@repositories/vocab";
import { Indexer } from "@services/indexing";
import { ModelsService } from "@services/models";
import { SchedulerService } from "@services/scheduler";
import BadRequestError from "@utils/errors/badRequestError";
import { normalizeConfig, readEnv, savePushedConfig, type RuntimeEnv } from "./config";

/**
 * Mutable application state: the backend can boot unconfigured (wizard mode)
 * and get its config pushed later via PUT /v1/config, which swaps in a fresh
 * DbContext (the repositories always read the active one via @database) and
 * restarts the indexer + scheduler.
 */
export class App {
    config: SereneSearchConfig | null = null;
    indexer: Indexer | null = null;
    /** Resolve to null once the role's ollama model is present, to the Error otherwise. */
    embedModelReady: Promise<Error | null> = Promise.resolve(null);
    answerModelReady: Promise<Error | null> = Promise.resolve(null);
    private stopScheduler: (() => void) | null = null;
    private applying: Promise<void> = Promise.resolve();

    constructor(public env: RuntimeEnv) {}

    /** Serialized: overlapping PUT /v1/config applies run strictly one at a time. */
    applyConfig(rawConfig: SereneSearchConfig, opts?: { persist?: boolean }): Promise<void> {
        const run = this.applying.catch(() => {}).then(() => this.doApply(rawConfig, opts));
        this.applying = run;
        return run;
    }

    private async doApply(
        rawConfig: SereneSearchConfig,
        opts?: { persist?: boolean },
    ): Promise<void> {
        const config = normalizeConfig(rawConfig);
        // validate BEFORE tearing anything down: a bad push must neither
        // destroy a working install nor come back as a 500
        if (config.search.type === "hybrid" && !config.ai?.embeddings?.model) {
            throw new BadRequestError("hybrid search requires ai.embeddings (provider + model)");
        }

        // an in-flight sync would otherwise keep writing the old corpus
        // through the context we are about to swap in
        await this.indexer?.stop();
        this.stopScheduler?.();
        this.stopScheduler = null;
        this.indexer = null;
        await currentDbContext()?.close().catch(() => {});
        setDbContext(null);

        try {
            // ollama-backed models pull in the background, PER ROLE: a broken
            // answers model must not block indexing, and vice versa
            this.embedModelReady = ModelsService.prepareProvider(config.ai?.embeddings, (msg) =>
                console.log(`ollama: ${msg}`),
            );
            this.answerModelReady = ModelsService.prepareProvider(config.ai?.answers, (msg) =>
                console.log(`ollama: ${msg}`),
            );

            setDbContext(
                new DbContext(this.env, {
                    table: config.serenedb?.table ?? DEFAULT_TABLE,
                    hybrid: config.search.type === "hybrid",
                    embeddings: config.ai?.embeddings,
                    modelsReady: this.embedModelReady,
                    vectorDistanceThreshold: config.search.vectorDistanceThreshold,
                    rrf: config.search.rrf,
                    stemming: config.search.stemming,
                    synonyms: config.search.synonyms,
                    stopwords: config.search.stopwords,
                }),
            );

            // schema work (incl. the ai_embed dimension probe) happens inside
            // the sync pipeline so config-apply stays fast while models download
            const prepare = async (): Promise<void> => {
                const embedErr = await this.embedModelReady;
                if (embedErr) throw embedErr;
                await SchemaRepository.ensureSchema(config);
            };
            await SchemaRepository.ensureAuxTables();
            await VocabRepository.ensureSchema();
            const indexer = new Indexer(config, this.env, prepare);
            indexer.lastRef = await MetaRepository.get("last_ref");

            // the new setup is live — only now expose and persist the config
            // (a failed apply keeps the last good file for the next boot)
            this.config = config;
            if (opts?.persist !== false) savePushedConfig(this.env, config);
            this.indexer = indexer;
            this.stopScheduler = SchedulerService.start(config, indexer);

            // schema change or empty index -> build right away (cheap checks only)
            const stats = await SectionsRepository.stats();
            if ((await SchemaRepository.needsRebuild()) || stats.sections === 0) {
                void indexer.sync();
            }
        } catch (err) {
            // half-applied state reads as "unconfigured", matching reality —
            // the wizard sees configured:false and can push a fixed config
            await currentDbContext()?.close().catch(() => {});
            setDbContext(null);
            this.config = null;
            this.indexer = null;
            throw err;
        }
    }

    async shutdown(): Promise<void> {
        this.stopScheduler?.();
        await this.indexer?.stop().catch(() => {});
        await currentDbContext()?.close().catch(() => {});
    }
}

/** The process-wide instance every controller/service reads through. */
export const app = new App(readEnv());
