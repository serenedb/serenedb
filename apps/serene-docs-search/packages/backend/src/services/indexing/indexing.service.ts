import { mkdirSync } from "node:fs";
import path from "node:path";
import {
    describeSource,
    type SereneSearchConfig,
    type SyncProgress,
} from "@serenedb/docs-search-core";
import { EmbeddingRepository } from "@repositories/embedding";
import { MetaRepository } from "@repositories/meta";
import { SectionsRepository, type Section } from "@repositories/sections";
import { vocabFrequencies, VocabRepository, VOCAB_SIGNATURE } from "@repositories/vocab";
import { ParsingService } from "@services/parsing";
import { SourcesService, type FetchContext, type FetchResult } from "@services/sources";
import type { RuntimeEnv } from "../../config";

type ProgressListener = (p: SyncProgress) => void;

function initialProgress(): SyncProgress {
    return {
        state: "idle",
        steps: {
            fetch: { status: "pending" },
            parse: { status: "pending" },
            embed: { status: "pending" },
            index: { status: "pending" },
        },
    };
}

/**
 * Runs the fetch -> parse -> embed -> index pipeline and exposes observable
 * progress (the widget's indexing screen mirrors these steps 1:1). Unlike the
 * const services, this is a per-config instance — it carries live pipeline
 * state (progress, listeners, the last synced ref).
 */
export class Indexer {
    private progress: SyncProgress = initialProgress();
    private listeners = new Set<ProgressListener>();
    private running = false;
    private cancelled = false;
    private current: Promise<boolean> | null = null;
    /** Version marker of the last successful sync (git sha / crawl stamp). */
    lastRef: string | null = null;

    constructor(
        private config: SereneSearchConfig,
        private env: RuntimeEnv,
        /** Pulls ollama models + ensures the schema; runs first in every sync. */
        private prepare?: () => Promise<void>,
    ) {}

    snapshot(): SyncProgress {
        return JSON.parse(JSON.stringify(this.progress)) as SyncProgress;
    }

    get isRunning(): boolean {
        return this.running;
    }

    onProgress(fn: ProgressListener): () => void {
        this.listeners.add(fn);
        return () => this.listeners.delete(fn);
    }

    private emit(mutate: (p: SyncProgress) => void): void {
        mutate(this.progress);
        const snap = this.snapshot();
        for (const fn of this.listeners) fn(snap);
    }

    /**
     * Cancels any in-flight sync and waits for it to unwind. Called on config
     * swap: the repositories resolve the ACTIVE DbContext at call time, so a
     * sync that outlived its config would otherwise write the old corpus
     * through the new context.
     */
    async stop(): Promise<void> {
        this.cancelled = true;
        await this.current?.catch(() => {});
    }

    /** Between-step guard: a cancelled sync must stop touching the database. */
    private ensureActive(): void {
        if (this.cancelled) throw new Error("sync cancelled: configuration changed");
    }

    /** Starts a sync unless one is already running. */
    sync(): Promise<boolean> {
        if (this.running || this.cancelled) return Promise.resolve(false);
        this.current = this.run();
        return this.current;
    }

    private async run(): Promise<boolean> {
        this.running = true;
        const started = Date.now();
        this.progress = initialProgress();
        this.emit((p) => {
            p.state = "running";
            p.startedAt = new Date(started).toISOString();
            p.source = describeSource(this.config.source);
            p.steps.fetch.status = "running";
            p.steps.fetch.detail = "preparing models & schema";
        });

        try {
            // 0 · prepare — ollama pulls + schema (both idempotent, can be slow
            // on first boot while models download)
            await this.prepare?.();
            this.ensureActive();
            this.emit((p) => {
                p.steps.fetch.detail = undefined;
            });

            // 1 · fetch
            const fetched = await this.fetch();
            this.ensureActive();
            this.emit((p) => {
                p.steps.fetch = { status: "done", files: fetched.files.length };
                p.steps.parse.status = "running";
            });

            // 2 · parse
            const sections: Section[] = [];
            for (const file of fetched.files) {
                sections.push(...(await ParsingService.parseFile(file, this.config.content)));
                this.emit((p) => {
                    p.steps.parse.sections = sections.length;
                });
            }
            this.emit((p) => {
                p.steps.parse = { status: "done", sections: sections.length };
            });
            this.ensureActive();

            // 3 · diff + upsert (snapshots: skip unchanged, prune deleted)
            const snapshots = this.config.sync.snapshots !== false;
            let changed: Section[];
            let pruned = 0;
            if (snapshots) {
                const existing = await SectionsRepository.existingHashes();
                changed = sections.filter((s) => existing.get(s.id) !== s.hash);
                const liveIds = new Set(sections.map((s) => s.id));
                const stale = [...existing.keys()].filter((id) => !liveIds.has(id));
                await SectionsRepository.deleteSections(stale);
                pruned = stale.length;
            } else {
                await SectionsRepository.truncate();
                changed = sections;
            }
            await SectionsRepository.upsertSections(changed);
            this.ensureActive();

            // 4 · embed (hybrid only)
            if (this.config.search.type === "hybrid") {
                this.emit((p) => {
                    p.steps.embed.status = "running";
                });
                await EmbeddingRepository.embedMissing((done, total) => {
                    this.emit((p) => {
                        p.steps.embed = { status: "running", done, total };
                    });
                });
                this.emit((p) => {
                    p.steps.embed = { ...p.steps.embed, status: "done" };
                });
            } else {
                this.emit((p) => {
                    p.steps.embed.status = "skipped";
                });
            }

            // 5 · refresh index so new rows are searchable
            this.ensureActive();
            this.emit((p) => {
                p.steps.index.status = "running";
            });
            // vocab powering DB-side "did you mean" — rebuilt when the corpus
            // changed, the tokenizer version changed, or the previous build
            // never finished (the marker is cleared first and set only on
            // success, so a half-written vocab is always rebuilt next sync)
            const vocabSig = await MetaRepository.get("vocab_signature");
            if (changed.length > 0 || pruned > 0 || vocabSig !== VOCAB_SIGNATURE) {
                try {
                    await MetaRepository.set("vocab_signature", "");
                    await VocabRepository.replaceAll(vocabFrequencies(sections));
                    await MetaRepository.set("vocab_signature", VOCAB_SIGNATURE);
                } catch (err) {
                    console.warn(
                        "vocab rebuild failed (did-you-mean degraded):",
                        (err as Error).message,
                    );
                }
            }
            await SectionsRepository.refreshIndex();
            const stats = await SectionsRepository.stats();
            this.lastRef = fetched.ref ?? null;
            await MetaRepository.set("last_ref", this.lastRef ?? "");
            await MetaRepository.set("last_sync_at", new Date().toISOString());

            this.emit((p) => {
                p.steps.index = {
                    status: "done",
                    detail: this.config.search.type === "hybrid" ? "fulltext + vector (ivf)" : "fulltext",
                };
                p.state = "done";
                p.finishedAt = new Date().toISOString();
                p.sections = stats.sections;
                p.documents = stats.documents;
                p.tookMs = Date.now() - started;
                p.incremental = snapshots && changed.length < sections.length;
            });
            console.log(
                `sync done: ${stats.sections} sections (${changed.length} changed, ${pruned} pruned) in ${Date.now() - started}ms`,
            );
            return true;
        } catch (err) {
            const message = (err as Error).message;
            console.error("sync failed:", err);
            this.emit((p) => {
                p.state = "error";
                p.error = message;
                p.finishedAt = new Date().toISOString();
                for (const step of Object.values(p.steps)) {
                    if (step.status === "running") step.status = "error";
                }
            });
            return false;
        } finally {
            this.running = false;
        }
    }

    private async fetch(): Promise<FetchResult> {
        const workDir = path.join(this.env.statePath, "work");
        mkdirSync(workDir, { recursive: true });
        const ctx: FetchContext = {
            workDir,
            extensions: this.config.content.extensions,
            exclude: this.config.content.exclude ?? [],
            onProgress: (files) =>
                this.emit((p) => {
                    p.steps.fetch.files = files;
                }),
        };
        return SourcesService.fetch(this.config.source, ctx);
    }
}
