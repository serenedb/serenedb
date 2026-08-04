import { parseInterval, type SereneSearchConfig } from "@serenedb/docs-search-core";
import type { Indexer } from "@services/indexing";
import { SourcesService } from "@services/sources";

const DEFAULT_INTERVAL = 60 * 60 * 1000;
const COMMIT_CHECK_INTERVAL = 60 * 1000;

export const SchedulerService = {
    /**
     * Drives automatic re-syncs:
     *  - poll     — full pipeline every interval (snapshots make it incremental)
     *  - commits  — cheap `git ls-remote` every minute, sync only when HEAD moved
     *  - webhook  — nothing scheduled; POST /v1/reindex triggers syncs
     */
    start: (config: SereneSearchConfig, indexer: Indexer): (() => void) => {
        const { mode } = config.sync;
        if (mode === "webhook") return () => {};

        if (mode === "commits" && config.source.type === "git" && !config.source.commit) {
            const source = config.source;
            const every = parseInterval(config.sync.interval) ?? COMMIT_CHECK_INTERVAL;
            const timer = setInterval(async () => {
                if (indexer.isRunning) return;
                try {
                    const head = await SourcesService.remoteHead(source);
                    if (head && head !== indexer.lastRef) {
                        console.log(`commit watch: ${indexer.lastRef ?? "?"} -> ${head}, syncing`);
                        await indexer.sync();
                    }
                } catch (err) {
                    console.warn("commit watch failed:", (err as Error).message);
                }
            }, Math.min(every, DEFAULT_INTERVAL));
            return () => clearInterval(timer);
        }

        const every = parseInterval(config.sync.interval) ?? DEFAULT_INTERVAL;
        const timer = setInterval(async () => {
            if (!indexer.isRunning) await indexer.sync();
        }, every);
        return () => clearInterval(timer);
    },
};
