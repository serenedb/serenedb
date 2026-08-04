import { Request, Response } from "express";
import type { HealthResponse } from "@serenedb/docs-search-core";
import { currentDbContext } from "@database";
import { asyncHandler } from "@middlewares/asyncHandler.middleware";
import { MetaRepository } from "@repositories/meta";
import { SectionsRepository } from "@repositories/sections";
import { app } from "../../app";

const VERSION = "0.9.0";

export const HealthController = {
    get: asyncHandler(async (_req: Request, res: Response) => {
        const ctx = currentDbContext();
        const dbVersion = ctx ? await ctx.serverVersion() : null;
        const stats = ctx
            ? await SectionsRepository.stats()
            : { sections: 0, documents: 0 };
        const lastSyncAt = ctx
            ? await MetaRepository.get("last_sync_at")
            : null;
        const config = app.config;
        const body: HealthResponse = {
            ok: dbVersion != null,
            version: VERSION,
            project: config?.project,
            serenedb: {
                connected: dbVersion != null,
                version: dbVersion ?? undefined,
            },
            index: {
                ready: stats.sections > 0,
                building: app.indexer?.isRunning ?? false,
                sections: stats.sections,
                documents: stats.documents,
                lastSyncAt: lastSyncAt ?? undefined,
            },
            features: {
                ai: Boolean(config?.ai?.enabled),
                hybrid: config?.search.type === "hybrid",
            },
            searchType: config?.search.type ?? "fulltext",
            searchSections: config?.search.sections,
        };
        res.json(body);
    }),
};
