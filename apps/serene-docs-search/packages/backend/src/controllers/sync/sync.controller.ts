import { Request, Response } from "express";
import { asyncHandler } from "@middlewares/asyncHandler.middleware";
import NotConfiguredError from "@utils/errors/notConfiguredError";
import { app } from "../../app";

export const SyncController = {
    /** Shared by POST /v1/sync and its CI-friendly alias POST /v1/reindex. */
    trigger: asyncHandler(async (_req: Request, res: Response) => {
        if (!app.indexer) throw new NotConfiguredError();
        const started = !app.indexer.isRunning;
        if (started) void app.indexer.sync();
        res.json({ started });
    }),

    progress: asyncHandler(async (req: Request, res: Response) => {
        const indexer = app.indexer;
        if (!indexer) throw new NotConfiguredError();
        if (req.query.stream === undefined) {
            return res.json(indexer.snapshot());
        }
        res.setHeader("Content-Type", "text/event-stream");
        res.setHeader("Cache-Control", "no-cache");
        res.setHeader("X-Accel-Buffering", "no"); // don't let nginx buffer the SSE
        res.flushHeaders();
        const send = (p: object) => res.write(`data: ${JSON.stringify(p)}\n\n`);
        send(indexer.snapshot());
        const off = indexer.onProgress(send);
        const ping = setInterval(() => res.write(": ping\n\n"), 15_000);
        res.on("close", () => {
            off();
            clearInterval(ping);
        });
    }),
};
