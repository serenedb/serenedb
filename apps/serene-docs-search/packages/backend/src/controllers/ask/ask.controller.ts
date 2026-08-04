import { NextFunction, Request, Response } from "express";
import { plainToInstance } from "class-transformer";
import { validate } from "class-validator";
import { asyncHandler } from "@middlewares/asyncHandler.middleware";
import { Services } from "@services";
import BadRequestError from "@utils/errors/badRequestError";
import NotConfiguredError from "@utils/errors/notConfiguredError";
import ServiceUnavailableError from "@utils/errors/serviceUnavailableError";
import CustomValidationError from "@utils/errors/validationError";
import { app } from "../../app";
import { AskDto } from "./dto/ask.dto";

export const AskController = {
    ask: asyncHandler(
        async (req: Request, res: Response, next: NextFunction) => {
            const data = plainToInstance(AskDto, req.body ?? {});
            const errors = await validate(data);
            if (errors.length > 0) {
                return next(new CustomValidationError(errors));
            }
            const config = app.config;
            if (!config) throw new NotConfiguredError();
            if (!config.ai?.enabled)
                throw new BadRequestError("AI answers are not enabled");
            if (!data.q.trim()) throw new BadRequestError("Missing question");

            // the ollama answer model may still be pulling on first boot
            const modelErr = await app.answerModelReady;
            if (modelErr) {
                throw new ServiceUnavailableError(
                    `AI answer model unavailable: ${modelErr.message}`,
                );
            }

            res.setHeader("Content-Type", "text/event-stream");
            res.setHeader("Cache-Control", "no-cache");
            res.setHeader("Connection", "keep-alive");
            // disable proxy buffering (nginx & friends) — without it the whole
            // SSE stream is held and flushed at once, so the tool trail and the
            // typed answer only appear after the request finishes
            res.setHeader("X-Accel-Buffering", "no");
            res.flushHeaders();

            const send = (ev: object) =>
                res.write(`data: ${JSON.stringify(ev)}\n\n`);
            const abort = new AbortController();
            // NB: req 'close' fires as soon as the body is consumed (Node 18+);
            // the response 'close' is the actual client-disconnect signal
            res.on("close", () => {
                if (!res.writableEnded) abort.abort();
            });
            try {
                await Services.ask.stream(
                    config.ai,
                    config.search.type === "hybrid",
                    data.q,
                    send,
                    abort.signal,
                    data.history,
                );
            } catch (err) {
                if (!abort.signal.aborted) {
                    send({ type: "error", message: (err as Error).message });
                }
            }
            res.end();
        },
    ),
};
