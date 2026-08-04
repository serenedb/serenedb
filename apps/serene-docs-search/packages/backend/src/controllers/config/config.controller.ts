import { NextFunction, Request, Response } from "express";
import { plainToInstance } from "class-transformer";
import { validate } from "class-validator";
import type { SereneSearchConfig } from "@serenedb/docs-search-core";
import { asyncHandler } from "@middlewares/asyncHandler.middleware";
import CustomValidationError from "@utils/errors/validationError";
import { app } from "../../app";
import { UpdateConfigDto } from "./dto/updateConfig.dto";

export const ConfigController = {
    get: asyncHandler(async (_req: Request, res: Response) => {
        const config = app.config;
        if (!config) return res.json({ configured: false });
        const redacted: SereneSearchConfig = JSON.parse(JSON.stringify(config));
        if (redacted.ai?.answers?.apiKey) redacted.ai.answers.apiKey = "••••";
        if (redacted.ai?.embeddings?.apiKey) redacted.ai.embeddings.apiKey = "••••";
        res.json({ configured: true, config: redacted });
    }),

    put: asyncHandler(async (req: Request, res: Response, next: NextFunction) => {
        const data = plainToInstance(UpdateConfigDto, req.body ?? {});
        const errors = await validate(data);
        if (errors.length > 0) {
            return next(new CustomValidationError(errors));
        }
        await app.applyConfig(req.body as SereneSearchConfig);
        res.json({ saved: true });
    }),
};
