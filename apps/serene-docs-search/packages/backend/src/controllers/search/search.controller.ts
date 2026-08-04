import { NextFunction, Request, Response } from "express";
import { plainToInstance } from "class-transformer";
import { validate } from "class-validator";
import { asyncHandler } from "@middlewares/asyncHandler.middleware";
import { Services } from "@services";
import CustomValidationError from "@utils/errors/validationError";
import { SearchDto } from "./dto/search.dto";

export const SearchController = {
    search: asyncHandler(async (req: Request, res: Response, next: NextFunction) => {
        const data = plainToInstance(SearchDto, req.body ?? {});
        const errors = await validate(data);
        if (errors.length > 0) {
            return next(new CustomValidationError(errors));
        }
        if (!data.q || !data.q.trim()) {
            return res.json({ query: "", mode: "fulltext", results: [], total: 0, tookMs: 0 });
        }

        const started = Date.now();
        try {
            res.json(await Services.search.search(data.q, data.mode, data.limit));
        } catch (err) {
            // index not built yet (first sync still running or failed) — an
            // empty result set is friendlier than a 500 with a table error
            if ((err as { code?: string }).code === "42P01") {
                return res.json({
                    query: data.q,
                    mode: "fulltext",
                    results: [],
                    total: 0,
                    tookMs: Date.now() - started,
                });
            }
            throw err;
        }
    }),
};
