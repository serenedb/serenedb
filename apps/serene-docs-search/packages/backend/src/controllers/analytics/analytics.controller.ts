import { NextFunction, Request, Response } from "express";
import { plainToInstance } from "class-transformer";
import { validate } from "class-validator";
import { asyncHandler } from "@middlewares/asyncHandler.middleware";
import { AnalyticsRepository } from "@repositories/analytics";
import CustomValidationError from "@utils/errors/validationError";
import { app } from "../../app";
import { ReportClickDto } from "./dto/reportClick.dto";
import { ReportQueryDto } from "./dto/reportQuery.dto";

// recordQuery / recordClick are fire-and-forget feedback beacons from the
// widget (no auth: they power the owner's analytics, carry no secrets and
// only increment counters) — recording failures never fail the request
export const AnalyticsController = {
    recordQuery: asyncHandler(async (req: Request, res: Response, next: NextFunction) => {
        const data = plainToInstance(ReportQueryDto, req.body ?? {});
        const errors = await validate(data);
        if (errors.length > 0) {
            return next(new CustomValidationError(errors));
        }
        if (!app.config) return res.json({ ok: false });
        await AnalyticsRepository
            .recordQuery(data.q.trim().toLowerCase(), Math.trunc(data.hits))
            .catch(() => {});
        res.json({ ok: true });
    }),

    recordClick: asyncHandler(async (req: Request, res: Response, next: NextFunction) => {
        const data = plainToInstance(ReportClickDto, req.body ?? {});
        const errors = await validate(data);
        if (errors.length > 0) {
            return next(new CustomValidationError(errors));
        }
        if (!app.config) return res.json({ ok: false });
        await AnalyticsRepository.recordClick(data.id, data.url, data.title ?? "").catch(() => {});
        res.json({ ok: true });
    }),

    report: asyncHandler(async (_req: Request, res: Response) => {
        res.json(await AnalyticsRepository.report());
    }),
};
