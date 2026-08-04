import express from "express";
import { analyticsRouter } from "./analytics.routes";
import { askRouter } from "./ask.routes";
import { configRouter } from "./config.routes";
import { healthRouter } from "./health.routes";
import { searchRouter } from "./search.routes";
import { sectionsRouter } from "./sections.routes";
import { syncRouter } from "./sync.routes";

export const mainRouter = express.Router();

mainRouter.use(healthRouter);
mainRouter.use(searchRouter);
mainRouter.use(sectionsRouter);
mainRouter.use(askRouter);
mainRouter.use(syncRouter);
mainRouter.use(analyticsRouter);
mainRouter.use(configRouter);
