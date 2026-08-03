import { Router } from "express";
import { Controllers } from "@controllers";
import { requireToken } from "@middlewares/auth.middleware";

export const analyticsRouter = Router();
const controller = Controllers.analytics;

analyticsRouter.post("/analytics/query", controller.recordQuery);
analyticsRouter.post("/analytics/click", controller.recordClick);
analyticsRouter.get("/analytics", requireToken, controller.report);
