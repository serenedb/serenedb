import { Router } from "express";
import { Controllers } from "@controllers";
import { requireToken } from "@middlewares/auth.middleware";

export const syncRouter = Router();
const controller = Controllers.sync;

syncRouter.post("/sync", requireToken, controller.trigger);
// CI-friendly alias per the wizard: POST /v1/reindex from a deploy hook
syncRouter.post("/reindex", requireToken, controller.trigger);
syncRouter.get("/sync/progress", controller.progress);
