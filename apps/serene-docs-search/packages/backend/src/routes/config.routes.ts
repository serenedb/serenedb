import { Router } from "express";
import { Controllers } from "@controllers";
import { requireToken } from "@middlewares/auth.middleware";

export const configRouter = Router();
const controller = Controllers.config;

configRouter.get("/config", requireToken, controller.get);
configRouter.put("/config", requireToken, controller.put);
