import { Router } from "express";
import { Controllers } from "@controllers";

export const healthRouter = Router();
const controller = Controllers.health;

healthRouter.get("/health", controller.get);
