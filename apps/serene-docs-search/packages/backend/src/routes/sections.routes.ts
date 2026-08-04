import { Router } from "express";
import { Controllers } from "@controllers";

export const sectionsRouter = Router();
const controller = Controllers.sections;

sectionsRouter.get("/section", controller.get);
