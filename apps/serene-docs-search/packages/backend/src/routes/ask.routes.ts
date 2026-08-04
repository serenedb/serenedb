import { Router } from "express";
import { Controllers } from "@controllers";

export const askRouter = Router();
const controller = Controllers.ask;

askRouter.post("/ask", controller.ask);
