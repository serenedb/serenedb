import { Router } from "express";
import { Controllers } from "@controllers";

export const searchRouter = Router();
const controller = Controllers.search;

searchRouter.post("/search", controller.search);
