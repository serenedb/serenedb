import cors from "cors";
import express from "express";
import { errorResponseHandler } from "@middlewares/errorResponseHandler.middleware";
import { mainRouter } from "@routes";

export function createServer(): express.Express {
    const server = express();

    server.use(cors());
    server.use(express.json({ limit: "1mb" }));
    server.use("/v1", mainRouter, errorResponseHandler);

    return server;
}
