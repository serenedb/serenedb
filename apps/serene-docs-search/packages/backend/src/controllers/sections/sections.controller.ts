import { Request, Response } from "express";
import { asyncHandler } from "@middlewares/asyncHandler.middleware";
import { SectionsRepository } from "@repositories/sections";
import { DocsTools } from "@services/tools";
import BadRequestError from "@utils/errors/badRequestError";
import NotConfiguredError from "@utils/errors/notConfiguredError";
import { app } from "../../app";

export const SectionsController = {
    /** Full text of one indexed section by its exact url — powers MCP read_section. */
    get: asyncHandler(async (req: Request, res: Response) => {
        const url = String(req.query.url ?? "").trim();
        if (!url) throw new BadRequestError("Missing url query parameter");
        if (!app.config) throw new NotConfiguredError();

        const section = await SectionsRepository.sectionByUrl(url);
        if (!section) {
            return res.status(404).json({ error: "section not found" });
        }
        const content = (await DocsTools.read({ id: section.id })) ?? "";
        res.json({
            id: section.id,
            url: section.url,
            title: section.title,
            crumb: section.crumb,
            content,
        });
    }),
};
