import { NextFunction, Request, Response } from "express";
import AuthorizationError from "@utils/errors/authorizationError";
import { app } from "../app";

/** Admin gate: only enforced when SERENE_SEARCH_TOKEN is set. */
export const requireToken = (req: Request, _res: Response, next: NextFunction): void => {
    const expected = app.env.token;
    if (!expected) return next();
    const got = /^Bearer\s+(.+)$/.exec(req.headers.authorization ?? "")?.[1];
    if (got === expected) return next();
    next(new AuthorizationError());
};
