import { NextFunction, Request, Response } from "express";

export const errorResponseHandler = (
    error: Error & { statusCode?: number },
    _req: Request,
    res: Response,
    _next: NextFunction,
): void => {
    // an SSE endpoint that already flushed cannot take a JSON error response
    if (res.headersSent) {
        res.end();
        return;
    }
    const statusCode = error.statusCode || 500;
    if (statusCode >= 500) console.error("unhandled error:", error);
    res.status(statusCode).json({
        success: false,
        message: error.message,
        type: error.statusCode ? error.constructor.name : "UnhandledError",
    });
};
