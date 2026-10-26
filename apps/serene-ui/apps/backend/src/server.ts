import { createServer } from "node:http";
import { RPCHandler } from "@orpc/server/node";
import { CORSPlugin } from "@orpc/server/plugins";
import { apiRouter } from "./routers";

const handler = new RPCHandler(apiRouter, {
    plugins: [new CORSPlugin()],
});

const server = createServer(async (req, res) => {
    const result = await handler.handle(req, res, {
        context: { headers: req.headers },
    });

    if (!result.matched) {
        res.statusCode = 404;
        res.end("No procedure matched");
    }
});

export const initServer = (port: number, host: string) => {
    server.listen(port, host, () =>
        console.log(`Listening on ${host}:${port}`),
    );
};
