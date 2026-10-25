import { os } from "@orpc/server";
import { ConnectionRouter } from "./connection.router";
import { DashboardRouter } from "./dashboard.router";
import { GithubRouter } from "./github.router";
import { QueryExecutionRouter } from "./query-execution.router";
import { SavedQueryRouter } from "./saved-query.router";

export const apiRouter = os.router({
    connection: ConnectionRouter,
    dashboard: DashboardRouter,
    github: GithubRouter,
    queryExecution: QueryExecutionRouter,
    savedQuery: SavedQueryRouter,
});
