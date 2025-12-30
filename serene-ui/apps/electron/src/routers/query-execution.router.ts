import { implement } from "@orpc/server";
import { apiContracts } from "@serene-ui/shared-core";
import { QueryExecutionService } from "@serene-ui/shared-backend";
import { EventPublisher } from "@orpc/server";
import { QueryWorkerPool } from "@serene-ui/shared-backend";

const os = implement(apiContracts.queryExecution);

export const executeQuery = os.execute.handler(async ({ input }) => {
    initializeWorkerPool();
    return await QueryExecutionService.execute(input);
});

const queryResultPublisher = new EventPublisher<{
    "query-result": Awaited<ReturnType<typeof QueryExecutionService.checkJob>>;
}>();

let workerPoolInitialized = false;

const initializeWorkerPool = () => {
    if (!workerPoolInitialized) {
        const workerPool = QueryWorkerPool.getInstance();
        workerPool.on("status-update", async ({ jobId }: { jobId: number }) => {
            try {
                const result = await QueryExecutionService.checkJob({ jobId });
                queryResultPublisher.publish("query-result", result);
            } catch (error) {
                console.error(
                    `Error publishing status update for job ${jobId}:`,
                    error,
                );
            }
        });
        workerPoolInitialized = true;
    }
};

export const subscribeQueryExecutionResult = os.subscribe.handler(
    async function* ({ input, signal }) {
        initializeWorkerPool();
        try {
            const initialResult = await QueryExecutionService.checkJob({
                jobId: input.jobId,
            });
            yield initialResult;

            if (
                initialResult.status === "success" ||
                initialResult.status === "failed"
            ) {
                return;
            }
        } catch (error) {}

        for await (const result of queryResultPublisher.subscribe(
            "query-result",
            {
                signal,
            },
        )) {
            if (result.id === input.jobId) {
                yield result;
                if (result.status === "success" || result.status === "failed") {
                    return;
                }
            }
        }
    },
);

export const QueryExecutionRouter = os.router({
    execute: executeQuery,
    subscribe: subscribeQueryExecutionResult,
});
