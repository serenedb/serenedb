import WorkerPool from "./worker-pool";
import { getWorkerPath } from "./worker-path";

interface QueryWorkerData {
    jobId: number;
    DBClientPath: string;
    user?: string;
    password?: string;
    host?: string;
    port?: number;
    socket?: string;
    database?: string;
    limit?: number;
}

class QueryWorkerPool {
    private static instance: WorkerPool<QueryWorkerData> | null = null;

    static getInstance(): WorkerPool<QueryWorkerData> {
        if (!QueryWorkerPool.instance) {
            QueryWorkerPool.instance = new WorkerPool<QueryWorkerData>({
                workerPath: getWorkerPath(),
                minPoolSize: 3,
                maxPoolSize: 15,
                idleTimeout: 60000,
                minIdleWorkers: 3,
            });
        }

        return QueryWorkerPool.instance;
    }

    static async terminate() {
        if (QueryWorkerPool.instance) {
            await QueryWorkerPool.instance.terminate();
            QueryWorkerPool.instance = null;
        }
    }

    static getStats() {
        if (!QueryWorkerPool.instance) {
            return null;
        }
        return QueryWorkerPool.instance.getPoolStats();
    }
}

export default QueryWorkerPool;
