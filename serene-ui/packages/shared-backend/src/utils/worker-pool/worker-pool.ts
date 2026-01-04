import { Worker } from "worker_threads";
import EventEmitter from "events";

interface WorkerPoolTask<T> {
    data: T;
    resolve: (value: any) => void;
    reject: (error: any) => void;
}

interface PoolWorker {
    worker: Worker;
    isIdle: boolean;
    lastUsed: number;
}

interface WorkerPoolOptions {
    workerPath: string;
    minPoolSize?: number;
    maxPoolSize?: number;
    idleTimeout?: number;
    minIdleWorkers?: number;
}

class WorkerPool<T> extends EventEmitter {
    private pool: PoolWorker[] = [];
    private readonly tasksQueue: WorkerPoolTask<T>[] = [];

    private readonly minPoolSize: number;
    private readonly maxPoolSize: number;
    private readonly workerPath: string;
    private readonly idleTimeout: number;
    private readonly minIdleWorkers: number;

    private shrinkTimer: NodeJS.Timeout | null = null;

    constructor({
        workerPath,
        minPoolSize = 3,
        maxPoolSize = 10,
        idleTimeout = 60000,
        minIdleWorkers = 3,
    }: WorkerPoolOptions) {
        super();
        this.minPoolSize = minPoolSize;
        this.maxPoolSize = maxPoolSize;
        this.workerPath = workerPath;
        this.idleTimeout = idleTimeout;
        this.minIdleWorkers = minIdleWorkers;
        this.pool = [];
        this.initPool();
        this.startShrinkTimer();
    }

    private initPool() {
        for (let i = 0; i < this.minPoolSize; i++) {
            this.createWorker();
        }
    }

    private createWorker(): PoolWorker {
        const worker = new Worker(this.workerPath);
        const poolWorker: PoolWorker = {
            worker,
            isIdle: true,
            lastUsed: Date.now(),
        };

        worker.on("message", (message) => {
            if (message.type === "log") {
                this.emit("log", message.message);
            } else if (message.type === "ready") {
                poolWorker.isIdle = true;
                poolWorker.lastUsed = Date.now();
            } else if (message.type === "status-update") {
                this.emit("status-update", {
                    jobId: message.jobId,
                    status: message.status,
                });
            }
        });

        worker.on("error", (error) => {
            this.emit("error", error);
            poolWorker.isIdle = true;
        });

        worker.on("exit", (code) => {
            const index = this.pool.indexOf(poolWorker);
            if (index > -1) {
                this.pool.splice(index, 1);
            }

            if (code !== 0) {
                this.emit("workerExit", { code, worker: poolWorker });
            }

            this.processQueue();
        });

        this.pool.push(poolWorker);
        return poolWorker;
    }

    private getIdleWorker(): PoolWorker | null {
        return this.pool.find((w) => w.isIdle) || null;
    }

    private getIdleWorkersCount(): number {
        return this.pool.filter((w) => w.isIdle).length;
    }

    private ensureMinIdleWorkers() {
        const idleCount = this.getIdleWorkersCount();
        const workersToCreate = this.minIdleWorkers - idleCount;

        for (let i = 0; i < workersToCreate; i++) {
            if (this.pool.length < this.maxPoolSize) {
                this.createWorker();
            } else {
                break;
            }
        }
    }

    private processQueue() {
        if (this.tasksQueue.length === 0) return;

        const idleWorker = this.getIdleWorker();
        if (!idleWorker) {
            return;
        }

        const task = this.tasksQueue.shift();
        if (!task) return;

        this.executeTaskOnWorker(idleWorker, task);
    }

    private executeTaskOnWorker(
        poolWorker: PoolWorker,
        task: WorkerPoolTask<T>,
    ) {
        poolWorker.isIdle = false;
        poolWorker.lastUsed = Date.now();

        const worker = poolWorker.worker;
        let isCompleted = false;

        const timeout = setTimeout(() => {
            if (!isCompleted) {
                isCompleted = true;
                worker.off("message", completeHandler);

                poolWorker.isIdle = true;
                poolWorker.lastUsed = Date.now();

                task.reject(new Error("Task execution timeout (30s)"));

                this.ensureMinIdleWorkers();
                this.processQueue();
            }
        }, 30000);

        const completeHandler = (message: any) => {
            if (message.type === "complete" && !isCompleted) {
                isCompleted = true;
                clearTimeout(timeout);
                worker.off("message", completeHandler);

                if (message.success) {
                    task.resolve({ success: true });
                } else {
                    task.reject(message.error || new Error("Task failed"));
                }

                poolWorker.isIdle = true;
                poolWorker.lastUsed = Date.now();

                this.ensureMinIdleWorkers();
                this.processQueue();
            }
        };

        worker.on("message", completeHandler);

        try {
            worker.postMessage(task.data);
        } catch (err) {
            isCompleted = true;
            clearTimeout(timeout);
            worker.off("message", completeHandler);

            poolWorker.isIdle = true;
            poolWorker.lastUsed = Date.now();

            task.reject(err);

            this.ensureMinIdleWorkers();
            this.processQueue();
        }
    }

    public executeTask(taskData: T): Promise<any> {
        return new Promise((resolve, reject) => {
            const task: WorkerPoolTask<T> = {
                data: taskData,
                resolve,
                reject,
            };

            const idleWorker = this.getIdleWorker();

            if (idleWorker) {
                this.executeTaskOnWorker(idleWorker, task);

                this.ensureMinIdleWorkers();
            } else {
                this.tasksQueue.push(task);
            }
        });
    }

    private startShrinkTimer() {
        if (this.shrinkTimer) {
            clearInterval(this.shrinkTimer);
        }

        this.shrinkTimer = setInterval(() => {
            this.shrinkPool();
        }, this.idleTimeout);
    }

    private shrinkPool() {
        const now = Date.now();
        const workersToRemove: PoolWorker[] = [];

        for (const poolWorker of this.pool) {
            if (
                poolWorker.isIdle &&
                now - poolWorker.lastUsed > this.idleTimeout &&
                this.pool.length > this.minPoolSize
            ) {
                workersToRemove.push(poolWorker);
            }
        }

        for (const poolWorker of workersToRemove) {
            if (this.pool.length <= this.minPoolSize) break;

            const index = this.pool.indexOf(poolWorker);
            if (index > -1) {
                this.pool.splice(index, 1);
                poolWorker.worker.terminate();
            }
        }
    }

    public getPoolStats() {
        return {
            totalWorkers: this.pool.length,
            idleWorkers: this.getIdleWorkersCount(),
            busyWorkers: this.pool.length - this.getIdleWorkersCount(),
            queuedTasks: this.tasksQueue.length,
            minPoolSize: this.minPoolSize,
            maxPoolSize: this.maxPoolSize,
        };
    }

    public async terminate() {
        if (this.shrinkTimer) {
            clearInterval(this.shrinkTimer);
            this.shrinkTimer = null;
        }

        for (const task of this.tasksQueue) {
            task.reject(new Error("Worker pool terminated"));
        }
        this.tasksQueue.length = 0;

        const terminationPromises = this.pool.map((poolWorker) =>
            poolWorker.worker.terminate(),
        );

        await Promise.all(terminationPromises);
        this.pool = [];
    }
}

export default WorkerPool;
