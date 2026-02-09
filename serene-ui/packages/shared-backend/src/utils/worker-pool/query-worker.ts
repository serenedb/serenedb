import Database from "better-sqlite3";
import { parentPort } from "worker_threads";
import Cursor from "pg-cursor";
import {
    getMultiPlatformHost,
    PoolManagerInstance,
} from "@serene-ui/shared-backend";
import { QueryExecutionJobSchema } from "@serene-ui/shared-core";

if (!parentPort) {
    console.error("Worker must be run as a worker thread");
    process.exit(1);
}

const log = (message: string) => {
    parentPort?.postMessage({ type: "log", message });
};

const sendComplete = (success: boolean, error?: any) => {
    parentPort?.postMessage({
        type: "complete",
        success,
        ...(error && { error }),
    });
};

const updateJobStatus = (
    db: Database.Database,
    jobId: number,
    status: "running" | "success" | "failed",
    data?: { result?: string; error?: string },
) => {
    const updates: string[] = [`status='${status}'`];
    const params: any[] = [];

    if (status === "running") {
        updates.push(`execution_started_at='${new Date().toISOString()}'`);
    } else {
        updates.push(`execution_finished_at='${new Date().toISOString()}'`);
        if (data?.result) {
            updates.push("result=?");
            params.push(data.result);
        }
        if (data?.error) {
            updates.push("error=?");
            params.push(data.error);
        }
    }

    params.push(jobId);
    db.prepare(`UPDATE jobs SET ${updates.join(", ")} WHERE id=?`).run(
        ...params,
    );

    parentPort?.postMessage({
        type: "status-update",
        jobId,
        status,
    });
};

const executeJob = async (taskData: any) => {
    const {
        jobId,
        user,
        password,
        host,
        port,
        socket,
        database,
        DBClientPath,
        limit,
    } = taskData;

    let db: Database.Database | null = null;
    let client: any = null;

    try {
        const syncResolvedHost = await getMultiPlatformHost(
            host || "localhost",
        );
        const pool = PoolManagerInstance.getPool(
            socket
                ? {
                      mode: "socket",
                      user,
                      password,
                      database,
                      socket,
                  }
                : {
                      mode: "host",
                      user,
                      password,
                      database,
                      host: syncResolvedHost,
                      port,
                  },
        );

        db = new Database(DBClientPath);

        client = await pool.connect();

        const job = db.prepare(`SELECT * FROM jobs WHERE id = ?`).get(jobId) as
            | QueryExecutionJobSchema
            | undefined;

        if (!job) {
            sendComplete(false);
            return;
        }

        updateJobStatus(db, jobId, "running");

        const cursor = client.query(
            new Cursor(
                job.query,
                JSON.parse(job.bind_vars?.toString() || "[]"),
            ),
        );

        let rows: any[];
        try {
            rows = await cursor.read(limit === -1 ? undefined : limit || 1000);
        } finally {
            try {
                await cursor.close();
            } catch (err) {
                console.error(
                    `[Worker ${jobId}] Error closing cursor for job ${jobId}:`,
                    err,
                );
            }
        }

        updateJobStatus(db, jobId, "success", {
            result: JSON.stringify(rows),
        });

        sendComplete(true);
    } catch (err: any) {
        console.error(`[Worker ${jobId}] Error in job ${jobId}:`, err);
        const errorPayload = serializeDriverError(err);
        const errorStr = JSON.stringify(errorPayload);
        if (db) {
            updateJobStatus(db, jobId, "failed", { error: errorStr });
        }
        log(errorStr);
        sendComplete(false, errorPayload);
    } finally {
        if (client) {
            client.release();
        }
        if (db) {
            db.close();
        }
    }
};

const serializeDriverError = (error: unknown) => {
    const flattenedError = unwrapAggregateError(error);

    if (flattenedError instanceof Error) {
        const errorObj = flattenedError as Error & Record<string, unknown>;

        return {
            message: errorObj.message,
            code:
                typeof errorObj.code === "string" ? errorObj.code : undefined,
            detail:
                typeof errorObj.detail === "string" ? errorObj.detail : undefined,
            hint: typeof errorObj.hint === "string" ? errorObj.hint : undefined,
            where:
                typeof errorObj.where === "string" ? errorObj.where : undefined,
            schema:
                typeof errorObj.schema === "string"
                    ? errorObj.schema
                    : undefined,
            table:
                typeof errorObj.table === "string" ? errorObj.table : undefined,
            column:
                typeof errorObj.column === "string"
                    ? errorObj.column
                    : undefined,
            constraint:
                typeof errorObj.constraint === "string"
                    ? errorObj.constraint
                    : undefined,
            severity:
                typeof errorObj.severity === "string"
                    ? errorObj.severity
                    : undefined,
            errno:
                typeof errorObj.errno === "number" ? errorObj.errno : undefined,
            syscall:
                typeof errorObj.syscall === "string"
                    ? errorObj.syscall
                    : undefined,
            address:
                typeof errorObj.address === "string"
                    ? errorObj.address
                    : undefined,
            port:
                typeof errorObj.port === "number" ? errorObj.port : undefined,
        };
    }

    if (typeof error === "string") {
        return { message: error };
    }

    return {
        message: "Unknown error",
    };
};

const unwrapAggregateError = (error: unknown) => {
    if (error instanceof AggregateError && error.errors?.length) {
        const first = error.errors.find((entry) => entry) ?? error.errors[0];
        return first ?? error;
    }

    if (error && typeof error === "object" && "errors" in error) {
        const errors = (error as { errors?: unknown }).errors;
        if (Array.isArray(errors) && errors.length > 0) {
            const first = errors.find((entry) => entry) ?? errors[0];
            return first ?? error;
        }
    }

    return error;
};

parentPort.on("message", (taskData) => {
    executeJob(taskData).catch((err) => {
        console.error("Unhandled error in worker:", err);
        sendComplete(false, err);
    });
});

parentPort.postMessage({ type: "ready" });
