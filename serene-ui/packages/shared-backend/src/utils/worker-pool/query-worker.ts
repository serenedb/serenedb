import Database from "better-sqlite3";
import { parentPort } from "worker_threads";
import Cursor from "pg-cursor";
import { parse } from "libpg-query";
import {
    getMultiPlatformHost,
    PoolManagerInstance,
} from "@serene-ui/shared-backend";
import {
    QueryExecutionJobSchema,
    QueryExecutionResultSchema,
} from "@serene-ui/shared-core";

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
    data?: {
        result?: string;
        error?: string;
    },
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

        const bindVars = JSON.parse(job.bind_vars?.toString() || "[]");
        const parsedStatements = await parseQueryStatements(job.query);
        const primaryStatement = parsedStatements[0];
        const command = primaryStatement?.command;
        const hasMultipleStatements = parsedStatements.length > 1;

        if (!hasMultipleStatements && shouldUseCursor(command)) {
            const cursor = client.query(new Cursor(job.query, bindVars));

            let rows: any[];
            try {
                rows = await cursor.read(
                    limit === -1 ? undefined : limit || 1000,
                );
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

            const actionType = primaryStatement?.actionType || "OTHER";

            const message = buildSuccessMessage({
                actionType,
                command,
                rowCount: rows.length,
                query: job.query,
            });
            const results: QueryExecutionResultSchema[] = [
                {
                    rows,
                    action_type: actionType,
                    message,
                },
            ];

            updateJobStatus(db, jobId, "success", {
                result: JSON.stringify(results),
            });
        } else {
            const queryResult = await client.query(job.query, bindVars);
            const results = normalizeDriverResults(
                queryResult,
                job.query,
                parsedStatements,
                limit,
            );

            updateJobStatus(db, jobId, "success", {
                result: JSON.stringify(results),
            });
        }

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
            code: typeof errorObj.code === "string" ? errorObj.code : undefined,
            detail:
                typeof errorObj.detail === "string"
                    ? errorObj.detail
                    : undefined,
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
            port: typeof errorObj.port === "number" ? errorObj.port : undefined,
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

type QueryActionType = "SELECT" | "INSERT" | "UPDATE" | "DELETE" | "OTHER";

type QueryDriverResult = {
    rows?: unknown[];
    command?: string;
    rowCount?: number | null;
};

type ParsedStatementInfo = {
    command?: string;
    actionType: QueryActionType;
};

const mapCommandToActionType = (command?: string): QueryActionType => {
    switch ((command || "").toUpperCase()) {
        case "SELECT":
            return "SELECT";
        case "INSERT":
            return "INSERT";
        case "UPDATE":
            return "UPDATE";
        case "DELETE":
            return "DELETE";
        default:
            return "OTHER";
    }
};

const shouldUseCursor = (command?: string): boolean => {
    if (!command) {
        return true;
    }

    return [
        "SELECT",
        "WITH",
        "VALUES",
        "TABLE",
        "SHOW",
        "EXPLAIN",
        "DESCRIBE",
        "DESC",
    ].includes(command);
};

const parseQueryStatements = async (
    query: string,
): Promise<ParsedStatementInfo[]> => {
    try {
        const parsed = (await parse(query)) as {
            stmts?: Array<{ stmt?: Record<string, unknown> }>;
        };

        return (parsed.stmts || []).map((statement) =>
            getStatementInfo(statement?.stmt),
        );
    } catch {
        return [];
    }
};

const extractLeadingCommand = (query: string): string | undefined => {
    const withoutComments = stripLeadingSqlComments(query);
    const match = withoutComments.match(/^([a-zA-Z]+)/);
    return match?.[1]?.toUpperCase();
};

const stripLeadingSqlComments = (query: string): string => {
    let remaining = query.trimStart();

    while (remaining.startsWith("--") || remaining.startsWith("/*")) {
        if (remaining.startsWith("--")) {
            const nextLine = remaining.indexOf("\n");
            if (nextLine === -1) {
                return "";
            }
            remaining = remaining.slice(nextLine + 1).trimStart();
            continue;
        }

        const endComment = remaining.indexOf("*/");
        if (endComment === -1) {
            return "";
        }

        remaining = remaining.slice(endComment + 2).trimStart();
    }

    return remaining;
};

const buildSuccessMessage = ({
    actionType,
    command,
    rowCount,
    query,
}: {
    actionType: QueryActionType;
    command?: string;
    rowCount?: number;
    query: string;
}): string => {
    if (command === "DROP") {
        const droppedName = extractDroppedEntityName(query);
        if (droppedName) {
            return `Successfully dropped ${droppedName}.`;
        }
        return "Successfully executed DROP statement.";
    }

    if (
        (actionType === "INSERT" ||
            actionType === "UPDATE" ||
            actionType === "DELETE") &&
        typeof rowCount === "number"
    ) {
        const noun = rowCount === 1 ? "row" : "rows";
        return `${actionType} completed successfully (${rowCount} ${noun} affected).`;
    }

    if (command && command !== "SELECT") {
        return `Successfully executed ${command} statement.`;
    }

    return "Query successfully executed.";
};

const normalizeDriverResults = (
    queryResult: QueryDriverResult | QueryDriverResult[],
    query: string,
    parsedStatements: ParsedStatementInfo[],
    limit?: number,
): QueryExecutionResultSchema[] => {
    const normalizedResults = Array.isArray(queryResult)
        ? queryResult
        : [queryResult];

    return normalizedResults.map((result, index) => {
        const statementInfo = parsedStatements[index];
        const command =
            statementInfo?.command ||
            (typeof result.command === "string"
                ? result.command.toUpperCase()
                : undefined);
        const actionType =
            statementInfo?.actionType || mapCommandToActionType(command);

        return {
            rows: normalizeRows(result.rows, limit),
            action_type: actionType,
            message: buildSuccessMessage({
                actionType,
                command,
                rowCount:
                    typeof result.rowCount === "number"
                        ? result.rowCount
                        : undefined,
                query,
            }),
        };
    });
};

const getStatementInfo = (
    stmt?: Record<string, unknown>,
): ParsedStatementInfo => {
    const statementType = stmt ? Object.keys(stmt)[0] : undefined;

    switch (statementType) {
        case "SelectStmt":
            return { command: "SELECT", actionType: "SELECT" };
        case "InsertStmt":
            return { command: "INSERT", actionType: "INSERT" };
        case "UpdateStmt":
            return { command: "UPDATE", actionType: "UPDATE" };
        case "DeleteStmt":
            return { command: "DELETE", actionType: "DELETE" };
        case "DropStmt":
            return { command: "DROP", actionType: "OTHER" };
        case "CreateStmt":
            return { command: "CREATE", actionType: "OTHER" };
        case "AlterTableStmt":
            return { command: "ALTER", actionType: "OTHER" };
        default:
            return {
                command: statementType
                    ? statementType.replace(/Stmt$/, "").toUpperCase()
                    : undefined,
                actionType: "OTHER",
            };
    }
};

const normalizeRows = (rows: unknown[] | undefined, limit?: number) => {
    const safeRows = Array.isArray(rows) ? rows : [];

    if (limit === -1) {
        return safeRows as Record<string, unknown>[];
    }

    return safeRows.slice(0, limit || 1000) as Record<string, unknown>[];
};

const extractDroppedEntityName = (query: string): string | undefined => {
    const normalized = stripLeadingSqlComments(query);
    const match = normalized.match(
        /^drop\s+(table|view|index|schema|database)\s+(if\s+exists\s+)?("[^"]+"|[a-zA-Z0-9_."]+)/i,
    );

    if (!match) {
        return undefined;
    }

    const entityType = match[1].toLowerCase();
    const entityName = match[3];

    return `${entityType} ${entityName}`;
};
