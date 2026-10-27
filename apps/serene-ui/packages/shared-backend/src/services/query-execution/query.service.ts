import {
    ExecuteQueryInput,
    ExecuteQueryOutput,
    QueryExecutionJobSchema,
    QueryExecutionResultSchema,
    SubscribeQueryExecutionInput,
} from "@serene-ui/shared-core";
import { parse } from "libpg-query";
import { QueryWorkerPool } from "../../utils/worker-pool/index.js";
import {
    ConnectionRepository,
    QueryExecutionJobRepository,
} from "../../repositories/index.js";
import { ORPCError } from "@orpc/server";
import { getMultiPlatformHost } from "../../utils/multiplatform-host.js";
import { DBClient } from "../../database/db-init.js";
import { PoolManagerInstance } from "../../database/pool-manager.js";

export const QueryExecutionService = {
    execute: async (input: ExecuteQueryInput): Promise<ExecuteQueryOutput> => {
        try {
            let connectionData: {
                mode?: "socket" | "host";
                user?: string;
                password?: string;
                host?: string;
                port?: number;
                socket?: string;
                database?: string;
            };
            if (input.connectionId) {
                const connection = ConnectionRepository.findOne({
                    id: input.connectionId,
                });

                if (!connection) {
                    throw new ORPCError("BAD_REQUEST", {
                        message: "No connection found",
                    });
                }

                if (connection.mode === "socket") {
                    connectionData = {
                        mode: "socket",
                        user: input.user || connection.user || "postgres",
                        password:
                            input.password || connection.password || "postgres",
                        socket: connection.socket,
                        database: input.database || connection.database || "",
                    };
                } else {
                    connectionData = {
                        mode: "host",
                        user: input.user || connection.user || "postgres",
                        password:
                            input.password || connection.password || "postgres",
                        host: input.host || connection.host,
                        port: input.port || connection.port,
                        database: input.database || connection.database || "",
                    };
                }
            } else {
                connectionData = {
                    mode: input.socket ? "socket" : "host",
                    user: input.user || "postgres",
                    password: input.password || "postgres",
                    host: input.host || undefined,
                    port: input.port || undefined,
                    socket: input.socket || undefined,
                    database: input.database || undefined,
                };
            }

            if (input.async) {
                const result = QueryExecutionJobRepository.create({
                    query: input.query,
                    bind_vars: input.bind_vars,
                    status: "pending",
                });

                const jobId = result?.id;
                if (!jobId) {
                    throw new ORPCError("INTERNAL_SERVER_ERROR", {
                        message: "No job created",
                    });
                }

                const pool = QueryWorkerPool.getInstance();

                const resolvedHost = await getMultiPlatformHost(
                    connectionData.host || "localhost",
                );

                pool.executeTask({
                    ...connectionData,
                    host: resolvedHost,
                    jobId,
                    DBClientPath: DBClient.name,
                    limit: input.limit || 1000,
                }).catch((err) => {
                    console.error(`Worker pool error for job ${jobId}:`, err);
                });

                return {
                    jobId,
                };
            }
            let result: any;

            const syncResolvedHost = await getMultiPlatformHost(
                connectionData.host || "localhost",
            );

            const pool = PoolManagerInstance.getPool(
                !connectionData.socket
                    ? {
                          mode: "host",
                          user: connectionData.user,
                          password: connectionData.password,
                          host: syncResolvedHost,
                          port: connectionData.port,
                          database: connectionData.database,
                      }
                    : {
                          mode: "socket",
                          user: connectionData.user,
                          password: connectionData.password,
                          socket: connectionData.socket,
                          database: connectionData.database,
                      },
            );
            const client = await pool.connect();
            result = await client.query(input.query, input.bind_vars || []);
            client.release();
            const parsedStatements = await parseQueryStatements(input.query);

            return {
                results: normalizeSyncResults(
                    result,
                    input.query,
                    parsedStatements,
                    input.limit,
                ),
            };
        } catch (error: any) {
            if (error instanceof ORPCError) {
                throw error;
            }

            const normalized = normalizeDriverError(error);

            throw new ORPCError("INTERNAL_SERVER_ERROR", {
                message: normalized.userMessage,
                data: normalized.data,
            });
        }
    },
    checkJob: async (
        input: SubscribeQueryExecutionInput,
    ): Promise<QueryExecutionJobSchema> => {
        const job = QueryExecutionJobRepository.findOne({
            id: input.jobId,
        });

        if (!job) {
            throw new ORPCError("BAD_REQUEST", {
                message: "Job not found",
            });
        }

        return job;
    },
    getPoolStats: () => {
        return QueryWorkerPool.getStats();
    },
};

type QueryDriverResult = {
    rows?: unknown[];
    command?: string;
    rowCount?: number | null;
};

const normalizeSyncResults = (
    result: QueryDriverResult | QueryDriverResult[],
    query: string,
    parsedStatements: ParsedStatementInfo[],
    limit?: number,
): QueryExecutionResultSchema[] => {
    const items = Array.isArray(result) ? result : [result];

    return items.map((item, index) => {
        const statementInfo = parsedStatements[index];
        const command =
            statementInfo?.command ||
            (typeof item.command === "string"
                ? item.command.toUpperCase()
                : undefined);
        const actionType =
            statementInfo?.actionType || mapCommandToActionType(command);

        return {
            rows: normalizeRows(item.rows, limit),
            action_type: actionType,
            message: buildSuccessMessage({
                actionType,
                command,
                rowCount:
                    typeof item.rowCount === "number"
                        ? item.rowCount
                        : undefined,
                query,
            }),
        };
    });
};

type ParsedStatementInfo = {
    command?: string;
    actionType: QueryActionType;
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

const normalizeRows = (rows: unknown[] | undefined, limit?: number) => {
    const normalizedRows = Array.isArray(rows) ? rows : [];

    if (limit === -1) {
        return normalizedRows as Record<string, unknown>[];
    }

    return normalizedRows.slice(0, limit || 1000) as Record<string, unknown>[];
};

const normalizeDriverError = (error: unknown) => {
    const flattenedError = unwrapAggregateError(error);
    const fallbackMessage = "Query execution failed";
    const rawMessage =
        flattenedError instanceof Error
            ? flattenedError.message
            : typeof error === "string"
              ? error
              : String(error);

    const errorObj = (flattenedError ?? {}) as Record<string, unknown>;
    const code =
        (typeof errorObj.code === "string" && errorObj.code) ||
        (typeof errorObj.sqlState === "string" && errorObj.sqlState) ||
        undefined;

    const detail =
        typeof errorObj.detail === "string" ? errorObj.detail : undefined;
    const hint = typeof errorObj.hint === "string" ? errorObj.hint : undefined;
    const where =
        typeof errorObj.where === "string" ? errorObj.where : undefined;
    const schema =
        typeof errorObj.schema === "string" ? errorObj.schema : undefined;
    const table =
        typeof errorObj.table === "string" ? errorObj.table : undefined;
    const column =
        typeof errorObj.column === "string" ? errorObj.column : undefined;
    const constraint =
        typeof errorObj.constraint === "string"
            ? errorObj.constraint
            : undefined;

    const userMessage =
        mapDriverErrorToUserMessage(code, rawMessage) || fallbackMessage;

    return {
        userMessage,
        data: {
            userMessage,
            message: rawMessage || fallbackMessage,
            code,
            detail,
            hint,
            where,
            schema,
            table,
            column,
            constraint,
        },
    };
};

const unwrapAggregateError = (error: unknown): unknown => {
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

const mapDriverErrorToUserMessage = (
    code?: string,
    message?: string,
): string | undefined => {
    if (code === "28P01") {
        return "Authentication failed: invalid username or password.";
    }
    if (code === "3D000") {
        return "Database does not exist.";
    }
    if (code === "28000") {
        return "Invalid authorization specification.";
    }
    if (code === "42501") {
        return "Insufficient privileges to access the database.";
    }
    if (code === "57P03") {
        return "Database is starting up. Try again in a moment.";
    }
    if (code === "53300") {
        return "Too many connections. Try again later.";
    }
    if (code === "ECONNREFUSED") {
        return "Connection refused. Check the host, port, and server status.";
    }
    if (code === "ENOTFOUND") {
        return "Host not found. Check the host name.";
    }
    if (code === "ETIMEDOUT") {
        return "Connection timed out. Check network connectivity.";
    }
    if (code === "ECONNRESET") {
        return "Connection was reset. Check server stability and network.";
    }

    if (message) {
        const lower = message.toLowerCase();
        if (lower.includes("password authentication failed")) {
            return "Authentication failed: invalid username or password.";
        }
        if (lower.includes("database") && lower.includes("does not exist")) {
            return "Database does not exist.";
        }
        if (lower.includes("role") && lower.includes("does not exist")) {
            return "User does not exist.";
        }
        if (lower.includes("connect") && lower.includes("econnrefused")) {
            return "Connection refused. Check the host, port, and server status.";
        }
        if (lower.includes("enotfound")) {
            return "Host not found. Check the host name.";
        }
        if (lower.includes("etimedout")) {
            return "Connection timed out. Check network connectivity.";
        }
    }

    return undefined;
};

type QueryActionType = "SELECT" | "INSERT" | "UPDATE" | "DELETE" | "OTHER";

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

const extractLeadingCommand = (query: string): string | undefined => {
    const withoutComments = stripLeadingSqlComments(query);
    const match = withoutComments.match(/^([a-zA-Z]+)/);
    return match?.[1]?.toUpperCase();
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
