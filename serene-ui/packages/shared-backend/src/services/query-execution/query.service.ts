import {
    ExecuteQueryInput,
    ExecuteQueryOutput,
    QueryExecutionJobSchema,
    SubscribeQueryExecutionInput,
} from "@serene-ui/shared-core";
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
                        password: input.password || connection.password || "",
                        socket: connection.socket,
                        database: input.database || connection.database || "",
                    };
                } else {
                    connectionData = {
                        mode: "host",
                        user: input.user || connection.user || "postgres",
                        password: input.password || connection.password || "",
                        host: input.host || connection.host,
                        port: input.port || connection.port,
                        database: input.database || connection.database || "",
                    };
                }
            } else {
                connectionData = {
                    mode: input.socket ? "socket" : "host",
                    user: input.user || "postgres",
                    password: input.password || "",
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

            return {
                result: result.rows,
            };
        } catch (error: any) {
            throw new ORPCError("INTERNAL_SERVER_ERROR", {
                message:
                    error.message || String(error) || "Query execution failed",
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
