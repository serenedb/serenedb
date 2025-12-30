import { PropsWithChildren, useEffect, useRef, useMemo } from "react";
import type { QueryResult } from "./types";
import {
    useConnection,
    useExecuteQuery,
    useQueryHistory,
    useSubscribeToQueryResult,
} from "@serene-ui/shared-frontend/entities";
import { toast } from "sonner";
import {
    QueryResultsContext,
    type ExecuteQueryResult,
    type ExecuteQueryError,
} from "./QueryResultsContext";
import { useQueryResultsState } from "./hooks/useQueryResultsState";
import {
    validateQuery,
    validateConnection,
    validateLimit,
    validateJobId,
} from "./utils/validation";
import { BindVarSchema, QueryExecutionJobSchema } from "@serene-ui/shared-core";

export const QueryResultsProvider = ({ children }: PropsWithChildren) => {
    const { currentConnection } = useConnection();
    const { addQueryHistory } = useQueryHistory();

    const {
        pendingJobs,
        setPendingJobs,
        setQueryResult,
        getQueryResult,
        scheduleCleanup,
        cancelCleanup,
        subscribe,
    } = useQueryResultsState();

    const { mutateAsync: executeQueryApi } = useExecuteQuery();
    const { subscribe: subscribeToQueryResult } = useSubscribeToQueryResult();
    const abortControllersRef = useRef<Map<number, AbortController>>(new Map());
    const lastProcessedJobsRef = useRef<Set<number>>(new Set());

    /**
     * Executes a query and returns the result or an error.
     * @param query The query to execute.
     * @param bind_vars Optional bind variables for the query.
     * @param saveToHistory Optional flag to save the query to history.
     * @param limit Optional limit for the query result.
     * @returns Promise that resolves to the query result or an error.
     */
    const executeQuery = async (
        query: string,
        bind_vars?: BindVarSchema[],
        saveToHistory = false,
        limit = 1000,
    ): Promise<ExecuteQueryResult | ExecuteQueryError> => {
        const queryError = validateQuery(query);
        if (queryError) {
            toast(queryError);
            return { success: false, error: queryError };
        }

        const connectionError = validateConnection(
            currentConnection.connectionId,
            currentConnection.database,
        );
        if (connectionError) {
            toast(connectionError);
            return { success: false, error: connectionError };
        }

        const limitError = validateLimit(limit);
        if (limitError) {
            toast(limitError);
            return { success: false, error: limitError };
        }

        try {
            const data = await executeQueryApi({
                async: true,
                query,
                connectionId: currentConnection.connectionId,
                database: currentConnection.database,
                bind_vars,
                limit: limit,
            });

            const jobIdError = validateJobId(data.jobId);
            if (jobIdError) {
                toast(jobIdError);
                return { success: false, error: jobIdError };
            }

            const jobId = Number(data.jobId);

            const initialResult: QueryResult = {
                jobId,
                status: "pending",
                query,
            };
            setQueryResult(jobId, initialResult);
            setPendingJobs((prev) => new Set([...prev, jobId]));

            if (saveToHistory) {
                try {
                    await addQueryHistory({
                        name: query,
                        query,
                        bind_vars: bind_vars || [],
                        executed_at: new Date().toISOString(),
                    });
                } catch (error) {
                    console.error("Failed to save query to history:", error);
                }
            }

            scheduleCleanup(jobId);

            return { success: true, jobId };
        } catch (error) {
            const errorMessage = "Failed to execute query";
            toast(errorMessage);
            return { success: false, error: errorMessage };
        }
    };

    useEffect(() => {
        const currentJobs = new Set(pendingJobs);
        const lastJobs = lastProcessedJobsRef.current;

        const hasChanges =
            currentJobs.size !== lastJobs.size ||
            Array.from(currentJobs).some((id) => !lastJobs.has(id)) ||
            Array.from(lastJobs).some((id) => !currentJobs.has(id));

        if (!hasChanges) {
            return;
        }

        lastProcessedJobsRef.current = new Set(currentJobs);
        const controllers = abortControllersRef.current;
        const pendingJobIdSet = new Set(pendingJobs);

        controllers.forEach((controller, jobId) => {
            if (!pendingJobIdSet.has(jobId)) {
                controller.abort();
                controllers.delete(jobId);
            }
        });

        pendingJobs.forEach((jobId) => {
            if (!controllers.has(jobId)) {
                const controller = new AbortController();
                controllers.set(jobId, controller);

                subscribeToQueryResult(
                    jobId,
                    (result: QueryExecutionJobSchema) => {
                        const currentQueryResult = getQueryResult(jobId);
                        let updatedResult: QueryResult;

                        if (result.status === "success") {
                            updatedResult = {
                                jobId,
                                status: result.status,
                                query: currentQueryResult?.query || "",
                                created_at: result?.created_at,
                                execution_started_at:
                                    result?.execution_started_at,
                                execution_finished_at:
                                    result?.execution_finished_at,
                                action_type: result.action_type,
                                bind_vars: currentQueryResult?.bind_vars || [],
                                result: result.result || [],
                            };
                        } else if (result.status === "failed") {
                            updatedResult = {
                                jobId,
                                status: result.status,
                                query: currentQueryResult?.query || "",
                                created_at: result?.created_at,
                                execution_started_at:
                                    result?.execution_started_at,
                                execution_finished_at:
                                    result?.execution_finished_at,
                                action_type: result.action_type,
                                bind_vars: currentQueryResult?.bind_vars || [],
                                error: result.error || "Unknown error",
                            };
                        } else {
                            updatedResult = {
                                jobId,
                                status: result.status,
                                query: currentQueryResult?.query || "",
                                created_at: result?.created_at,
                                execution_started_at:
                                    result?.execution_started_at,
                                execution_finished_at:
                                    result?.execution_finished_at,
                                bind_vars: currentQueryResult?.bind_vars || [],
                            };
                        }

                        setQueryResult(jobId, updatedResult);

                        if (
                            result.status === "success" ||
                            result.status === "failed"
                        ) {
                            setPendingJobs((prev) => {
                                const next = new Set(prev);
                                next.delete(jobId);
                                return next;
                            });
                            cancelCleanup(jobId);
                            controllers.delete(jobId);
                        }
                    },
                    controller.signal,
                ).catch((error: unknown) => {
                    if (!controller.signal.aborted) {
                        console.error("Subscription error:", error);
                    }
                });
            }
        });
        return () => {
            controllers.forEach((controller) => controller.abort());
            controllers.clear();
            lastProcessedJobsRef.current.clear();
        };
    }, [pendingJobs]);

    return (
        <QueryResultsContext.Provider value={{ executeQuery, subscribe }}>
            {children}
        </QueryResultsContext.Provider>
    );
};
