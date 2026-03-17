import { PropsWithChildren, useEffect, useRef } from "react";
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
    type ExecuteQueryBatchResult,
    type ExecuteQueryBatchJob,
    type ExecuteQueryError,
} from "./QueryResultsContext";
import { useQueryResultsState } from "./hooks/useQueryResultsState";
import {
    validateQuery,
    validateConnection,
    validateLimit,
    validateJobId,
} from "./utils/validation";
import { splitQueries, toStatementRange } from "./utils";
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

    const validateExecutionInput = (
        query: string,
        limit: number,
    ): ExecuteQueryError | null => {
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

        return null;
    };

    const saveQueryToHistory = async (
        query: string,
        bind_vars: BindVarSchema[] | undefined,
    ) => {
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
    };

    const normalizeBindVars = (bind_vars: BindVarSchema[] | undefined) =>
        bind_vars
            ?.map((bindVar) => bindVar.value)
            .filter((bindVar): bindVar is string => bindVar !== undefined);

    const startQueryJob = async (
        query: string,
        bind_vars: BindVarSchema[] | undefined,
        limit: number,
        metadata?: Partial<
            Pick<
                QueryResult,
                | "statementIndex"
                | "statementQuery"
                | "statementRange"
                | "sourceQuery"
            >
        >,
    ): Promise<ExecuteQueryResult | ExecuteQueryError> => {
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
                bind_vars: normalizeBindVars(bind_vars),
                statementIndex: metadata?.statementIndex,
                statementQuery: metadata?.statementQuery,
                statementRange: metadata?.statementRange,
                sourceQuery: metadata?.sourceQuery,
            };

            setQueryResult(jobId, initialResult);
            setPendingJobs((prev) => new Set([...prev, jobId]));
            scheduleCleanup(jobId);

            return { success: true, jobId };
        } catch (error) {
            const errorMessage = "Failed to execute query";
            toast(errorMessage);
            return { success: false, error: errorMessage };
        }
    };

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
        const validationError = validateExecutionInput(query, limit);
        if (validationError) {
            return validationError;
        }

        const result = await startQueryJob(query, bind_vars, limit);
        if (result.success && saveToHistory) {
            await saveQueryToHistory(query, bind_vars);
        }

        return result;
    };

    const executeQueryBatch = async (
        query: string,
        bind_vars?: BindVarSchema[],
        saveToHistory = false,
        limit = 1000,
        onJobStarted?: (job: ExecuteQueryBatchJob) => void,
    ): Promise<ExecuteQueryBatchResult | ExecuteQueryError> => {
        const validationError = validateExecutionInput(query, limit);
        if (validationError) {
            return validationError;
        }

        const statements = await splitQueries(query);
        if (statements.length === 0) {
            const emptyQueryError = validateQuery("");
            const errorMessage = emptyQueryError || "Query cannot be empty";
            toast(errorMessage);
            return { success: false, error: errorMessage };
        }

        const jobs: ExecuteQueryBatchJob[] = [];
        let stoppedOnError: ExecuteQueryBatchResult["stoppedOnError"];

        const waitForQueryCompletion = (jobId: number) =>
            new Promise<QueryResult>((resolve) => {
                let unsubscribe: () => void = () => {};
                unsubscribe = subscribe(jobId, (result) => {
                    if (
                        result.status !== "success" &&
                        result.status !== "failed"
                    ) {
                        return;
                    }

                    unsubscribe();
                    resolve(result);
                });
            });

        for (const statement of statements) {
            const result = await startQueryJob(
                statement.query,
                bind_vars,
                limit,
                {
                    statementIndex: statement.statementIndex,
                    statementQuery: statement.query,
                    statementRange: toStatementRange(statement),
                    sourceQuery: query,
                },
            );

            if (!result.success) {
                if (jobs.length === 0) {
                    return result;
                }

                stoppedOnError = {
                    statementIndex: statement.statementIndex,
                    error: result.error,
                };
                toast.error("Query execution error", {
                    description: `Stopped after statement ${statement.statementIndex + 1}: ${result.error}`,
                });
                break;
            }

            const startedJob = {
                jobId: result.jobId,
                statementIndex: statement.statementIndex,
                statementQuery: statement.query,
                sourceQuery: query,
                statementRange: toStatementRange(statement),
            };
            jobs.push(startedJob);
            onJobStarted?.(startedJob);

            const finalResult = await waitForQueryCompletion(result.jobId);
            if (finalResult.status === "failed") {
                stoppedOnError = {
                    statementIndex: statement.statementIndex,
                    error: finalResult.error,
                };
                toast.error("Query execution error", {
                    description: `Stopped after statement ${statement.statementIndex + 1}: ${finalResult.error}`,
                });
                break;
            }
        }

        if (saveToHistory && jobs.length > 0) {
            await saveQueryToHistory(query, bind_vars);
        }

        return {
            success: true,
            jobs,
            stoppedOnError,
        };
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
                                bind_vars: currentQueryResult?.bind_vars,
                                results: result.results || [],
                                statementIndex:
                                    currentQueryResult?.statementIndex,
                                statementRange:
                                    currentQueryResult?.statementRange,
                                statementQuery:
                                    currentQueryResult?.statementQuery,
                                sourceQuery: currentQueryResult?.sourceQuery,
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
                                bind_vars: currentQueryResult?.bind_vars,
                                error: result.error || "Unknown error",
                                statementIndex:
                                    currentQueryResult?.statementIndex,
                                statementRange:
                                    currentQueryResult?.statementRange,
                                statementQuery:
                                    currentQueryResult?.statementQuery,
                                sourceQuery: currentQueryResult?.sourceQuery,
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
                                bind_vars: currentQueryResult?.bind_vars,
                                statementIndex:
                                    currentQueryResult?.statementIndex,
                                statementRange:
                                    currentQueryResult?.statementRange,
                                statementQuery:
                                    currentQueryResult?.statementQuery,
                                sourceQuery: currentQueryResult?.sourceQuery,
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
        <QueryResultsContext.Provider
            value={{ executeQuery, executeQueryBatch, subscribe }}>
            {children}
        </QueryResultsContext.Provider>
    );
};
