import { useMutation } from "@tanstack/react-query";
import { useCallback, useMemo } from "react";
import { orpc, apiClient } from "../../../shared/api/orpc";
import {
    BindVarSchema,
    ExecuteQueryInput,
    QueryExecutionJobSchema,
    SubscribeQueryExecutionOutput,
} from "@serene-ui/shared-core";
import { prepareQuery } from "../model";

export const useExecuteQuery = <T>() => {
    return useMutation({
        mutationFn: async (
            queryData: Omit<ExecuteQueryInput, "bind_vars"> & {
                bind_vars?: BindVarSchema[];
            },
        ) => {
            const { query: preparedQuery, values } = prepareQuery(
                queryData.query,
                queryData.bind_vars,
            );

            const response = await orpc.queryExecution.execute.call({
                ...queryData,
                query: preparedQuery,
                bind_vars: values,
                limit: queryData.limit ?? 1000,
            } as ExecuteQueryInput);

            const typedResonse = {
                ...response,
                result: response.result as T,
            };

            return typedResonse;
        },
    });
};

export const useSubscribeToQueryResult = () => {
    const subscribe = useCallback(
        async (
            jobId: number,
            onData: (data: SubscribeQueryExecutionOutput) => void,
            signal?: AbortSignal,
        ) => {
            try {
                const iterator = await apiClient.queryExecution.subscribe(
                    { jobId },
                    { signal },
                );
                for await (const result of iterator) {
                    onData(result);
                    if (
                        result.status === "success" ||
                        result.status === "failed"
                    ) {
                        break;
                    }
                }
            } catch (error: unknown) {
                if (signal?.aborted) return;
                throw error;
            }
        },
        [],
    );

    return useMemo(() => ({ subscribe }), [subscribe]);
};
