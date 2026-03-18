import { createContext, useContext } from "react";
import type { QueryResult, StatementRange } from "./types";
import { BindVarSchema } from "@serene-ui/shared-core";

export interface ExecuteQueryResult {
    success: true;
    jobId: number;
}

export interface ExecuteQueryError {
    success: false;
    error: string;
}

export interface ExecuteQueryBatchJob {
    jobId: number;
    statementIndex: number;
    statementQuery: string;
    sourceQuery: string;
    statementRange: StatementRange;
}

export interface ExecuteQueryBatchResult {
    success: true;
    jobs: ExecuteQueryBatchJob[];
    stoppedOnError?: {
        statementIndex: number;
        error: string;
    };
}

export interface QueryResultsContextType {
    executeQuery: (
        query: string,
        bind_vars?: BindVarSchema[],
        saveToHistory?: boolean,
        limit?: number,
    ) => Promise<ExecuteQueryResult | ExecuteQueryError>;
    executeQueryBatch: (
        query: string,
        bind_vars?: BindVarSchema[],
        saveToHistory?: boolean,
        limit?: number,
        onJobStarted?: (job: ExecuteQueryBatchJob) => void,
    ) => Promise<ExecuteQueryBatchResult | ExecuteQueryError>;
    subscribe: (
        jobId: number,
        callback: (result: QueryResult) => void,
    ) => () => void;
}

export const QueryResultsContext = createContext<
    QueryResultsContextType | undefined
>(undefined);

export const useQueryResults = (): QueryResultsContextType => {
    const context = useContext(QueryResultsContext);
    if (!context) {
        throw new Error(
            "useQueryResults must be used within a QueryResultsProvider",
        );
    }
    return context;
};
