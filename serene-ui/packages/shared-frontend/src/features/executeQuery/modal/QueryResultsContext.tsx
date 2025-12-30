import { createContext, useContext } from "react";
import type { QueryResult } from "./types";
import { BindVarSchema } from "@serene-ui/shared-core";

export interface ExecuteQueryResult {
    success: true;
    jobId: number;
}

export interface ExecuteQueryError {
    success: false;
    error: string;
}

export interface QueryResultsContextType {
    executeQuery: (
        query: string,
        bind_vars?: BindVarSchema[],
        saveToHistory?: boolean,
        limit?: number,
    ) => Promise<ExecuteQueryResult | ExecuteQueryError>;
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
