import { BindVarSchema } from "@serene-ui/shared-core";

export interface ConsoleStatementRange {
    startOffset: number;
    endOffset: number;
}

export interface ConsoleTab {
    id: number;
    type: "query";
    value: string;
    bind_vars?: BindVarSchema[];
    selectedResultIndex?: number;
    results: {
        jobId: number;
        status: "running" | "pending" | "success" | "failed";
        rows: Record<string, any>[] | undefined;
        statementIndex: number;
        statementQuery: string;
        sourceQuery: string;
        statementRange: ConsoleStatementRange;
        error?: string;
        message?: string;
        action_type?: "SELECT" | "INSERT" | "UPDATE" | "DELETE" | "OTHER";
        created_at?: string;
        execution_started_at?: string;
        execution_finished_at?: string;
    }[];
}
