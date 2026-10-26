import type { QueryResult } from "../../../../features/executeQuery";

export type ConsoleExecutionMode =
    | "sequential"
    | "sequentialIgnoreErrors"
    | "transaction";

export interface ConsoleStatementRange {
    startOffset: number;
    endOffset: number;
}

export interface ConsoleResult {
    jobId: number;
    rows: Record<string, unknown>[] | undefined;
    status: "success" | "failed" | "pending" | "running" | "";
    statementIndex?: number;
    statementQuery?: string;
    statementType?: string;
    statementRange?: ConsoleStatementRange;
    sourceQuery?: string;
    error?: string;
    message?: string;
    created_at?: string;
    execution_started_at?: string;
    execution_finished_at?: string;
    received_at?: string;
    action_type?: "SELECT" | "INSERT" | "UPDATE" | "DELETE" | "OTHER";
}

export interface PendingConsoleResult {
    jobId: number;
    statementIndex: number;
    statementQuery: string;
    statementType?: string;
    sourceQuery: string;
    statementRange: ConsoleStatementRange;
}

export type ConsoleTabExecutionStatus = "running" | "success" | "failed" | "";

export interface EditorPanelParams {
    query?: string;
    results?: ConsoleResult[];
    selectedResultIndex?: number;
    highlightJobIds?: number[];
    tabExecutionStatus?: ConsoleTabExecutionStatus;
    runOnMountMode?: ConsoleExecutionMode;
}

export interface ResultsPanelParams {
    sourcePanelId: string;
    initialState?: NormalizedEditorPanelParams;
}

export interface NormalizedEditorPanelParams {
    query: string;
    results: ConsoleResult[];
    selectedResultIndex: number;
    highlightJobIds: number[];
    tabExecutionStatus: ConsoleTabExecutionStatus;
    runOnMountMode?: ConsoleExecutionMode;
}

export type SuccessfulQueryResult = Extract<QueryResult, { status: "success" }>;

export type EditorPanelParamsUpdater =
    | Partial<EditorPanelParams>
    | ((
          current: NormalizedEditorPanelParams,
      ) => Partial<EditorPanelParams>);
