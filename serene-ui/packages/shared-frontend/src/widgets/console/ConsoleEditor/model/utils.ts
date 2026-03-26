import type { DockviewApi } from "dockview";
import {
    CONSOLE_EDITOR_PANEL_COMPONENT,
    createEditorPanelParams,
    createPanelId,
    createPanelTitle,
} from "./consts";
import type {
    ConsoleResult,
    EditorPanelParams,
    NormalizedEditorPanelParams,
    SuccessfulQueryResult,
} from "./types";

export const isPendingResult = (result: ConsoleResult) =>
    result.status === "pending" || result.status === "running";

export const hasResolvedResults = (results: ConsoleResult[]) =>
    results.some(
        (result) => result.status === "success" || result.status === "failed",
    );

export const normalizePanelParams = (
    params?: EditorPanelParams,
): NormalizedEditorPanelParams => ({
    query: typeof params?.query === "string" ? params.query : "",
    results: Array.isArray(params?.results) ? params.results : [],
    selectedResultIndex:
        typeof params?.selectedResultIndex === "number"
            ? params.selectedResultIndex
            : 0,
    highlightJobIds: Array.isArray(params?.highlightJobIds)
        ? params.highlightJobIds
        : [],
    runOnMountMode: params?.runOnMountMode,
});

export const getSelectedResultIndex = (
    results: ConsoleResult[],
    selectedResultIndex: number,
) =>
    results.length
        ? Math.min(Math.max(0, selectedResultIndex), results.length - 1)
        : -1;

export const toConsoleResults = (
    result: SuccessfulQueryResult,
    currentResult: ConsoleResult,
    receivedAt: string,
): ConsoleResult[] => {
    const nextResults =
        result.results && result.results.length > 0
            ? result.results
            : [{ rows: [], message: undefined, action_type: undefined }];

    return nextResults.map((queryResult, index) => ({
        ...currentResult,
        jobId: result.jobId,
        rows: queryResult.rows,
        status: "success" as const,
        message: queryResult.message,
        action_type: queryResult.action_type,
        created_at: result.created_at,
        execution_started_at: result.execution_started_at,
        execution_finished_at: result.execution_finished_at,
        received_at: receivedAt,
        statementIndex:
            nextResults.length > 1
                ? (currentResult.statementIndex ?? 0) + index
                : currentResult.statementIndex,
        statementQuery:
            nextResults.length > 1
                ? `${currentResult.statementQuery} [result ${index + 1}]`
                : currentResult.statementQuery,
    }));
};

export const addEditorPanel = (
    api: DockviewApi,
    params: Partial<EditorPanelParams> = {},
) =>
    api.addPanel({
        id: createPanelId(),
        component: CONSOLE_EDITOR_PANEL_COMPONENT,
        title: createPanelTitle(api.totalPanels + 1),
        params: createEditorPanelParams(params),
    });
