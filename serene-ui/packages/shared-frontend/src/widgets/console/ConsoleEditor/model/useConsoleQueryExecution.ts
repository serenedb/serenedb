import { useCallback, useEffect, useMemo } from "react";
import type { MutableRefObject } from "react";
import type { IDockviewPanelProps } from "dockview";
import {
    useQueryResults,
    useQuerySubscription,
} from "../../../../features/executeQuery";
import { addEditorPanel, isPendingResult, toConsoleResults } from "./utils";
import type {
    ConsoleExecutionMode,
    ConsoleResult,
    EditorPanelParams,
    NormalizedEditorPanelParams,
    PendingConsoleResult,
} from "./types";

interface UseConsoleQueryExecutionParams {
    containerApi: IDockviewPanelProps<EditorPanelParams>["containerApi"];
    panelState: NormalizedEditorPanelParams;
    paramsRef: MutableRefObject<NormalizedEditorPanelParams>;
    updatePanelParams: (
        updater:
            | Partial<EditorPanelParams>
            | ((
                  current: NormalizedEditorPanelParams,
              ) => Partial<EditorPanelParams>),
    ) => void;
    showResultsPanel: (
        activate?: boolean,
        initialState?: NormalizedEditorPanelParams,
    ) => void;
    limit: number;
}

export const useConsoleQueryExecution = ({
    containerApi,
    panelState,
    paramsRef,
    updatePanelParams,
    showResultsPanel,
    limit,
}: UseConsoleQueryExecutionParams) => {
    const { executeQuery, executeQueryBatch } = useQueryResults();

    const appendPendingResults = useCallback(
        (resultsToAdd: PendingConsoleResult[]) => {
            if (!resultsToAdd.length) {
                return;
            }

            updatePanelParams((current) => {
                const nextResults = [
                    ...current.results,
                    ...resultsToAdd.map((result) => ({
                        jobId: result.jobId,
                        rows: [],
                        status: "pending" as const,
                        statementIndex: result.statementIndex,
                        statementQuery: result.statementQuery,
                        sourceQuery: result.sourceQuery,
                        statementRange: result.statementRange,
                    })),
                ];

                return {
                    results: nextResults,
                    selectedResultIndex: Math.max(0, nextResults.length - 1),
                };
            });
        },
        [updatePanelParams],
    );

    const handleExecute = useCallback(
        async (mode: ConsoleExecutionMode) => {
            const current = paramsRef.current;

            if (mode === "sequential") {
                const result = await executeQueryBatch(
                    current.query,
                    [],
                    true,
                    limit,
                    (job) => {
                        appendPendingResults([
                            {
                                jobId: job.jobId,
                                statementIndex: job.statementIndex,
                                statementQuery: job.statementQuery,
                                sourceQuery: job.sourceQuery,
                                statementRange: job.statementRange,
                            },
                        ]);
                    },
                );

                if (!result.success) {
                    return;
                }

                return;
            }

            const result = await executeQuery(current.query, [], true, limit);

            if (!result.success) {
                return;
            }

            appendPendingResults([
                {
                    jobId: result.jobId,
                    statementIndex: 0,
                    statementQuery: current.query,
                    sourceQuery: current.query,
                    statementRange: {
                        startOffset: 0,
                        endOffset: current.query.length,
                    },
                },
            ]);
        },
        [appendPendingResults, executeQuery, executeQueryBatch, limit, paramsRef],
    );

    const handleExecuteInNewTab = useCallback(() => {
        const current = paramsRef.current;
        const panel = addEditorPanel(containerApi, {
            query: current.query,
            results: [],
            selectedResultIndex: 0,
            runOnMountMode: "sequential",
        });

        panel.api.setActive();
    }, [containerApi, paramsRef]);

    const pendingJobIds = useMemo(
        () =>
            Array.from(
                new Set(
                    panelState.results
                        .filter(isPendingResult)
                        .map((result) => result.jobId),
                ),
            ),
        [panelState.results],
    );

    useQuerySubscription(pendingJobIds, (_jobId, result) => {
        const receivedAt = new Date().toISOString();
        let nextPanelState: NormalizedEditorPanelParams | undefined;

        updatePanelParams((current) => {
            let nextSelectedResultIndex = current.selectedResultIndex;

            const nextResults = current.results.flatMap(
                (currentResult, index) => {
                    if (currentResult.jobId !== result.jobId) {
                        return [currentResult];
                    }

                    const baseResult: ConsoleResult = {
                        ...currentResult,
                        status: result.status,
                        error:
                            result.status === "failed"
                                ? result.error
                                : currentResult.error,
                        created_at: result.created_at,
                        execution_started_at: result.execution_started_at,
                        execution_finished_at: result.execution_finished_at,
                        received_at: receivedAt,
                        statementIndex:
                            result.statementIndex ?? currentResult.statementIndex,
                        statementQuery:
                            result.statementQuery ?? currentResult.statementQuery,
                        sourceQuery:
                            result.sourceQuery ?? currentResult.sourceQuery,
                        statementRange:
                            result.statementRange ?? currentResult.statementRange,
                    };

                    if (result.status === "success") {
                        const resolvedResults = toConsoleResults(
                            result,
                            baseResult,
                            receivedAt,
                        );

                        if (current.selectedResultIndex === index) {
                            nextSelectedResultIndex =
                                index + resolvedResults.length - 1;
                        } else if (current.selectedResultIndex > index) {
                            nextSelectedResultIndex =
                                current.selectedResultIndex +
                                resolvedResults.length -
                                1;
                        }

                        return resolvedResults;
                    }

                    return [baseResult];
                },
            );

            if (!nextResults.length) {
                return {
                    results: nextResults,
                    selectedResultIndex: 0,
                };
            }

            nextPanelState = {
                ...current,
                results: nextResults,
                selectedResultIndex: Math.min(
                    Math.max(0, nextSelectedResultIndex),
                    nextResults.length - 1,
                ),
            };

            if (result.status === "success" || result.status === "failed") {
                showResultsPanel(false, nextPanelState);
            }

            return {
                results: nextResults,
                selectedResultIndex: nextPanelState.selectedResultIndex,
            };
        });
    });

    useEffect(() => {
        if (!panelState.runOnMountMode) {
            return;
        }

        const mode = panelState.runOnMountMode;
        updatePanelParams({ runOnMountMode: undefined });
        void handleExecute(mode);
    }, [handleExecute, panelState.runOnMountMode, updatePanelParams]);

    return {
        handleExecute,
        handleExecuteInNewTab,
    };
};
