import { useCallback, useEffect, useMemo } from "react";
import type { MutableRefObject } from "react";
import type { IDockviewPanelProps } from "dockview";
import {
    useQueryResults,
    useQuerySubscription,
} from "../../../../features/executeQuery";
import { useConsole } from "../../Console/model/ConsoleContext";
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
    panelId: string;
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
    notifyResultsReady: (status: "success" | "failed") => void;
    limit: number;
}

export const useConsoleQueryExecution = ({
    containerApi,
    panelId,
    panelState,
    paramsRef,
    updatePanelParams,
    showResultsPanel,
    notifyResultsReady,
    limit,
}: UseConsoleQueryExecutionParams) => {
    const { executeQuery, executeQueryBatch } = useQueryResults();
    const { upsertExecutionHistoryEntries } = useConsole();

    const appendPendingResults = useCallback(
        (
            resultsToAdd: PendingConsoleResult[],
            options?: {
                showPanel?: boolean;
            },
        ) => {
            if (!resultsToAdd.length) {
                return;
            }

            const panelTitle = containerApi.getPanel(panelId)?.api.title;

            let nextPanelState: NormalizedEditorPanelParams | undefined;

            updatePanelParams((current) => {
                const nextHighlightJobIds = Array.from(
                    new Set([
                        ...current.highlightJobIds,
                        ...resultsToAdd.map((result) => result.jobId),
                    ]),
                );
                const nextResults = [
                    ...current.results,
                    ...resultsToAdd.map((result) => ({
                        jobId: result.jobId,
                        rows: [],
                        status: "pending" as const,
                        statementIndex: result.statementIndex,
                        statementQuery: result.statementQuery,
                        statementType: result.statementType,
                        sourceQuery: result.sourceQuery,
                        statementRange: result.statementRange,
                    })),
                ];

                nextPanelState = {
                    ...current,
                    results: nextResults,
                    selectedResultIndex: Math.max(0, nextResults.length - 1),
                    highlightJobIds: nextHighlightJobIds,
                };

                return {
                    results: nextResults,
                    selectedResultIndex: nextPanelState.selectedResultIndex,
                    highlightJobIds: nextHighlightJobIds,
                    tabExecutionStatus: "running",
                };
            });

            upsertExecutionHistoryEntries(
                resultsToAdd.map((result) => ({
                    panelId,
                    panelTitle,
                    jobId: result.jobId,
                    status: "pending",
                    statementIndex: result.statementIndex,
                    statementQuery: result.statementQuery,
                    sourceQuery: result.sourceQuery,
                    execution_started_at: undefined,
                    execution_finished_at: undefined,
                    created_at: undefined,
                    received_at: undefined,
                })),
            );

            if (options?.showPanel && nextPanelState) {
                showResultsPanel(false, nextPanelState);
            }
        },
        [
            containerApi,
            panelId,
            showResultsPanel,
            updatePanelParams,
            upsertExecutionHistoryEntries,
        ],
    );

    const handleExecute = useCallback(
        async (mode: ConsoleExecutionMode) => {
            const current = paramsRef.current;
            updatePanelParams({ highlightJobIds: [] });

            if (mode !== "transaction") {
                let shouldShowResultsPanel = true;

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
                                statementType: job.statementType,
                                sourceQuery: job.sourceQuery,
                                statementRange: job.statementRange,
                            },
                        ], {
                            showPanel: shouldShowResultsPanel,
                        });
                        shouldShowResultsPanel = false;
                    },
                    {
                        continueOnError: mode === "sequentialIgnoreErrors",
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
            ], {
                showPanel: true,
            });
        },
        [appendPendingResults, executeQuery, executeQueryBatch, limit, paramsRef],
    );

    const handleExecuteInNewTab = useCallback(
        (mode: ConsoleExecutionMode = "sequential") => {
            const current = paramsRef.current;
            const panel = addEditorPanel(containerApi, {
                query: current.query,
                results: [],
                selectedResultIndex: 0,
                runOnMountMode: mode,
            });

            panel.api.setActive();
        },
        [containerApi, paramsRef],
    );

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
        const panelTitle = containerApi.getPanel(panelId)?.api.title;
        const receivedAt = new Date().toISOString();
        let nextPanelState: NormalizedEditorPanelParams | undefined;
        let shouldNotifyResultsReady = false;
        let notificationStatus: "success" | "failed" = "success";
        let historyEntriesToUpsert: Parameters<
            typeof upsertExecutionHistoryEntries
        >[0] = [];

        updatePanelParams((current) => {
            const hadPendingResults = current.results.some(isPendingResult);
            const pendingJobIds = new Set(
                current.results
                    .filter(isPendingResult)
                    .map((currentResult) => currentResult.jobId),
            );
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
                        statementType:
                            result.statementType ?? currentResult.statementType,
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
                    tabExecutionStatus: current.tabExecutionStatus,
                };
            }

            historyEntriesToUpsert = nextResults
                .filter((nextResult) => nextResult.jobId === result.jobId)
                .map((nextResult) => ({
                    panelId,
                    panelTitle,
                    jobId: nextResult.jobId,
                    status: nextResult.status,
                    statementIndex: nextResult.statementIndex,
                    statementQuery: nextResult.statementQuery,
                    sourceQuery: nextResult.sourceQuery,
                    created_at: nextResult.created_at,
                    execution_started_at: nextResult.execution_started_at,
                    execution_finished_at: nextResult.execution_finished_at,
                    received_at: nextResult.received_at,
                }));

            nextPanelState = {
                ...current,
                results: nextResults,
                selectedResultIndex: Math.min(
                    Math.max(0, nextSelectedResultIndex),
                    nextResults.length - 1,
                ),
            };
            const hasPendingResults = nextResults.some(isPendingResult);

            if (hadPendingResults && !hasPendingResults) {
                shouldNotifyResultsReady = true;
                notificationStatus = nextResults.some(
                    (currentResult) =>
                        pendingJobIds.has(currentResult.jobId) &&
                        currentResult.status === "failed",
                )
                    ? "failed"
                    : "success";
            }

            const nextTabExecutionStatus = hasPendingResults
                ? "running"
                : hadPendingResults && !hasPendingResults
                  ? notificationStatus
                  : current.tabExecutionStatus;

            return {
                results: nextResults,
                selectedResultIndex: nextPanelState.selectedResultIndex,
                tabExecutionStatus: nextTabExecutionStatus,
            };
        });

        if (historyEntriesToUpsert.length) {
            upsertExecutionHistoryEntries(historyEntriesToUpsert);
        }

        if (nextPanelState && shouldNotifyResultsReady) {
            notifyResultsReady(notificationStatus);
        }
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
