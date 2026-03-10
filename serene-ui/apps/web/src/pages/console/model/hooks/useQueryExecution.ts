import { useCallback, useMemo } from "react";
import { useQuerySubscription } from "@serene-ui/shared-frontend/features";
import type { ConsoleTab } from "@serene-ui/shared-frontend/widgets";
import type { PendingConsoleResult } from "../ConsoleContext";
import type { QueryExecutionResultSchema } from "@serene-ui/shared-core";

export interface UseConsoleQueryExecutionProps {
    tabs: ConsoleTab[];
    selectedTabId: number;
    updateTab: (
        id: number,
        tabUpdate:
            | Partial<ConsoleTab>
            | ((tab: ConsoleTab) => Partial<ConsoleTab>),
    ) => void;
    isMaximized: boolean;
    isMaximizedResultsShown: boolean;
    toggleMaximizedResults: () => void;
}

export interface UseConsoleQueryExecutionReturn {
    addPendingResults: (
        results: PendingConsoleResult[],
        tabId?: number,
    ) => void;
}

export const useQueryExecution = ({
    tabs,
    selectedTabId,
    updateTab,
    isMaximized,
    isMaximizedResultsShown,
    toggleMaximizedResults,
}: UseConsoleQueryExecutionProps): UseConsoleQueryExecutionReturn => {
    const toConsoleResults = (
        jobId: number,
        results: QueryExecutionResultSchema[],
        currentResult: ConsoleTab["results"][number],
        status: "success",
    ) =>
        results.map((result, index) => ({
            ...currentResult,
            rows: result.rows,
            status,
            message: result.message,
            action_type: result.action_type,
            statementIndex:
                results.length > 1
                    ? (currentResult.statementIndex ?? 0) + index
                    : currentResult.statementIndex,
            statementQuery:
                results.length > 1
                    ? `${currentResult.statementQuery} [result ${index + 1}]`
                    : currentResult.statementQuery,
            jobId,
        }));

    const addPendingResults = useCallback(
        (resultsToAdd: PendingConsoleResult[], tabId?: number) => {
            if (!resultsToAdd.length) {
                return;
            }

            const targetTabId = tabId !== undefined ? tabId : selectedTabId;

            updateTab(targetTabId, (tab) => {
                const nextResults = [
                    ...tab.results,
                    ...resultsToAdd.map((result) => ({
                        jobId: result.jobId,
                        status: "pending" as const,
                        rows: [],
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
        [selectedTabId, updateTab],
    );

    const pendingJobIds = useMemo(() => {
        const jobIds = new Set<number>();
        tabs.forEach((tab) => {
            tab.results.forEach((result) => {
                if (
                    result.status === "pending" ||
                    result.status === "running"
                ) {
                    jobIds.add(result.jobId);
                }
            });
        });
        return Array.from(jobIds);
    }, [tabs]);

    useQuerySubscription(pendingJobIds, (_jobId, result) => {
        const tab = tabs.find((t) =>
            t.results.some((r) => r.jobId === result.jobId),
        );

        if (!tab) {
            return;
        }

        if (
            (result.status === "success" || result.status === "failed") &&
            !isMaximizedResultsShown &&
            isMaximized
        ) {
            toggleMaximizedResults();
        }

        updateTab(tab.id, (currentTab) => ({
            results: currentTab.results.flatMap((r) => {
                if (r.jobId !== result.jobId) {
                    return [r];
                }

                if (result.status === "success") {
                    return toConsoleResults(
                        result.jobId,
                        result.results || [],
                        {
                            ...r,
                            statementIndex:
                                result.statementIndex ?? r.statementIndex,
                            statementQuery:
                                result.statementQuery ?? r.statementQuery,
                            sourceQuery: result.sourceQuery ?? r.sourceQuery,
                            statementRange:
                                result.statementRange ?? r.statementRange,
                            created_at: result.created_at,
                            execution_started_at: result.execution_started_at,
                            execution_finished_at: result.execution_finished_at,
                        },
                        "success",
                    );
                }

                return [
                    {
                        ...r,
                        status: result.status,
                        error:
                            result.status === "failed" ? result.error : r.error,
                        statementIndex:
                            result.statementIndex ?? r.statementIndex,
                        statementQuery:
                            result.statementQuery ?? r.statementQuery,
                        sourceQuery: result.sourceQuery ?? r.sourceQuery,
                        statementRange:
                            result.statementRange ?? r.statementRange,
                        created_at: result.created_at,
                        execution_started_at: result.execution_started_at,
                        execution_finished_at: result.execution_finished_at,
                    },
                ];
            }),
        }));
    });

    return {
        addPendingResults,
    };
};
