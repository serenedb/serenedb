import { useCallback, useMemo } from "react";
import { useQuerySubscription } from "@serene-ui/shared-frontend/features";
import type { ConsoleTab } from "@serene-ui/shared-frontend/widgets";

export interface UseConsoleQueryExecutionProps {
    tabs: ConsoleTab[];
    selectedTabId: number;
    updateTab: (id: number, tabUpdate: Partial<ConsoleTab>) => void;
    isMaximized: boolean;
    isMaximizedResultsShown: boolean;
    toggleMaximizedResults: () => void;
}

export interface UseConsoleQueryExecutionReturn {
    addJobId: (jobId: number, tabId?: number) => void;
}

export const useQueryExecution = ({
    tabs,
    selectedTabId,
    updateTab,
    isMaximized,
    isMaximizedResultsShown,
    toggleMaximizedResults,
}: UseConsoleQueryExecutionProps): UseConsoleQueryExecutionReturn => {
    const addJobId = useCallback(
        (jobId: number, tabId?: number) => {
            const targetTabId = tabId !== undefined ? tabId : selectedTabId;
            const tab = tabs.find((t) => t.id === targetTabId);
            if (!tab) return;

            updateTab(targetTabId, {
                results: [
                    ...tab.results,
                    { jobId, status: "pending", rows: [] },
                ],
            });
        },
        [tabs, selectedTabId, updateTab],
    );

    const pendingJobIds = useMemo(() => {
        const jobIds = new Set<number>();
        tabs.forEach((tab) => {
            tab.results.forEach((result) => {
                if (result.status === "pending") {
                    jobIds.add(result.jobId);
                }
            });
        });
        return Array.from(jobIds);
    }, [tabs]);

    useQuerySubscription(pendingJobIds, (_jobId, result) => {
        if (result.status !== "success" && result.status !== "failed") {
            return;
        }

        const tab = tabs.find((t) =>
            t.results.some((r) => r.jobId === result.jobId),
        );

        if (!tab) {
            return;
        }

        if (!isMaximizedResultsShown && isMaximized) {
            toggleMaximizedResults();
        }

        updateTab(tab.id, {
            results: tab.results.map((r) =>
                r.jobId === result.jobId
                    ? {
                          ...r,
                          rows:
                              result.status === "success"
                                  ? result.result
                                  : undefined,
                          status: result.status,
                          error:
                              result.status === "failed"
                                  ? result.error
                                  : undefined,
                          created_at: result.created_at,
                          execution_started_at: result.execution_started_at,
                          execution_finished_at: result.execution_finished_at,
                      }
                    : r,
            ),
        });
    });

    return {
        addJobId,
    };
};
