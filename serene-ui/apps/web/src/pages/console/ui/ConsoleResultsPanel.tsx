import {
    ResizablePanel,
    ResizableHandle,
} from "@serene-ui/shared-frontend/shared";
import { QueryResults } from "@serene-ui/shared-frontend/widgets";
import type { ConsoleTab } from "@serene-ui/shared-frontend/widgets";
import { useMemo } from "react";
import { useConsole } from "../model";

interface ConsoleResultsPanelProps {
    currentTab: ConsoleTab;
    isMaximized: boolean;
    isMaximizedResultsShown: boolean;
}

export const ConsoleResultsPanel = ({
    currentTab,
    isMaximized,
    isMaximizedResultsShown,
}: ConsoleResultsPanelProps) => {
    const { selectResult } = useConsole();
    const selectedResultIndex = useMemo(
        () =>
            Math.min(
                Math.max(
                    0,
                    currentTab.selectedResultIndex ??
                        currentTab.results.length - 1,
                ),
                Math.max(0, currentTab.results.length - 1),
            ),
        [currentTab.results.length, currentTab.selectedResultIndex],
    );

    // Only render if not maximized, or maximized with results shown
    if (isMaximized && !isMaximizedResultsShown) {
        return null;
    }

    return (
        <>
            <ResizableHandle className="bg-border" tabIndex={-1} />
            <ResizablePanel
                className="flex flex-col"
                minSize={30}
                defaultSize={50}>
                <QueryResults
                    results={currentTab.results}
                    selectedResultIndex={selectedResultIndex}
                    onSelectResult={(resultIndex) => {
                        selectResult(currentTab.id, resultIndex);
                    }}
                />
            </ResizablePanel>
        </>
    );
};
