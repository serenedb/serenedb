import {
    ResizablePanel,
    ResizableHandle,
} from "@serene-ui/shared-frontend/shared";
import { QueryResults } from "@serene-ui/shared-frontend/widgets";
import type { ConsoleTab } from "@serene-ui/shared-frontend/widgets";
import { useMemo } from "react";

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
    const selectedResultIndex = useMemo(
        () => currentTab.results.length - 1,
        [currentTab.results.length],
    );

    // Only render if not maximized, or maximized with results shown
    if (isMaximized && !isMaximizedResultsShown) {
        return null;
    }

    return (
        <>
            <ResizableHandle className="bg-border" tabIndex={-1}  />
            <ResizablePanel
                className="flex flex-col"
                minSize={30}
                defaultSize={50}>
                <QueryResults
                    results={currentTab.results}
                    selectedResultIndex={selectedResultIndex}
                />
            </ResizablePanel>
        </>
    );
};
