import {
    ResizablePanel,
    ResizablePanelGroup,
} from "@serene-ui/shared-frontend/shared";
import { useConsole } from "../model";
import { useConsoleLayout } from "@serene-ui/shared-frontend/features";
import { ConsoleErrorBoundary } from "./ConsoleErrorBoundary";
import { ConsoleLeftPanel } from "./ConsoleLeftPanel";
import { ConsoleEditorPanel } from "./ConsoleEditorPanel";
import { ConsoleResultsPanel } from "./ConsoleResultsPanel";
import { useMemo } from "react";

const ConsoleContent = () => {
    const { tabs, selectedTabId } = useConsole();

    const { layout, isMaximized, isMaximizedResultsShown } = useConsoleLayout();

    const currentTab = useMemo(
        () => tabs.find((tab) => tab.id === selectedTabId),
        [tabs, selectedTabId],
    );

    if (!currentTab) {
        return (
            <div className="flex h-full w-full items-center justify-center">
                <p className="text-muted-foreground">No tab available</p>
            </div>
        );
    }

    return (
        <ResizablePanelGroup direction="horizontal" className="w-full h-full">
            {!isMaximized && <ConsoleLeftPanel />}

            <ResizablePanel defaultSize={70}>
                <ResizablePanelGroup
                    direction={
                        layout === "horizontal" ? "horizontal" : "vertical"
                    }>
                    <ResizablePanel
                        className="flex pt-2"
                        minSize={30}
                        defaultSize={50}>
                        <ConsoleEditorPanel layout={layout} />
                    </ResizablePanel>
                    <ConsoleResultsPanel
                        currentTab={currentTab}
                        isMaximized={isMaximized}
                        isMaximizedResultsShown={isMaximizedResultsShown}
                    />
                </ResizablePanelGroup>
            </ResizablePanel>
        </ResizablePanelGroup>
    );
};

export const ConsolePage = () => {
    return (
        <ConsoleErrorBoundary>
            <ConsoleContent />
        </ConsoleErrorBoundary>
    );
};
