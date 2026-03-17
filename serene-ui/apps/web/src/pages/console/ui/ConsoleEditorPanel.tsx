import type { BindVarSchema } from "@serene-ui/shared-core";
import {
    ResizablePanel,
    ResizablePanelGroup,
} from "@serene-ui/shared-frontend/shared";
import {
    BindVariables,
    ConsoleEditor,
} from "@serene-ui/shared-frontend/widgets";
import { useCallback, useMemo } from "react";
import { useConsole } from "../model";

interface ConsoleEditorPanelProps {
    layout: "horizontal" | "vertical";
}

export const ConsoleEditorPanel = ({ layout }: ConsoleEditorPanelProps) => {
    const {
        tabs,
        selectedTabId,
        addTab,
        selectTab,
        removeTab,
        updateTab,
        addPendingResults,
        limit,
        setLimit,
        editorRef,
    } = useConsole();
    const currentTab = useMemo(
        () => tabs.find((tab) => tab.id === selectedTabId),
        [tabs, selectedTabId],
    );

    const handleChangeBindVars = useCallback(
        (bind_vars: BindVarSchema[]) => {
            updateTab(selectedTabId, { bind_vars });
        },
        [selectedTabId, updateTab],
    );

    if (!currentTab) {
        return (
            <div className="flex h-full w-full items-center justify-center">
                <p className="text-muted-foreground">No tab selected</p>
            </div>
        );
    }

    const selectedResultIndex = Math.min(
        Math.max(0, currentTab.selectedResultIndex ?? currentTab.results.length - 1),
        Math.max(0, currentTab.results.length - 1),
    );
    const activeResult = currentTab.results[selectedResultIndex];
    const highlightRange =
        activeResult &&
        currentTab.value === activeResult.sourceQuery &&
        activeResult.statementRange
            ? activeResult.statementRange
            : undefined;
    const highlightVariant = activeResult?.status === "failed" ? "error" : "default";

    return (
        <ResizablePanelGroup
            direction={layout === "horizontal" ? "vertical" : "horizontal"}>
            <ResizablePanel>
                <ConsoleEditor
                    tabs={tabs}
                    selectedTabId={selectedTabId}
                    addTab={addTab}
                    selectTab={selectTab}
                    removeTab={removeTab}
                    updateTab={updateTab}
                    addPendingResults={addPendingResults}
                    limit={limit}
                    editorRef={editorRef}
                    setLimit={setLimit}
                    highlightRange={highlightRange}
                    highlightVariant={highlightRange ? highlightVariant : undefined}
                />
            </ResizablePanel>
            {currentTab.bind_vars?.length ? (
                <ResizablePanel className="max-w-65">
                    <BindVariables
                        bind_vars={currentTab.bind_vars}
                        setBindVars={handleChangeBindVars}
                        className="bg-background border-l rounded-none"
                    />
                </ResizablePanel>
            ) : null}
        </ResizablePanelGroup>
    );
};
