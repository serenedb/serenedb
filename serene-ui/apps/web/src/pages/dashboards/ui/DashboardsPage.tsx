import React from "react";
import {
    DashboardCardEditor,
    DashboardGrid,
    DashboardsMenu,
    DashboardsTopbar,
    ResizableHandle,
    ResizablePanel,
    ResizablePanelGroup,
} from "@serene-ui/shared-frontend";
import { useDashboardPage } from "../model";

const PANEL_LAYOUT_RESIZE_DELAY_MS = 220;

export const DashboardsPage = () => {
    const [isPanelDragging, setIsPanelDragging] = React.useState(false);
    const [isPanelLayoutResizing, setIsPanelLayoutResizing] =
        React.useState(false);
    const layoutResizeTimeoutRef = React.useRef<number | null>(null);
    const hasMountedRef = React.useRef(false);
    const {
        chartsRefreshToken,
        currentDashboard,
        editedBlock,
        closeEditor,
        openEditor,
        refreshAllCharts,
        setEditedBlock,
        setCurrentDashboardId,
        isEditorOpened,
        isExplorerOpened,
        toggleExplorer,
    } = useDashboardPage();
    const clearPanelLayoutResizeTimeout = React.useCallback(() => {
        if (layoutResizeTimeoutRef.current === null) {
            return;
        }

        window.clearTimeout(layoutResizeTimeoutRef.current);
        layoutResizeTimeoutRef.current = null;
    }, []);

    const handlePanelDragging = React.useCallback((isDragging: boolean) => {
        setIsPanelDragging(isDragging);
    }, []);

    React.useEffect(() => {
        if (!hasMountedRef.current) {
            hasMountedRef.current = true;
            return;
        }

        clearPanelLayoutResizeTimeout();
        setIsPanelLayoutResizing(true);

        layoutResizeTimeoutRef.current = window.setTimeout(() => {
            setIsPanelLayoutResizing(false);
            layoutResizeTimeoutRef.current = null;
        }, PANEL_LAYOUT_RESIZE_DELAY_MS);
    }, [clearPanelLayoutResizeTimeout, isEditorOpened, isExplorerOpened]);

    React.useEffect(
        () => () => {
            clearPanelLayoutResizeTimeout();
        },
        [clearPanelLayoutResizeTimeout],
    );

    const isPanelResizing = isPanelDragging || isPanelLayoutResizing;

    const panelLayoutId = React.useMemo(() => {
        if (isExplorerOpened && isEditorOpened) {
            return "dashboards-layout-explorer-main-editor";
        }

        if (isExplorerOpened) {
            return "dashboards-layout-explorer-main";
        }

        if (isEditorOpened) {
            return "dashboards-layout-main-editor";
        }

        return "dashboards-layout-main";
    }, [isEditorOpened, isExplorerOpened]);

    const dashboardContent = (
        <div className="flex min-h-0 min-w-0 flex-1 flex-col overflow-hidden">
            <DashboardsTopbar
                currentDashboard={currentDashboard}
                isExplorerOpened={isExplorerOpened}
                onToggleExplorer={toggleExplorer}
                onRefreshAllCharts={refreshAllCharts}
            />
            <DashboardGrid
                currentDashboard={currentDashboard}
                editedBlock={editedBlock}
                isPanelResizing={isPanelResizing}
                manualRefreshToken={chartsRefreshToken}
                onCloseEditor={closeEditor}
                onEditBlock={openEditor}
            />
        </div>
    );

    return (
        <ResizablePanelGroup
            direction="horizontal"
            autoSaveId={panelLayoutId}
            className="h-full w-full min-h-0 min-w-0 overflow-hidden">
            {isExplorerOpened && (
                <>
                    <ResizablePanel
                        className="flex min-h-0 min-w-0 overflow-hidden"
                        defaultSize={20}
                        minSize={20}
                        maxSize={35}>
                        <DashboardsMenu
                            onCurrentDashboardChange={setCurrentDashboardId}
                        />
                    </ResizablePanel>
                    <ResizableHandle
                        className="bg-border"
                        tabIndex={-1}
                        onDragging={handlePanelDragging}
                    />
                </>
            )}
            <ResizablePanel
                className="flex min-h-0 min-w-0 overflow-hidden"
                defaultSize={isExplorerOpened || isEditorOpened ? 50 : 100}
                minSize={25}>
                {dashboardContent}
            </ResizablePanel>
            {isEditorOpened && (
                <>
                    <ResizableHandle
                        className="bg-border"
                        tabIndex={-1}
                        onDragging={handlePanelDragging}
                    />
                    <ResizablePanel
                        className="flex min-h-0 min-w-0 overflow-hidden"
                        defaultSize={30}
                        minSize={20}
                        maxSize={50}>
                        <DashboardCardEditor
                            editedBlock={editedBlock}
                            onClose={closeEditor}
                            onEditedBlockChange={setEditedBlock}
                        />
                    </ResizablePanel>
                </>
            )}
        </ResizablePanelGroup>
    );
};
