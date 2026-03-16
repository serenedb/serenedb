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

export const DashboardsPage = () => {
    const [isPanelResizing, setIsPanelResizing] = React.useState(false);
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
    const handlePanelDragging = React.useCallback((isDragging: boolean) => {
        setIsPanelResizing(isDragging);
    }, []);

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
