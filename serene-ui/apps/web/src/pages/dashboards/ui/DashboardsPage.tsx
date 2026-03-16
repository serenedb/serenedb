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
    const {
        currentDashboard,
        editedBlock,
        handleCloseEditor,
        handleOpenEditor,
        handleSetEditedBlock,
        handleSetCurrentDashboard,
        isEditorOpened,
        isExplorerOpened,
        toggleExplorer,
    } = useDashboardPage();

    const dashboardContent = (
        <div className="flex min-h-0 min-w-0 flex-1 flex-col overflow-hidden">
            <DashboardsTopbar
                currentDashboard={currentDashboard}
                isExplorerOpened={isExplorerOpened}
                onToggleExplorer={toggleExplorer}
            />
            <DashboardGrid
                currentDashboard={currentDashboard}
                editedBlock={editedBlock}
                onCloseEditor={handleCloseEditor}
                onEditBlock={handleOpenEditor}
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
                            handleSetCurrentDashboard={
                                handleSetCurrentDashboard
                            }
                        />
                    </ResizablePanel>
                    <ResizableHandle className="bg-border" tabIndex={-1} />
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
                    <ResizableHandle className="bg-border" tabIndex={-1} />
                    <ResizablePanel
                        className="flex min-h-0 min-w-0 overflow-hidden"
                        defaultSize={30}
                        minSize={20}
                        maxSize={50}>
                        <DashboardCardEditor
                            editedBlock={editedBlock}
                            onClose={handleCloseEditor}
                            onEditedBlockChange={handleSetEditedBlock}
                        />
                    </ResizablePanel>
                </>
            )}
        </ResizablePanelGroup>
    );
};
