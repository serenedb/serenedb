import {
    DashboardGrid,
    DashboardsMenu,
    DashboardsTopbar,
    ResizableHandle,
    ResizablePanel,
    ResizablePanelGroup,
} from "@serene-ui/shared-frontend";

export const DashboardsPage = () => {
    return (
        <ResizablePanelGroup
            direction="horizontal"
            className="h-full w-full min-h-0 overflow-hidden">
            <ResizablePanel className="flex min-h-0" defaultSize={20}>
                <DashboardsMenu />
            </ResizablePanel>
            <ResizableHandle className="bg-border" tabIndex={-1} />
            <ResizablePanel className="flex min-h-0" defaultSize={80}>
                <div className="flex min-h-0 flex-1 flex-col overflow-hidden">
                    <DashboardsTopbar />
                    <DashboardGrid />
                </div>
            </ResizablePanel>
        </ResizablePanelGroup>
    );
};
