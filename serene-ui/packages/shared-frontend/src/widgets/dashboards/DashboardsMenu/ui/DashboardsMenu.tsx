import React from "react";

import { DashboardsMenuProvider, useDashboardsMenuResize } from "../model";
import { DashboardsList } from "./DashboardsList";
import { FavoritesList } from "./FavoritesList";
import { SavedQueriesList } from "./SavedQueriesList";

interface DashboardsMenuComponentProps {
    onCurrentDashboardChange: (dashboardId: number) => void;
}

const DashboardsMenuContent: React.FC<DashboardsMenuComponentProps> = ({
    onCurrentDashboardChange,
}) => {
    const {
        bodyHeights,
        containerRef,
        getNextOpenSectionId,
        getResizeHandler,
    } = useDashboardsMenuResize();

    return (
        <div className="flex flex-1 flex-col" data-testid="dashboardsMenu-root">
            <div className="flex h-12 items-center border-b-1 bg-background px-4">
                <p className="text-primary-foreground uppercase text-xs font-extrabold">
                    Dashboards
                </p>
            </div>
            <div ref={containerRef} className="flex min-h-0 flex-1 flex-col">
                <FavoritesList
                    bodyHeight={bodyHeights.favorites ?? 0}
                    showResizeHandle={Boolean(
                        getNextOpenSectionId("favorites"),
                    )}
                    onResizePointerDown={getResizeHandler("favorites")}
                />
                <DashboardsList
                    bodyHeight={bodyHeights.dashboards ?? 0}
                    onCurrentDashboardChange={onCurrentDashboardChange}
                    showResizeHandle={Boolean(
                        getNextOpenSectionId("dashboards"),
                    )}
                    onResizePointerDown={getResizeHandler("dashboards")}
                />
                <SavedQueriesList bodyHeight={bodyHeights.savedQueries ?? 0} />
            </div>
        </div>
    );
};

export const DashboardsMenu: React.FC<DashboardsMenuComponentProps> = ({
    onCurrentDashboardChange,
}) => {
    return (
        <DashboardsMenuProvider>
            <DashboardsMenuContent
                onCurrentDashboardChange={onCurrentDashboardChange}
            />
        </DashboardsMenuProvider>
    );
};
