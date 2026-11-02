import React from "react";
import { useGetFavoriteDashboards } from "../../../../entities/dashboard";
import type { ExplorerNodeData } from "../../../shared/Explorer";

import { DashboardsMenuSection } from "./DashboardsMenuSection";
import { DashboardExplorer } from "./DashboardExplorer";
type FavoritesListProps = {
    bodyHeight: number;
    onCurrentDashboardChange: (dashboardId: number) => void;
    showResizeHandle?: boolean;
    onResizePointerDown?: (event: React.PointerEvent<HTMLDivElement>) => void;
};

export function FavoritesList({
    bodyHeight,
    onCurrentDashboardChange,
    showResizeHandle = false,
    onResizePointerDown,
}: FavoritesListProps) {
    const {
        data: dashboards,
        isFetched: isDataFetched,
        isLoading: isDataLoading,
    } = useGetFavoriteDashboards();

    const initialData: ExplorerNodeData[] = (dashboards ?? []).map(
        (dashboard) => ({
            id: `favorite-dashboard-${dashboard.id}`,
            name: dashboard.name,
            type: "dashboard",
            parentId: null,
            context: {
                dashboardId: dashboard.id,
                dashboardFavorite: dashboard.favorite,
                action: () => onCurrentDashboardChange(dashboard.id),
            },
        }),
    );

    return (
        <DashboardsMenuSection
            sectionId="favorites"
            title="Favorites"
            bodyHeight={bodyHeight}
            showResizeHandle={showResizeHandle}
            onResizePointerDown={onResizePointerDown}>
            <DashboardExplorer
                initialData={initialData}
                isDataFetched={isDataFetched && !isDataLoading}
                emptyState="No favorites yet"
            />
        </DashboardsMenuSection>
    );
}
