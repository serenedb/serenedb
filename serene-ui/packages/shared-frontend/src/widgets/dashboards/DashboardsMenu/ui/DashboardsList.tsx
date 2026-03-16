import React from "react";
import { useGetDashboards } from "../../../../entities/dashboard";
import { CreateDashboardButton } from "../../../../features";
import type { ExplorerNodeData } from "../../../shared/Explorer";

import { DashboardsMenuSection } from "./DashboardsMenuSection";
import { DashboardExplorer } from "./DashboardExplorer";
type DashboardsListProps = {
    bodyHeight: number;
    onCurrentDashboardChange: (dashboardId: number) => void;
    showResizeHandle?: boolean;
    onResizePointerDown?: (event: React.PointerEvent<HTMLDivElement>) => void;
};

export function DashboardsList({
    bodyHeight,
    onCurrentDashboardChange,
    showResizeHandle = false,
    onResizePointerDown,
}: DashboardsListProps) {
    const {
        data: dashboards,
        isFetched: isDataFetched,
        isLoading: isDataLoading,
    } = useGetDashboards();

    const initialData: ExplorerNodeData[] = (dashboards ?? []).map(
        (dashboard) => ({
            id: `dashboard-${dashboard.id}`,
            name: dashboard.name,
            type: "dashboard",
            parentId: null,
            context: {
                dashboardId: dashboard.id,
                action: () => onCurrentDashboardChange(dashboard.id),
            },
        }),
    );

    return (
        <DashboardsMenuSection
            sectionId="dashboards"
            title="Dashboards"
            bodyHeight={bodyHeight}
            showResizeHandle={showResizeHandle}
            onResizePointerDown={onResizePointerDown}
            actions={
                <CreateDashboardButton
                    onCreateDashboard={onCurrentDashboardChange}
                />
            }>
            <DashboardExplorer
                initialData={initialData}
                isDataFetched={isDataFetched && !isDataLoading}
                emptyState="No dashboards yet"
            />
        </DashboardsMenuSection>
    );
}
