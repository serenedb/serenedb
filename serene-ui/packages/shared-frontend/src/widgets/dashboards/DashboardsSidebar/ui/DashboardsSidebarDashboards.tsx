import React from "react";
import { useGetDashboards } from "../../../../entities/dashboard";
import { DashboardsSidebarDashboardList } from "./DashboardsSidebarDashboardList";

export const DashboardsSidebarDashboards: React.FC = () => {
    const {
        data: dashboards,
        isFetched: isDataFetched,
        isLoading: isDataLoading,
    } = useGetDashboards();

    return (
        <DashboardsSidebarDashboardList
            dashboards={dashboards}
            isDataFetched={isDataFetched}
            isDataLoading={isDataLoading}
            emptyState="No dashboards yet"
            sectionId="dashboards"
        />
    );
};
