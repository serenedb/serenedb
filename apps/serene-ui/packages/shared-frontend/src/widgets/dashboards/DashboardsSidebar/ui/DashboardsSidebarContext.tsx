import React from "react";

interface DashboardsSidebarContextValue {
    onCurrentDashboardChange: (dashboardId: number) => void;
}

export const DashboardsSidebarContext =
    React.createContext<DashboardsSidebarContextValue | null>(null);

export const useDashboardsSidebarContext = () => {
    const context = React.useContext(DashboardsSidebarContext);

    if (!context) {
        throw new Error(
            "useDashboardsSidebarContext must be used within a DashboardsSidebar",
        );
    }

    return context;
};
