import { createContext, useContext } from "react";
import type { DashboardBlockSchema, DashboardSchema } from "@serene-ui/shared-core";

export interface DashboardPageContextType {
    chartsRefreshToken: number;
    currentDashboard: DashboardSchema | null;
    currentDashboardId: number | null;
    editedBlock: DashboardBlockSchema | null;
    closeEditor: () => void;
    openEditor: (block: DashboardBlockSchema) => void;
    refreshAllCharts: () => void;
    setEditedBlock: (block: DashboardBlockSchema | null) => void;
    setCurrentDashboardId: (dashboardId: number | null) => void;
    isEditorOpened: boolean;
    isDashboardsFetched: boolean;
    isDashboardsLoading: boolean;
    isExplorerOpened: boolean;
    toggleExplorer: () => void;
}

export const DashboardPageContext = createContext<
    DashboardPageContextType | undefined
>(undefined);

export const useDashboardPage = (): DashboardPageContextType => {
    const context = useContext(DashboardPageContext);

    if (!context) {
        throw new Error(
            "useDashboardPage must be used within a DashboardPageProvider",
        );
    }

    return context;
};
