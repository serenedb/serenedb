import { createContext, useContext } from "react";
import type { DashboardBlockSchema, DashboardSchema } from "@serene-ui/shared-core";

export interface DashboardPageContextType {
    currentDashboard: DashboardSchema | null;
    currentDashboardId: number | null;
    editedBlock: DashboardBlockSchema | null;
    handleCloseEditor: () => void;
    handleOpenEditor: (block: DashboardBlockSchema) => void;
    handleSetEditedBlock: (block: DashboardBlockSchema | null) => void;
    handleSetCurrentDashboard: (dashboardId: number | null) => void;
    isEditorOpened: boolean;
    isDashboardsFetched: boolean;
    isDashboardsLoading: boolean;
    isExplorerOpened: boolean;
    toggleEditor: () => void;
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
