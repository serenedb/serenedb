import type { DashboardBlockSchema } from "@serene-ui/shared-core";
import { useGetDashboards } from "@serene-ui/shared-frontend";
import { useCallback, useEffect, useMemo, useState } from "react";

import { DashboardPageContext } from "./DashboardContextProvider";

export const DashboardPageProvider = ({
    children,
}: {
    children: React.ReactNode;
}) => {
    const [currentDashboardId, setCurrentDashboardId] = useState<number | null>(
        null,
    );
    const [editedBlock, setEditedBlock] = useState<DashboardBlockSchema | null>(
        null,
    );
    const [isExplorerOpened, setIsExplorerOpened] = useState(true);
    const [isEditorOpened, setIsEditorOpened] = useState(false);
    const {
        data: dashboards,
        isFetched: isDashboardsFetched,
        isLoading: isDashboardsLoading,
    } = useGetDashboards();

    const currentDashboard = useMemo(() => {
        if (currentDashboardId === null) {
            return null;
        }

        return (
            dashboards?.find(
                (dashboard) => dashboard.id === currentDashboardId,
            ) ?? null
        );
    }, [currentDashboardId, dashboards]);

    useEffect(() => {
        if (
            currentDashboardId === null ||
            !isDashboardsFetched ||
            isDashboardsLoading
        ) {
            return;
        }

        const hasCurrentDashboard =
            dashboards?.some(
                (dashboard) => dashboard.id === currentDashboardId,
            ) ?? false;

        if (!hasCurrentDashboard) {
            setCurrentDashboardId(null);
        }
    }, [
        currentDashboardId,
        dashboards,
        isDashboardsFetched,
        isDashboardsLoading,
    ]);

    const toggleExplorer = useCallback(() => {
        setIsExplorerOpened((prev) => {
            const next = !prev;

            return next;
        });
    }, []);

    const toggleEditor = useCallback(() => {
        setIsEditorOpened((prev) => {
            const next = !prev;

            setIsExplorerOpened(!next);
            setEditedBlock(null);

            return next;
        });
    }, []);

    const handleOpenEditor = useCallback((block: DashboardBlockSchema) => {
        setEditedBlock(block);
        setIsEditorOpened(true);
        setIsExplorerOpened(false);
    }, []);

    const handleCloseEditor = useCallback(() => {
        setIsEditorOpened(false);
        setEditedBlock(null);
    }, []);

    return (
        <DashboardPageContext.Provider
            value={{
                currentDashboard,
                currentDashboardId,
                editedBlock,
                handleCloseEditor,
                handleOpenEditor,
                handleSetEditedBlock: setEditedBlock,
                handleSetCurrentDashboard: setCurrentDashboardId,
                isEditorOpened,
                isDashboardsFetched,
                isDashboardsLoading,
                isExplorerOpened,
                toggleEditor,
                toggleExplorer,
            }}>
            {children}
        </DashboardPageContext.Provider>
    );
};
