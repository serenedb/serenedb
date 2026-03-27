import type { DashboardBlockSchema } from "@serene-ui/shared-core";
import { useGetDashboards } from "@serene-ui/shared-frontend";
import { navigationMap } from "@serene-ui/shared-frontend/shared";
import { useCallback, useEffect, useMemo, useState } from "react";
import { useLocation, useNavigate } from "react-router-dom";

import { DashboardPageContext } from "./DashboardContextProvider";

const DASHBOARD_EXPLORER_STATE_KEY = "dashboards-explorer-opened";

export const DashboardPageProvider = ({
    children,
}: {
    children: React.ReactNode;
}) => {
    const navigate = useNavigate();
    const location = useLocation();
    const [currentDashboardId, setCurrentDashboardIdState] = useState<
        number | null
    >(null);
    const [editedBlock, setEditedBlock] = useState<DashboardBlockSchema | null>(
        null,
    );
    const [isExplorerOpened, setIsExplorerOpened] = useState(() => {
        if (typeof window === "undefined") {
            return true;
        }

        const storedValue = window.localStorage.getItem(
            DASHBOARD_EXPLORER_STATE_KEY,
        );

        if (storedValue === null) {
            return true;
        }

        return storedValue === "true";
    });
    const [isEditorOpened, setIsEditorOpened] = useState(false);
    const [chartsRefreshToken, setChartsRefreshToken] = useState(0);
    const {
        data: dashboards,
        isFetched: isDashboardsFetched,
        isLoading: isDashboardsLoading,
    } = useGetDashboards();
    const urlDashboardId = useMemo(() => {
        if (!location.pathname.startsWith(`${navigationMap.dashboards}/`)) {
            return null;
        }

        const rawId = location.pathname.slice(
            `${navigationMap.dashboards}/`.length,
        );

        if (!/^\d+$/.test(rawId)) {
            return null;
        }

        return Number(rawId);
    }, [location.pathname]);

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
        if (urlDashboardId === null) {
            return;
        }

        if (currentDashboardId !== urlDashboardId) {
            setCurrentDashboardIdState(urlDashboardId);
        }
    }, [currentDashboardId, urlDashboardId]);

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
            setCurrentDashboardIdState(null);

            if (urlDashboardId !== null) {
                navigate(navigationMap.dashboards, { replace: true });
            }
        }
    }, [
        currentDashboardId,
        dashboards,
        isDashboardsFetched,
        isDashboardsLoading,
        navigate,
        urlDashboardId,
    ]);

    useEffect(() => {
        if (typeof window === "undefined") {
            return;
        }

        window.localStorage.setItem(
            DASHBOARD_EXPLORER_STATE_KEY,
            String(isExplorerOpened),
        );
    }, [isExplorerOpened]);

    const setCurrentDashboardId = useCallback(
        (dashboardId: number | null) => {
            setCurrentDashboardIdState(dashboardId);

            if (dashboardId === null) {
                if (location.pathname !== navigationMap.dashboards) {
                    navigate(navigationMap.dashboards);
                }

                return;
            }

            const dashboardPath = `${navigationMap.dashboards}/${dashboardId}`;

            if (location.pathname !== dashboardPath) {
                navigate(dashboardPath);
            }
        },
        [location.pathname, navigate],
    );

    const toggleExplorer = useCallback(() => {
        setIsExplorerOpened((prev) => {
            const next = !prev;

            return next;
        });
    }, []);

    const openEditor = useCallback((block: DashboardBlockSchema) => {
        setEditedBlock(block);
        setIsEditorOpened(true);
        setIsExplorerOpened(false);
    }, []);

    const closeEditor = useCallback(() => {
        setIsEditorOpened(false);
        setEditedBlock(null);
    }, []);

    const refreshAllCharts = useCallback(() => {
        setChartsRefreshToken((currentValue) => currentValue + 1);
    }, []);

    return (
        <DashboardPageContext.Provider
            value={{
                chartsRefreshToken,
                currentDashboard,
                currentDashboardId,
                editedBlock,
                closeEditor,
                openEditor,
                refreshAllCharts,
                setEditedBlock,
                setCurrentDashboardId,
                isEditorOpened,
                isDashboardsFetched,
                isDashboardsLoading,
                isExplorerOpened,
                toggleExplorer,
            }}>
            {children}
        </DashboardPageContext.Provider>
    );
};
