import { useCallback, useEffect, useMemo, useState } from "react";
import type { ReactNode } from "react";
import { ConsoleContext } from "./ConsoleContext";
import {
    CONSOLE_LIMIT_STORAGE_KEY,
    CONSOLE_SIDEBAR_COLLAPSED_STORAGE_KEY,
    CONSOLE_SETTINGS_SIDEBAR_COLLAPSED_STORAGE_KEY,
    CONSOLE_EXECUTION_HISTORY_SIDEBAR_COLLAPSED_STORAGE_KEY,
    DEFAULT_CONSOLE_QUERY_LIMIT,
} from "./consts";

const readStoredLimit = () => {
    if (typeof window === "undefined") {
        return DEFAULT_CONSOLE_QUERY_LIMIT;
    }

    const storedLimit = Number(
        window.localStorage.getItem(CONSOLE_LIMIT_STORAGE_KEY),
    );

    return Number.isFinite(storedLimit) && storedLimit > 0
        ? storedLimit
        : DEFAULT_CONSOLE_QUERY_LIMIT;
};

const readStoredSidebarCollapsed = () => {
    if (typeof window === "undefined") {
        return false;
    }

    return (
        window.localStorage.getItem(CONSOLE_SIDEBAR_COLLAPSED_STORAGE_KEY) ===
        "true"
    );
};

const readStoredSettingsSidebarCollapsed = () => {
    if (typeof window === "undefined") {
        return true;
    }

    return (
        window.localStorage.getItem(
            CONSOLE_SETTINGS_SIDEBAR_COLLAPSED_STORAGE_KEY,
        ) !== "false"
    );
};

const readStoredExecutionHistorySidebarCollapsed = () => {
    if (typeof window === "undefined") {
        return true;
    }

    return (
        window.localStorage.getItem(
            CONSOLE_EXECUTION_HISTORY_SIDEBAR_COLLAPSED_STORAGE_KEY,
        ) !== "false"
    );
};

export const ConsoleProvider = ({
    children,
}: {
    children: ReactNode;
}) => {
    const [limit, setLimitState] = useState(readStoredLimit);
    const [sidebarCollapsed, setSidebarCollapsedState] = useState(
        readStoredSidebarCollapsed,
    );
    const [settingsSidebarCollapsed, setSettingsSidebarCollapsedState] =
        useState(readStoredSettingsSidebarCollapsed);
    const [
        executionHistorySidebarCollapsed,
        setExecutionHistorySidebarCollapsedState,
    ] = useState(readStoredExecutionHistorySidebarCollapsed);

    const setLimit = useCallback((nextLimit: number) => {
        setLimitState(
            Number.isFinite(nextLimit) && nextLimit > 0
                ? nextLimit
                : DEFAULT_CONSOLE_QUERY_LIMIT,
        );
    }, []);

    const setSidebarCollapsed = useCallback((collapsed: boolean) => {
        setSidebarCollapsedState(collapsed);
    }, []);

    const toggleSidebar = useCallback(() => {
        setSidebarCollapsedState((current) => !current);
    }, []);

    const setSettingsSidebarCollapsed = useCallback((collapsed: boolean) => {
        setSettingsSidebarCollapsedState(collapsed);
    }, []);

    const toggleSettingsSidebar = useCallback(() => {
        setSettingsSidebarCollapsedState((current) => {
            const nextCollapsed = !current;

            if (!nextCollapsed) {
                setExecutionHistorySidebarCollapsedState(true);
            }

            return nextCollapsed;
        });
    }, []);

    const setExecutionHistorySidebarCollapsed = useCallback(
        (collapsed: boolean) => {
            setExecutionHistorySidebarCollapsedState(collapsed);
        },
        [],
    );

    const toggleExecutionHistorySidebar = useCallback(() => {
        setExecutionHistorySidebarCollapsedState((current) => {
            const nextCollapsed = !current;

            if (!nextCollapsed) {
                setSettingsSidebarCollapsedState(true);
            }

            return nextCollapsed;
        });
    }, []);

    useEffect(() => {
        if (typeof window === "undefined") {
            return;
        }

        window.localStorage.setItem(
            CONSOLE_LIMIT_STORAGE_KEY,
            JSON.stringify(limit),
        );
    }, [limit]);

    useEffect(() => {
        if (typeof window === "undefined") {
            return;
        }

        window.localStorage.setItem(
            CONSOLE_SIDEBAR_COLLAPSED_STORAGE_KEY,
            String(sidebarCollapsed),
        );
    }, [sidebarCollapsed]);

    useEffect(() => {
        if (typeof window === "undefined") {
            return;
        }

        window.localStorage.setItem(
            CONSOLE_SETTINGS_SIDEBAR_COLLAPSED_STORAGE_KEY,
            String(settingsSidebarCollapsed),
        );
    }, [settingsSidebarCollapsed]);

    useEffect(() => {
        if (typeof window === "undefined") {
            return;
        }

        window.localStorage.setItem(
            CONSOLE_EXECUTION_HISTORY_SIDEBAR_COLLAPSED_STORAGE_KEY,
            String(executionHistorySidebarCollapsed),
        );
    }, [executionHistorySidebarCollapsed]);

    const value = useMemo(
        () => ({
            limit,
            setLimit,
            sidebarCollapsed,
            setSidebarCollapsed,
            toggleSidebar,
            settingsSidebarCollapsed,
            setSettingsSidebarCollapsed,
            toggleSettingsSidebar,
            executionHistorySidebarCollapsed,
            setExecutionHistorySidebarCollapsed,
            toggleExecutionHistorySidebar,
        }),
        [
            limit,
            setLimit,
            sidebarCollapsed,
            setSidebarCollapsed,
            toggleSidebar,
            settingsSidebarCollapsed,
            setSettingsSidebarCollapsed,
            toggleSettingsSidebar,
            executionHistorySidebarCollapsed,
            setExecutionHistorySidebarCollapsed,
            toggleExecutionHistorySidebar,
        ],
    );

    return (
        <ConsoleContext.Provider value={value}>{children}</ConsoleContext.Provider>
    );
};
