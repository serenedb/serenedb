import { useCallback, useEffect, useMemo, useState } from "react";
import type { ReactNode } from "react";
import { ConsoleContext } from "./ConsoleContext";
import {
    CONSOLE_LIMIT_STORAGE_KEY,
    CONSOLE_SIDEBAR_COLLAPSED_STORAGE_KEY,
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

export const ConsoleProvider = ({
    children,
}: {
    children: ReactNode;
}) => {
    const [limit, setLimitState] = useState(readStoredLimit);
    const [sidebarCollapsed, setSidebarCollapsedState] = useState(
        readStoredSidebarCollapsed,
    );

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

    const value = useMemo(
        () => ({
            limit,
            setLimit,
            sidebarCollapsed,
            setSidebarCollapsed,
            toggleSidebar,
        }),
        [limit, setLimit, sidebarCollapsed, setSidebarCollapsed, toggleSidebar],
    );

    return (
        <ConsoleContext.Provider value={value}>{children}</ConsoleContext.Provider>
    );
};
