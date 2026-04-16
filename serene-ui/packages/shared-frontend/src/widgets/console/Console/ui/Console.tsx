import React, { useEffect, useLayoutEffect, useRef } from "react";
import {
    GridviewReact,
    type GridviewReadyEvent,
    Orientation,
} from "dockview";
import { ConsoleSidebar } from "../../ConsoleSidebar";
import { useDockviewLayoutSync } from "../../../../shared/hooks";
import { ConsoleMainArea } from "./ConsoleMainArea";
import {
    CONSOLE_MAIN_AREA_MIN_WIDTH,
    CONSOLE_GRID_EDITOR_PANEL_ID,
    CONSOLE_GRID_SIDEBAR_PANEL_ID,
    CONSOLE_SIDEBAR_MIN_SIZE,
    CONSOLE_SIDEBAR_SIZE,
    ConsoleProvider,
    useConsole,
} from "../model";

const CONSOLE_LAYOUT_STORAGE_KEY = "console:grid-layout";

const restoreLayout = (event: GridviewReadyEvent, storageKey: string) => {
    const rawLayout = localStorage.getItem(storageKey);
    if (!rawLayout) {
        return false;
    }

    try {
        event.api.fromJSON(JSON.parse(rawLayout));
        return true;
    } catch (error) {
        console.warn("Failed to restore console layout:", error);
        localStorage.removeItem(storageKey);
        return false;
    }
};

const ensureMainAreaPanel = (event: GridviewReadyEvent) => {
    if (event.api.getPanel(CONSOLE_GRID_EDITOR_PANEL_ID)) {
        return;
    }

    try {
        event.api.addPanel({
            id: CONSOLE_GRID_EDITOR_PANEL_ID,
            component: "editor",
        });
    } catch (error) {
        console.warn("Failed to add console main area panel:", error);
        localStorage.removeItem(CONSOLE_LAYOUT_STORAGE_KEY);
    }
};

const ensureSidebarPanel = (event: GridviewReadyEvent) => {
    if (event.api.getPanel(CONSOLE_GRID_SIDEBAR_PANEL_ID)) {
        return;
    }

    ensureMainAreaPanel(event);

    try {
        event.api.addPanel({
            id: CONSOLE_GRID_SIDEBAR_PANEL_ID,
            component: "sidebar",
            minimumWidth: CONSOLE_SIDEBAR_MIN_SIZE,
            size: CONSOLE_SIDEBAR_SIZE,
            position: {
                referencePanel: CONSOLE_GRID_EDITOR_PANEL_ID,
                direction: "left",
            },
        });
    } catch (error) {
        console.warn("Failed to add console sidebar panel:", error);
        localStorage.removeItem(CONSOLE_LAYOUT_STORAGE_KEY);
    }
};

const ConsoleLayout: React.FC = () => {
    const { sidebarCollapsed, setSidebarCollapsed } = useConsole();
    const gridEventRef = useRef<GridviewReadyEvent | null>(null);
    const [api, setApi] = React.useState<GridviewReadyEvent["api"]>();
    const containerRef = useDockviewLayoutSync<HTMLDivElement>(api);
    const components = React.useMemo(() => {
        return {
            sidebar: () => {
                return <ConsoleSidebar />;
            },
            editor: () => {
                return <ConsoleMainArea />;
            },
        };
    }, []);

    const onReady = (event: GridviewReadyEvent) => {
        gridEventRef.current = event;
        setApi(event.api);
        const restored =
            restoreLayout(event, CONSOLE_LAYOUT_STORAGE_KEY);

        ensureMainAreaPanel(event);
        const sidebarPanel = event.api.getPanel(CONSOLE_GRID_SIDEBAR_PANEL_ID);
        if (sidebarCollapsed && sidebarPanel) {
            event.api.removePanel(sidebarPanel);
        } else if (!sidebarCollapsed && !sidebarPanel) {
            ensureSidebarPanel(event);
        }

        if (restored) {
            return;
        }
    };

    useEffect(() => {
        if (!api) {
            return;
        }

        const disposable = api.onDidLayoutChange(() => {
            try {
                localStorage.setItem(
                    CONSOLE_LAYOUT_STORAGE_KEY,
                    JSON.stringify(api.toJSON()),
                );
            } catch (error) {
                console.warn("Failed to save console layout:", error);
            }
        });

        return () => disposable.dispose();
    }, [api]);

    useEffect(() => {
        if (!api || sidebarCollapsed) {
            return;
        }

        const checkAvailableWidth = () => {
            const mainAreaPanel = api.getPanel(CONSOLE_GRID_EDITOR_PANEL_ID);
            if (!mainAreaPanel) {
                return;
            }

            if (mainAreaPanel.width < CONSOLE_MAIN_AREA_MIN_WIDTH) {
                setSidebarCollapsed(true);
            }
        };

        checkAvailableWidth();
        const disposable = api.onDidLayoutChange(checkAvailableWidth);

        return () => disposable.dispose();
    }, [api, sidebarCollapsed, setSidebarCollapsed]);

    useLayoutEffect(() => {
        const event = gridEventRef.current;
        if (!event) {
            return;
        }

        const sidebarPanel = event.api.getPanel(CONSOLE_GRID_SIDEBAR_PANEL_ID);

        if (sidebarCollapsed) {
            if (sidebarPanel) {
                event.api.removePanel(sidebarPanel);
            }
            return;
        }

        ensureSidebarPanel(event);
    }, [sidebarCollapsed]);

    return (
        <div
            ref={containerRef}
            className="h-full w-full min-h-0 min-w-0 overflow-hidden">
            <GridviewReact
                components={components}
                onReady={onReady}
                orientation={Orientation.HORIZONTAL}
            />
        </div>
    );
};

export const Console: React.FC = () => {
    return (
        <ConsoleProvider>
            <ConsoleLayout />
        </ConsoleProvider>
    );
};
