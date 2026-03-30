import React, { useEffect, useRef } from "react";
import {
    GridviewReact,
    type GridviewReadyEvent,
    Orientation,
} from "dockview";
import { ConsoleSidebar } from "../../ConsoleSidebar";
import { ConsoleMainArea } from "./ConsoleMainArea";
import {
    CONSOLE_GRID_EDITOR_PANEL_ID,
    CONSOLE_GRID_SIDEBAR_PANEL_ID,
    CONSOLE_SIDEBAR_MIN_SIZE,
    CONSOLE_SIDEBAR_SIZE,
    ConsoleProvider,
    useConsole,
} from "../model";

const CONSOLE_LAYOUT_STORAGE_KEY = "console:layout";

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
        return false;
    }
};

const ensureMainAreaPanel = (event: GridviewReadyEvent) => {
    if (event.api.getPanel(CONSOLE_GRID_EDITOR_PANEL_ID)) {
        return;
    }

    event.api.addPanel({
        id: CONSOLE_GRID_EDITOR_PANEL_ID,
        component: "editor",
    });
};

const ensureSidebarPanel = (event: GridviewReadyEvent) => {
    if (event.api.getPanel(CONSOLE_GRID_SIDEBAR_PANEL_ID)) {
        return;
    }

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
};

const ConsoleLayout: React.FC = () => {
    const { sidebarCollapsed } = useConsole();
    const gridEventRef = useRef<GridviewReadyEvent | null>(null);
    const [api, setApi] = React.useState<GridviewReadyEvent["api"]>();
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

        if (restored) {
            return;
        }

        if (!sidebarCollapsed) {
            ensureSidebarPanel(event);
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
        <GridviewReact
            components={components}
            onReady={onReady}
            orientation={Orientation.HORIZONTAL}
        />
    );
};

export const Console: React.FC = () => {
    return (
        <ConsoleProvider>
            <ConsoleLayout />
        </ConsoleProvider>
    );
};
