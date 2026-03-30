import React, { useEffect, useRef } from "react";
import { GridviewReact, type GridviewReadyEvent, Orientation } from "dockview";
import { ConsoleEditor } from "../../ConsoleEditor";
import { useConsole } from "../model";
import {
    CONSOLE_GRID_EDITOR_PANEL_ID,
    CONSOLE_GRID_RIGHT_SIDEBAR_PANEL_ID,
    CONSOLE_RIGHT_SIDEBAR_SIZE,
} from "../model/consts";
import { ConsoleRightSidebar } from "./ConsoleRightSidebar";

const CONSOLE_MAIN_AREA_LAYOUT_STORAGE_KEY = "console:main-area-layout";

const restoreLayout = (event: GridviewReadyEvent, storageKey: string) => {
    const rawLayout = localStorage.getItem(storageKey);
    if (!rawLayout) {
        return false;
    }

    try {
        event.api.fromJSON(JSON.parse(rawLayout));
        return true;
    } catch (error) {
        console.warn("Failed to restore console main area layout:", error);
        return false;
    }
};

const ensureEditorPanel = (event: GridviewReadyEvent) => {
    if (event.api.getPanel(CONSOLE_GRID_EDITOR_PANEL_ID)) {
        return;
    }

    event.api.addPanel({
        id: CONSOLE_GRID_EDITOR_PANEL_ID,
        component: "editor",
    });
};

const ensureRightSidebarPanel = (event: GridviewReadyEvent) => {
    if (event.api.getPanel(CONSOLE_GRID_RIGHT_SIDEBAR_PANEL_ID)) {
        return;
    }

    event.api.addPanel({
        id: CONSOLE_GRID_RIGHT_SIDEBAR_PANEL_ID,
        component: "rightSidebar",
        size: CONSOLE_RIGHT_SIDEBAR_SIZE,
        minimumWidth: 300,
        position: {
            referencePanel: CONSOLE_GRID_EDITOR_PANEL_ID,
            direction: "right",
        },
    });
};

export const ConsoleMainArea: React.FC = () => {
    const { settingsSidebarCollapsed, executionHistorySidebarCollapsed } =
        useConsole();
    const gridEventRef = useRef<GridviewReadyEvent | null>(null);
    const [api, setApi] = React.useState<GridviewReadyEvent["api"]>();
    const rightSidebarWidthRef = useRef(CONSOLE_RIGHT_SIDEBAR_SIZE);

    const isRightSidebarVisible =
        !settingsSidebarCollapsed || !executionHistorySidebarCollapsed;
    const components = React.useMemo(() => {
        return {
            editor: () => {
                return <ConsoleEditor />;
            },
            rightSidebar: () => {
                return <ConsoleRightSidebar />;
            },
        };
    }, []);

    const onReady = (event: GridviewReadyEvent) => {
        gridEventRef.current = event;
        setApi(event.api);
        const restored =
            restoreLayout(event, CONSOLE_MAIN_AREA_LAYOUT_STORAGE_KEY);

        ensureEditorPanel(event);

        if (restored) {
            return;
        }

        if (isRightSidebarVisible) {
            ensureRightSidebarPanel(event);
        }
    };

    useEffect(() => {
        if (!api) {
            return;
        }

        const disposable = api.onDidLayoutChange(() => {
            try {
                localStorage.setItem(
                    CONSOLE_MAIN_AREA_LAYOUT_STORAGE_KEY,
                    JSON.stringify(api.toJSON()),
                );
            } catch (error) {
                console.warn("Failed to save console main area layout:", error);
            }
        });

        return () => disposable.dispose();
    }, [api]);

    useEffect(() => {
        const event = gridEventRef.current;
        if (!event) {
            return;
        }

        const rightPanel = event.api.getPanel(
            CONSOLE_GRID_RIGHT_SIDEBAR_PANEL_ID,
        );

        if (!isRightSidebarVisible) {
            if (rightPanel) {
                if (rightPanel.width > 1) {
                    rightSidebarWidthRef.current = rightPanel.width;
                }
                event.api.removePanel(rightPanel);
            }
            return;
        }

        if (!rightPanel) {
            ensureRightSidebarPanel(event);
        }
    }, [isRightSidebarVisible]);

    useEffect(() => {
        const event = gridEventRef.current;
        if (!event || !isRightSidebarVisible) {
            return;
        }

        const rightPanel = event.api.getPanel(
            CONSOLE_GRID_RIGHT_SIDEBAR_PANEL_ID,
        );
        if (rightPanel && rightPanel.width > 1) {
            rightSidebarWidthRef.current = rightPanel.width;
        }
    }, [
        settingsSidebarCollapsed,
        executionHistorySidebarCollapsed,
        isRightSidebarVisible,
    ]);

    return (
        <GridviewReact
            components={components}
            onReady={onReady}
            orientation={Orientation.HORIZONTAL}
        />
    );
};
