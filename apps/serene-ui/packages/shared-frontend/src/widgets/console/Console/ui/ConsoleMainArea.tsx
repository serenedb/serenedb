import React, { useEffect, useLayoutEffect, useRef } from "react";
import { GridviewReact, type GridviewReadyEvent, Orientation } from "dockview";
import { ConsoleEditor } from "../../ConsoleEditor";
import { useDockviewLayoutSync } from "../../../../shared/hooks";
import { useConsole } from "../model";
import {
    CONSOLE_EDITOR_MIN_WIDTH,
    CONSOLE_GRID_EDITOR_PANEL_ID,
    CONSOLE_GRID_RIGHT_SIDEBAR_PANEL_ID,
    CONSOLE_RIGHT_SIDEBAR_MIN_SIZE,
    CONSOLE_RIGHT_SIDEBAR_SIZE,
} from "../model/consts";
import { ConsoleRightSidebar } from "./ConsoleRightSidebar";

const CONSOLE_MAIN_AREA_LAYOUT_STORAGE_KEY = "console:main-grid-layout";

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
        localStorage.removeItem(storageKey);
        return false;
    }
};

const ensureEditorPanel = (event: GridviewReadyEvent) => {
    if (event.api.getPanel(CONSOLE_GRID_EDITOR_PANEL_ID)) {
        return;
    }

    try {
        event.api.addPanel({
            id: CONSOLE_GRID_EDITOR_PANEL_ID,
            component: "editor",
        });
    } catch (error) {
        console.warn("Failed to add console editor panel:", error);
        localStorage.removeItem(CONSOLE_MAIN_AREA_LAYOUT_STORAGE_KEY);
    }
};

const ensureRightSidebarPanel = (event: GridviewReadyEvent, size: number) => {
    if (event.api.getPanel(CONSOLE_GRID_RIGHT_SIDEBAR_PANEL_ID)) {
        return;
    }

    ensureEditorPanel(event);

    try {
        event.api.addPanel({
            id: CONSOLE_GRID_RIGHT_SIDEBAR_PANEL_ID,
            component: "rightSidebar",
            size,
            minimumWidth: CONSOLE_RIGHT_SIDEBAR_MIN_SIZE,
            position: {
                referencePanel: CONSOLE_GRID_EDITOR_PANEL_ID,
                direction: "right",
            },
        });
    } catch (error) {
        console.warn("Failed to add console right sidebar panel:", error);
        localStorage.removeItem(CONSOLE_MAIN_AREA_LAYOUT_STORAGE_KEY);
    }
};

export const ConsoleMainArea: React.FC = () => {
    const {
        sidebarCollapsed,
        settingsSidebarCollapsed,
        executionHistorySidebarCollapsed,
        setSettingsSidebarCollapsed,
        setExecutionHistorySidebarCollapsed,
    } = useConsole();
    const gridEventRef = useRef<GridviewReadyEvent | null>(null);
    const [api, setApi] = React.useState<GridviewReadyEvent["api"]>();
    const containerRef = useDockviewLayoutSync<HTMLDivElement>(api);
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
        const rightPanel = event.api.getPanel(CONSOLE_GRID_RIGHT_SIDEBAR_PANEL_ID);
        if (!isRightSidebarVisible && rightPanel) {
            if (rightPanel.width > 1) {
                rightSidebarWidthRef.current = rightPanel.width;
            }
            event.api.removePanel(rightPanel);
        } else if (isRightSidebarVisible && !rightPanel) {
            ensureRightSidebarPanel(event, rightSidebarWidthRef.current);
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
                    CONSOLE_MAIN_AREA_LAYOUT_STORAGE_KEY,
                    JSON.stringify(api.toJSON()),
                );
            } catch (error) {
                console.warn("Failed to save console main area layout:", error);
            }
        });

        return () => disposable.dispose();
    }, [api]);

    useLayoutEffect(() => {
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
            ensureRightSidebarPanel(event, rightSidebarWidthRef.current);
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

    useEffect(() => {
        if (!api || !sidebarCollapsed || !isRightSidebarVisible) {
            return;
        }

        const checkAvailableWidth = () => {
            const editorPanel = api.getPanel(CONSOLE_GRID_EDITOR_PANEL_ID);
            if (!editorPanel) {
                return;
            }

            if (editorPanel.width < CONSOLE_EDITOR_MIN_WIDTH) {
                setSettingsSidebarCollapsed(true);
                setExecutionHistorySidebarCollapsed(true);
            }
        };

        checkAvailableWidth();
        const disposable = api.onDidLayoutChange(checkAvailableWidth);

        return () => disposable.dispose();
    }, [
        api,
        sidebarCollapsed,
        isRightSidebarVisible,
        setSettingsSidebarCollapsed,
        setExecutionHistorySidebarCollapsed,
    ]);

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
