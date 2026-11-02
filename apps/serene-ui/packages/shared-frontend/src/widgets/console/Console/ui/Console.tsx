import React, { useEffect, useLayoutEffect, useRef } from "react";
import {
    GridviewReact,
    type GridviewReadyEvent,
    Orientation,
} from "dockview";
import { ConsoleSidebar } from "../../ConsoleSidebar";
import {
    useDockviewLayoutSync,
    useSidebarFocusController,
} from "../../../../shared/hooks";
import {
    DEFAULT_HOTKEYS,
    useAppHotkey,
} from "../../../../shared/hotkeys";
import { focusAdjacentConsoleEditorGroup } from "../../ConsoleEditor/model";
import { ConsoleMainArea } from "./ConsoleMainArea";
import {
    CONSOLE_EDITOR_ROOT_SELECTOR,
    CONSOLE_GRID_EDITOR_PANEL_ID,
    CONSOLE_GRID_SIDEBAR_PANEL_ID,
    CONSOLE_SIDEBAR_ROOT_SELECTOR,
    CONSOLE_SIDEBAR_SECTION_IDS,
    CONSOLE_SIDEBAR_MIN_SIZE,
    CONSOLE_SIDEBAR_SIZE,
    ConsoleProvider,
    useConsole,
} from "../model";

const CONSOLE_LAYOUT_STORAGE_KEY = "console:grid-layout";
const CONSOLE_SIDEBAR_MAX_WIDTH_RATIO = 0.5;

const getConsoleSidebarMaxWidth = (api: GridviewReadyEvent["api"]) => {
    return Math.max(
        CONSOLE_SIDEBAR_MIN_SIZE,
        Math.floor(api.width * CONSOLE_SIDEBAR_MAX_WIDTH_RATIO),
    );
};

const setSidebarWidthConstraints = (api: GridviewReadyEvent["api"]) => {
    const sidebarPanel = api.getPanel(CONSOLE_GRID_SIDEBAR_PANEL_ID);
    if (!sidebarPanel) {
        return;
    }

    sidebarPanel.api.setConstraints({
        maximumWidth: () => getConsoleSidebarMaxWidth(api),
    });
};

const clampSidebarWidth = (api: GridviewReadyEvent["api"]) => {
    const sidebarPanel = api.getPanel(CONSOLE_GRID_SIDEBAR_PANEL_ID);
    if (!sidebarPanel) {
        return;
    }

    const maximumWidth = getConsoleSidebarMaxWidth(api);

    if (sidebarPanel.width > maximumWidth) {
        sidebarPanel.api.setSize({
            width: maximumWidth,
        });
    }
};

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
    const existingPanel = event.api.getPanel(CONSOLE_GRID_SIDEBAR_PANEL_ID);
    if (existingPanel) {
        setSidebarWidthConstraints(event.api);
        clampSidebarWidth(event.api);
        return;
    }

    ensureMainAreaPanel(event);

    try {
        event.api.addPanel({
            id: CONSOLE_GRID_SIDEBAR_PANEL_ID,
            component: "sidebar",
            minimumWidth: CONSOLE_SIDEBAR_MIN_SIZE,
            maximumWidth: getConsoleSidebarMaxWidth(event.api),
            size: Math.min(
                CONSOLE_SIDEBAR_SIZE,
                getConsoleSidebarMaxWidth(event.api),
            ),
            position: {
                referencePanel: CONSOLE_GRID_EDITOR_PANEL_ID,
                direction: "left",
            },
        });
        setSidebarWidthConstraints(event.api);
        clampSidebarWidth(event.api);
    } catch (error) {
        console.warn("Failed to add console sidebar panel:", error);
        localStorage.removeItem(CONSOLE_LAYOUT_STORAGE_KEY);
    }
};

const ConsoleLayout: React.FC = () => {
    const { consoleEditorApi, sidebarCollapsed, setSidebarCollapsed } =
        useConsole();
    const gridEventRef = useRef<GridviewReadyEvent | null>(null);
    const [api, setApi] = React.useState<GridviewReadyEvent["api"]>();
    const containerRef = useDockviewLayoutSync<HTMLDivElement>(api);
    const {
        focusLastEditor,
        focusNextSidebarSection,
        focusPreviousSidebarSection,
        focusSidebar,
        isSidebarFocused,
        restoreSidebarFocusAfterRender,
    } = useSidebarFocusController({
        sidebarRootSelector: CONSOLE_SIDEBAR_ROOT_SELECTOR,
        editorRootSelectors: [CONSOLE_EDITOR_ROOT_SELECTOR],
        sectionOrder: [...CONSOLE_SIDEBAR_SECTION_IDS],
    });
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
        } else if (sidebarPanel) {
            setSidebarWidthConstraints(event.api);
            clampSidebarWidth(event.api);
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
            clampSidebarWidth(api);

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

    const handleToggleSidebarFocus = React.useCallback(() => {
        if (isSidebarFocused()) {
            setSidebarCollapsed(true);

            if (typeof window === "undefined") {
                focusLastEditor();
                return;
            }

            window.requestAnimationFrame(() => {
                focusLastEditor();
            });
            return;
        }

        if (sidebarCollapsed) {
            setSidebarCollapsed(false);
            restoreSidebarFocusAfterRender();
            return;
        }

        if (!focusSidebar()) {
            restoreSidebarFocusAfterRender();
        }
    }, [
        focusLastEditor,
        focusSidebar,
        isSidebarFocused,
        restoreSidebarFocusAfterRender,
        setSidebarCollapsed,
        sidebarCollapsed,
    ]);

    const handleFocusDown = React.useCallback(() => {
        if (isSidebarFocused()) {
            focusNextSidebarSection();
            return;
        }

        if (consoleEditorApi) {
            focusAdjacentConsoleEditorGroup(consoleEditorApi, "down");
        }
    }, [consoleEditorApi, focusNextSidebarSection, isSidebarFocused]);

    const handleFocusUp = React.useCallback(() => {
        if (isSidebarFocused()) {
            focusPreviousSidebarSection();
            return;
        }

        if (consoleEditorApi) {
            focusAdjacentConsoleEditorGroup(consoleEditorApi, "up");
        }
    }, [consoleEditorApi, focusPreviousSidebarSection, isSidebarFocused]);

    useAppHotkey(
        DEFAULT_HOTKEYS.CONSOLE_TOGGLE_EXPLORER_EDITOR,
        handleToggleSidebarFocus,
        [handleToggleSidebarFocus],
    );
    useAppHotkey(
        DEFAULT_HOTKEYS.CONSOLE_FOCUS_WINDOW_DOWN,
        handleFocusDown,
        [handleFocusDown],
    );
    useAppHotkey(
        DEFAULT_HOTKEYS.CONSOLE_FOCUS_WINDOW_UP,
        handleFocusUp,
        [handleFocusUp],
    );

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
