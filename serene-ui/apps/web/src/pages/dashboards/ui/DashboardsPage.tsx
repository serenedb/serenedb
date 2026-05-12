import React, { useEffect, useLayoutEffect, useRef } from "react";
import { GridviewReact, type GridviewReadyEvent, Orientation } from "dockview";
import {
    DEFAULT_HOTKEYS,
    DashboardCardEditor,
    DashboardGrid,
    DashboardsSidebar,
    DashboardsTopbar,
    useAppHotkey,
    useDockviewLayoutSync,
    useSidebarFocusController,
} from "@serene-ui/shared-frontend";
import { useDashboardPage } from "../model";

const DASHBOARDS_GRID_LAYOUT_STORAGE_KEY = "dashboards:grid-layout";
const DASHBOARDS_GRID_SIDEBAR_PANEL_ID = "dashboards-sidebar";
const DASHBOARDS_GRID_MAIN_PANEL_ID = "dashboards-main";
const DASHBOARDS_GRID_EDITOR_PANEL_ID = "dashboards-editor";
const DASHBOARDS_SIDEBAR_ROOT_SELECTOR =
    "[data-dashboards-sidebar-root='true']";
const DASHBOARDS_MAIN_ROOT_SELECTOR = "[data-dashboards-main-root='true']";
const DASHBOARDS_EDITOR_ROOT_SELECTOR = "[data-dashboards-editor-root='true']";
const DASHBOARDS_SIDEBAR_SECTION_IDS = [
    "favorites",
    "dashboards",
    "savedQueries",
] as const;

const DASHBOARDS_SIDEBAR_DEFAULT_SIZE = 280;
const DASHBOARDS_SIDEBAR_MIN_SIZE = 200;
const DASHBOARDS_EDITOR_DEFAULT_SIZE = 420;
const DASHBOARDS_EDITOR_MIN_SIZE = 320;

const PANEL_RESIZE_DEBOUNCE_MS = 150;

const DashboardsLayoutContext = React.createContext<{ isResizing: boolean }>({
    isResizing: false,
});

const SidebarPanel: React.FC = () => {
    const { setCurrentDashboardId } = useDashboardPage();
    return (
        <DashboardsSidebar onCurrentDashboardChange={setCurrentDashboardId} />
    );
};

const MainPanel: React.FC = () => {
    const {
        chartsRefreshToken,
        currentDashboard,
        editedBlock,
        closeEditor,
        openEditor,
        refreshAllCharts,
        setCurrentDashboardId,
        isExplorerOpened,
        toggleExplorer,
    } = useDashboardPage();
    const { isResizing } = React.useContext(DashboardsLayoutContext);

    return (
        <div
            className="flex min-h-0 min-w-0 flex-1 flex-col overflow-hidden h-full"
            data-dashboards-main-root="true">
            <DashboardsTopbar
                currentDashboard={currentDashboard}
                isExplorerOpened={isExplorerOpened}
                onToggleExplorer={toggleExplorer}
                onRefreshAllCharts={refreshAllCharts}
            />
            <DashboardGrid
                currentDashboard={currentDashboard}
                editedBlock={editedBlock}
                isPanelResizing={isResizing}
                manualRefreshToken={chartsRefreshToken}
                onCreateDashboard={setCurrentDashboardId}
                onCloseEditor={closeEditor}
                onEditBlock={openEditor}
            />
        </div>
    );
};

const EditorPanel: React.FC = () => {
    const { editedBlock, closeEditor, setEditedBlock } = useDashboardPage();
    return (
        <div
            className="h-full w-full min-h-0 min-w-0"
            data-dashboards-editor-root="true">
            <DashboardCardEditor
                editedBlock={editedBlock}
                onClose={closeEditor}
                onEditedBlockChange={setEditedBlock}
            />
        </div>
    );
};

const components = {
    sidebar: SidebarPanel,
    main: MainPanel,
    editor: EditorPanel,
};

const restoreLayout = (
    event: GridviewReadyEvent,
    storageKey: string,
): boolean => {
    const rawLayout = localStorage.getItem(storageKey);
    if (!rawLayout) {
        return false;
    }

    try {
        event.api.fromJSON(JSON.parse(rawLayout));
        return true;
    } catch (error) {
        console.warn("Failed to restore dashboards layout:", error);
        return false;
    }
};

const ensureMainPanel = (event: GridviewReadyEvent) => {
    if (event.api.getPanel(DASHBOARDS_GRID_MAIN_PANEL_ID)) {
        return;
    }

    event.api.addPanel({
        id: DASHBOARDS_GRID_MAIN_PANEL_ID,
        component: "main",
        minimumWidth: 300,
    });
};

const ensureSidebarPanel = (event: GridviewReadyEvent, size: number) => {
    if (event.api.getPanel(DASHBOARDS_GRID_SIDEBAR_PANEL_ID)) {
        return;
    }

    ensureMainPanel(event);

    try {
        event.api.addPanel({
            id: DASHBOARDS_GRID_SIDEBAR_PANEL_ID,
            component: "sidebar",
            minimumWidth: DASHBOARDS_SIDEBAR_MIN_SIZE,
            size,
            position: {
                referencePanel: DASHBOARDS_GRID_MAIN_PANEL_ID,
                direction: "left",
            },
        });
    } catch (error) {
        console.warn("Failed to add dashboards sidebar panel:", error);
    }
};

const ensureEditorPanel = (event: GridviewReadyEvent, size: number) => {
    if (event.api.getPanel(DASHBOARDS_GRID_EDITOR_PANEL_ID)) {
        return;
    }

    ensureMainPanel(event);

    try {
        event.api.addPanel({
            id: DASHBOARDS_GRID_EDITOR_PANEL_ID,
            component: "editor",
            minimumWidth: DASHBOARDS_EDITOR_MIN_SIZE,
            size,
            position: {
                referencePanel: DASHBOARDS_GRID_MAIN_PANEL_ID,
                direction: "right",
            },
        });
    } catch (error) {
        console.warn("Failed to add dashboards editor panel:", error);
    }
};

export const DashboardsPage = () => {
    const gridEventRef = useRef<GridviewReadyEvent | null>(null);
    const [api, setApi] = React.useState<GridviewReadyEvent["api"]>();
    const containerRef = useDockviewLayoutSync<HTMLDivElement>(api);
    const sidebarWidthRef = useRef(DASHBOARDS_SIDEBAR_DEFAULT_SIZE);
    const editorWidthRef = useRef(DASHBOARDS_EDITOR_DEFAULT_SIZE);
    const [isResizing, setIsResizing] = React.useState(false);
    const resizeDebounceRef = useRef<number | null>(null);

    const { isEditorOpened, isExplorerOpened, toggleExplorer } =
        useDashboardPage();
    const {
        focusLastEditor,
        focusNextSidebarSection,
        focusPreviousSidebarSection,
        focusSidebar,
        isSidebarFocused,
        restoreSidebarFocusAfterRender,
    } = useSidebarFocusController({
        sidebarRootSelector: DASHBOARDS_SIDEBAR_ROOT_SELECTOR,
        editorRootSelectors: [
            DASHBOARDS_EDITOR_ROOT_SELECTOR,
            DASHBOARDS_MAIN_ROOT_SELECTOR,
        ],
        sectionOrder: [...DASHBOARDS_SIDEBAR_SECTION_IDS],
    });

    const layoutContextValue = React.useMemo(
        () => ({ isResizing }),
        [isResizing],
    );

    const onReady = (event: GridviewReadyEvent) => {
        gridEventRef.current = event;
        setApi(event.api);
        const restored = restoreLayout(
            event,
            DASHBOARDS_GRID_LAYOUT_STORAGE_KEY,
        );

        ensureMainPanel(event);

        if (restored) {
            // Sync editor with current state.
            const editorPanel = event.api.getPanel(
                DASHBOARDS_GRID_EDITOR_PANEL_ID,
            );
            if (editorPanel) {
                if (editorPanel.width > 1) {
                    editorWidthRef.current = editorPanel.width;
                }

                if (!isEditorOpened) {
                    event.api.removePanel(editorPanel);
                }
            } else if (isEditorOpened) {
                ensureEditorPanel(event, editorWidthRef.current);
            }

            // Sync sidebar with current state.
            const sidebarPanel = event.api.getPanel(
                DASHBOARDS_GRID_SIDEBAR_PANEL_ID,
            );
            if (!isExplorerOpened && sidebarPanel) {
                if (sidebarPanel.width > 1) {
                    sidebarWidthRef.current = sidebarPanel.width;
                }
                event.api.removePanel(sidebarPanel);
            } else if (isExplorerOpened && !sidebarPanel) {
                ensureSidebarPanel(event, sidebarWidthRef.current);
            }
            return;
        }

        if (isExplorerOpened) {
            ensureSidebarPanel(event, sidebarWidthRef.current);
        }
    };

    useEffect(() => {
        if (!api) {
            return;
        }

        const disposable = api.onDidLayoutChange(() => {
            try {
                localStorage.setItem(
                    DASHBOARDS_GRID_LAYOUT_STORAGE_KEY,
                    JSON.stringify(api.toJSON()),
                );
            } catch (error) {
                console.warn("Failed to save dashboards layout:", error);
            }

            setIsResizing(true);
            if (resizeDebounceRef.current !== null) {
                window.clearTimeout(resizeDebounceRef.current);
            }
            resizeDebounceRef.current = window.setTimeout(() => {
                setIsResizing(false);
                resizeDebounceRef.current = null;
            }, PANEL_RESIZE_DEBOUNCE_MS);
        });

        return () => disposable.dispose();
    }, [api]);

    useEffect(() => {
        return () => {
            if (resizeDebounceRef.current !== null) {
                window.clearTimeout(resizeDebounceRef.current);
            }
        };
    }, []);

    const handleToggleSidebarFocus = React.useCallback(() => {
        if (isSidebarFocused()) {
            toggleExplorer();

            if (typeof window === "undefined") {
                focusLastEditor();
                return;
            }

            window.requestAnimationFrame(() => {
                focusLastEditor();
            });
            return;
        }

        if (!isExplorerOpened) {
            toggleExplorer();
            restoreSidebarFocusAfterRender();
            return;
        }

        if (!focusSidebar()) {
            restoreSidebarFocusAfterRender();
        }
    }, [
        focusLastEditor,
        focusSidebar,
        isExplorerOpened,
        isSidebarFocused,
        restoreSidebarFocusAfterRender,
        toggleExplorer,
    ]);

    const handleFocusDown = React.useCallback(() => {
        if (isSidebarFocused()) {
            focusNextSidebarSection();
        }
    }, [focusNextSidebarSection, isSidebarFocused]);

    const handleFocusUp = React.useCallback(() => {
        if (isSidebarFocused()) {
            focusPreviousSidebarSection();
        }
    }, [focusPreviousSidebarSection, isSidebarFocused]);

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

        const sidebarPanel = event.api.getPanel(
            DASHBOARDS_GRID_SIDEBAR_PANEL_ID,
        );

        if (!isExplorerOpened) {
            if (sidebarPanel) {
                if (sidebarPanel.width > 1) {
                    sidebarWidthRef.current = sidebarPanel.width;
                }
                event.api.removePanel(sidebarPanel);
            }
            return;
        }

        ensureSidebarPanel(event, sidebarWidthRef.current);
    }, [isExplorerOpened]);

    useLayoutEffect(() => {
        const event = gridEventRef.current;
        if (!event) {
            return;
        }

        const editorPanel = event.api.getPanel(DASHBOARDS_GRID_EDITOR_PANEL_ID);

        if (!isEditorOpened) {
            if (editorPanel) {
                if (editorPanel.width > 1) {
                    editorWidthRef.current = editorPanel.width;
                }
                event.api.removePanel(editorPanel);
            }
            return;
        }

        ensureEditorPanel(event, editorWidthRef.current);
    }, [isEditorOpened]);

    return (
        <DashboardsLayoutContext.Provider value={layoutContextValue}>
            <div
                ref={containerRef}
                className="h-full w-full min-h-0 min-w-0 overflow-hidden">
                <GridviewReact
                    components={components}
                    onReady={onReady}
                    orientation={Orientation.HORIZONTAL}
                />
            </div>
        </DashboardsLayoutContext.Provider>
    );
};
