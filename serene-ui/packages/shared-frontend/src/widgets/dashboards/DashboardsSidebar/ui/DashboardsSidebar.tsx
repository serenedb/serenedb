import React from "react";
import {
    DockviewIDisposable,
    PaneviewReact,
    PaneviewReadyEvent,
    type SerializedPaneview,
} from "dockview";
import { useDockviewLayoutSync } from "../../../../shared/hooks";
import { DashboardsSidebarContext } from "./DashboardsSidebarContext";
import { DashboardsSidebarHeader } from "./DashboardsSidebarHeader";
import { DashboardsSidebarTopbar } from "./DashboardsSidebarTopbar";
import { DashboardsSidebarFavorites } from "./DashboardsSidebarFavorites";
import { DashboardsSidebarDashboards } from "./DashboardsSidebarDashboards";
import { DashboardsSidebarSavedQueries } from "./DashboardsSidebarSavedQueries";
import { DashboardsSidebarFavoriteSavedQueriesProvider } from "../model";

interface DashboardsSidebarProps {
    onCurrentDashboardChange: (dashboardId: number) => void;
}

const DASHBOARDS_SIDEBAR_LAYOUT_STORAGE_KEY = "dashboards:sidebar-layout";
const DASHBOARDS_SIDEBAR_FAVORITES_PANEL_ID = "panel_1";
const DASHBOARDS_SIDEBAR_DASHBOARDS_PANEL_ID = "panel_2";
const DASHBOARDS_SIDEBAR_SAVED_QUERIES_PANEL_ID = "panel_3";

const components = {
    favorites: () => <DashboardsSidebarFavorites />,
    dashboards: () => <DashboardsSidebarDashboards />,
    savedQueries: () => <DashboardsSidebarSavedQueries />,
};

const headerComponents = {
    default: DashboardsSidebarHeader,
};

const sanitizeLayout = (value: unknown): unknown => {
    if (Array.isArray(value)) {
        return value.map((entry) => sanitizeLayout(entry));
    }

    if (!value || typeof value !== "object") {
        return value;
    }

    const record = value as Record<string, unknown>;
    const sanitized: Record<string, unknown> = {};

    Object.entries(record).forEach(([key, entry]) => {
        if (key === "icon") {
            return;
        }

        sanitized[key] = sanitizeLayout(entry);
    });

    return sanitized;
};

const restoreLayout = (
    event: PaneviewReadyEvent,
    storageKey: string,
): boolean => {
    const rawLayout = localStorage.getItem(storageKey);
    if (!rawLayout) {
        return false;
    }

    try {
        const sanitizedLayout = sanitizeLayout(JSON.parse(rawLayout));
        event.api.fromJSON(sanitizedLayout as SerializedPaneview);
        return true;
    } catch (error) {
        console.warn("Failed to restore dashboards sidebar layout:", error);
        return false;
    }
};

const ensureFavoritesPanel = (event: PaneviewReadyEvent) => {
    if (
        event.api.panels.some(
            (panel) => panel.id === DASHBOARDS_SIDEBAR_FAVORITES_PANEL_ID,
        )
    ) {
        return;
    }

    event.api.addPanel({
        id: DASHBOARDS_SIDEBAR_FAVORITES_PANEL_ID,
        component: "favorites",
        headerComponent: "default",
        params: {
            title: "Favorites",
            kind: "favorites",
        },
        title: "Favorites",
        headerSize: 36,
    });
};

const ensureDashboardsPanel = (event: PaneviewReadyEvent) => {
    if (
        event.api.panels.some(
            (panel) => panel.id === DASHBOARDS_SIDEBAR_DASHBOARDS_PANEL_ID,
        )
    ) {
        return;
    }

    event.api.addPanel({
        id: DASHBOARDS_SIDEBAR_DASHBOARDS_PANEL_ID,
        component: "dashboards",
        headerComponent: "default",
        params: {
            title: "Dashboards",
            kind: "dashboards",
        },
        title: "Dashboards",
        headerSize: 36,
    });
};

const ensureSavedQueriesPanel = (event: PaneviewReadyEvent) => {
    if (
        event.api.panels.some(
            (panel) => panel.id === DASHBOARDS_SIDEBAR_SAVED_QUERIES_PANEL_ID,
        )
    ) {
        return;
    }

    event.api.addPanel({
        id: DASHBOARDS_SIDEBAR_SAVED_QUERIES_PANEL_ID,
        component: "savedQueries",
        headerComponent: "default",
        params: {
            title: "Saved Queries",
            kind: "savedQueries",
        },
        title: "Saved Queries",
        headerSize: 36,
    });
};

export const DashboardsSidebar: React.FC<DashboardsSidebarProps> = ({
    onCurrentDashboardChange,
}) => {
    const [api, setApi] = React.useState<PaneviewReadyEvent["api"]>();
    const containerRef = useDockviewLayoutSync<HTMLDivElement>(api);

    const contextValue = React.useMemo(
        () => ({ onCurrentDashboardChange }),
        [onCurrentDashboardChange],
    );

    const equalizeExpandedPanels = React.useCallback(
        (event: PaneviewReadyEvent) => {
            const expandedPanels = event.api.panels.filter(
                (panel) => panel.api.isExpanded,
            );

            if (expandedPanels.length <= 1) {
                return;
            }

            const collapsedPanelsHeight = event.api.panels
                .filter((panel) => !panel.api.isExpanded)
                .reduce((sum, panel) => sum + panel.height, 0);

            const availableExpandedHeight = Math.max(
                0,
                event.api.height - collapsedPanelsHeight,
            );
            const targetPanelHeight =
                availableExpandedHeight / expandedPanels.length;

            expandedPanels.forEach((panel) => {
                panel.api.setSize({
                    size: Math.max(targetPanelHeight, panel.minimumSize),
                });
            });
        },
        [],
    );

    React.useEffect(() => {
        if (!api) {
            return;
        }

        const disposable = api.onDidLayoutChange(() => {
            try {
                localStorage.setItem(
                    DASHBOARDS_SIDEBAR_LAYOUT_STORAGE_KEY,
                    JSON.stringify(sanitizeLayout(api.toJSON())),
                );
            } catch (error) {
                console.warn(
                    "Failed to save dashboards sidebar layout:",
                    error,
                );
            }
        });

        return () => disposable.dispose();
    }, [api]);

    const onReady = (event: PaneviewReadyEvent) => {
        setApi(event.api);
        const restored = restoreLayout(
            event,
            DASHBOARDS_SIDEBAR_LAYOUT_STORAGE_KEY,
        );

        ensureFavoritesPanel(event);
        ensureDashboardsPanel(event);
        ensureSavedQueriesPanel(event);

        const expansionListeners = new Map<string, DockviewIDisposable>();
        const bindPanelExpansionListener = (
            panel: (typeof event.api.panels)[number],
        ) => {
            expansionListeners.get(panel.id)?.dispose();

            expansionListeners.set(
                panel.id,
                panel.api.onDidExpansionChange((expansionEvent) => {
                    if (!expansionEvent.isExpanded) {
                        return;
                    }

                    equalizeExpandedPanels(event);
                }),
            );
        };

        event.api.panels.forEach(bindPanelExpansionListener);
        event.api.onDidAddView(bindPanelExpansionListener);

        if (!restored) {
            requestAnimationFrame(() => {
                event.api.panels.forEach((panel) => {
                    if (panel.id !== DASHBOARDS_SIDEBAR_DASHBOARDS_PANEL_ID) {
                        panel.api.setExpanded(false);
                    }
                });
            });
        }

        event.api.onDidRemoveView((panel) => {
            expansionListeners.get(panel.id)?.dispose();
            expansionListeners.delete(panel.id);
        });
    };

    return (
        <DashboardsSidebarFavoriteSavedQueriesProvider>
            <DashboardsSidebarContext.Provider value={contextValue}>
                <div
                    ref={containerRef}
                    className="flex flex-col h-full w-full"
                    data-dashboards-sidebar-root="true">
                    <DashboardsSidebarTopbar />
                    <PaneviewReact
                        onReady={onReady}
                        components={components}
                        headerComponents={headerComponents}
                    />
                </div>
            </DashboardsSidebarContext.Provider>
        </DashboardsSidebarFavoriteSavedQueriesProvider>
    );
};
