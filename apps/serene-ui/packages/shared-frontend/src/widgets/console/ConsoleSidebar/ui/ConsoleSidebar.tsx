import React from "react";
import {
    DockviewIDisposable,
    PaneviewReact,
    PaneviewReadyEvent,
    type SerializedPaneview,
} from "dockview";
import { ConsoleSidebarTopbar } from "./ConsoleSidebarTopbar";
import { ConsoleSidebarHeader } from "./ConsoleSidebarHeader";
import { ConsoleSidebarSavedQueries } from "./ConsoleSidebarSavedQueries";
import { ConsoleSidebarPinned } from "./ConsoleSidebarPinned";
import { EntitiesIcon, SavedQueriesIcon } from "@serene-ui/shared-frontend";
import { PinIcon } from "../../../../shared/ui/icons/index";
import { ConsoleExplorer } from "../../ConsoleExplorer";
import { useDockviewLayoutSync } from "../../../../shared/hooks";
import { ConsoleSidebarPinnedProvider } from "../model";

interface ConsoleSidebarProps {}

const CONSOLE_SIDEBAR_LAYOUT_STORAGE_KEY = "console:sidebar-layout";
const CONSOLE_SIDEBAR_PINNED_PANEL_ID = "panel_1";
const CONSOLE_SIDEBAR_ENTITIES_PANEL_ID = "panel_2";
const CONSOLE_SIDEBAR_SAVED_QUERIES_PANEL_ID = "panel_3";

const components = {
    entities: () => {
        return <ConsoleExplorer />;
    },
    pinned: () => {
        return <ConsoleSidebarPinned />;
    },
    savedQueries: () => {
        return <ConsoleSidebarSavedQueries />;
    },
};
const headerComponents = {
    default: ConsoleSidebarHeader,
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

const restoreLayout = (event: PaneviewReadyEvent, storageKey: string) => {
    const rawLayout = localStorage.getItem(storageKey);
    if (!rawLayout) {
        return false;
    }

    try {
        const sanitizedLayout = sanitizeLayout(JSON.parse(rawLayout));
        event.api.fromJSON(sanitizedLayout as SerializedPaneview);
        return true;
    } catch (error) {
        console.warn("Failed to restore console sidebar layout:", error);
        return false;
    }
};

const ensurePinnedPanel = (event: PaneviewReadyEvent) => {
    if (
        event.api.panels.some(
            (panel) => panel.id === CONSOLE_SIDEBAR_PINNED_PANEL_ID,
        )
    ) {
        return;
    }

    event.api.addPanel({
        id: CONSOLE_SIDEBAR_PINNED_PANEL_ID,
        component: "pinned",
        headerComponent: "default",
        params: {
            title: "Pinned",
            icon: <PinIcon className="size-3.5" />,
            kind: "pinned",
        },
        title: "Pinned",
        headerSize: 36,
    });
};

const ensureEntitiesPanel = (event: PaneviewReadyEvent) => {
    if (
        event.api.panels.some(
            (panel) => panel.id === CONSOLE_SIDEBAR_ENTITIES_PANEL_ID,
        )
    ) {
        return;
    }

    event.api.addPanel({
        id: CONSOLE_SIDEBAR_ENTITIES_PANEL_ID,
        component: "entities",
        headerComponent: "default",
        params: {
            title: "Entities",
            icon: <EntitiesIcon className="size-3.5" />,
            kind: "entities",
        },
        title: "Entities",
        headerSize: 36,
    });
};

const ensureSavedQueriesPanel = (event: PaneviewReadyEvent) => {
    if (
        event.api.panels.some(
            (panel) => panel.id === CONSOLE_SIDEBAR_SAVED_QUERIES_PANEL_ID,
        )
    ) {
        return;
    }

    event.api.addPanel({
        id: CONSOLE_SIDEBAR_SAVED_QUERIES_PANEL_ID,
        component: "savedQueries",
        headerComponent: "default",
        params: {
            title: "Saved queries",
            icon: <SavedQueriesIcon className="size-3.5" />,
            kind: "savedQueries",
        },
        title: "Saved queries",
        headerSize: 36,
    });
};

export const ConsoleSidebar: React.FC<ConsoleSidebarProps> = () => {
    const [api, setApi] = React.useState<PaneviewReadyEvent["api"]>();
    const containerRef = useDockviewLayoutSync<HTMLDivElement>(api);

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

    const equalizeEntitiesAndSavedQueries = React.useCallback(
        (event: PaneviewReadyEvent) => {
            const entitiesPanel = event.api.panels.find(
                (panel) => panel.params?.kind === "entities",
            );
            const savedQueriesPanel = event.api.panels.find(
                (panel) => panel.params?.kind === "savedQueries",
            );

            if (!entitiesPanel || !savedQueriesPanel) {
                return;
            }

            if (
                !entitiesPanel.api.isExpanded ||
                !savedQueriesPanel.api.isExpanded
            ) {
                return;
            }

            const targetPanelHeight =
                (entitiesPanel.height + savedQueriesPanel.height) / 2;

            entitiesPanel.api.setSize({
                size: Math.max(targetPanelHeight, entitiesPanel.minimumSize),
            });
            savedQueriesPanel.api.setSize({
                size: Math.max(
                    targetPanelHeight,
                    savedQueriesPanel.minimumSize,
                ),
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
                    CONSOLE_SIDEBAR_LAYOUT_STORAGE_KEY,
                    JSON.stringify(sanitizeLayout(api.toJSON())),
                );
            } catch (error) {
                console.warn("Failed to save console sidebar layout:", error);
            }
        });

        return () => disposable.dispose();
    }, [api]);

    const onReady = (event: PaneviewReadyEvent) => {
        setApi(event.api);
        const restored = restoreLayout(
            event,
            CONSOLE_SIDEBAR_LAYOUT_STORAGE_KEY,
        );

        ensurePinnedPanel(event);
        ensureEntitiesPanel(event);
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
                    equalizeEntitiesAndSavedQueries(event);
                }),
            );
        };

        event.api.panels.forEach(bindPanelExpansionListener);
        event.api.onDidAddView(bindPanelExpansionListener);

        if (!restored) {
            requestAnimationFrame(() => {
                equalizeEntitiesAndSavedQueries(event);
            });
        }

        event.api.onDidRemoveView((panel) => {
            expansionListeners.get(panel.id)?.dispose();
            expansionListeners.delete(panel.id);
        });
    };

    return (
        <ConsoleSidebarPinnedProvider>
            <div
                ref={containerRef}
                className="flex flex-col h-full w-full"
                data-console-sidebar-root="true">
                <ConsoleSidebarTopbar />
                <PaneviewReact
                    onReady={onReady}
                    components={components}
                    headerComponents={headerComponents}
                />
            </div>
        </ConsoleSidebarPinnedProvider>
    );
};
