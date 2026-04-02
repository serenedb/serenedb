import React from "react";
import {
    DockviewIDisposable,
    PaneviewReact,
    PaneviewReadyEvent,
} from "dockview";
import { ConsoleSidebarTopbar } from "./ConsoleSidebarTopbar";
import { ConsoleSidebarHeader } from "./ConsoleSidebarHeader";
import { ConsoleSidebarSavedQueries } from "./ConsoleSidebarSavedQueries";
import { ConsoleSidebarPinned } from "./ConsoleSidebarPinned";
import {
    EntitiesIcon,
    SavedQueriesIcon,
} from "@serene-ui/shared-frontend";
import { PinIcon } from "../../../../shared/ui/icons/index";
import { ConsoleExplorer } from "../../ConsoleExplorer";
import { useDockviewLayoutSync } from "../../../../shared/hooks";
import { ConsoleSidebarPinnedProvider } from "../model";

interface ConsoleSidebarProps {}

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

            if (!entitiesPanel.api.isExpanded || !savedQueriesPanel.api.isExpanded) {
                return;
            }

            const targetPanelHeight =
                (entitiesPanel.height + savedQueriesPanel.height) / 2;

            entitiesPanel.api.setSize({
                size: Math.max(targetPanelHeight, entitiesPanel.minimumSize),
            });
            savedQueriesPanel.api.setSize({
                size: Math.max(targetPanelHeight, savedQueriesPanel.minimumSize),
            });
        },
        [],
    );

    const onReady = (event: PaneviewReadyEvent) => {
        setApi(event.api);
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

        event.api.addPanel({
            id: "panel_1",
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

        event.api.addPanel({
            id: "panel_2",
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

        event.api.addPanel({
            id: "panel_3",
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

        event.api.panels.forEach(bindPanelExpansionListener);
        event.api.onDidAddView(bindPanelExpansionListener);

        requestAnimationFrame(() => {
            equalizeEntitiesAndSavedQueries(event);
        });

        event.api.onDidRemoveView((panel) => {
            expansionListeners.get(panel.id)?.dispose();
            expansionListeners.delete(panel.id);
        });
    };

    return (
        <ConsoleSidebarPinnedProvider>
            <div ref={containerRef} className="flex flex-col h-full w-full">
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
