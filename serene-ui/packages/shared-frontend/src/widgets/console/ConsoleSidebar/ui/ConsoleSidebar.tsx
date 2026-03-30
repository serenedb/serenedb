import React from "react";
import {
    DockviewIDisposable,
    PaneviewReact,
    PaneviewReadyEvent,
} from "dockview";
import { ConsoleSidebarTopbar } from "./ConsoleSidebarTopbar";
import { ConsoleSidebarHeader } from "./ConsoleSidebarHeader";
import { ConsoleSidebarSavedQueries } from "./ConsoleSidebarSavedQueries";
import {
    EntitiesIcon,
    SavedQueriesIcon,
    StarIcon,
} from "@serene-ui/shared-frontend";
import { ConsoleExplorer } from "../../ConsoleExplorer";

interface ConsoleSidebarProps {}

const components = {
    entities: () => {
        return <ConsoleExplorer />;
    },
    favorites: () => {
        return <></>;
    },
    savedQueries: () => {
        return <ConsoleSidebarSavedQueries />;
    },
};
const headerComponents = {
    default: ConsoleSidebarHeader,
};

export const ConsoleSidebar: React.FC<ConsoleSidebarProps> = () => {
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

    const onReady = (event: PaneviewReadyEvent) => {
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

        event.api.addPanel({
            id: "panel_1",
            component: "favorites",
            headerComponent: "default",
            params: {
                title: "Favorites",
                icon: <StarIcon className="size-3.5" />,
                kind: "favorites",
            },
            title: "Favorites",
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

        event.api.onDidRemoveView((panel) => {
            expansionListeners.get(panel.id)?.dispose();
            expansionListeners.delete(panel.id);
        });
    };

    return (
        <div className="flex flex-col h-full w-full">
            <ConsoleSidebarTopbar />
            <PaneviewReact
                onReady={onReady}
                components={components}
                headerComponents={headerComponents}
            />
        </div>
    );
};
