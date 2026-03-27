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

interface ConsoleProps {}

const components = {
    sidebar: () => {
        return <ConsoleSidebar />;
    },
    editor: () => {
        return <ConsoleMainArea />;
    },
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

    const onReady = (event: GridviewReadyEvent) => {
        gridEventRef.current = event;
        ensureMainAreaPanel(event);

        if (!sidebarCollapsed) {
            ensureSidebarPanel(event);
        }
    };

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

export const Console: React.FC<ConsoleProps> = () => {
    return (
        <ConsoleProvider>
            <ConsoleLayout />
        </ConsoleProvider>
    );
};
