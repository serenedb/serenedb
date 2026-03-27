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

const components = {
    editor: () => {
        return <ConsoleEditor />;
    },
    rightSidebar: () => {
        return <ConsoleRightSidebar />;
    },
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
        minimumWidth: 1,
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
    const rightSidebarWidthRef = useRef(CONSOLE_RIGHT_SIDEBAR_SIZE);

    const isRightSidebarVisible =
        !settingsSidebarCollapsed || !executionHistorySidebarCollapsed;

    const onReady = (event: GridviewReadyEvent) => {
        gridEventRef.current = event;
        ensureEditorPanel(event);

        if (isRightSidebarVisible) {
            ensureRightSidebarPanel(event);
        }
    };

    useEffect(() => {
        const event = gridEventRef.current;
        if (!event) {
            return;
        }

        const rightPanel = event.api.getPanel(CONSOLE_GRID_RIGHT_SIDEBAR_PANEL_ID);

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

        const addedOrExistingRightPanel = event.api.getPanel(
            CONSOLE_GRID_RIGHT_SIDEBAR_PANEL_ID,
        );
        if (addedOrExistingRightPanel) {
            addedOrExistingRightPanel.api.setSize({
                size: Math.max(rightSidebarWidthRef.current, 1),
            });
        }
    }, [isRightSidebarVisible]);

    useEffect(() => {
        const event = gridEventRef.current;
        if (!event || !isRightSidebarVisible) {
            return;
        }

        const rightPanel = event.api.getPanel(CONSOLE_GRID_RIGHT_SIDEBAR_PANEL_ID);
        if (rightPanel && rightPanel.width > 1) {
            rightSidebarWidthRef.current = rightPanel.width;
        }
    }, [settingsSidebarCollapsed, executionHistorySidebarCollapsed, isRightSidebarVisible]);

    return (
        <GridviewReact
            components={components}
            onReady={onReady}
            orientation={Orientation.HORIZONTAL}
        />
    );
};
