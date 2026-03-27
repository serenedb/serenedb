import React, { useEffect, useRef } from "react";
import {
    GridviewReact,
    type GridviewReadyEvent,
    Orientation,
} from "dockview";
import { ConsoleEditor } from "../../ConsoleEditor";
import { ConsoleSidebar } from "../../ConsoleSidebar";
import { ConsoleSettings } from "../../ConsoleSettings";
import { ConsoleExecutionHistory } from "../../ConsoleExecutionHistory";
import {
    CONSOLE_GRID_EDITOR_PANEL_ID,
    CONSOLE_GRID_EXECUTION_HISTORY_PANEL_ID,
    CONSOLE_GRID_SETTINGS_PANEL_ID,
    CONSOLE_GRID_SIDEBAR_PANEL_ID,
    CONSOLE_RIGHT_SIDEBAR_MIN_SIZE,
    CONSOLE_RIGHT_SIDEBAR_SIZE,
    CONSOLE_SIDEBAR_MIN_SIZE,
    CONSOLE_SIDEBAR_SIZE,
    ConsoleProvider,
    useConsole,
} from "../model";

interface ConsoleProps {}

const components = {
    editor: () => {
        return <ConsoleEditor />;
    },
    sidebar: () => {
        return <ConsoleSidebar />;
    },
    settings: () => {
        return <ConsoleSettings />;
    },
    executionHistory: () => {
        return <ConsoleExecutionHistory />;
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

const ensureSidebarPanel = (event: GridviewReadyEvent) => {
    if (event.api.getPanel(CONSOLE_GRID_SIDEBAR_PANEL_ID)) {
        return;
    }

    event.api.addPanel({
        id: CONSOLE_GRID_SIDEBAR_PANEL_ID,
        component: "sidebar",
        minimumWidth: CONSOLE_SIDEBAR_MIN_SIZE,
        maximumWidth: CONSOLE_SIDEBAR_SIZE,
        size: CONSOLE_SIDEBAR_SIZE,
        position: {
            referencePanel: CONSOLE_GRID_EDITOR_PANEL_ID,
            direction: "left",
        },
    });
};

const ensureSettingsPanel = (event: GridviewReadyEvent) => {
    if (event.api.getPanel(CONSOLE_GRID_SETTINGS_PANEL_ID)) {
        return;
    }

    event.api.addPanel({
        id: CONSOLE_GRID_SETTINGS_PANEL_ID,
        component: "settings",
        minimumWidth: CONSOLE_RIGHT_SIDEBAR_MIN_SIZE,
        maximumWidth: CONSOLE_RIGHT_SIDEBAR_SIZE,
        size: CONSOLE_RIGHT_SIDEBAR_SIZE,
        position: {
            referencePanel: CONSOLE_GRID_EDITOR_PANEL_ID,
            direction: "right",
        },
    });
};

const ensureExecutionHistoryPanel = (event: GridviewReadyEvent) => {
    if (event.api.getPanel(CONSOLE_GRID_EXECUTION_HISTORY_PANEL_ID)) {
        return;
    }

    event.api.addPanel({
        id: CONSOLE_GRID_EXECUTION_HISTORY_PANEL_ID,
        component: "executionHistory",
        minimumWidth: CONSOLE_RIGHT_SIDEBAR_MIN_SIZE,
        maximumWidth: CONSOLE_RIGHT_SIDEBAR_SIZE,
        size: CONSOLE_RIGHT_SIDEBAR_SIZE,
        position: {
            referencePanel: CONSOLE_GRID_EDITOR_PANEL_ID,
            direction: "right",
        },
    });
};

const ConsoleLayout: React.FC = () => {
    const {
        sidebarCollapsed,
        settingsSidebarCollapsed,
        executionHistorySidebarCollapsed,
    } = useConsole();
    const gridEventRef = useRef<GridviewReadyEvent | null>(null);

    const onReady = (event: GridviewReadyEvent) => {
        gridEventRef.current = event;
        ensureEditorPanel(event);

        if (!sidebarCollapsed) {
            ensureSidebarPanel(event);
        }

        if (!settingsSidebarCollapsed) {
            ensureSettingsPanel(event);
        }

        if (!executionHistorySidebarCollapsed) {
            ensureExecutionHistoryPanel(event);
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

    useEffect(() => {
        const event = gridEventRef.current;
        if (!event) {
            return;
        }

        const settingsPanel = event.api.getPanel(CONSOLE_GRID_SETTINGS_PANEL_ID);

        if (settingsSidebarCollapsed) {
            if (settingsPanel) {
                event.api.removePanel(settingsPanel);
            }
            return;
        }

        ensureSettingsPanel(event);
    }, [settingsSidebarCollapsed]);

    useEffect(() => {
        const event = gridEventRef.current;
        if (!event) {
            return;
        }

        const executionHistoryPanel = event.api.getPanel(
            CONSOLE_GRID_EXECUTION_HISTORY_PANEL_ID,
        );

        if (executionHistorySidebarCollapsed) {
            if (executionHistoryPanel) {
                event.api.removePanel(executionHistoryPanel);
            }
            return;
        }

        ensureExecutionHistoryPanel(event);
    }, [executionHistorySidebarCollapsed]);

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
