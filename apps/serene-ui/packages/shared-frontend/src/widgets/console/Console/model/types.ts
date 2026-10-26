import type { DockviewApi } from "dockview";

export type ConsoleExecutionAlertMode = "always" | "onlyUnseen" | "never";

export type ConsoleExecutionStatus =
    | "success"
    | "failed"
    | "pending"
    | "running"
    | "";

export type ConsoleExecutionHistorySidebarTab = "running" | "history";

export interface ConsoleExecutionHistoryEntry {
    id: string;
    panelId: string;
    panelTitle?: string;
    jobId: number;
    status: ConsoleExecutionStatus;
    statementIndex?: number;
    statementQuery?: string;
    sourceQuery?: string;
    created_at?: string;
    execution_started_at?: string;
    execution_finished_at?: string;
    received_at?: string;
    updated_at: string;
}

export interface ConsoleExecutionHistoryEntryInput
    extends Omit<ConsoleExecutionHistoryEntry, "id" | "updated_at"> {}

export interface ConsoleContextType {
    limit: number;
    setLimit: (limit: number) => void;
    spawnResultsInFirstTab: boolean;
    setSpawnResultsInFirstTab: (value: boolean) => void;
    colorfulTypesInResults: boolean;
    setColorfulTypesInResults: (value: boolean) => void;
    selectRelatedResultOnTabChange: boolean;
    setSelectRelatedResultOnTabChange: (value: boolean) => void;
    showJsonByDefault: boolean;
    setShowJsonByDefault: (value: boolean) => void;
    showSavedQueriesInAutocomplete: boolean;
    setShowSavedQueriesInAutocomplete: (value: boolean) => void;
    showExecutionHistoryInAutocomplete: boolean;
    setShowExecutionHistoryInAutocomplete: (value: boolean) => void;
    showAutocomplete: boolean;
    setShowAutocomplete: (value: boolean) => void;
    executeSequentiallyByDefault: boolean;
    setExecuteSequentiallyByDefault: (value: boolean) => void;
    executeInNewTabByDefault: boolean;
    setExecuteInNewTabByDefault: (value: boolean) => void;
    alertOnExecution: ConsoleExecutionAlertMode;
    setAlertOnExecution: (value: ConsoleExecutionAlertMode) => void;
    sidebarCollapsed: boolean;
    setSidebarCollapsed: (collapsed: boolean) => void;
    toggleSidebar: () => void;
    settingsSidebarCollapsed: boolean;
    setSettingsSidebarCollapsed: (collapsed: boolean) => void;
    toggleSettingsSidebar: () => void;
    executionHistorySidebarCollapsed: boolean;
    setExecutionHistorySidebarCollapsed: (collapsed: boolean) => void;
    toggleExecutionHistorySidebar: () => void;
    executionHistoryActiveTab: ConsoleExecutionHistorySidebarTab;
    setExecutionHistoryActiveTab: (
        tab: ConsoleExecutionHistorySidebarTab,
    ) => void;
    executionHistoryPanelFilter: string;
    setExecutionHistoryPanelFilter: (panelId: string) => void;
    openExecutionHistorySidebar: (options?: {
        tab?: ConsoleExecutionHistorySidebarTab;
        panelId?: string;
    }) => void;
    executionHistoryEntries: ConsoleExecutionHistoryEntry[];
    upsertExecutionHistoryEntries: (
        entries: ConsoleExecutionHistoryEntryInput[],
    ) => void;
    clearExecutionHistoryEntries: () => void;
    consoleEditorApi?: DockviewApi;
    setConsoleEditorApi: (api?: DockviewApi) => void;
}
