export type ConsoleExecutionAlertMode = "always" | "onlyUnseen" | "never";

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
}
