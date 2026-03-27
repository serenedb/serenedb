export interface ConsoleContextType {
    limit: number;
    setLimit: (limit: number) => void;
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
