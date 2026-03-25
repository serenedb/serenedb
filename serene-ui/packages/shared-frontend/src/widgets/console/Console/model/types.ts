export interface ConsoleContextType {
    limit: number;
    setLimit: (limit: number) => void;
    sidebarCollapsed: boolean;
    setSidebarCollapsed: (collapsed: boolean) => void;
    toggleSidebar: () => void;
}
