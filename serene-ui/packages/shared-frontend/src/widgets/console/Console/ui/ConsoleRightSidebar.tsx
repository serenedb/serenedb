import { useConsole } from "../model";
import { ConsoleExecutionHistory } from "../../ConsoleExecutionHistory";
import { ConsoleSettings } from "../../ConsoleSettings";

export const ConsoleRightSidebar = () => {
    const { settingsSidebarCollapsed, executionHistorySidebarCollapsed } =
        useConsole();

    if (!settingsSidebarCollapsed) {
        return <ConsoleSettings />;
    }

    if (!executionHistorySidebarCollapsed) {
        return <ConsoleExecutionHistory />;
    }

    return <div className="h-full w-full" />;
};
