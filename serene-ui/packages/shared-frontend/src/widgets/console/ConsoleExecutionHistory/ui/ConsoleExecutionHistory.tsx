import { useConsole } from "../../Console/model";
import { ConsoleExecutionHistoryTopbar } from "./ConsoleExecutionHistoryTopbar";

export const ConsoleExecutionHistory = () => {
    const { setExecutionHistorySidebarCollapsed } = useConsole();

    return (
        <div className="flex h-full w-full flex-col">
            <ConsoleExecutionHistoryTopbar
                onClose={() => setExecutionHistorySidebarCollapsed(true)}
            />
        </div>
    );
};
