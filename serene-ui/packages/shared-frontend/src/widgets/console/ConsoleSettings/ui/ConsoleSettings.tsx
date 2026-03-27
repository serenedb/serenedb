import { useConsole } from "../../Console/model";
import { ConsoleSettingsTopbar } from "./ConsoleSettingsTopbar";

export const ConsoleSettings = () => {
    const { setSettingsSidebarCollapsed } = useConsole();

    return (
        <div className="flex h-full w-full flex-col">
            <ConsoleSettingsTopbar onClose={() => setSettingsSidebarCollapsed(true)} />
        </div>
    );
};
