import {
    Button,
    NotificationsListButton,
    QueryHistoryIcon,
    SettingsIcon,
    SidebarIcon,
    cn,
} from "@serene-ui/shared-frontend";
import { useConsole } from "../../Console/model";
import { SwitchConnection } from "@serene-ui/shared-frontend";

export const ConsoleEditorTopbar = () => {
    const {
        sidebarCollapsed,
        toggleSidebar,
        settingsSidebarCollapsed,
        toggleSettingsSidebar,
        executionHistorySidebarCollapsed,
        toggleExecutionHistorySidebar,
    } = useConsole();

    return (
        <div className="flex min-h-[48.5px] w-full items-center justify-between border-b-[0.5px] px-2.5">
            <div className="flex items-center gap-2 min-w-30">
                <Button
                    size="icon"
                    variant="ghost"
                    onClick={toggleSidebar}
                    className={cn({
                        "text-muted-foreground": sidebarCollapsed,
                    })}>
                    <SidebarIcon />
                </Button>
                <p className="text-xs font-black uppercase dark:text-primary-foreground">
                    Console
                </p>
            </div>
            <SwitchConnection />
            <div className="flex items-center gap-1 min-w-30 justify-end">
                <Button
                    size="icon"
                    variant="ghost"
                    onClick={toggleSettingsSidebar}
                    title="Toggle settings sidebar"
                    className={cn({
                        "text-accent-foreground bg-accent":
                            !settingsSidebarCollapsed,
                    })}>
                    <SettingsIcon className="size-4" />
                </Button>
                <Button
                    size="icon"
                    variant="ghost"
                    onClick={toggleExecutionHistorySidebar}
                    title="Toggle execution history sidebar"
                    className={cn({
                        "text-accent-foreground bg-accent":
                            !executionHistorySidebarCollapsed,
                    })}>
                    <QueryHistoryIcon className="size-4" />
                </Button>
                <NotificationsListButton />
            </div>
        </div>
    );
};
