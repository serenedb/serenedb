import { Button, SidebarIcon, cn } from "@serene-ui/shared-frontend";
import { useConsole } from "../../Console/model";

export const ConsoleEditorTopbar = () => {
    const { sidebarCollapsed, toggleSidebar } = useConsole();

    return (
        <div className="flex min-h-[48.5px] w-full items-center justify-between border-b-[0.5px] border-l-[0.5px] px-2.5">
            <div className="flex items-center gap-2">
                <Button
                    size="icon"
                    variant="ghost"
                    onClick={toggleSidebar}
                    className={cn({
                        "text-muted-foreground": sidebarCollapsed,
                    })}>
                    <SidebarIcon />
                </Button>
                <p className="text-xs font-black uppercase text-primary-foreground">
                    Console
                </p>
            </div>
        </div>
    );
};
