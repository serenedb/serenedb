import { SidebarIcon } from "lucide-react";
import { Button } from "@serene-ui/shared-frontend";
import React from "react";

interface DashboardsTopbarProps {}

export const DashboardsTopbar: React.FC<DashboardsTopbarProps> = () => {
    return (
        <div className="w-full h-12 flex px-2.5 items-center justify-between border-b-1">
            <div className="flex gap-4 items-center">
                <Button size="icon" variant="secondary">
                    <SidebarIcon />
                </Button>
                <p className="text-xs text-primary-foreground">My Dashboard</p>
            </div>
            <div className="flex gap-1">
                <Button size="icon" variant="secondary"></Button>
                <Button size="icon" variant="secondary"></Button>
                <Button size="icon" variant="secondary"></Button>
            </div>
        </div>
    );
};
