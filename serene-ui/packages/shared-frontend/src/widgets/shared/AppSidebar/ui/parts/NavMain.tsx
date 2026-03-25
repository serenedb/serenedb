import { useLocation, useNavigate } from "react-router-dom";
import {
    Button,
    cn,
    ConsoleIcon,
    DashboardsIcon,
    navigationMap,
    ReplicationIcon,
} from "@serene-ui/shared-frontend/shared";
import type { SidebarButton } from "../../model/types";
import { useMemo } from "react";

export const NavMain = () => {
    const navigate = useNavigate();
    const location = useLocation();
    const buttons: SidebarButton[] = [
        {
            title: "Console",
            icon: <ConsoleIcon />,
            link: navigationMap.console,
        },
        {
            title: "Dashboards",
            icon: <DashboardsIcon />,
            link: navigationMap.dashboards,
        },
    ];

    const getAction = (item: SidebarButton) => {
        if (item.action) return item.action;
        return () => {
            if (item.link) navigate(item.link);
        };
    };

    return (
        <div className="p-2.5 flex flex-col gap-1.5">
            {buttons.map((item, index) => {
                return (
                    <Button
                        variant="ghost"
                        className={cn("", {
                            "bg-accent text-accent-foreground":
                                item.link && location.pathname === item.link,
                        })}
                        key={index}
                        size={"icon"}
                        onClick={getAction(item)}>
                        {item.icon}
                    </Button>
                );
            })}
        </div>
    );
};
