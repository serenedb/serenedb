import { useLocation, useNavigate } from "react-router-dom";
import {
    ConsoleIcon,
    FramesIcon,
    navigationMap,
    ReplicationIcon,
    SidebarGroup,
    SidebarGroupLabel,
    SidebarMenu,
    SidebarMenuButton,
    SidebarMenuItem,
} from "@serene-ui/shared-frontend/shared";
import type { SidebarButton } from "../../model/types";

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
            title: "Frames",
            icon: <FramesIcon />,
            link: navigationMap.frames,
        },
        {
            title: "Replication",
            icon: <ReplicationIcon />,
            link: navigationMap.replication,
        },
    ];

    const getAction = (item: SidebarButton) => {
        if (item.action) return item.action;
        return () => {
            if (item.link) navigate(item.link);
        };
    };

    return (
        <SidebarGroup>
            <SidebarGroupLabel>Main</SidebarGroupLabel>
            <SidebarMenu>
                {buttons.map((item, index) => (
                    <SidebarMenuItem key={index}>
                        <SidebarMenuButton
                            isActive={location.pathname === item.link}
                            onClick={getAction(item)}
                            tooltip={item.title}>
                            {item.icon}
                            <span>{item.title}</span>
                        </SidebarMenuButton>
                    </SidebarMenuItem>
                ))}
            </SidebarMenu>
        </SidebarGroup>
    );
};
