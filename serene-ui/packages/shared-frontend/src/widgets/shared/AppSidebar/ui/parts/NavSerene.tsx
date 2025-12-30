import {
    SidebarGroup,
    SidebarGroupLabel,
    SidebarMenu,
    SidebarMenuItem,
    SidebarMenuButton,
} from "@serene-ui/shared-frontend/shared";
import { useNavigate } from "react-router-dom";
import type { SidebarButton } from "../../model/types";

export const NavSerene = () => {
    const navigate = useNavigate();
    const buttons: SidebarButton[] = [];

    const getAction = (item: SidebarButton) => {
        if (item.action) return item.action;
        return () => {
            if (item.link) navigate(item.link);
        };
    };

    if (buttons.length === 0) return <></>;
    return (
        <SidebarGroup>
            <SidebarGroupLabel>SereneDB</SidebarGroupLabel>
            <SidebarMenu>
                {buttons.map((item, index) => (
                    <SidebarMenuItem key={index}>
                        <SidebarMenuButton
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
