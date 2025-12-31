import { useNavigate } from "react-router-dom";
import type { SidebarButton } from "../../model/types";
import { SearchIcon } from "lucide-react";
import {
    SupportIcon,
    SidebarGroup,
    SidebarMenu,
    SidebarMenuItem,
    SidebarMenuButton,
} from "@serene-ui/shared-frontend/shared";
import { useCommandModal } from "@serene-ui/shared-frontend/features";
import { useSupportModal } from "@serene-ui/shared-frontend/features";

export const NavBottom = () => {
    const navigate = useNavigate();
    const { setOpen: setOpenSearch } = useCommandModal();
    const { setOpen: setOpenSupport } = useSupportModal();
    const buttons: SidebarButton[] = [
        {
            title: "Support",
            icon: <SupportIcon />,
            action: () => setOpenSupport(true),
        },
        {
            title: "Search",
            icon: <SearchIcon />,
            action: () => setOpenSearch(true),
        },
    ];

    const getAction = (item: SidebarButton) => {
        if (item.action) return item.action;
        return () => {
            if (item.link) navigate(item.link);
        };
    };

    return (
        <SidebarGroup className="mt-auto">
            <SidebarMenu>
                {buttons.map((item, index) => (
                    <SidebarMenuItem key={index}>
                        <SidebarMenuButton
                            title={item.title.toLowerCase()}
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
