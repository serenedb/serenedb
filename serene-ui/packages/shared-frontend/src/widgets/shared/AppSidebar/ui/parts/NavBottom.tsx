import {
    useChangeTheme,
    useCommandModal,
    useSupportModal,
} from "@serene-ui/shared-frontend/features";
import {
    DarkThemeIcon,
    LightThemeIcon,
    SidebarGroup,
    SidebarMenu,
    SidebarMenuButton,
    SidebarMenuItem,
    SupportIcon,
} from "@serene-ui/shared-frontend/shared";
import { SearchIcon } from "lucide-react";
import { useNavigate } from "react-router-dom";
import type { SidebarButton } from "../../model/types";

export const NavBottom = () => {
    const navigate = useNavigate();
    const { setOpen: setOpenSearch } = useCommandModal();
    const { setOpen: setOpenSupport } = useSupportModal();
    const { theme, changeTheme } = useChangeTheme();

    const buttons: SidebarButton[] = [
        {
            title: "Change theme",
            icon: theme === "dark" ? <LightThemeIcon /> : <DarkThemeIcon />,
            action: () => changeTheme(),
        },
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
