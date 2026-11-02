import {
    useChangeTheme,
    useCommandModal,
    useSupportModal,
} from "@serene-ui/shared-frontend/features";
import {
    Button,
    DarkThemeIcon,
    GithubIcon,
    LightThemeIcon,
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
        <div className="mt-auto flex flex-col gap-1">
            <div className="p-2.5 flex flex-col gap-1.5">
                {buttons.map((item, index) => {
                    return (
                        <Button
                            variant="ghost"
                            key={index}
                            size={"icon"}
                            title={item.title.toLowerCase()}
                            aria-label={item.title.toLowerCase()}
                            onClick={getAction(item)}>
                            {item.icon}
                        </Button>
                    );
                })}
                <Button
                    size="icon"
                    variant="secondary"
                    title="Open GitHub repository">
                    <GithubIcon />
                </Button>
            </div>
        </div>
    );
};
