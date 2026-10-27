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
    return <></>;
};
