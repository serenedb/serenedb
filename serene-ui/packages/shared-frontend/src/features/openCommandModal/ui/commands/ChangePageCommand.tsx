import { navigationMap } from "@serene-ui/shared-frontend/shared";
import { useNavigate } from "react-router-dom";
import { CommandSection } from "../../model";
import { CommandItemBase } from "./CommandItemBase";

export interface ChangePageCommandProps {
    isSection?: boolean;
    route?: (typeof navigationMap)[keyof typeof navigationMap];
    title?: string;
    icon: React.ReactNode;
}

export const ChangePageCommand = ({
    isSection = false,
    route,
    title,
    icon,
}: ChangePageCommandProps) => {
    const navigate = useNavigate();

    const handleChangePage = () => {
        if (route) {
            navigate(route);
        }
    };

    return (
        <CommandItemBase
            isSection={isSection}
            title={title}
            icon={icon}
            section={CommandSection.Pages}
            sectionLabel="Change page"
            onSelect={route !== undefined ? handleChangePage : undefined}
        />
    );
};
