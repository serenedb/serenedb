import {
    DarkThemeIcon,
    LightThemeIcon,
} from "@serene-ui/shared-frontend/shared";
import { CommandSection } from "../../model";
import { CommandItemBase } from "./CommandItemBase";
import { useChangeTheme } from "@serene-ui/shared-frontend/features";

export interface ToggleThemeCommandProps {}

export const ToggleThemeCommand = ({}: ToggleThemeCommandProps) => {
    const { theme, changeTheme } = useChangeTheme();

    return (
        <CommandItemBase
            isSection={false}
            title={"Toggle Theme"}
            icon={theme === "dark" ? <LightThemeIcon /> : <DarkThemeIcon />}
            section={CommandSection.Utilities}
            sectionLabel="Utilities"
            onSelect={() => changeTheme()}
        />
    );
};
