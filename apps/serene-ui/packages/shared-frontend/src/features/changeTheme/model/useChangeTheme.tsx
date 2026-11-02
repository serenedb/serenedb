import { useTheme } from "@serene-ui/shared-frontend/shared";

export const useChangeTheme = () => {
    const { theme, setTheme } = useTheme();

    const changeTheme = () => {
        setTheme(theme === "light" ? "dark" : "light");
    };

    return {
        theme,
        changeTheme,
    };
};
