import { ThemeProvider } from "@serene-ui/shared-frontend/shared";

export const WithTheme = ({ children }: { children: React.ReactNode }) => {
    return <ThemeProvider defaultTheme="system">{children}</ThemeProvider>;
};
