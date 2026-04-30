import { createContext, useContext, useEffect, useState } from "react";

type Theme = "dark" | "light" | "system";

type ThemeProviderProps = {
    children: React.ReactNode;
    defaultTheme?: Theme;
    storageKey?: string;
};

type ThemeProviderState = {
    theme: Theme;
    setTheme: (theme: Theme) => void;
};

const initialState: ThemeProviderState = {
    theme: "dark",
    setTheme: () => null,
};

const ThemeProviderContext = createContext<ThemeProviderState>(initialState);

const isTheme = (value: unknown): value is Theme =>
    value === "dark" || value === "light" || value === "system";

const getStoredTheme = (storageKey: string, defaultTheme: Theme) => {
    if (typeof window === "undefined") {
        return defaultTheme;
    }

    try {
        const storedTheme = window.localStorage.getItem(storageKey);

        if (isTheme(storedTheme)) {
            return storedTheme;
        }
    } catch {
        // Ignore localStorage read failures and fall back to other sources.
    }

    if (isTheme(window.sereneTheme?.preference)) {
        return window.sereneTheme.preference;
    }

    return defaultTheme;
};

const resolveTheme = (theme: Theme) => {
    if (theme === "system") {
        return window.matchMedia("(prefers-color-scheme: dark)").matches
            ? "dark"
            : "light";
    }

    return theme;
};

const applyTheme = (theme: Theme) => {
    const root = window.document.documentElement;
    const resolvedTheme = resolveTheme(theme);

    root.classList.remove("light", "dark");
    root.classList.add(resolvedTheme);
    root.style.colorScheme = resolvedTheme;
};

export function ThemeProvider({
    children,
    defaultTheme = "dark",
    storageKey = "vite-ui-theme",
    ...props
}: ThemeProviderProps) {
    const [theme, setTheme] = useState<Theme>(() =>
        getStoredTheme(storageKey, defaultTheme),
    );

    useEffect(() => {
        const root = window.document.documentElement;
        const mediaQuery = window.matchMedia("(prefers-color-scheme: dark)");
        root.classList.add("no-theme-transition");

        const removeTransitions = () => {
            root.classList.remove("no-theme-transition");
        };
        const raf = window.requestAnimationFrame(() => {
            window.requestAnimationFrame(removeTransitions);
        });

        const syncTheme = () => {
            applyTheme(theme);
        };

        syncTheme();

        if (theme === "system") {
            mediaQuery.addEventListener("change", syncTheme);
        }

        window.sereneTheme?.setPreference(theme);

        return () => {
            if (theme === "system") {
                mediaQuery.removeEventListener("change", syncTheme);
            }
            window.cancelAnimationFrame(raf);
            removeTransitions();
        };
    }, [theme]);

    const value = {
        theme,
        setTheme: (theme: Theme) => {
            try {
                window.localStorage.setItem(storageKey, theme);
            } catch {
                // Ignore localStorage write failures and still apply the theme.
            }
            setTheme(theme);
        },
    };

    return (
        <ThemeProviderContext.Provider {...props} value={value}>
            {children}
        </ThemeProviderContext.Provider>
    );
}

export const useTheme = () => {
    const context = useContext(ThemeProviderContext);

    if (context === undefined)
        throw new Error("useTheme must be used within a ThemeProvider");

    return context;
};
