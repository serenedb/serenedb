import {
    DEFAULT_HOTKEYS,
    useAppHotkey,
} from "@serene-ui/shared-frontend/shared";
import { PropsWithChildren, useCallback, useEffect, useState } from "react";
import {
    ConsoleLayoutContext,
    ConsoleLayoutVariants,
} from "./ConsoleLayoutContext";

const STORAGE_KEY = "system:console-layout";
const DEFAULT_LAYOUT: ConsoleLayoutVariants = "horizontal";

const getInitialLayout = (): ConsoleLayoutVariants => {
    const stored = localStorage.getItem(STORAGE_KEY);
    return stored === "horizontal" || stored === "vertical"
        ? stored
        : DEFAULT_LAYOUT;
};

export const ConsoleLayoutProvider = ({ children }: PropsWithChildren) => {
    const [layout, setLayout] =
        useState<ConsoleLayoutVariants>(getInitialLayout);
    const [isMaximized, setIsMaximized] = useState(false);
    const [isMaximizedResultsShown, setIsMaximizedResultsShown] =
        useState(false);

    useEffect(() => {
        try {
            localStorage.setItem(STORAGE_KEY, layout);
        } catch (error) {
            console.warn("Failed to save console layout:", error);
        }
    }, [layout]);

    const toggleLayout = useCallback(() => {
        setLayout((prev) =>
            prev === "horizontal" ? "vertical" : "horizontal",
        );
    }, []);

    const toggleMaximized = useCallback(() => {
        setIsMaximized((prev) => !prev);
    }, []);

    const toggleMaximizedResults = useCallback(() => {
        setIsMaximizedResultsShown((prev) => !prev);
    }, []);

    useAppHotkey(DEFAULT_HOTKEYS.CONSOLE_TOGGLE_LAYOUT, toggleLayout, [
        toggleLayout,
    ]);

    return (
        <ConsoleLayoutContext.Provider
            value={{
                layout,
                isMaximized,
                isMaximizedResultsShown,
                toggleLayout,
                toggleMaximizedResults,
                toggleMaximized,
            }}>
            {children}
        </ConsoleLayoutContext.Provider>
    );
};
