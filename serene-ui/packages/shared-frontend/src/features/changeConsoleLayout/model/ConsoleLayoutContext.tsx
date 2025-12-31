import { createContext, useContext } from "react";

export type ConsoleLayoutVariants = "horizontal" | "vertical";

export interface ConsoleLayoutContextType {
    layout: ConsoleLayoutVariants;
    isMaximized: boolean;
    isMaximizedResultsShown: boolean;
    toggleLayout: () => void;
    toggleMaximizedResults: () => void;
    toggleMaximized: () => void;
}

export const ConsoleLayoutContext = createContext<
    ConsoleLayoutContextType | undefined
>(undefined);

export const useConsoleLayout = (): ConsoleLayoutContextType => {
    const context = useContext(ConsoleLayoutContext);
    if (!context) {
        throw new Error(
            "useConsoleLayout must be used within a ConsoleLayoutProvider",
        );
    }
    return context;
};
