import { createContext, useContext } from "react";
import type { ConsoleContextType } from "./types";

export const ConsoleContext = createContext<ConsoleContextType | undefined>(
    undefined,
);

export const useConsole = (): ConsoleContextType => {
    const context = useContext(ConsoleContext);
    if (!context) {
        throw new Error("useConsole must be used within a ConsoleProvider");
    }
    return context;
};
