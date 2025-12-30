import { createContext, useContext } from "react";
import type { ConsoleTab } from "@serene-ui/shared-frontend/widgets";

export interface ConsoleContextType {
    tabs: ConsoleTab[];
    addTab: (type: "query") => void;
    removeTab: (id: number) => void;
    selectTab: (id: number) => void;
    selectedTabId: number;
    updateTab: (id: number, tabUpdate: Partial<ConsoleTab>) => void;
    addJobId: (jobId: number, tabId?: number) => void;
    limit: number;
    setLimit: (limit: number) => void;
    explorerRef: React.RefObject<HTMLDivElement | null>;
    editorRef: React.RefObject<HTMLElement | null>;
}

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
