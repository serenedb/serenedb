import { useContext, createContext } from "react";

export interface DatabasesContextType {
    databases: string[];
    isLoading: boolean;
    error: Error | null;
    refetchDatabases: () => Promise<void>;
}

export const DatabasesContext = createContext<DatabasesContextType | undefined>(
    undefined,
);

export const useDatabases = () => {
    const context = useContext(DatabasesContext);
    if (!context) {
        throw new Error("useDatabases must be used within a DatabasesProvider");
    }
    return context;
};
