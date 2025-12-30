import { SavedQuerySchema } from "@serene-ui/shared-core";
import { QueryResult } from "../../executeQuery";
import { createContext, useContext } from "react";

interface SavedQueriesModalContextType {
    open: boolean;
    setOpen: React.Dispatch<React.SetStateAction<boolean>>;
    currentSavedQuery: SavedQuerySchema | undefined;
    setCurrentSavedQuery: React.Dispatch<
        React.SetStateAction<SavedQuerySchema | undefined>
    >;
    setJobId: React.Dispatch<React.SetStateAction<number | undefined>>;
    handleSaveQuery: () => void;
    handleDeleteSavedQuery: () => void;
    result: QueryResult | undefined;
    isQueryRunning: boolean;
    setIsQueryRunning: React.Dispatch<React.SetStateAction<boolean>>;
}

export const SavedQueriesModalContext = createContext<
    SavedQueriesModalContextType | undefined
>(undefined);

export const useSavedQueriesModal = () => {
    const context = useContext(SavedQueriesModalContext);
    if (!context) {
        throw new Error(
            "useSavedQueriesModal must be used within a SavedQueriesModalProvider",
        );
    }
    return context;
};
