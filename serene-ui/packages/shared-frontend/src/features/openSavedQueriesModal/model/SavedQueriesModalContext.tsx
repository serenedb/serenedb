import { SavedQuerySchema } from "@serene-ui/shared-core";
import { createContext, useContext } from "react";

type OpenCreateSavedQueryPayload = {
    query?: string;
    bindVars?: SavedQuerySchema["bind_vars"];
};

interface SavedQueriesModalContextType {
    open: boolean;
    setOpen: React.Dispatch<React.SetStateAction<boolean>>;
    modalMode: "create" | "edit";
    currentSavedQuery: SavedQuerySchema | undefined;
    setCurrentSavedQuery: React.Dispatch<
        React.SetStateAction<SavedQuerySchema | undefined>
    >;
    openCreateModal: (payload?: OpenCreateSavedQueryPayload) => void;
    openEditModal: (savedQuery: SavedQuerySchema) => void;
    handleSaveQuery: () => void;
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
