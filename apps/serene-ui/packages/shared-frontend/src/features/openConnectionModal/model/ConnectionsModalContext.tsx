import { ConnectionSchema } from "@serene-ui/shared-core";
import { createContext, useContext } from "react";

interface ConnectionsModalContextType {
    open: boolean;
    setOpen: React.Dispatch<React.SetStateAction<boolean>>;
    currentConnection: ConnectionSchema;
    setCurrentConnection: React.Dispatch<
        React.SetStateAction<ConnectionSchema>
    >;
    handleInputChange: (e: React.ChangeEvent<HTMLInputElement>) => void;
    handleSelectChange: (
        key: keyof ConnectionSchema,
        value: ConnectionSchema[keyof ConnectionSchema],
    ) => void;
    handleAddConnection: () => Promise<boolean>;
    isAddPending: boolean;
    handleUpdateConnection: () => Promise<boolean>;
    isUpdatePending: boolean;
    handleDeleteConnection: () => Promise<boolean>;
    isDeletePending: boolean;
    isGlobalDisabled: boolean;
    setIsGlobalDisabled: React.Dispatch<React.SetStateAction<boolean>>;
}

export const ConnectionsModalContext = createContext<
    ConnectionsModalContextType | undefined
>(undefined);

export const useConnectionsModal = () => {
    const context = useContext(ConnectionsModalContext);
    if (!context) {
        throw new Error(
            "useConnectionsModal must be used within a ConnectionsModalProvider",
        );
    }
    return context;
};
