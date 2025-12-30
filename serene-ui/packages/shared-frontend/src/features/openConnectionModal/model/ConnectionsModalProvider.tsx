import { useTestConnection } from "@serene-ui/shared-frontend/features";

import { useCallback, useMemo, useState } from "react";
import { ConnectionsModal } from "../ui/ConnectionsModal";
import { ConnectionsModalContext } from "./ConnectionsModalContext";
import { useConnectionMutations } from "./hooks/useConnectionMutations";
import { ConnectionSchema } from "@serene-ui/shared-core";

export const EMPTY_CONNECTION = {
    id: -1,
    name: "",
    type: "postgres",
    ssl: false,
    host: "",
    port: "" as any,
    database: "",
    mode: "host",
    authMethod: "password",
    user: "",
} as ConnectionSchema;

export const ConnectionsModalProvider = ({
    children,
}: {
    children: React.ReactNode;
}) => {
    const [open, setOpen] = useState(false);
    const [currentConnection, setCurrentConnection] =
        useState<ConnectionSchema>(EMPTY_CONNECTION);
    const [isGlobalDisabled, setIsGlobalDisabled] = useState(false);

    const { handleTestConnection } = useTestConnection();

    const {
        handleAddConnection: mutateAddConnection,
        handleUpdateConnection: mutateUpdateConnection,
        handleDeleteConnection: mutateDeleteConnection,
        isAddPending,
        isUpdatePending,
        isDeletePending,
    } = useConnectionMutations(handleTestConnection, setCurrentConnection);

    const handleInputChange = (e: React.ChangeEvent<HTMLInputElement>) => {
        setCurrentConnection((prev) => ({
            ...prev,
            [e.target.id]: e.target.value,
        }));
    };

    const handleSelectChange = <K extends keyof ConnectionSchema>(
        key: K,
        value: ConnectionSchema[K],
    ) => {
        setCurrentConnection((prev) => ({ ...prev, [key]: value }));
    };

    const handleAddConnection = useCallback(async () => {
        setIsGlobalDisabled(true);
        const result = await mutateAddConnection(currentConnection);
        setIsGlobalDisabled(false);
        return result;
    }, [mutateAddConnection, currentConnection]);

    const handleUpdateConnection = useCallback(async () => {
        setIsGlobalDisabled(true);
        const result = await mutateUpdateConnection(currentConnection);
        setIsGlobalDisabled(false);
        return result;
    }, [mutateUpdateConnection, currentConnection]);

    const handleDeleteConnection = useCallback(async () => {
        setIsGlobalDisabled(true);
        const result = await mutateDeleteConnection(currentConnection.id);
        setIsGlobalDisabled(false);
        return result;
    }, [mutateDeleteConnection, currentConnection.id]);

    const value = useMemo(
        () => ({
            open,
            setOpen,
            currentConnection,
            setCurrentConnection,
            handleInputChange,
            handleSelectChange,
            handleAddConnection,
            handleUpdateConnection,
            handleDeleteConnection,
            isAddPending,
            isUpdatePending,
            isDeletePending,
            isGlobalDisabled,
            setIsGlobalDisabled,
        }),
        [
            open,
            currentConnection,
            isAddPending,
            isUpdatePending,
            isDeletePending,
            handleAddConnection,
            handleUpdateConnection,
            handleDeleteConnection,
            setIsGlobalDisabled,
        ],
    );

    return (
        <ConnectionsModalContext.Provider value={value}>
            {children}
            <ConnectionsModal />
        </ConnectionsModalContext.Provider>
    );
};
