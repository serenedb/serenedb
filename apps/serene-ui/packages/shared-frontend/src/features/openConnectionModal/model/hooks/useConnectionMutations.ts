import { useCallback } from "react";
import { toast } from "sonner";
import {
    useAddConnection,
    useDeleteConnection,
    useUpdateConnection,
} from "@serene-ui/shared-frontend/entities";
import { ConnectionSchema } from "@serene-ui/shared-core";
import { getErrorMessage } from "@serene-ui/shared-frontend/shared";

export interface UseConnectionMutationsReturn {
    handleAddConnection: (connection: ConnectionSchema) => Promise<boolean>;
    handleUpdateConnection: (connection: ConnectionSchema) => Promise<boolean>;
    handleDeleteConnection: (id: number) => Promise<boolean>;
    isAddPending: boolean;
    isUpdatePending: boolean;
    isDeletePending: boolean;
}

export const useConnectionMutations = (
    onTestConnection: (connection: ConnectionSchema) => Promise<void>,
    setCurrentConnection: (connection: ConnectionSchema) => void,
): UseConnectionMutationsReturn => {
    const { mutateAsync: addConnection, isPending: isAddRequestPending } =
        useAddConnection();
    const { mutateAsync: updateConnection, isPending: isUpdateRequestPending } =
        useUpdateConnection();
    const { mutateAsync: deleteConnection, isPending: isDeleteRequestPending } =
        useDeleteConnection();

    const showError = useCallback(
        (title: string, error: unknown, fallbackMessage: string) => {
            const description = getErrorMessage(error, fallbackMessage);
            toast.error(title, {
                duration: 3000,
                description: description === title ? undefined : description,
                action: {
                    label: "Close",
                    onClick: (e) => e.stopPropagation(),
                },
            });
        },
        [],
    );

    /**
     * Tests a connection and shows an error if it fails.
     *
     * @param connection - The connection to test.
     * @returns A promise that resolves to a boolean indicating success.
     */
    const testConnectionOrFail = useCallback(
        async (connection: ConnectionSchema): Promise<boolean> => {
            try {
                await onTestConnection(connection);
            } catch (error) {
                showError(
                    "Connection test failed",
                    error,
                    "Connection test failed",
                );
                return false;
            }

            return true;
        },
        [onTestConnection, showError],
    );

    /**
     * Prepares a connection payload with default values.
     *
     * @param connection - The connection to prepare.
     * @returns The prepared connection object.
     */
    const prepareConnectionPayload = useCallback(
        (connection: ConnectionSchema): ConnectionSchema => {
            if (connection.mode === "host") {
                return {
                    ...connection,
                    name: connection.name || "localhost",
                    user: connection.user || "postgres",
                    port: connection.port ? Number(connection.port) : 5432,
                    host: connection.host || "localhost",
                };
            }

            return {
                ...connection,
                name: connection.name || "localhost",
                user: connection.user || "postgres",
                port: connection.port ? Number(connection.port) : 5432,
            };
        },
        [],
    );

    /**
     * Adds a new connection.
     *
     * @param connection - The connection to add.
     * @returns A promise that resolves to a boolean indicating success.
     */
    const handleAddConnection = useCallback(
        async (connection: ConnectionSchema): Promise<boolean> => {
            if (!(await testConnectionOrFail(connection))) return false;

            try {
                const data = await addConnection(
                    prepareConnectionPayload(connection),
                );

                setCurrentConnection(data);
                toast.success("Connection successfully added!");
            } catch (error) {
                console.error("Error during connection addition:", error);
                showError(
                    "Failed to add connection",
                    error,
                    "Failed to add connection",
                );
                return false;
            }

            return true;
        },
        [
            testConnectionOrFail,
            addConnection,
            prepareConnectionPayload,
            setCurrentConnection,
            showError,
        ],
    );

    /**
     * Updates an existing connection.
     *
     * @param connection - The connection to update.
     * @returns A promise that resolves to a boolean indicating success.
     */
    const handleUpdateConnection = useCallback(
        async (connection: ConnectionSchema): Promise<boolean> => {
            if (!(await testConnectionOrFail(connection))) return false;

            try {
                const data = await updateConnection(
                    prepareConnectionPayload(connection),
                );

                setCurrentConnection(data);
                toast.success("Connection successfully updated!");
            } catch (error) {
                console.error("Error during connection update:", error);
                showError(
                    "Failed to update connection",
                    error,
                    "Failed to update connection",
                );
                return false;
            }

            return true;
        },
        [
            testConnectionOrFail,
            updateConnection,
            prepareConnectionPayload,
            setCurrentConnection,
            showError,
        ],
    );

    /**
     * Deletes a connection.
     *
     * @param id - The ID of the connection to delete.
     * @returns A promise that resolves to a boolean indicating success.
     */
    const handleDeleteConnection = useCallback(
        async (id: number): Promise<boolean> => {
            try {
                await deleteConnection({
                    id,
                });

                setCurrentConnection({
                    id: -1,
                    name: "",
                    host: "",
                    port: "" as any,
                    user: "",
                    password: "",
                    database: "",
                    mode: "host",
                    type: "postgres",
                    ssl: false,
                    authMethod: "password",
                });

                toast.success("Connection successfully deleted!");
                return true;
            } catch (error) {
                console.error("Error during connection deletion:", error);
                showError(
                    "Failed to delete connection",
                    error,
                    "Failed to delete connection",
                );
                return false;
            }
        },
        [deleteConnection, showError],
    );

    return {
        handleAddConnection,
        handleUpdateConnection,
        handleDeleteConnection,
        isAddPending: isAddRequestPending,
        isUpdatePending: isUpdateRequestPending,
        isDeletePending: isDeleteRequestPending,
    };
};
