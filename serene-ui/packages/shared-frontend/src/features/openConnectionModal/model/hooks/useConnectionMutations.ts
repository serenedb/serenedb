import { useCallback } from "react";
import { toast } from "sonner";
import {
    useAddConnection,
    useDeleteConnection,
    useUpdateConnection,
} from "@serene-ui/shared-frontend/entities";
import { ConnectionSchema } from "@serene-ui/shared-core";

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

    const showError = useCallback((title: string, description?: string) => {
        toast.error(title, {
            duration: 3000,
            description,
            action: {
                label: "Close",
                onClick: (e) => e.stopPropagation(),
            },
        });
    }, []);

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
                const message =
                    error instanceof Error ? error.message : String(error);
                showError("Connection test failed", message);
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
                showError("An unexpected error occurred during the addition.");
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
                showError("An unexpected error occurred during the update.");
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
                showError("An unexpected error occurred during deletion.");
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
