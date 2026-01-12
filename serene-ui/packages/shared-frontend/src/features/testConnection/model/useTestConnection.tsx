import { ExecuteQueryInput } from "@serene-ui/shared-core";
import { useExecuteQuery } from "@serene-ui/shared-frontend/entities";

export const useTestConnection = () => {
    const { mutateAsync: testConnection, isPending } = useExecuteQuery();

    const handleTestConnection = async (
        connection: Omit<ExecuteQueryInput, "query">,
    ) => {
        await testConnection({
            ...connection,
            port: connection.port
                ? parseInt(connection.port.toString())
                : undefined,
            bind_vars: [],
            query: "SELECT 1;",
        });
    };

    return { handleTestConnection, isPending };
};
