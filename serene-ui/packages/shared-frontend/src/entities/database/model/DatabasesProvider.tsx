import { useEffect, useState, useMemo, useRef } from "react";
import { DatabasesContext } from "./DatabasesContext";
import {
    useConnection,
    useExecuteQuery,
    useGetConnections,
} from "@serene-ui/shared-frontend";

type DatabaseRecord = { name: string };

const GET_DATABASES_QUERY = `
    SELECT datname AS name FROM pg_database WHERE datistemplate = false;
`;

export const DatabasesProvider = ({
    children,
}: {
    children: React.ReactNode;
}) => {
    const [databases, setDatabases] = useState<string[]>([]);
    const [isLoading, setIsLoading] = useState(false);
    const [error, setError] = useState<Error | null>(null);

    const { mutateAsync: executeQuery } = useExecuteQuery<DatabaseRecord[]>();
    const { data: connections } = useGetConnections();
    const { currentConnection, setCurrentConnection } = useConnection();

    const abortControllerRef = useRef<AbortController | null>(null);
    const hasLoadedRef = useRef(false);

    const connectionIdKey = currentConnection.connectionId;

    const activeConnection = useMemo(() => {
        return connections?.find(
            (c) => c.id === currentConnection.connectionId,
        );
    }, [connections, currentConnection.connectionId]);

    useEffect(() => {
        if (
            activeConnection?.database &&
            activeConnection.database.trim() !== "" &&
            activeConnection.database !== currentConnection.database
        ) {
            setCurrentConnection((prev) => ({
                ...prev,
                database: activeConnection.database,
            }));
        }
    }, [
        activeConnection?.database,
        currentConnection.database,
        setCurrentConnection,
    ]);

    useEffect(() => {
        hasLoadedRef.current = false;
        if (!connectionIdKey || connectionIdKey === -1) {
            setDatabases([]);

            return;
        }

        hasLoadedRef.current = true;

        const loadDatabases = async () => {
            abortControllerRef.current?.abort();
            abortControllerRef.current = new AbortController();
            const currentAbortController = abortControllerRef.current;

            setIsLoading(true);
            setError(null);

            try {
                const data = await executeQuery({
                    query: GET_DATABASES_QUERY,
                    connectionId: connectionIdKey,
                });

                if (currentAbortController.signal.aborted) return;

                if (!data) {
                    setDatabases([]);
                    return;
                }

                setDatabases(data.result.map((database) => database.name));
            } catch (err) {
                if (currentAbortController.signal.aborted) return;
                console.error("Failed to fetch databases:", err);
                setError(
                    err instanceof Error ? err : new Error("Unknown error"),
                );
                setDatabases([]);
            } finally {
                if (!currentAbortController.signal.aborted) {
                    setIsLoading(false);
                }
            }
        };

        loadDatabases();
    }, [connectionIdKey]);

    const fetchDatabases = useMemo(() => {
        return async () => {
            hasLoadedRef.current = false;
            abortControllerRef.current?.abort();
            abortControllerRef.current = new AbortController();
            const currentAbortController = abortControllerRef.current;

            if (!connectionIdKey || connectionIdKey === -1) {
                setDatabases([]);
                return;
            }

            setIsLoading(true);
            setError(null);

            try {
                const data = await executeQuery({
                    query: GET_DATABASES_QUERY,
                    connectionId: connectionIdKey,
                });

                if (currentAbortController.signal.aborted) return;

                if (!data) {
                    setDatabases([]);
                    return;
                }

                setDatabases(data.result.map((database) => database.name));
                hasLoadedRef.current = true;
            } catch (err) {
                if (currentAbortController.signal.aborted) return;
                console.error("Failed to fetch databases:", err);
                setError(
                    err instanceof Error ? err : new Error("Unknown error"),
                );
                setDatabases([]);
            } finally {
                if (!currentAbortController.signal.aborted) {
                    setIsLoading(false);
                }
            }
        };
    }, [connectionIdKey]);

    const contextValue = useMemo(
        () => ({
            databases,
            isLoading,
            error,
            refetchDatabases: fetchDatabases,
        }),
        [databases, isLoading, error, fetchDatabases],
    );

    return (
        <DatabasesContext.Provider value={contextValue}>
            {children}
        </DatabasesContext.Provider>
    );
};
