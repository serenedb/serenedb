import { useEffect, useState, useMemo, useRef } from "react";
import { DatabasesContext } from "./DatabasesContext";
import {
    useConnection,
    useExecuteQuery,
    useGetConnections,
} from "@serene-ui/shared-frontend";
import { useSchemaMetadataVersion } from "@serene-ui/shared-frontend/shared";
import type { QueryExecutionResultSchema } from "@serene-ui/shared-core";

type DatabaseRecord = { name: string };

type CachedDatabasesEntry = {
    data: string[];
    loadedAt: number;
    version: number;
    promise?: Promise<string[]>;
};

const GET_DATABASES_QUERY = `
    SELECT datname AS name FROM pg_database WHERE datistemplate = false;
`;

const DATABASES_CACHE_TTL_MS = 30_000;
const databasesCache = new Map<string, CachedDatabasesEntry>();

const getDatabasesCacheKey = (connectionId: number) => `${connectionId}`;

const isCacheFresh = (entry: CachedDatabasesEntry) =>
    Date.now() - entry.loadedAt < DATABASES_CACHE_TTL_MS;

const extractDatabases = (data: {
    results?: QueryExecutionResultSchema[];
}): string[] =>
    (data.results?.[0]?.rows || []).map(
        (database) => (database as DatabaseRecord).name,
    );

const loadDatabasesForConnection = async (
    executeQuery: (
        input: {
            query: string;
            connectionId: number;
        },
    ) => Promise<{
        results?: QueryExecutionResultSchema[];
    }>,
    connectionId: number,
) => {
    const data = await executeQuery({
        query: GET_DATABASES_QUERY,
        connectionId,
    });

    return extractDatabases(data);
};

const getOrLoadDatabasesForConnection = (
    executeQuery: (
        input: {
            query: string;
            connectionId: number;
        },
    ) => Promise<{
        results?: QueryExecutionResultSchema[];
    }>,
    connectionId: number,
    version: number,
) => {
    const cacheKey = getDatabasesCacheKey(connectionId);
    const cachedEntry = databasesCache.get(cacheKey);

    if (
        cachedEntry &&
        cachedEntry.version === version &&
        isCacheFresh(cachedEntry)
    ) {
        return Promise.resolve(cachedEntry.data);
    }

    if (cachedEntry?.promise && cachedEntry.version === version) {
        return cachedEntry.promise;
    }

    const promise = loadDatabasesForConnection(executeQuery, connectionId)
        .then((databases) => {
            databasesCache.set(cacheKey, {
                data: databases,
                loadedAt: Date.now(),
                version,
            });

            return databases;
        })
        .catch((error) => {
            databasesCache.delete(cacheKey);
            throw error;
        });

    databasesCache.set(cacheKey, {
        data: cachedEntry?.data ?? [],
        loadedAt: cachedEntry?.loadedAt ?? 0,
        version,
        promise,
    });

    return promise;
};

export const DatabasesProvider = ({
    children,
}: {
    children: React.ReactNode;
}) => {
    const [databases, setDatabases] = useState<string[]>([]);
    const [isLoading, setIsLoading] = useState(false);
    const [error, setError] = useState<Error | null>(null);

    const { mutateAsync: executeQuery } =
        useExecuteQuery<QueryExecutionResultSchema[]>();
    const { data: connections } = useGetConnections();
    const { currentConnection, setCurrentConnection } = useConnection();

    const abortControllerRef = useRef<AbortController | null>(null);
    const hasLoadedRef = useRef(false);
    const autoSelectedConnectionIdRef = useRef<number | null>(null);

    const connectionIdKey = currentConnection.connectionId;
    const connectionMetadataVersion = useSchemaMetadataVersion({
        level: "connection",
        connectionId: connectionIdKey,
    });

    const activeConnection = useMemo(() => {
        return connections?.find(
            (c) => c.id === currentConnection.connectionId,
        );
    }, [connections, currentConnection.connectionId]);

    useEffect(() => {
        if (!connectionIdKey || connectionIdKey === -1) {
            autoSelectedConnectionIdRef.current = null;
            return;
        }

        if (!activeConnection) {
            return;
        }

        if (autoSelectedConnectionIdRef.current === connectionIdKey) {
            return;
        }

        autoSelectedConnectionIdRef.current = connectionIdKey;

        const defaultDatabase = activeConnection.database?.trim();

        // Apply the connection's default database only once per connection
        // switch so it doesn't fight with database validation in the combobox.
        if (!defaultDatabase) {
            return;
        }

        setCurrentConnection((prev) => {
            if (prev.connectionId !== connectionIdKey || prev.database) {
                return prev;
            }

            return {
                ...prev,
                database: defaultDatabase,
            };
        });
    }, [activeConnection, connectionIdKey, setCurrentConnection]);

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
                const nextDatabases = await getOrLoadDatabasesForConnection(
                    executeQuery,
                    connectionIdKey,
                    connectionMetadataVersion,
                );

                if (currentAbortController.signal.aborted) return;

                setDatabases(nextDatabases);
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
    }, [connectionIdKey, connectionMetadataVersion, executeQuery]);

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
                const cacheKey = getDatabasesCacheKey(connectionIdKey);
                databasesCache.delete(cacheKey);
                const nextDatabases = await loadDatabasesForConnection(
                    executeQuery,
                    connectionIdKey,
                );

                if (currentAbortController.signal.aborted) return;

                databasesCache.set(cacheKey, {
                    data: nextDatabases,
                    loadedAt: Date.now(),
                    version: connectionMetadataVersion,
                });

                setDatabases(nextDatabases);
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
    }, [connectionIdKey, connectionMetadataVersion, executeQuery]);

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
