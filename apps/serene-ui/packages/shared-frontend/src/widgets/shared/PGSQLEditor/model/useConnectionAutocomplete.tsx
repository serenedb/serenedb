import { useEffect, useMemo, useRef, useState } from "react";
import {
    useConnection,
    useExecuteQuery,
    useGetQueryHistory,
    useGetSavedQueries,
} from "@serene-ui/shared-frontend/entities";
import { useSchemaMetadataVersion } from "@serene-ui/shared-frontend/shared";
import type { QueryExecutionResultSchema } from "@serene-ui/shared-core";

type AutocompleteData = {
    tables: string[];
    systemTables: string[];
    views: string[];
    indexes: string[];
    sequences: string[];
    schemas: string[];
    columns: string[];
    savedQueries: Array<{
        name: string;
        query: string;
    }>;
    queryHistory: Array<{
        query: string;
        executedAt: string;
    }>;
};

type SharedAutocompleteData = Omit<
    AutocompleteData,
    "savedQueries" | "queryHistory"
>;

type AutocompleteKey = keyof SharedAutocompleteData;

type CachedAutocompleteEntry = {
    data: SharedAutocompleteData;
    loadedAt: number;
    version: number;
    promise?: Promise<SharedAutocompleteData>;
};

const EMPTY_SHARED_AUTOCOMPLETE: SharedAutocompleteData = {
    tables: [],
    systemTables: [],
    views: [],
    indexes: [],
    sequences: [],
    schemas: [],
    columns: [],
};

const EMPTY_AUTOCOMPLETE: AutocompleteData = {
    ...EMPTY_SHARED_AUTOCOMPLETE,
    savedQueries: [],
    queryHistory: [],
};

const AUTOCOMPLETE_CACHE_TTL_MS = 30_000;

const AUTOCOMPLETE_QUERIES: ReadonlyArray<[AutocompleteKey, string]> = [
    [
        "tables",
        `SELECT c.relname as name
         FROM pg_class c
         JOIN pg_namespace n ON n.oid = c.relnamespace
         WHERE c.relkind IN ('r', 'p')
           AND n.nspname NOT LIKE 'pg_%'
           AND n.nspname <> 'information_schema';`,
    ],
    [
        "systemTables",
        `SELECT c.relname as name
         FROM pg_class c
         JOIN pg_namespace n ON n.oid = c.relnamespace
         WHERE c.relkind IN ('r', 'p')
           AND (
               n.nspname LIKE 'pg_%'
               OR n.nspname = 'information_schema'
           );`,
    ],
    [
        "views",
        "SELECT relname as name FROM pg_class WHERE relkind = 'v';",
    ],
    ["indexes", "SELECT indexname as name FROM pg_indexes;"],
    [
        "sequences",
        "SELECT relname as name FROM pg_class WHERE relkind = 'S';",
    ],
    [
        "schemas",
        `SELECT nspname as name
         FROM pg_namespace
         WHERE nspname NOT LIKE 'pg_%'
           AND nspname <> 'information_schema';`,
    ],
    [
        "columns",
        `SELECT DISTINCT a.attname as name
         FROM pg_attribute a
         JOIN pg_class c ON c.oid = a.attrelid
         JOIN pg_namespace n ON n.oid = c.relnamespace
         WHERE a.attnum > 0
           AND NOT a.attisdropped
           AND c.relkind IN ('r', 'p', 'v', 'm', 'f')
           AND n.nspname NOT LIKE 'pg_%'
           AND n.nspname <> 'information_schema';`,
    ],
];

const AUTOCOMPLETE_QUERY = AUTOCOMPLETE_QUERIES.map(([, query]) =>
    query.trim(),
).join("\n\n");

const autocompleteCache = new Map<string, CachedAutocompleteEntry>();

const toAutocompleteCacheKey = (connectionId: number, database: string) =>
    `${connectionId}:${database}`;

const extractNames = (rows: unknown[] | undefined) =>
    Array.from(
        new Set(
            (rows ?? [])
                .filter(
                    (row): row is { name: string } =>
                        typeof (row as { name?: unknown })?.name === "string",
                )
                .map((row) => row.name),
        ),
    ).sort();

const parseAutocompleteResults = (
    results: QueryExecutionResultSchema[] | undefined,
): SharedAutocompleteData =>
    AUTOCOMPLETE_QUERIES.reduce<SharedAutocompleteData>(
        (accumulator, [key], index) => {
            accumulator[key] = extractNames(results?.[index]?.rows);
            return accumulator;
        },
        {
            ...EMPTY_SHARED_AUTOCOMPLETE,
        },
    );

const isCacheFresh = (entry: CachedAutocompleteEntry) =>
    Date.now() - entry.loadedAt < AUTOCOMPLETE_CACHE_TTL_MS;

const loadAutocompleteData = async (
    executeQuery: (
        input: {
            connectionId: number;
            database: string;
            query: string;
        },
    ) => Promise<{
        results?: QueryExecutionResultSchema[];
    }>,
    connectionId: number,
    database: string,
) => {
    const response = await executeQuery({
        connectionId,
        database,
        query: AUTOCOMPLETE_QUERY,
    });

    return parseAutocompleteResults(response.results);
};

const getOrLoadAutocompleteData = (
    cacheKey: string,
    version: number,
    executeQuery: (
        input: {
            connectionId: number;
            database: string;
            query: string;
        },
    ) => Promise<{
        results?: QueryExecutionResultSchema[];
    }>,
    connectionId: number,
    database: string,
) => {
    const cachedEntry = autocompleteCache.get(cacheKey);
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

    const promise = loadAutocompleteData(
        executeQuery,
        connectionId,
        database,
    )
        .then((data) => {
            autocompleteCache.set(cacheKey, {
                data,
                loadedAt: Date.now(),
                version,
            });

            return data;
        })
        .catch((error) => {
            autocompleteCache.delete(cacheKey);
            throw error;
        });

    autocompleteCache.set(cacheKey, {
        data: cachedEntry?.data ?? EMPTY_SHARED_AUTOCOMPLETE,
        loadedAt: cachedEntry?.loadedAt ?? 0,
        version,
        promise,
    });

    return promise;
};

export const useConnectionAutocomplete = () => {
    const { currentConnection } = useConnection();
    const { mutateAsync: executeQuery } =
        useExecuteQuery<QueryExecutionResultSchema[]>();
    const { data: savedQueries } = useGetSavedQueries();
    const { data: queryHistory } = useGetQueryHistory();

    const executeQueryRef = useRef(executeQuery);
    useEffect(() => {
        executeQueryRef.current = executeQuery;
    }, [executeQuery]);

    const [sharedAutocomplete, setSharedAutocomplete] =
        useState<SharedAutocompleteData>(EMPTY_SHARED_AUTOCOMPLETE);

    const connectionId = currentConnection.connectionId;
    const database = currentConnection.database?.trim();
    const schemaMetadataVersion = useSchemaMetadataVersion({
        level: "database",
        connectionId,
        database,
    });
    useEffect(() => {
        if (!connectionId || connectionId === -1 || !database) {
            setSharedAutocomplete(EMPTY_SHARED_AUTOCOMPLETE);
            return;
        }

        const cacheKey = toAutocompleteCacheKey(connectionId, database);
        const cachedEntry = autocompleteCache.get(cacheKey);
        let isCancelled = false;

        if (
            cachedEntry?.data &&
            cachedEntry.version === schemaMetadataVersion
        ) {
            setSharedAutocomplete(cachedEntry.data);
        } else {
            setSharedAutocomplete(EMPTY_SHARED_AUTOCOMPLETE);
        }

        getOrLoadAutocompleteData(
            cacheKey,
            schemaMetadataVersion,
            executeQueryRef.current,
            connectionId,
            database,
        )
            .then((data) => {
                if (!isCancelled) {
                    setSharedAutocomplete(data);
                }
            })
            .catch((error) => {
                if (!isCancelled) {
                    console.error(
                        "Failed to load connection autocomplete:",
                        error,
                    );
                    setSharedAutocomplete(EMPTY_SHARED_AUTOCOMPLETE);
                }
            });

        return () => {
            isCancelled = true;
        };
    }, [connectionId, database, schemaMetadataVersion]);

    const normalizedSavedQueries = useMemo(
        () =>
            Array.from(
                new Map(
                    (savedQueries ?? [])
                        .filter(
                            (query) =>
                                typeof query.name === "string" &&
                                query.name.length > 0 &&
                                typeof query.query === "string" &&
                                query.query.length > 0,
                        )
                        .map((query) => [
                            `${query.query}\u0000${query.name}`,
                            {
                                name: query.name,
                                query: query.query,
                            },
                        ]),
                ).values(),
            ).sort(
                (left, right) =>
                    left.name.localeCompare(right.name) ||
                    left.query.localeCompare(right.query),
            ),
        [savedQueries],
    );

    const normalizedQueryHistory = useMemo(
        () =>
            Array.from(
                new Map(
                    (queryHistory ?? [])
                        .filter(
                            (query) =>
                                typeof query.query === "string" &&
                                query.query.length > 0 &&
                                typeof query.executed_at === "string" &&
                                query.executed_at.length > 0,
                        )
                        .map((query) => [
                            `${query.query}\u0000${query.executed_at}`,
                            {
                                query: query.query,
                                executedAt: query.executed_at,
                            },
                        ]),
                ).values(),
            ),
        [queryHistory],
    );

    return useMemo(
        () => ({
            ...EMPTY_AUTOCOMPLETE,
            ...sharedAutocomplete,
            savedQueries: normalizedSavedQueries,
            queryHistory: normalizedQueryHistory,
        }),
        [normalizedQueryHistory, normalizedSavedQueries, sharedAutocomplete],
    );
};
