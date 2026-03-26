import { useEffect, useRef, useState, useCallback } from "react";
import { useQueryResults } from "@serene-ui/shared-frontend/features";
import {
    useConnection,
    useGetQueryHistory,
    useGetSavedQueries,
} from "@serene-ui/shared-frontend/entities";
import { useQuerySubscription } from "@serene-ui/shared-frontend/features";

type AutocompleteData = {
    tables: string[];
    views: string[];
    indexes: string[];
    savedQueries: Array<{
        name: string;
        query: string;
    }>;
    queryHistory: Array<{
        query: string;
        executedAt: string;
    }>;
};

type AutocompleteKey = "tables" | "views" | "indexes";

export const useConnectionAutocomplete = () => {
    const { currentConnection } = useConnection();
    const { executeQuery } = useQueryResults();
    const { data: savedQueries } = useGetSavedQueries();
    const { data: queryHistory } = useGetQueryHistory();

    const executeQueryRef = useRef(executeQuery);
    useEffect(() => {
        executeQueryRef.current = executeQuery;
    });

    const dataRef = useRef<
        Omit<AutocompleteData, "savedQueries" | "queryHistory">
    >({
        tables: [],
        views: [],
        indexes: [],
    });

    const jobMapRef = useRef<Record<number, AutocompleteKey>>({});
    const subscribedJobs = useRef<number[]>([]);

    const [autocomplete, setAutocomplete] = useState<AutocompleteData>({
        tables: [],
        views: [],
        indexes: [],
        savedQueries: [],
        queryHistory: [],
    });

    const handleResult = useCallback((jobId: number, result: any) => {
        if (result?.status !== "success") return;

        const key = jobMapRef.current[jobId];
        if (!key) return;

        const rows = Array.isArray(result.results?.[0]?.rows)
            ? result.results[0].rows
            : [];

        const names = Array.from(
            new Set(
                (rows as unknown[])
                    .filter(
                        (r): r is { name: string } =>
                            typeof (r as any)?.name === "string",
                    )
                    .map((r) => r.name),
            ),
        ).sort();

        if (dataRef.current[key].join("|") === names.join("|")) return;

        dataRef.current[key] = names;

        setAutocomplete((prev) => ({
            ...prev,
            [key]: names,
        }));
    }, []);

    useQuerySubscription(subscribedJobs.current, handleResult);

    useEffect(() => {
        if (!savedQueries) return;

        const names = Array.from(
            new Map(
                savedQueries
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
        );

        setAutocomplete((prev) => ({
            ...prev,
            savedQueries: names,
        }));
    }, [savedQueries]);

    useEffect(() => {
        if (!queryHistory) return;

        const names = Array.from(
            new Map(
                queryHistory
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
        );

        setAutocomplete((prev) => ({
            ...prev,
            queryHistory: names,
        }));
    }, [queryHistory]);

    useEffect(() => {
        const { connectionId, database } = currentConnection;

        if (!connectionId || !database) {
            dataRef.current = { tables: [], views: [], indexes: [] };
            jobMapRef.current = {};
            subscribedJobs.current = [];
            setAutocomplete((prev) => ({
                ...prev,
                tables: [],
                views: [],
                indexes: [],
            }));
            return;
        }

        jobMapRef.current = {};
        subscribedJobs.current = [];
        dataRef.current = { tables: [], views: [], indexes: [] };

        setAutocomplete((prev) => ({
            ...prev,
            tables: [],
            views: [],
            indexes: [],
        }));

        const load = async () => {
            const queries: [AutocompleteKey, string][] = [
                [
                    "tables",
                    "SELECT relname as name FROM pg_class WHERE relkind = 'r'",
                ],
                [
                    "views",
                    "SELECT relname as name FROM pg_class WHERE relkind = 'v'",
                ],
                ["indexes", "SELECT indexname as name FROM pg_indexes"],
            ];

            for (const [key, sql] of queries) {
                const res = await executeQueryRef.current(sql);
                if (!res.success) continue;

                jobMapRef.current[res.jobId] = key;
                subscribedJobs.current.push(res.jobId);
            }
        };

        load();
    }, [currentConnection.connectionId, currentConnection.database]);

    return autocomplete;
};
