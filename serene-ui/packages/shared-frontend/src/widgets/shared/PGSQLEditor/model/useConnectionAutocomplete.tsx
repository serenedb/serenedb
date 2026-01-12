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
    savedQueries: string[];
    queryHistory: string[];
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

        const names = Array.from(
            new Set(
                (result.result as unknown[])
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
            new Set(savedQueries.map((q) => q.query)),
        ).sort() as string[];

        setAutocomplete((prev) => ({
            ...prev,
            savedQueries: names,
        }));
    }, [savedQueries]);

    useEffect(() => {
        if (!queryHistory) return;

        const names = Array.from(
            new Set(queryHistory.map((q) => q.query)),
        ).sort();

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
