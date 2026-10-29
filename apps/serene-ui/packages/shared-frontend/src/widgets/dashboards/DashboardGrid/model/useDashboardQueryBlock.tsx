import React from "react";
import type {
    DashboardBlockSchema,
    DashboardSchema,
    QueryExecutionResultSchema,
} from "@serene-ui/shared-core";
import {
    useExecuteQuery,
    useGetConnections,
} from "../../../../entities";
import { getErrorMessage } from "../../../../shared";
import { parseDashboardNumericValue } from "../../model/dashboardChartColumns";

type DashboardQueryBlock = Extract<
    DashboardBlockSchema,
    {
        type:
            | "table"
            | "single_string"
            | "bar_chart"
            | "line_chart"
            | "area_chart"
            | "pie_chart";
    }
>;

type QueryRow = Record<string, unknown>;

type DashboardQueryBlockStatus =
    | "missing_query"
    | "missing_connection"
    | "missing_database"
    | "loading"
    | "error"
    | "ready";

const QUERY_EXECUTION_DEBOUNCE_MS = 300;
const PIE_CHART_COLORS = [
    "var(--chart-1)",
    "var(--chart-2)",
    "var(--chart-3)",
    "var(--chart-4)",
    "var(--chart-5)",
    "var(--chart-6)",
    "var(--chart-7)",
    "var(--chart-8)",
    "var(--chart-9)",
    "var(--chart-10)",
] as const;

const normalizeRows = (
    block: DashboardQueryBlock,
    rows: QueryRow[],
): QueryRow[] => {
    switch (block.type) {
        case "bar_chart":
            return rows.map((row) => ({
                ...row,
                ...Object.fromEntries(
                    block.series.map((series) => [
                        series.key,
                        parseDashboardNumericValue(row[series.key]) ??
                            row[series.key],
                    ]),
                ),
            }));
        case "line_chart":
            return rows.map((row) => ({
                ...row,
                ...Object.fromEntries(
                    block.series.map((series) => [
                        series.key,
                        parseDashboardNumericValue(row[series.key]) ??
                            row[series.key],
                    ]),
                ),
            }));
        case "area_chart":
            return rows.map((row) => ({
                ...row,
                ...Object.fromEntries(
                    block.series.map((series) => [
                        series.key,
                        parseDashboardNumericValue(row[series.key]) ??
                            row[series.key],
                    ]),
                ),
            }));
        case "pie_chart":
            return rows.map((row, index) => {
                const currentColor = row[block.color_key];

                return {
                    ...row,
                    [block.value_key]:
                        parseDashboardNumericValue(row[block.value_key]) ??
                        row[block.value_key],
                    [block.color_key]:
                        typeof currentColor === "string" && currentColor.trim()
                            ? currentColor
                            : PIE_CHART_COLORS[index % PIE_CHART_COLORS.length],
                };
            });
        default:
            return rows;
    }
};

interface UseDashboardQueryBlockProps {
    block: DashboardQueryBlock;
    dashboard?: DashboardSchema | null;
    manualRefreshToken?: number;
}

export const useDashboardQueryBlock = ({
    block,
    dashboard,
    manualRefreshToken = 0,
}: UseDashboardQueryBlockProps) => {
    const { data: connections } = useGetConnections();
    const { mutateAsync: executeQuery } =
        useExecuteQuery<QueryExecutionResultSchema[]>();
    const requestIdRef = React.useRef(0);
    const [autoRefreshToken, setAutoRefreshToken] = React.useState(0);
    const [rawRows, setRawRows] = React.useState<QueryRow[]>([]);
    const [status, setStatus] =
        React.useState<DashboardQueryBlockStatus>("missing_query");
    const [errorMessage, setErrorMessage] = React.useState<string | null>(null);

    const selectedConnection = React.useMemo(
        () =>
            connections?.find(
                (connection) => connection.id === block.connection_id,
            ),
        [block.connection_id, connections],
    );

    const currentDatabase =
        block.database ?? selectedConnection?.database ?? "";
    const query = block.query.trim();
    const limit = block.custom_row_limit_enabled
        ? block.custom_row_limit
        : dashboard?.row_limit ?? 1000;
    const refreshIntervalSeconds = block.custom_refresh_interval_enabled
        ? block.custom_refresh_interval
        : dashboard?.refresh_interval ?? 60;
    const autoRefreshEnabled = block.custom_refresh_interval_enabled
        ? true
        : (dashboard?.auto_refresh ?? false);

    React.useEffect(() => {
        if (!autoRefreshEnabled || refreshIntervalSeconds <= 0) {
            return;
        }

        const intervalId = window.setInterval(() => {
            setAutoRefreshToken((currentValue) => currentValue + 1);
        }, refreshIntervalSeconds * 1000);

        return () => {
            window.clearInterval(intervalId);
        };
    }, [autoRefreshEnabled, refreshIntervalSeconds]);

    React.useEffect(() => {
        if (!query) {
            setRawRows([]);
            setErrorMessage(null);
            setStatus("missing_query");
            return;
        }

        if (block.connection_id === undefined) {
            setRawRows([]);
            setErrorMessage(null);
            setStatus("missing_connection");
            return;
        }

        if (!currentDatabase) {
            setRawRows([]);
            setErrorMessage(null);
            setStatus("missing_database");
            return;
        }

        const requestId = requestIdRef.current + 1;
        requestIdRef.current = requestId;
        setStatus("loading");
        setErrorMessage(null);

        const timeoutId = window.setTimeout(() => {
            void (async () => {
                try {
                    const response = await executeQuery({
                        query,
                        connectionId: block.connection_id,
                        database: currentDatabase,
                        limit,
                    });

                    if (requestIdRef.current !== requestId) {
                        return;
                    }

                    setRawRows((response.results?.[0]?.rows ?? []) as QueryRow[]);
                    setStatus("ready");
                } catch (error) {
                    if (requestIdRef.current !== requestId) {
                        return;
                    }

                    setRawRows([]);
                    setStatus("error");
                    setErrorMessage(
                        getErrorMessage(error, "Failed to load dashboard card"),
                    );
                }
            })();
        }, QUERY_EXECUTION_DEBOUNCE_MS);

        return () => {
            window.clearTimeout(timeoutId);
        };
    }, [
        block.connection_id,
        currentDatabase,
        executeQuery,
        limit,
        query,
        autoRefreshToken,
        manualRefreshToken,
    ]);

    const rows = React.useMemo(
        () => normalizeRows(block, rawRows),
        [block, rawRows],
    );

    return {
        rows,
        status,
        errorMessage,
    };
};
