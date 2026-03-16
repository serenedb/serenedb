import React from "react";
import type { QueryExecutionResultSchema } from "@serene-ui/shared-core";
import { useExecuteQuery, useGetConnections } from "../../../../entities";
import { getErrorMessage } from "../../../../shared";
import {
    type DashboardChartBlock,
    type DashboardQueryRow,
    DASHBOARD_CHART_COLORS,
    collectDashboardColumnMetadata,
} from "../../model/dashboardChartColumns";

const GET_DATABASES_QUERY = `
    SELECT datname AS name FROM pg_database WHERE datistemplate = false;
`;

const getNextSeries = (
    block: Extract<DashboardChartBlock, { type: "bar_chart" | "line_chart" }>,
    columnName: string,
    checked: boolean,
) => {
    const isSelected = block.series.some((item) => item.key === columnName);

    if (checked && !isSelected) {
        return [
            ...block.series,
            {
                key: columnName,
                label: columnName,
                color:
                    DASHBOARD_CHART_COLORS[
                        block.series.length % DASHBOARD_CHART_COLORS.length
                    ],
            },
        ];
    }

    if (!checked && isSelected) {
        if (block.series.length <= 1) {
            return block.series;
        }

        return block.series.filter((item) => item.key !== columnName);
    }

    return block.series;
};

interface UseDashboardSelectChartParamsProps {
    block: DashboardChartBlock;
    onBlockChange?: (block: DashboardChartBlock) => void;
}

export const useDashboardSelectChartParams = ({
    block,
    onBlockChange,
}: UseDashboardSelectChartParamsProps) => {
    const { data: connections } = useGetConnections();
    const { mutateAsync: executeQuery, isPending: isRowsPending } =
        useExecuteQuery<QueryExecutionResultSchema[]>();
    const { mutateAsync: executeDatabaseQuery, isPending: isDatabasesPending } =
        useExecuteQuery<QueryExecutionResultSchema[]>();
    const requestIdRef = React.useRef(0);
    const databasesRequestIdRef = React.useRef(0);
    const [rows, setRows] = React.useState<DashboardQueryRow[]>([]);
    const [databases, setDatabases] = React.useState<string[]>([]);
    const [error, setError] = React.useState<string | null>(null);

    const selectedConnection = React.useMemo(
        () =>
            connections?.find(
                (connection) => connection.id === block.connection_id,
            ),
        [block.connection_id, connections],
    );

    const currentDatabase =
        block.database ?? selectedConnection?.database ?? "";

    const handleConnectionChange = React.useCallback(
        (connectionId: number) => {
            const nextConnectionId =
                connectionId === -1 ? undefined : connectionId;

            if (block.connection_id === nextConnectionId) {
                return;
            }

            const nextBlock = {
                ...block,
                connection_id: nextConnectionId,
                database:
                    nextConnectionId === undefined
                        ? undefined
                        : (connections?.find(
                              (connection) =>
                                  connection.id === nextConnectionId,
                          )?.database ?? undefined),
            } satisfies DashboardChartBlock;

            onBlockChange?.(nextBlock);
        },
        [block, connections, onBlockChange],
    );

    const handleDatabaseChange = React.useCallback(
        (database: string) => {
            onBlockChange?.({
                ...block,
                database: database || undefined,
            });
        },
        [block, onBlockChange],
    );

    React.useEffect(() => {
        if (block.connection_id === undefined) {
            setDatabases([]);
            return;
        }

        const requestId = databasesRequestIdRef.current + 1;
        databasesRequestIdRef.current = requestId;

        void (async () => {
            try {
                const response = await executeDatabaseQuery({
                    query: GET_DATABASES_QUERY,
                    connectionId: block.connection_id,
                    limit: 1000,
                });

                if (databasesRequestIdRef.current !== requestId) {
                    return;
                }

                setDatabases(
                    (
                        (response.results?.[0]?.rows ?? []) as Array<{
                            name?: unknown;
                        }>
                    )
                        .map((row) =>
                            typeof row.name === "string" ? row.name : undefined,
                        )
                        .filter((database): database is string =>
                            Boolean(database),
                        ),
                );
            } catch {
                if (databasesRequestIdRef.current !== requestId) {
                    return;
                }

                setDatabases([]);
            }
        })();
    }, [block.connection_id, executeDatabaseQuery]);

    React.useEffect(() => {
        if (!block.database) {
            return;
        }

        if (databases.length === 0) {
            return;
        }

        if (databases.includes(block.database)) {
            return;
        }

        handleDatabaseChange("");
    }, [block.database, databases, handleDatabaseChange]);

    React.useEffect(() => {
        const query = block.query.trim();

        if (!query) {
            setRows([]);
            setError("Add a query to inspect its columns.");
            return;
        }

        if (block.connection_id === undefined) {
            setRows([]);
            setError("Select a local connection for this query.");
            return;
        }

        if (!selectedConnection) {
            setRows([]);
            setError("Selected connection is not available.");
            return;
        }

        if (!currentDatabase) {
            setRows([]);
            setError("Select a local database for this query.");
            return;
        }

        const requestId = requestIdRef.current + 1;
        requestIdRef.current = requestId;
        setError(null);

        void (async () => {
            try {
                const response = await executeQuery({
                    query,
                    connectionId: block.connection_id,
                    database: currentDatabase,
                    limit: 100,
                });

                if (requestIdRef.current !== requestId) {
                    return;
                }

                setRows((response.results?.[0]?.rows ?? []) as DashboardQueryRow[]);
            } catch (nextError) {
                if (requestIdRef.current !== requestId) {
                    return;
                }

                setRows([]);
                setError(
                    getErrorMessage(
                        nextError,
                        "Failed to execute query for chart params.",
                    ),
                );
            }
        })();
    }, [
        block.query,
        block.connection_id,
        currentDatabase,
        executeQuery,
        selectedConnection,
    ]);

    const columns = React.useMemo(() => {
        const selectedSeriesColors =
            block.type === "bar_chart" || block.type === "line_chart"
                ? Object.fromEntries(
                      block.series.map((item) => [item.key, item.color]),
                  )
                : undefined;

        return collectDashboardColumnMetadata({
            rows,
            selectedSeriesColors,
        });
    }, [block, rows]);

    const handleXChange = React.useCallback(
        (columnName: string, checked: boolean) => {
            if (!checked) {
                return;
            }

            if (block.type === "bar_chart") {
                onBlockChange?.({
                    ...block,
                    category_key: columnName,
                });
                return;
            }

            if (block.type === "line_chart") {
                onBlockChange?.({
                    ...block,
                    x_axis_key: columnName,
                });
            }
        },
        [block, onBlockChange],
    );

    const handleYChange = React.useCallback(
        (columnName: string, checked: boolean) => {
            if (block.type === "pie_chart") {
                if (!checked) {
                    return;
                }

                onBlockChange?.({
                    ...block,
                    value_key: columnName,
                });
                return;
            }

            onBlockChange?.({
                ...block,
                series: getNextSeries(block, columnName, checked),
            });
        },
        [block, onBlockChange],
    );

    const handleDimensionChange = React.useCallback(
        (columnName: string, checked: boolean) => {
            if (!checked || block.type !== "pie_chart") {
                return;
            }

            onBlockChange?.({
                ...block,
                name_key: columnName,
            });
        },
        [block, onBlockChange],
    );

    return {
        columns,
        currentDatabase,
        databases,
        error,
        isDatabasesPending,
        isRowsPending,
        handleConnectionChange,
        handleDatabaseChange,
        handleXChange,
        handleYChange,
        handleDimensionChange,
    };
};
