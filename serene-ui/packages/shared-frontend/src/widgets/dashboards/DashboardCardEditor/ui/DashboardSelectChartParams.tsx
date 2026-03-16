import React from "react";
import type { QueryExecutionResultSchema } from "@serene-ui/shared-core";
import { useExecuteQuery, useGetConnections } from "../../../../entities";
import { getErrorMessage } from "../../../../shared";
import {
    ArrowDownIcon,
    Checkbox,
    CheckIcon,
    cn,
    Command,
    CommandEmpty,
    CommandGroup,
    CommandInput,
    CommandItem,
    CommandList,
    DatabaseIcon,
    Label,
    Popover,
    PopoverContent,
    PopoverTrigger,
    Skeleton,
    Table,
    TableBody,
    TableCell,
    TableHead,
    TableHeader,
    TableRow,
} from "@serene-ui/shared-frontend";
import {
    type DashboardChartBlock,
    type DashboardQueryRow,
    DASHBOARD_CHART_COLORS,
    collectDashboardColumnMetadata,
    getDashboardColumnRoleState,
} from "../../model/dashboardChartColumns";
import { ConnectionsCombobox } from "../../../shared/ConnectionsCombobox";

interface DashboardSelectChartParamsProps {
    block: DashboardChartBlock;
    onBlockChange?: (block: DashboardChartBlock) => void;
}

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
                color: DASHBOARD_CHART_COLORS[
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

const DatabasesComboboxLocal: React.FC<{
    databases: string[];
    currentDatabase: string;
    isLoading: boolean;
    setCurrentDatabase: (database: string) => void;
}> = ({ databases, currentDatabase, isLoading, setCurrentDatabase }) => {
    const [open, setOpen] = React.useState(false);

    return (
        <Popover open={open} onOpenChange={setOpen}>
            <PopoverTrigger asChild>
                <button
                    type="button"
                    aria-expanded={open}
                    aria-label="Select database"
                    className="border-border bg-secondary flex h-8 w-full items-center justify-between rounded-md border px-3 py-2 text-sm">
                    <div className="flex min-w-0 flex-1 items-center gap-2 overflow-hidden">
                        <DatabaseIcon className="shrink-0" />
                        {isLoading && databases.length === 0 ? (
                            <Skeleton className="h-4 max-w-40 flex-1" />
                        ) : (
                            <span className="block flex-1 truncate text-left text-secondary-foreground/70">
                                {currentDatabase || "Select database"}
                            </span>
                        )}
                    </div>
                    <ArrowDownIcon className={cn(open ? "rotate-180" : "")} />
                </button>
            </PopoverTrigger>
            <PopoverContent
                sideOffset={5}
                variant="secondary"
                className="w-full p-0">
                <Command className="bg-transparent">
                    <CommandInput
                        placeholder="Search databases"
                        className="h-9"
                    />
                    <CommandList>
                        <CommandEmpty>No databases found.</CommandEmpty>
                        <CommandGroup>
                            {databases.map((database) => (
                                <CommandItem
                                    key={database}
                                    value={database}
                                    onSelect={(currentValue: string) => {
                                        setCurrentDatabase(
                                            currentValue === currentDatabase
                                                ? ""
                                                : currentValue,
                                        );
                                        setOpen(false);
                                    }}>
                                    {database}
                                    <CheckIcon
                                        className={cn(
                                            "ml-auto",
                                            currentDatabase === database
                                                ? "opacity-100"
                                                : "opacity-0",
                                        )}
                                    />
                                </CommandItem>
                            ))}
                        </CommandGroup>
                    </CommandList>
                </Command>
            </PopoverContent>
        </Popover>
    );
};

export const DashboardSelectChartParams: React.FC<
    DashboardSelectChartParamsProps
> = ({ block, onBlockChange }) => {
    const { data: connections } = useGetConnections();
    const { mutateAsync: executeQuery, isPending } =
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

                setRows(
                    (response.results?.[0]?.rows ?? []) as DashboardQueryRow[],
                );
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

            const nextSeries = getNextSeries(block, columnName, checked);

            onBlockChange?.({
                ...block,
                series: nextSeries,
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

    return (
        <div className="flex flex-col gap-2 px-4">
            <div className="flex flex-col gap-2">
                <Label>Connection</Label>
                <ConnectionsCombobox
                    currentConnectionId={block.connection_id ?? -1}
                    setCurrentConnectionId={(connectionId) => {
                        handleConnectionChange(connectionId);
                    }}
                />
            </div>
            <div className="flex flex-col gap-2">
                <Label>Database</Label>
                <DatabasesComboboxLocal
                    databases={databases}
                    currentDatabase={currentDatabase}
                    isLoading={isDatabasesPending}
                    setCurrentDatabase={(database) => {
                        handleDatabaseChange(database);
                    }}
                />
            </div>
            <div className="rounded-md border mt-2">
                {isPending ? (
                    <div className="p-4 text-sm text-muted-foreground">
                        Executing query...
                    </div>
                ) : error ? (
                    <div className="p-4 text-sm text-destructive">{error}</div>
                ) : columns.length === 0 ? (
                    <div className="p-4 text-sm text-muted-foreground">
                        Query returned no rows, so column types could not be
                        inferred yet.
                    </div>
                ) : (
                    <Table className="text-xs">
                        <TableHeader>
                            <TableRow>
                                <TableHead className="w-12">Color</TableHead>
                                <TableHead>Column</TableHead>
                                <TableHead>Type</TableHead>
                                <TableHead className="w-12 text-center">
                                    X
                                </TableHead>
                                <TableHead className="w-12 text-center">
                                    Y
                                </TableHead>
                                <TableHead className="w-24 text-center">
                                    Dimension
                                </TableHead>
                            </TableRow>
                        </TableHeader>
                        <TableBody>
                            {columns.map((column) => {
                                const roleState = getDashboardColumnRoleState(
                                    block,
                                    column,
                                );

                                return (
                                    <TableRow key={column.name}>
                                        <TableCell>
                                            <div
                                                className="size-2.5 rounded-full"
                                                style={{
                                                    backgroundColor:
                                                        column.color,
                                                }}
                                            />
                                        </TableCell>
                                        <TableCell className="font-medium">
                                            {column.name}
                                        </TableCell>
                                        <TableCell className="text-muted-foreground">
                                            {column.type}
                                        </TableCell>
                                        <TableCell className="text-center">
                                            <Checkbox
                                                checked={roleState.isXChecked}
                                                disabled={roleState.isXDisabled}
                                                onCheckedChange={(checked) => {
                                                    handleXChange(
                                                        column.name,
                                                        checked === true,
                                                    );
                                                }}
                                            />
                                        </TableCell>
                                        <TableCell className="text-center">
                                            <Checkbox
                                                checked={roleState.isYChecked}
                                                disabled={roleState.isYDisabled}
                                                onCheckedChange={(checked) => {
                                                    handleYChange(
                                                        column.name,
                                                        checked === true,
                                                    );
                                                }}
                                            />
                                        </TableCell>
                                        <TableCell className="text-center">
                                            <Checkbox
                                                checked={
                                                    roleState.isDimensionChecked
                                                }
                                                disabled={
                                                    roleState.isDimensionDisabled
                                                }
                                                onCheckedChange={(checked) => {
                                                    handleDimensionChange(
                                                        column.name,
                                                        checked === true,
                                                    );
                                                }}
                                            />
                                        </TableCell>
                                    </TableRow>
                                );
                            })}
                        </TableBody>
                    </Table>
                )}
            </div>
        </div>
    );
};
