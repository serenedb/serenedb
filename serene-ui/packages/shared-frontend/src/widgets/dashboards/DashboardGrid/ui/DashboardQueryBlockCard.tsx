import React from "react";
import type { DashboardBlockSchema, DashboardSchema } from "@serene-ui/shared-core";
import {
    Button,
    Table,
    TableBody,
    TableCell,
    TableHead,
    TableHeader,
    TableRow,
} from "../../../../shared/ui";
import {
    type DashboardChartBlock,
    type DashboardColumnMeta,
    type DashboardQueryRow,
    collectDashboardColumnMetadata,
    formatDashboardDateValue,
    formatDashboardDisplayValue,
    normalizeDashboardChartValue,
    parseDashboardNumericValue,
} from "../../model/dashboardChartColumns";
import {
    isDashboardInteractiveBlock,
    syncDashboardInteractiveSelection,
} from "../model/interactiveSelection";
import { useDashboardQueryBlock } from "../model/useDashboardQueryBlock";
import { DashboardChartCardBase } from "./cards/DashboardChartCardBase";
import { DashboardHorizontalBarchartCard } from "./cards/DashboardHorizontalBarchartCard";
import { DashboardInteractiveChartCard } from "./cards/DashboardInteractiveBarchartCard";
import { DashboardInteractiveLinechartCard } from "./cards/DashboardInteractiveLinechartCard";
import { DashboardInteractivePiechartCard } from "./cards/DashboardInteractivePiechartCard";
import { DashboardLinechartCard } from "./cards/DashboardLinechartCard";
import { DashboardPiechartCard } from "./cards/DashboardPiechartCard";
import { DashboardVerticalBarchartCard } from "./cards/DashboardVerticalBarchartCard";

type DashboardQueryBlock = Extract<
    DashboardBlockSchema,
    {
        type:
            | "table"
            | "single_string"
            | "bar_chart"
            | "line_chart"
            | "pie_chart";
    }
>;

type ChartDatumValue = string | number | null | undefined;
type ChartDatum = Record<string, ChartDatumValue>;

interface DashboardQueryBlockCardProps {
    block: DashboardQueryBlock;
    dashboard?: DashboardSchema | null;
    isMoving: boolean;
    onDelete?: () => void | Promise<void>;
    onDuplicate?: () => void | Promise<void>;
    onEdit?: () => void;
}

const hasCategoryValue = (value: unknown) =>
    value !== null && value !== undefined && value !== "";

const hasNumericValue = (value: unknown) =>
    parseDashboardNumericValue(value) !== null;

const getColumnMetaByName = (columns: DashboardColumnMeta[]) =>
    new Map(columns.map((column) => [column.name, column]));

const DashboardQueryStateCard: React.FC<{
    name?: string;
    description?: string;
    title: string;
    details?: string;
    onDelete?: () => void | Promise<void>;
    onDuplicate?: () => void | Promise<void>;
    onEdit?: () => void;
}> = ({
    name,
    description,
    title,
    details,
    onDelete,
    onDuplicate,
    onEdit,
}) => {
    return (
        <DashboardChartCardBase
            name={name}
            description={description}
            onDelete={onDelete}
            onDuplicate={onDuplicate}
            onEdit={onEdit}>
            <div className="flex min-h-0 flex-1 items-center justify-center p-4">
                <div className="flex max-w-64 flex-col items-center gap-3 text-center">
                    <p className="text-sm font-medium text-primary-foreground">
                        {title}
                    </p>
                    {details ? (
                        <p className="text-xs text-muted-foreground">
                            {details}
                        </p>
                    ) : null}
                    {onEdit ? (
                        <Button
                            type="button"
                            size="small"
                            variant="secondary"
                            onMouseDown={(event) => {
                                event.stopPropagation();
                            }}
                            onClick={onEdit}>
                            Edit
                        </Button>
                    ) : null}
                </div>
            </div>
        </DashboardChartCardBase>
    );
};

const DashboardSingleStringCard: React.FC<{
    block: Extract<DashboardQueryBlock, { type: "single_string" }>;
    rows: DashboardQueryRow[];
    columnsByName: Map<string, DashboardColumnMeta>;
    onDelete?: () => void | Promise<void>;
    onDuplicate?: () => void | Promise<void>;
    onEdit?: () => void;
}> = ({ block, rows, columnsByName, onDelete, onDuplicate, onEdit }) => {
    const selectedColumn = columnsByName.get(block.column);
    const rawValue = rows[0]?.[block.column];
    const resolvedValue =
        rawValue === null || rawValue === undefined || rawValue === ""
            ? block.fallback_value
            : formatDashboardDisplayValue(rawValue, selectedColumn?.type);

    if (!resolvedValue) {
        return (
            <DashboardQueryStateCard
                name={block.name}
                description={block.description}
                title="No data returned"
                details="This query did not return a value for the selected column."
                onDelete={onDelete}
                onDuplicate={onDuplicate}
                onEdit={onEdit}
            />
        );
    }

    return (
        <DashboardChartCardBase
            name={block.name}
            description={block.description}
            onDelete={onDelete}
            onDuplicate={onDuplicate}
            onEdit={onEdit}>
            <div className="flex min-h-0 flex-1 flex-col justify-center gap-2 p-4">
                <p className="text-3xl leading-none font-semibold break-words">
                    {resolvedValue}
                </p>
                <p className="text-xs font-extrabold text-muted-foreground uppercase">
                    {block.column}
                </p>
            </div>
        </DashboardChartCardBase>
    );
};

const DashboardTableCard: React.FC<{
    block: Extract<DashboardQueryBlock, { type: "table" }>;
    rows: DashboardQueryRow[];
    columns: DashboardColumnMeta[];
    onDelete?: () => void | Promise<void>;
    onDuplicate?: () => void | Promise<void>;
    onEdit?: () => void;
}> = ({ block, rows, columns, onDelete, onDuplicate, onEdit }) => {
    const columnsByName = React.useMemo(() => getColumnMetaByName(columns), [
        columns,
    ]);
    const previewColumns = React.useMemo(() => {
        const orderedColumnNames =
            block.columns?.length
                ? block.columns
                : columns.map((column) => column.name);

        return orderedColumnNames.slice(0, 6).map((columnName) => {
            return (
                columnsByName.get(columnName) ?? {
                    name: columnName,
                    type: "null",
                    color: "",
                    isNumeric: false,
                    isDateLike: false,
                    isCategoryLike: false,
                } satisfies DashboardColumnMeta
            );
        });
    }, [block.columns, columns, columnsByName]);
    const previewRows = rows.slice(0, 10);

    if (previewColumns.length === 0) {
        return (
            <DashboardQueryStateCard
                name={block.name}
                description={block.description}
                title="No data returned"
                details="This query returned no columns to preview."
                onDelete={onDelete}
                onDuplicate={onDuplicate}
                onEdit={onEdit}
            />
        );
    }

    return (
        <DashboardChartCardBase
            name={block.name}
            description={block.description}
            onDelete={onDelete}
            onDuplicate={onDuplicate}
            onEdit={onEdit}>
            <div className="min-h-0 flex-1 overflow-auto p-2">
                <Table>
                    <TableHeader>
                        <TableRow>
                            {previewColumns.map((column) => (
                                <TableHead key={column.name}>
                                    {column.name}
                                </TableHead>
                            ))}
                        </TableRow>
                    </TableHeader>
                    <TableBody>
                        {previewRows.map((row, index) => (
                            <TableRow key={index}>
                                {previewColumns.map((column) => (
                                    <TableCell
                                        key={column.name}
                                        className="max-w-40 truncate">
                                        {formatDashboardDisplayValue(
                                            row[column.name],
                                            column.type,
                                        )}
                                    </TableCell>
                                ))}
                            </TableRow>
                        ))}
                    </TableBody>
                </Table>
            </div>
        </DashboardChartCardBase>
    );
};

const toChartRow = (row: DashboardQueryRow): ChartDatum =>
    Object.fromEntries(
        Object.entries(row).map(([key, value]) => [
            key,
            normalizeDashboardChartValue(value) as ChartDatumValue,
        ]),
    );

const getBarLineRenderState = (
    block: Extract<DashboardChartBlock, { type: "bar_chart" | "line_chart" }>,
    rows: DashboardQueryRow[],
    columnsByName: Map<string, DashboardColumnMeta>,
) => {
    const axisKey =
        block.type === "bar_chart" ? block.category_key : block.x_axis_key;
    const axisColumn = columnsByName.get(axisKey);
    const validAxisColumn =
        axisColumn && axisColumn.isCategoryLike ? axisColumn : undefined;

    if (!validAxisColumn) {
        return {
            axisColumn: undefined,
            rows: [] as ChartDatum[],
            series: [],
        };
    }

    const validSeries = block.series.filter((series) => {
        const column = columnsByName.get(series.key);
        return (
            Boolean(column?.isNumeric) && series.key !== validAxisColumn.name
        );
    });

    if (validSeries.length === 0) {
        return {
            axisColumn: validAxisColumn,
            rows: [] as ChartDatum[],
            series: [],
        };
    }

    const validRows = rows
        .filter((row) => {
            const axisValue = row[validAxisColumn.name];

            if (!hasCategoryValue(axisValue)) {
                return false;
            }

            return validSeries.some((series) =>
                hasNumericValue(row[series.key]),
            );
        })
        .map(toChartRow);

    return {
        axisColumn: validAxisColumn,
        rows: validRows,
        series: validSeries,
    };
};

const getPieRenderState = (
    block: Extract<DashboardChartBlock, { type: "pie_chart" }>,
    rows: DashboardQueryRow[],
    columnsByName: Map<string, DashboardColumnMeta>,
) => {
    const dimensionColumn = columnsByName.get(block.name_key);
    const valueColumn = columnsByName.get(block.value_key);

    if (
        !dimensionColumn?.isCategoryLike ||
        !valueColumn?.isNumeric ||
        dimensionColumn.name === valueColumn.name
    ) {
        return {
            dimensionColumn: undefined,
            valueColumn: undefined,
            rows: [] as ChartDatum[],
        };
    }

    const validRows = rows
        .filter((row) => {
            return (
                hasCategoryValue(row[dimensionColumn.name]) &&
                hasNumericValue(row[valueColumn.name])
            );
        })
        .map(toChartRow);

    return {
        dimensionColumn,
        valueColumn,
        rows: validRows,
    };
};

export const DashboardQueryBlockCard: React.FC<DashboardQueryBlockCardProps> = ({
    block,
    dashboard,
    isMoving,
    onDelete,
    onDuplicate,
    onEdit,
}) => {
    const { rows, status, errorMessage } = useDashboardQueryBlock({
        block,
        dashboard,
    });
    const columns = React.useMemo(
        () =>
            collectDashboardColumnMetadata({
                rows,
            }),
        [rows],
    );
    const columnsByName = React.useMemo(() => getColumnMetaByName(columns), [
        columns,
    ]);
    const barLineDateFormatter = React.useCallback(
        (value: string | number) => formatDashboardDateValue(value),
        [],
    );
    const interactiveSelectionValues = React.useMemo(() => {
        if (!isDashboardInteractiveBlock(block) || status !== "ready") {
            return null;
        }

        if (block.type === "bar_chart" || block.type === "line_chart") {
            return getBarLineRenderState(block, rows, columnsByName).series.map(
                (series) => series.key,
            );
        }

        const { dimensionColumn, valueColumn, rows: chartRows } =
            getPieRenderState(block, rows, columnsByName);

        if (!dimensionColumn || !valueColumn) {
            return [];
        }

        return Array.from(
            new Set(
                chartRows
                    .map((row) => row[dimensionColumn.name])
                    .filter(
                        (value): value is string | number =>
                            typeof value === "string" ||
                            typeof value === "number",
                    )
                    .map(String),
            ),
        );
    }, [block, columnsByName, rows, status]);
    const interactiveSelectionValuesKey = React.useMemo(
        () => interactiveSelectionValues?.join("\u0000") ?? "",
        [interactiveSelectionValues],
    );
    const interactiveSelectionBlockKey = React.useMemo(() => {
        if (!isDashboardInteractiveBlock(block)) {
            return "static";
        }

        if (block.type === "pie_chart") {
            return `pie:${block.interactive ? "interactive" : "static"}`;
        }

        return `${block.type}:${block.variant}`;
    }, [block]);

    React.useEffect(() => {
        if (
            !isDashboardInteractiveBlock(block) ||
            status !== "ready" ||
            !interactiveSelectionValues
        ) {
            return;
        }

        syncDashboardInteractiveSelection({
            dashboardId: dashboard?.id ?? block.dashboard_id,
            blockId: block.id,
            query: block.query,
            availableValues: interactiveSelectionValues,
        });
    }, [
        block.id,
        block.dashboard_id,
        block.query,
        dashboard?.id,
        interactiveSelectionBlockKey,
        interactiveSelectionValuesKey,
        status,
    ]);

    if (status === "missing_query") {
        return (
            <DashboardQueryStateCard
                name={block.name}
                description={block.description}
                title="Set up your chart"
                details="Add a query and choose chart params in Edit."
                onDelete={onDelete}
                onDuplicate={onDuplicate}
                onEdit={onEdit}
            />
        );
    }

    if (status === "missing_connection") {
        return (
            <DashboardQueryStateCard
                name={block.name}
                description={block.description}
                title="Set up your chart"
                details="Select a local connection in Edit."
                onDelete={onDelete}
                onDuplicate={onDuplicate}
                onEdit={onEdit}
            />
        );
    }

    if (status === "missing_database") {
        return (
            <DashboardQueryStateCard
                name={block.name}
                description={block.description}
                title="Set up your chart"
                details="Select a local database in Edit."
                onDelete={onDelete}
                onDuplicate={onDuplicate}
                onEdit={onEdit}
            />
        );
    }

    if (status === "loading") {
        return (
            <DashboardQueryStateCard
                name={block.name}
                description={block.description}
                title="Loading chart..."
                onDelete={onDelete}
                onDuplicate={onDuplicate}
                onEdit={onEdit}
            />
        );
    }

    if (status === "error") {
        return (
            <DashboardQueryStateCard
                name={block.name}
                description={block.description}
                title="Could not load data"
                details={errorMessage ?? "Failed to load dashboard card"}
                onDelete={onDelete}
                onDuplicate={onDuplicate}
                onEdit={onEdit}
            />
        );
    }

    if (block.type === "table") {
        return (
            <DashboardTableCard
                block={block}
                rows={rows}
                columns={columns}
                onDelete={onDelete}
                onDuplicate={onDuplicate}
                onEdit={onEdit}
            />
        );
    }

    if (block.type === "single_string") {
        return (
            <DashboardSingleStringCard
                block={block}
                rows={rows}
                columnsByName={columnsByName}
                onDelete={onDelete}
                onDuplicate={onDuplicate}
                onEdit={onEdit}
            />
        );
    }

    if (block.type === "bar_chart") {
        const { axisColumn, rows: chartRows, series } = getBarLineRenderState(
            block,
            rows,
            columnsByName,
        );

        if (!axisColumn || series.length === 0 || chartRows.length === 0) {
            return (
                <DashboardQueryStateCard
                    name={block.name}
                    description={block.description}
                    title="Set up your chart"
                    details="Map your query columns in Edit."
                    onDelete={onDelete}
                    onDuplicate={onDuplicate}
                    onEdit={onEdit}
                />
            );
        }

        const primarySeries = series[0];
        const formatCategoryLabel = axisColumn.isDateLike
            ? barLineDateFormatter
            : undefined;

        if (block.variant === "interactive") {
            return (
                <DashboardInteractiveChartCard
                    dashboardId={dashboard?.id ?? block.dashboard_id}
                    blockId={block.id}
                    query={block.query}
                    data={chartRows}
                    series={series}
                    xAxisKey={axisColumn.name}
                    defaultActiveKey={block.default_active_key}
                    isMoving={isMoving}
                    valueLabel={block.value_label}
                    formatXAxisTick={formatCategoryLabel}
                    formatTooltipLabel={formatCategoryLabel}
                    name={block.name}
                    description={block.description}
                    onDelete={onDelete}
                    onDuplicate={onDuplicate}
                    onEdit={onEdit}
                />
            );
        }

        if (block.variant === "horizontal") {
            return (
                <DashboardHorizontalBarchartCard
                    data={chartRows}
                    yAxisKey={axisColumn.name}
                    barKey={primarySeries?.key ?? axisColumn.name}
                    barLabel={primarySeries?.label}
                    barColor={primarySeries?.color}
                    series={series}
                    isStacked={block.is_stacked}
                    isMoving={isMoving}
                    formatYAxisTick={formatCategoryLabel}
                    formatTooltipLabel={formatCategoryLabel}
                    name={block.name}
                    description={block.description}
                    onDelete={onDelete}
                    onDuplicate={onDuplicate}
                    onEdit={onEdit}
                />
            );
        }

        return (
            <DashboardVerticalBarchartCard
                data={chartRows}
                xAxisKey={axisColumn.name}
                barKey={primarySeries?.key ?? axisColumn.name}
                barLabel={primarySeries?.label}
                barColor={primarySeries?.color}
                series={series}
                isStacked={block.is_stacked}
                isMoving={isMoving}
                formatXAxisTick={formatCategoryLabel}
                formatTooltipLabel={formatCategoryLabel}
                name={block.name}
                description={block.description}
                onDelete={onDelete}
                onDuplicate={onDuplicate}
                onEdit={onEdit}
            />
        );
    }

    if (block.type === "line_chart") {
        const { axisColumn, rows: chartRows, series } = getBarLineRenderState(
            block,
            rows,
            columnsByName,
        );

        if (!axisColumn || series.length === 0 || chartRows.length === 0) {
            return (
                <DashboardQueryStateCard
                    name={block.name}
                    description={block.description}
                    title="Set up your chart"
                    details="Map your query columns in Edit."
                    onDelete={onDelete}
                    onDuplicate={onDuplicate}
                    onEdit={onEdit}
                />
            );
        }

        const primarySeries = series[0];
        const formatCategoryLabel = axisColumn.isDateLike
            ? barLineDateFormatter
            : undefined;

        if (block.variant === "interactive") {
            return (
                <DashboardInteractiveLinechartCard
                    dashboardId={dashboard?.id ?? block.dashboard_id}
                    blockId={block.id}
                    query={block.query}
                    data={chartRows}
                    series={series}
                    xAxisKey={axisColumn.name}
                    defaultActiveKey={block.default_active_key}
                    isMoving={isMoving}
                    valueLabel={block.value_label}
                    lineType={block.line_type}
                    formatXAxisTick={formatCategoryLabel}
                    formatTooltipLabel={formatCategoryLabel}
                    name={block.name}
                    description={block.description}
                    onDelete={onDelete}
                    onDuplicate={onDuplicate}
                    onEdit={onEdit}
                />
            );
        }

        return (
            <DashboardLinechartCard
                data={chartRows}
                xAxisKey={axisColumn.name}
                lineKey={primarySeries?.key ?? axisColumn.name}
                lineLabel={primarySeries?.label}
                lineColor={primarySeries?.color}
                series={series}
                lineType={block.line_type}
                isMoving={isMoving}
                formatXAxisTick={formatCategoryLabel}
                formatTooltipLabel={formatCategoryLabel}
                name={block.name}
                description={block.description}
                onDelete={onDelete}
                onDuplicate={onDuplicate}
                onEdit={onEdit}
            />
        );
    }

    const {
        dimensionColumn,
        valueColumn,
        rows: chartRows,
    } = getPieRenderState(block, rows, columnsByName);

    if (!dimensionColumn || !valueColumn || chartRows.length === 0) {
        return (
            <DashboardQueryStateCard
                name={block.name}
                description={block.description}
                title="Set up your chart"
                details="Map your query columns in Edit."
                onDelete={onDelete}
                onDuplicate={onDuplicate}
                onEdit={onEdit}
            />
        );
    }

    const formatDimensionLabel = dimensionColumn.isDateLike
        ? barLineDateFormatter
        : undefined;

    if (block.interactive) {
        return (
            <DashboardInteractivePiechartCard
                dashboardId={dashboard?.id ?? block.dashboard_id}
                blockId={block.id}
                query={block.query}
                data={chartRows}
                nameKey={dimensionColumn.name}
                valueKey={valueColumn.name}
                defaultActiveKey={block.default_active_key}
                isMoving={isMoving}
                valueLabel={block.value_label}
                colorKey={block.color_key}
                formatSliceLabel={formatDimensionLabel}
                name={block.name}
                description={block.description}
                onDelete={onDelete}
                onDuplicate={onDuplicate}
                onEdit={onEdit}
            />
        );
    }

    return (
        <DashboardPiechartCard
            data={chartRows}
            nameKey={dimensionColumn.name}
            valueKey={valueColumn.name}
            valueLabel={block.value_label}
            colorKey={block.color_key}
            showLabels={block.show_labels}
            variant={block.variant}
            showCenterLabel={block.show_center_label}
            centerValue={block.center_value}
            centerLabel={block.center_label}
            isMoving={isMoving}
            formatSliceLabel={formatDimensionLabel}
            name={block.name}
            description={block.description}
            onDelete={onDelete}
            onDuplicate={onDuplicate}
            onEdit={onEdit}
        />
    );
};
