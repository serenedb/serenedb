import React from "react";
import type { DashboardSchema } from "@serene-ui/shared-core";
import { useUpdateDashboardCard } from "../../../../entities/dashboard-card";
import {
    Table,
    TableBody,
    TableCell,
    TableHead,
    TableHeader,
    TableRow,
} from "../../../../shared/ui";
import {
    type DashboardColumnMeta,
    type DashboardQueryRow,
    formatDashboardDisplayValue,
} from "../../model/dashboardChartColumns";
import {
    type DashboardQueryBlock,
    getBarLineRenderState,
    getPieRenderState,
} from "../model/dashboardQueryBlockCard";
import { useDashboardQueryBlockCardState } from "../model/useDashboardQueryBlockCardState";
import { DashboardHorizontalBarchartCard } from "./cards/DashboardHorizontalBarchartCard";
import { DashboardAreaChartCard } from "./cards/DashboardAreaChartCard";
import { DashboardInteractiveAreaChartCard } from "./cards/DashboardInteractiveAreaChartCard";
import { DashboardInteractiveChartCard } from "./cards/DashboardInteractiveBarchartCard";
import { DashboardInteractiveLinechartCard } from "./cards/DashboardInteractiveLinechartCard";
import { DashboardInteractivePiechartCard } from "./cards/DashboardInteractivePiechartCard";
import { DashboardLinechartCard } from "./cards/DashboardLinechartCard";
import { DashboardPiechartCard } from "./cards/DashboardPiechartCard";
import { DashboardQueryStateCard } from "./cards/DashboardQueryStateCard";
import { DashboardVerticalBarchartCard } from "./cards/DashboardVerticalBarchartCard";
import { DashboardChartCardBase } from "./cards/DashboardChartCardBase";
import {
    getDashboardSavedQueryDragActive,
    getDashboardSavedQueryDragPayload,
    hasDashboardSavedQueryDragData,
    setDashboardSavedQueryDragActive,
    subscribeDashboardSavedQueryDragActive,
} from "../../model/savedQueryDrag";

type DashboardQueryBlockStatus =
    | "missing_query"
    | "missing_connection"
    | "missing_database"
    | "loading"
    | "error"
    | "ready";

interface DashboardQueryBlockCardProps {
    block: DashboardQueryBlock;
    dashboard?: DashboardSchema | null;
    isMoving: boolean;
    manualRefreshToken?: number;
    onDelete?: () => void | Promise<void>;
    onDuplicate?: () => void | Promise<void>;
    onEdit?: () => void;
}

const SETUP_STATE_DETAILS: Record<
    Exclude<DashboardQueryBlockStatus, "loading" | "error" | "ready">,
    string
> = {
    missing_query: "Add a query and choose chart params in Edit.",
    missing_connection: "Select a local connection in Edit.",
    missing_database: "Select a local database in Edit.",
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
    const columnsByName = React.useMemo(
        () => new Map(columns.map((column) => [column.name, column])),
        [columns],
    );

    const previewColumns = React.useMemo(() => {
        const orderedColumnNames =
            block.columns?.length ? block.columns : columns.map((column) => column.name);

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
                                <TableHead key={column.name}>{column.name}</TableHead>
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

const renderSetupState = ({
    block,
    status,
    onDelete,
    onDuplicate,
    onEdit,
}: {
    block: DashboardQueryBlock;
    status: Exclude<DashboardQueryBlockStatus, "loading" | "error" | "ready">;
    onDelete?: () => void | Promise<void>;
    onDuplicate?: () => void | Promise<void>;
    onEdit?: () => void;
}) => {
    return (
        <DashboardQueryStateCard
            name={block.name}
            description={block.description}
            title="Set up your chart"
            details={SETUP_STATE_DETAILS[status]}
            onDelete={onDelete}
            onDuplicate={onDuplicate}
            onEdit={onEdit}
        />
    );
};

const DashboardQueryDropHintCard: React.FC<{
    name?: string;
    description?: string;
    onDelete?: () => void | Promise<void>;
    onDuplicate?: () => void | Promise<void>;
    onEdit?: () => void;
}> = ({ name, description, onDelete, onDuplicate, onEdit }) => {
    return (
        <DashboardChartCardBase
            name={name}
            description={description}
            onDelete={onDelete}
            onDuplicate={onDuplicate}
            onEdit={onEdit}>
            <div className="flex min-h-0 flex-1 p-2">
                <div className="flex min-h-0 flex-1 items-center justify-center rounded-xs border border-dashed border-border text-center">
                    <p className="text-xs font-medium uppercase text-muted-foreground">
                        Drag query here
                    </p>
                </div>
            </div>
        </DashboardChartCardBase>
    );
};

export const DashboardQueryBlockCard: React.FC<DashboardQueryBlockCardProps> = ({
    block,
    dashboard,
    isMoving,
    manualRefreshToken = 0,
    onDelete,
    onDuplicate,
    onEdit,
}) => {
    const [isDropTargetActive, setIsDropTargetActive] = React.useState(false);
    const [isSavedQueryDragActive, setIsSavedQueryDragActive] = React.useState(
        getDashboardSavedQueryDragActive,
    );
    const { mutate: updateDashboardCard, isPending: isApplyingSavedQuery } =
        useUpdateDashboardCard();
    const { rows, status, errorMessage, columns, columnsByName, barLineDateFormatter } =
        useDashboardQueryBlockCardState({
            block,
            dashboard,
            manualRefreshToken,
        });
    const dashboardId = dashboard?.id ?? block.dashboard_id;
    const isDropZoneVisible = isSavedQueryDragActive || isDropTargetActive;

    React.useEffect(() => {
        return subscribeDashboardSavedQueryDragActive(setIsSavedQueryDragActive);
    }, []);

    const handleDragEnter = React.useCallback(
        (event: React.DragEvent<HTMLDivElement>) => {
            if (!hasDashboardSavedQueryDragData(event.dataTransfer)) {
                return;
            }

            event.preventDefault();
            setIsDropTargetActive(true);
        },
        [],
    );

    const handleDragOver = React.useCallback(
        (event: React.DragEvent<HTMLDivElement>) => {
            if (!hasDashboardSavedQueryDragData(event.dataTransfer)) {
                return;
            }

            event.preventDefault();
            event.dataTransfer.dropEffect = "copy";
            setIsDropTargetActive(true);
        },
        [],
    );

    const handleDragLeave = React.useCallback(
        (event: React.DragEvent<HTMLDivElement>) => {
            const nextTarget = event.relatedTarget;

            if (
                nextTarget instanceof Node &&
                event.currentTarget.contains(nextTarget)
            ) {
                return;
            }

            setIsDropTargetActive(false);
        },
        [],
    );

    const handleDrop = React.useCallback(
        (event: React.DragEvent<HTMLDivElement>) => {
            if (!hasDashboardSavedQueryDragData(event.dataTransfer)) {
                return;
            }

            event.preventDefault();
            setIsDropTargetActive(false);
            setDashboardSavedQueryDragActive(false);

            if (!dashboardId || block.id < 0) {
                return;
            }

            const payload = getDashboardSavedQueryDragPayload(event.dataTransfer);

            if (!payload || payload.query === block.query) {
                return;
            }

            const { dashboard_id: _dashboardId, ...nextCard } = block;

            updateDashboardCard({
                dashboardId,
                card: {
                    ...nextCard,
                    query: payload.query,
                },
            });
        },
        [block, dashboardId, updateDashboardCard],
    );

    const renderCardContent = () => {
        if (
            status === "missing_query" ||
            status === "missing_connection" ||
            status === "missing_database"
        ) {
            return renderSetupState({
                block,
                status,
                onDelete,
                onDuplicate,
                onEdit,
            });
        }

        if (status === "loading" || isApplyingSavedQuery) {
            return (
                <DashboardQueryStateCard
                    name={block.name}
                    description={block.description}
                    title={isApplyingSavedQuery ? "Applying query..." : "Loading chart..."}
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
            if (isMoving) {
                return (
                    <DashboardQueryStateCard
                        name={block.name}
                        description={block.description}
                        title="Resizing..."
                        details="Table preview is temporarily hidden while resizing."
                        onDelete={onDelete}
                        onDuplicate={onDuplicate}
                        onEdit={onEdit}
                    />
                );
            }

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
            if (isMoving) {
                return (
                    <DashboardQueryStateCard
                        name={block.name}
                        description={block.description}
                        title="Resizing..."
                        details="Value preview is temporarily hidden while resizing."
                        onDelete={onDelete}
                        onDuplicate={onDuplicate}
                        onEdit={onEdit}
                    />
                );
            }

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
                        dashboardId={dashboardId}
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
                        dashboardId={dashboardId}
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

        if (block.type === "area_chart") {
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
                    <DashboardInteractiveAreaChartCard
                        dashboardId={dashboardId}
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
                <DashboardAreaChartCard
                    data={chartRows}
                    xAxisKey={axisColumn.name}
                    areaKey={primarySeries?.key ?? axisColumn.name}
                    areaLabel={primarySeries?.label}
                    areaColor={primarySeries?.color}
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
                    dashboardId={dashboardId}
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

    return (
        <div
            className="flex min-h-0 flex-1"
            onDragEnter={handleDragEnter}
            onDragOver={handleDragOver}
            onDragLeave={handleDragLeave}
            onDrop={handleDrop}>
            {isDropZoneVisible ? (
                <DashboardQueryDropHintCard
                    name={block.name}
                    description={block.description}
                    onDelete={onDelete}
                    onDuplicate={onDuplicate}
                    onEdit={onEdit}
                />
            ) : (
                renderCardContent()
            )}
        </div>
    );
};
