import type { DashboardBlockSchema } from "@serene-ui/shared-core";
import {
    type DashboardChartBlock,
    type DashboardColumnMeta,
    type DashboardQueryRow,
    normalizeDashboardChartValue,
    parseDashboardNumericValue,
} from "../../model/dashboardChartColumns";
import { isDashboardInteractiveBlock } from "./useInteractiveSelection";

export type DashboardQueryBlock = Extract<
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
export type ChartDatum = Record<string, ChartDatumValue>;

const hasCategoryValue = (value: unknown) =>
    value !== null && value !== undefined && value !== "";

const hasNumericValue = (value: unknown) =>
    parseDashboardNumericValue(value) !== null;

export const getColumnMetaByName = (columns: DashboardColumnMeta[]) =>
    new Map(columns.map((column) => [column.name, column]));

const toChartRow = (row: DashboardQueryRow): ChartDatum =>
    Object.fromEntries(
        Object.entries(row).map(([key, value]) => [
            key,
            normalizeDashboardChartValue(value) as ChartDatumValue,
        ]),
    );

export const getBarLineRenderState = (
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

export const getPieRenderState = (
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

export const getInteractiveSelectionValues = ({
    block,
    status,
    rows,
    columnsByName,
}: {
    block: DashboardQueryBlock;
    status: string;
    rows: DashboardQueryRow[];
    columnsByName: Map<string, DashboardColumnMeta>;
}) => {
    if (!isDashboardInteractiveBlock(block) || status !== "ready") {
        return null;
    }

    if (block.type === "bar_chart" || block.type === "line_chart") {
        return getBarLineRenderState(block, rows, columnsByName).series.map(
            (series) => series.key,
        );
    }

    const {
        dimensionColumn,
        valueColumn,
        rows: chartRows,
    } = getPieRenderState(block, rows, columnsByName);

    if (!dimensionColumn || !valueColumn) {
        return [];
    }

    return Array.from(
        new Set(
            chartRows
                .map((row) => row[dimensionColumn.name])
                .filter(
                    (value): value is string | number =>
                        typeof value === "string" || typeof value === "number",
                )
                .map(String),
        ),
    );
};
