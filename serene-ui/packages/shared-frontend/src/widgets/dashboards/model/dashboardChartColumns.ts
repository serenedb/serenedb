import type { DashboardBlockSchema } from "@serene-ui/shared-core";

export type DashboardChartBlock = Extract<
    DashboardBlockSchema,
    { type: "bar_chart" | "line_chart" | "area_chart" | "pie_chart" }
>;

export type DashboardQueryRow = Record<string, unknown>;
export type DashboardColumnType =
    | "null"
    | "int"
    | "float"
    | "boolean"
    | "date"
    | "string"
    | "array"
    | "json";

export interface DashboardColumnMeta {
    name: string;
    type: DashboardColumnType;
    color: string;
    isNumeric: boolean;
    isDateLike: boolean;
    isCategoryLike: boolean;
}

export interface DashboardColumnRoleState {
    isXChecked: boolean;
    isYChecked: boolean;
    isDimensionChecked: boolean;
    isXDisabled: boolean;
    isYDisabled: boolean;
    isDimensionDisabled: boolean;
}

export const DASHBOARD_CHART_COLORS = [
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

const DATE_ONLY_PATTERN = /^\d{4}-\d{2}-\d{2}$/;
const DATE_TIME_PATTERN =
    /^\d{4}-\d{2}-\d{2}[ T]\d{2}:\d{2}(?::\d{2}(?:\.\d{1,6})?)?(?:Z|[+-]\d{2}:?\d{2})?$/i;
const DATE_HINT_PATTERN = /^\d{4}-\d{2}-\d{2}/;
const NUMERIC_PATTERN =
    /^[+-]?(?:\d+(?:\.\d+)?|\.\d+)(?:[eE][+-]?\d+)?$/;

const dateOnlyFormatter = new Intl.DateTimeFormat(undefined, {
    dateStyle: "medium",
});
const dateTimeFormatter = new Intl.DateTimeFormat(undefined, {
    dateStyle: "medium",
    timeStyle: "short",
});

const parseDateValue = (value: unknown) => {
    if (value instanceof Date) {
        return Number.isNaN(value.getTime()) ? null : value;
    }

    if (typeof value !== "string") {
        return null;
    }

    const trimmedValue = value.trim();

    if (!trimmedValue) {
        return null;
    }

    if (
        !DATE_ONLY_PATTERN.test(trimmedValue) &&
        !DATE_TIME_PATTERN.test(trimmedValue) &&
        !DATE_HINT_PATTERN.test(trimmedValue)
    ) {
        return null;
    }

    const parsedDate = new Date(trimmedValue);

    return Number.isNaN(parsedDate.getTime()) ? null : parsedDate;
};

export const isDashboardDateLikeValue = (value: unknown) =>
    parseDateValue(value) !== null;

export const parseDashboardNumericValue = (value: unknown) => {
    if (typeof value === "number") {
        return Number.isFinite(value) ? value : null;
    }

    if (typeof value !== "string") {
        return null;
    }

    const trimmedValue = value.trim();

    if (!trimmedValue || !NUMERIC_PATTERN.test(trimmedValue)) {
        return null;
    }

    const parsedValue = Number(trimmedValue);

    return Number.isFinite(parsedValue) ? parsedValue : null;
};

export const isDashboardNumericLikeValue = (value: unknown) =>
    parseDashboardNumericValue(value) !== null;

const inferNumericColumnType = (value: number, rawValue: unknown) => {
    if (
        typeof rawValue === "string" &&
        /[.eE]/.test(rawValue.trim())
    ) {
        return "float" as const;
    }

    return Number.isInteger(value) ? ("int" as const) : ("float" as const);
};

export const inferDashboardColumnType = (
    values: unknown[],
): DashboardColumnType => {
    const sample = values.find(
        (value) => value !== null && value !== undefined,
    );

    if (sample === undefined) {
        return "null";
    }

    const numericValue = parseDashboardNumericValue(sample);

    if (numericValue !== null) {
        return inferNumericColumnType(numericValue, sample);
    }

    if (typeof sample === "boolean") {
        return "boolean";
    }

    if (isDashboardDateLikeValue(sample)) {
        return "date";
    }

    if (typeof sample === "string") {
        return "string";
    }

    if (Array.isArray(sample)) {
        return "array";
    }

    return "json";
};

export const collectDashboardColumnMetadata = ({
    rows,
    selectedSeriesColors,
}: {
    rows: DashboardQueryRow[];
    selectedSeriesColors?: Record<string, string | undefined>;
}) => {
    const seenColumns = new Set<string>();
    const orderedColumns: string[] = [];

    rows.forEach((row) => {
        Object.keys(row).forEach((columnName) => {
            if (seenColumns.has(columnName)) {
                return;
            }

            seenColumns.add(columnName);
            orderedColumns.push(columnName);
        });
    });

    return orderedColumns.map((columnName, index) => {
        const type = inferDashboardColumnType(
            rows.map((row) => row[columnName]),
        );

        return {
            name: columnName,
            type,
            color:
                selectedSeriesColors?.[columnName] ??
                DASHBOARD_CHART_COLORS[index % DASHBOARD_CHART_COLORS.length],
            isNumeric: type === "int" || type === "float",
            isDateLike: type === "date",
            isCategoryLike: type === "string" || type === "date",
        } satisfies DashboardColumnMeta;
    });
};

export const getDashboardColumnRoleState = (
    block: DashboardChartBlock,
    column: DashboardColumnMeta,
): DashboardColumnRoleState => {
    if (block.type === "pie_chart") {
        const isYChecked = block.value_key === column.name;
        const isDimensionChecked = block.name_key === column.name;

        return {
            isXChecked: false,
            isYChecked,
            isDimensionChecked,
            isXDisabled: true,
            isYDisabled:
                isDimensionChecked || (!column.isNumeric && !isYChecked),
            isDimensionDisabled:
                isYChecked ||
                (!column.isCategoryLike && !isDimensionChecked),
        };
    }

    const xAxisKey =
        block.type === "bar_chart" ? block.category_key : block.x_axis_key;
    const isXChecked = xAxisKey === column.name;
    const isYChecked = block.series.some((item) => item.key === column.name);

    return {
        isXChecked,
        isYChecked,
        isDimensionChecked: false,
        isXDisabled: isYChecked || (!column.isCategoryLike && !isXChecked),
        isYDisabled:
            isXChecked ||
            (!column.isNumeric && !isYChecked) ||
            (isYChecked && block.series.length <= 1),
        isDimensionDisabled: true,
    };
};

const isDateOnlyValue = (value: unknown) =>
    value instanceof Date
        ? value.getHours() === 0 &&
          value.getMinutes() === 0 &&
          value.getSeconds() === 0 &&
          value.getMilliseconds() === 0
        : typeof value === "string" && DATE_ONLY_PATTERN.test(value.trim());

export const formatDashboardDateValue = (value: unknown) => {
    const parsedDate = parseDateValue(value);

    if (!parsedDate) {
        return typeof value === "string" || typeof value === "number"
            ? String(value)
            : "";
    }

    return isDateOnlyValue(value)
        ? dateOnlyFormatter.format(parsedDate)
        : dateTimeFormatter.format(parsedDate);
};

export const formatDashboardDisplayValue = (
    value: unknown,
    columnType?: DashboardColumnType,
) => {
    if (value === null || value === undefined) {
        return "null";
    }

    if (columnType === "date" || isDashboardDateLikeValue(value)) {
        return formatDashboardDateValue(value);
    }

    if (typeof value === "string" || typeof value === "number") {
        return String(value);
    }

    if (typeof value === "boolean") {
        return value ? "true" : "false";
    }

    if (value instanceof Date) {
        return formatDashboardDateValue(value);
    }

    try {
        return JSON.stringify(value);
    } catch {
        return String(value);
    }
};

export const normalizeDashboardChartValue = (value: unknown) => {
    if (value === null || value === undefined) {
        return value;
    }

    const numericValue = parseDashboardNumericValue(value);

    if (numericValue !== null) {
        return numericValue;
    }

    if (typeof value === "string" || typeof value === "number") {
        return value;
    }

    if (value instanceof Date) {
        return value.toISOString();
    }

    if (typeof value === "boolean") {
        return value ? "true" : "false";
    }

    try {
        return JSON.stringify(value);
    } catch {
        return String(value);
    }
};
