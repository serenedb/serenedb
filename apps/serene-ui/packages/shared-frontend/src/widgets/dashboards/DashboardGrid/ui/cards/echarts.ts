import type { EChartsOption } from "echarts";

export type DashboardEChartDatum = Record<
    string,
    string | number | null | undefined
>;

export interface DashboardEChartSeriesConfig {
    key: string;
    label: string;
    color: string;
}

const CSS_VAR_PATTERN = /^var\((--[^,\s)]+)(?:,[^)]+)?\)$/;

export const resolveEChartColor = (color: string) => {
    if (typeof window === "undefined") {
        return color;
    }

    const match = color.match(CSS_VAR_PATTERN);

    if (!match) {
        return color;
    }

    const resolvedValue = window
        .getComputedStyle(document.documentElement)
        .getPropertyValue(match[1])
        .trim();

    return resolvedValue || color;
};

export const toNumericOrNull = (value: unknown) => {
    if (typeof value === "number" && Number.isFinite(value)) {
        return value;
    }

    if (typeof value === "string") {
        const normalized = value.trim();

        if (!normalized) {
            return null;
        }

        const parsed = Number(normalized);

        if (Number.isFinite(parsed)) {
            return parsed;
        }
    }

    return null;
};

const escapeHtml = (value: unknown) =>
    String(value)
        .replace(/&/g, "&amp;")
        .replace(/</g, "&lt;")
        .replace(/>/g, "&gt;")
        .replace(/\"/g, "&quot;")
        .replace(/'/g, "&#039;");

const getSeriesName = (param: unknown) => {
    if (typeof param !== "object" || !param || !("seriesName" in param)) {
        return "";
    }

    const seriesName = (param as { seriesName?: unknown }).seriesName;
    return typeof seriesName === "string" ? seriesName : "";
};

const getSeriesColor = (param: unknown) => {
    if (typeof param !== "object" || !param || !("color" in param)) {
        return "#999";
    }

    const color = (param as { color?: unknown }).color;
    return typeof color === "string" ? color : "#999";
};

const getSeriesValue = (param: unknown) => {
    if (typeof param !== "object" || !param || !("value" in param)) {
        return null;
    }

    const value = (param as { value?: unknown }).value;

    if (typeof value === "number" && Number.isFinite(value)) {
        return value;
    }

    if (typeof value === "string") {
        return value;
    }

    return null;
};

const getAxisLabel = (params: unknown) => {
    if (Array.isArray(params) && params.length > 0) {
        const first = params[0];
        if (typeof first === "object" && first && "axisValueLabel" in first) {
            const axisValueLabel = (first as { axisValueLabel?: unknown })
                .axisValueLabel;
            if (
                typeof axisValueLabel === "string" ||
                typeof axisValueLabel === "number"
            ) {
                return axisValueLabel;
            }
        }

        if (typeof first === "object" && first && "name" in first) {
            const name = (first as { name?: unknown }).name;
            if (typeof name === "string" || typeof name === "number") {
                return name;
            }
        }
    }

    if (typeof params === "object" && params && "name" in params) {
        const name = (params as { name?: unknown }).name;
        if (typeof name === "string" || typeof name === "number") {
            return name;
        }
    }

    return "";
};

export const buildAxisTooltipFormatter = ({
    formatTooltipLabel,
}: {
    formatTooltipLabel?: (value: string | number) => string;
}) => {
    return (params: unknown) => {
        if (!Array.isArray(params) || params.length === 0) {
            return "";
        }

        const rawLabel = getAxisLabel(params);
        const label =
            typeof rawLabel === "string" || typeof rawLabel === "number"
                ? formatTooltipLabel?.(rawLabel) ?? String(rawLabel)
                : "";

        const rows = params
            .map((param) => {
                const seriesName = getSeriesName(param);
                const color = getSeriesColor(param);
                const value = getSeriesValue(param);

                return `<div style=\"display:flex;align-items:center;justify-content:space-between;gap:12px;\"><span style=\"display:inline-flex;align-items:center;gap:6px;\"><span style=\"display:inline-block;width:8px;height:8px;border-radius:999px;background:${escapeHtml(color)}\"></span>${escapeHtml(seriesName)}</span><span style=\"font-variant-numeric:tabular-nums;\">${escapeHtml(value ?? "-")}</span></div>`;
            })
            .join("");

        return `<div style=\"display:grid;gap:6px;min-width:140px;\"><div style=\"font-weight:600;\">${escapeHtml(label)}</div>${rows}</div>`;
    };
};

export const buildItemTooltipFormatter = ({
    formatTooltipLabel,
}: {
    formatTooltipLabel?: (value: string | number) => string;
}) => {
    return (param: unknown) => {
        const rawName = getAxisLabel(param);
        const name =
            typeof rawName === "string" || typeof rawName === "number"
                ? formatTooltipLabel?.(rawName) ?? String(rawName)
                : "";
        const color = getSeriesColor(param);
        const value = getSeriesValue(param);

        return `<div style=\"display:grid;gap:6px;min-width:120px;\"><div style=\"display:inline-flex;align-items:center;gap:6px;font-weight:600;\"><span style=\"display:inline-block;width:8px;height:8px;border-radius:999px;background:${escapeHtml(color)}\"></span>${escapeHtml(name)}</div><div style=\"font-variant-numeric:tabular-nums;\">${escapeHtml(value ?? "-")}</div></div>`;
    };
};

const STEP_LINE_TYPES: ReadonlySet<string> = new Set([
    "step",
    "stepBefore",
    "stepAfter",
]);

const SMOOTH_LINE_TYPES: ReadonlySet<string> = new Set([
    "basis",
    "basisClosed",
    "basisOpen",
    "bump",
    "bumpX",
    "bumpY",
    "monotone",
    "monotoneX",
    "monotoneY",
    "natural",
]);

export const mapLineTypeToECharts = (
    lineType: string,
): { smooth: boolean; step: false | "start" | "end" | "middle" } => {
    if (lineType === "linear" || lineType === "linearClosed") {
        return {
            smooth: false,
            step: false,
        };
    }

    if (STEP_LINE_TYPES.has(lineType)) {
        return {
            smooth: false,
            step:
                lineType === "stepBefore"
                    ? "start"
                    : lineType === "stepAfter"
                      ? "end"
                      : "middle",
        } as const;
    }

    if (SMOOTH_LINE_TYPES.has(lineType)) {
        return {
            smooth: true,
            step: false,
        };
    }

    return {
        smooth: true,
        step: false,
    };
};

export type DashboardEChartLineStyle = ReturnType<typeof mapLineTypeToECharts>;

export const baseCartesianGrid = (): NonNullable<EChartsOption["grid"]> => ({
    top: 12,
    left: 12,
    right: 12,
    bottom: 12,
    containLabel: true,
});
