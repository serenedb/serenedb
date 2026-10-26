import { useMemo } from "react";
import ReactECharts from "echarts-for-react";
import type { EChartsOption } from "echarts";

import { DashboardChartCardBase } from "./DashboardChartCardBase";
import {
    baseCartesianGrid,
    buildAxisTooltipFormatter,
    mapLineTypeToECharts,
    resolveEChartColor,
    toNumericOrNull,
} from "./echarts";

export interface DashboardAreaChartCardDatum {
    [key: string]: string | number | null | undefined;
}

export interface DashboardAreaChartCardSeries {
    key: string;
    label: string;
    color: string;
}

export type DashboardAreaChartType =
    | "basis"
    | "basisClosed"
    | "basisOpen"
    | "bump"
    | "bumpX"
    | "bumpY"
    | "linear"
    | "linearClosed"
    | "monotone"
    | "monotoneX"
    | "monotoneY"
    | "natural"
    | "step"
    | "stepAfter"
    | "stepBefore";

interface DashboardAreaChartCardProps {
    name?: string;
    description?: string;
    onDelete?: () => void | Promise<void>;
    onDuplicate?: () => void | Promise<void>;
    onEdit?: () => void;
    data: DashboardAreaChartCardDatum[];
    xAxisKey: string;
    areaKey: string;
    areaLabel?: string;
    areaColor?: string;
    series?: DashboardAreaChartCardSeries[];
    lineType?: DashboardAreaChartType;
    isMoving?: boolean;
    formatXAxisTick?: (value: string | number) => string;
    formatTooltipLabel?: (value: string | number) => string;
}

export const DashboardAreaChartCard: React.FC<DashboardAreaChartCardProps> = ({
    name,
    description,
    onDelete,
    onDuplicate,
    onEdit,
    data,
    xAxisKey,
    areaKey,
    areaLabel = "Value",
    areaColor = "var(--chart-1)",
    series,
    lineType = "natural",
    isMoving = false,
    formatXAxisTick,
    formatTooltipLabel,
}) => {
    const normalizedSeries = useMemo(
        () =>
            series?.length
                ? series
                : [{ key: areaKey, label: areaLabel, color: areaColor }],
        [areaColor, areaKey, areaLabel, series],
    );

    const { smooth, step } = mapLineTypeToECharts(lineType);

    const chartOption = useMemo<EChartsOption>(() => {
        const categoryData = data.map((item) => String(item[xAxisKey] ?? ""));

        return {
            animation: false,
            grid: baseCartesianGrid(),
            tooltip: {
                trigger: "axis",
                formatter: buildAxisTooltipFormatter({ formatTooltipLabel }),
                borderWidth: 1,
                backgroundColor: "var(--background)",
                confine: false,
                appendToBody: true,
            },
            xAxis: {
                type: "category",
                data: categoryData,
                axisLine: { show: false },
                axisTick: { show: false },
                axisLabel: {
                    formatter: (value: string | number) =>
                        formatXAxisTick?.(value) ?? String(value),
                },
            },
            yAxis: {
                type: "value",
                axisLine: { show: false },
                axisTick: { show: false },
                splitLine: {
                    lineStyle: {
                        color: "rgba(148, 163, 184, 0.25)",
                    },
                },
            },
            series: normalizedSeries.map((item) => {
                const color = resolveEChartColor(item.color);

                return {
                    type: "line",
                    name: item.label,
                    data: data.map((row) => toNumericOrNull(row[item.key])),
                    showSymbol: false,
                    smooth,
                    step,
                    lineStyle: {
                        width: 2,
                        color,
                    },
                    itemStyle: {
                        color,
                    },
                    areaStyle: {
                        color,
                        opacity: 0.2,
                    },
                };
            }),
        };
    }, [data, formatTooltipLabel, formatXAxisTick, normalizedSeries, smooth, step, xAxisKey]);

    return (
        <DashboardChartCardBase
            name={name}
            description={description}
            onDelete={onDelete}
            onDuplicate={onDuplicate}
            onEdit={onEdit}>
            {isMoving ? (
                <div className="flex min-h-0 flex-1 items-center justify-center p-2">
                    <div className="bg-muted/30 border-border/50 flex h-full w-full max-h-full max-w-full flex-col justify-center self-center overflow-hidden rounded-xs border p-4">
                        <div className="min-h-0 flex-1">
                            <svg
                                viewBox="0 0 240 96"
                                className="h-full w-full overflow-visible">
                                <polygon
                                    fill="currentColor"
                                    className="text-muted-foreground/15"
                                    points="0,76 22,64 44,68 66,30 88,38 110,28 132,52 154,22 176,26 198,14 220,34 240,18 240,96 0,96"
                                />
                                <polyline
                                    fill="none"
                                    stroke="currentColor"
                                    strokeWidth="3"
                                    className="text-muted-foreground/30"
                                    points="0,76 22,64 44,68 66,30 88,38 110,28 132,52 154,22 176,26 198,14 220,34 240,18"
                                />
                            </svg>
                        </div>
                    </div>
                </div>
            ) : (
                <div className="aspect-auto flex-1 min-h-0 w-full pt-4">
                    <ReactECharts
                        option={chartOption}
                        notMerge
                        lazyUpdate
                        style={{ height: "100%", width: "100%" }}
                    />
                </div>
            )}
        </DashboardChartCardBase>
    );
};
