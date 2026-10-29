import { useMemo } from "react";
import ReactECharts from "echarts-for-react";
import type { EChartsOption } from "echarts";

import { DashboardChartCardBase } from "./DashboardChartCardBase";
import {
    baseCartesianGrid,
    buildAxisTooltipFormatter,
    resolveEChartColor,
    toNumericOrNull,
} from "./echarts";

export interface DashboardVerticalBarchartCardDatum {
    [key: string]: string | number | null | undefined;
}

export interface DashboardVerticalBarchartCardSeries {
    key: string;
    label: string;
    color: string;
}

interface DashboardVerticalBarchartCardProps {
    name?: string;
    description?: string;
    onDelete?: () => void | Promise<void>;
    onDuplicate?: () => void | Promise<void>;
    onEdit?: () => void;
    data: DashboardVerticalBarchartCardDatum[];
    xAxisKey: string;
    barKey: string;
    barLabel?: string;
    barColor?: string;
    series?: DashboardVerticalBarchartCardSeries[];
    isStacked?: boolean;
    isMoving?: boolean;
    formatXAxisTick?: (value: string | number) => string;
    formatTooltipLabel?: (value: string | number) => string;
}

export const DashboardVerticalBarchartCard: React.FC<
    DashboardVerticalBarchartCardProps
> = ({
    name,
    description,
    onDelete,
    onDuplicate,
    onEdit,
    data,
    xAxisKey,
    barKey,
    barLabel = "Value",
    barColor = "var(--chart-1)",
    series,
    isStacked = false,
    isMoving = false,
    formatXAxisTick,
    formatTooltipLabel,
}) => {
    const normalizedSeries = useMemo(
        () =>
            series?.length
                ? series
                : [{ key: barKey, label: barLabel, color: barColor }],
        [barColor, barKey, barLabel, series],
    );

    const showLegend = isStacked && normalizedSeries.length > 1;

    const chartOption = useMemo<EChartsOption>(() => {
        const categoryData = data.map((item) => String(item[xAxisKey] ?? ""));

        return {
            animation: false,
            grid: {
                ...baseCartesianGrid(),
                bottom: showLegend ? 36 : 12,
            },
            tooltip: {
                trigger: "axis",
                axisPointer: { type: "shadow" },
                formatter: buildAxisTooltipFormatter({ formatTooltipLabel }),
                borderWidth: 1,
                backgroundColor: "var(--background)",
                confine: false,
                appendToBody: true,
            },
            legend: showLegend
                ? {
                      bottom: 0,
                      data: normalizedSeries.map((item) => item.label),
                  }
                : undefined,
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
            series: normalizedSeries.map((item) => ({
                type: "bar",
                name: item.label,
                stack: isStacked ? "stack" : undefined,
                emphasis: { focus: "series" },
                data: data.map((row) => toNumericOrNull(row[item.key])),
                itemStyle: {
                    color: resolveEChartColor(item.color),
                    borderRadius: 8,
                },
            })),
        };
    }, [data, formatTooltipLabel, formatXAxisTick, isStacked, normalizedSeries, showLegend, xAxisKey]);

    return (
        <DashboardChartCardBase
            name={name}
            description={description}
            onDelete={onDelete}
            onDuplicate={onDuplicate}
            onEdit={onEdit}>
            {isMoving ? (
                <div className="flex min-h-0 flex-1 items-center justify-center p-2">
                    <div className="bg-muted/30 border-border/50 flex h-full w-full max-h-full max-w-full items-end self-center overflow-hidden rounded-xs border p-4">
                        <div className="flex h-full min-h-0 w-full items-end gap-2">
                            {Array.from({ length: 6 }, (_, index) => (
                                <div
                                    key={index}
                                    className="bg-muted-foreground/20 flex-1 rounded-t-[8px]"
                                    style={{
                                        height: `${35 + ((index % 4) + 1) * 12}%`,
                                    }}
                                />
                            ))}
                        </div>
                    </div>
                </div>
            ) : (
                <div className="flex-1 py-2 min-h-0 w-full flex flex-col items-center justify-center">
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
