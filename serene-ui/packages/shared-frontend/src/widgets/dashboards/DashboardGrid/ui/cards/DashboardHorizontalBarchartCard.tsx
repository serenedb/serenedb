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

export interface DashboardHorizontalBarchartCardDatum {
    [key: string]: string | number | null | undefined;
}

export interface DashboardHorizontalBarchartCardSeries {
    key: string;
    label: string;
    color: string;
}

interface DashboardHorizontalBarchartCardProps {
    name?: string;
    description?: string;
    onDelete?: () => void | Promise<void>;
    onDuplicate?: () => void | Promise<void>;
    onEdit?: () => void;
    data: DashboardHorizontalBarchartCardDatum[];
    yAxisKey: string;
    barKey: string;
    barLabel?: string;
    barColor?: string;
    series?: DashboardHorizontalBarchartCardSeries[];
    isStacked?: boolean;
    isMoving?: boolean;
    formatYAxisTick?: (value: string | number) => string;
    formatTooltipLabel?: (value: string | number) => string;
}

export const DashboardHorizontalBarchartCard: React.FC<
    DashboardHorizontalBarchartCardProps
> = ({
    name,
    description,
    onDelete,
    onDuplicate,
    onEdit,
    data,
    yAxisKey,
    barKey,
    barLabel = "Value",
    barColor = "var(--chart-1)",
    series,
    isStacked = false,
    isMoving = false,
    formatYAxisTick,
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
        const categoryData = data.map((item) => String(item[yAxisKey] ?? ""));

        return {
            animation: false,
            grid: {
                ...baseCartesianGrid(),
                bottom: showLegend ? 36 : 12,
                left: 24,
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
                type: "value",
                axisLine: { show: false },
                axisTick: { show: false },
                splitLine: { show: false },
            },
            yAxis: {
                type: "category",
                data: categoryData,
                axisLine: { show: false },
                axisTick: { show: false },
                axisLabel: {
                    formatter: (value: string | number) =>
                        formatYAxisTick?.(value) ?? String(value),
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
                    borderRadius: 6,
                },
            })),
        };
    }, [data, formatTooltipLabel, formatYAxisTick, isStacked, normalizedSeries, showLegend, yAxisKey]);

    return (
        <DashboardChartCardBase
            name={name}
            description={description}
            onDelete={onDelete}
            onDuplicate={onDuplicate}
            onEdit={onEdit}>
            {isMoving ? (
                <div className="flex min-h-0 flex-1 items-center justify-center p-2">
                    <div className="bg-muted/30 h-full border-border/50 flex  w-full max-h-full max-w-full flex-col justify-center gap-2 self-center overflow-hidden rounded-xs border p-4">
                        {Array.from({ length: 6 }, (_, index) => (
                            <div
                                key={index}
                                className="flex items-center gap-2">
                                <div className="bg-muted-foreground/20 h-3 w-12 rounded-[4px]" />
                                <div
                                    className="bg-muted-foreground/20 h-6 rounded-r-[6px]"
                                    style={{
                                        width: `${35 + ((index % 4) + 1) * 12}%`,
                                    }}
                                />
                            </div>
                        ))}
                    </div>
                </div>
            ) : (
                <div className="aspect-auto flex-1 py-2 min-h-0 w-full flex flex-col items-center justify-center">
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
