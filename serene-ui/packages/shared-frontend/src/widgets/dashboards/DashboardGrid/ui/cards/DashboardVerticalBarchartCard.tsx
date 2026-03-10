import { useMemo } from "react";
import {
    ChartConfig,
    ChartContainer,
    ChartLegend,
    ChartLegendContent,
    ChartTooltip,
    ChartTooltipContent,
} from "@serene-ui/shared-frontend";
import { Bar, BarChart, CartesianGrid, XAxis } from "recharts";

import { DashboardChartCardBase } from "./DashboardChartCardBase";

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
    data: DashboardVerticalBarchartCardDatum[];
    xAxisKey: string;
    barKey: string;
    barLabel?: string;
    barColor?: string;
    series?: DashboardVerticalBarchartCardSeries[];
    isStacked?: boolean;
    isResizing?: boolean;
    formatXAxisTick?: (value: string | number) => string;
    formatTooltipLabel?: (value: string | number) => string;
}

export const DashboardVerticalBarchartCard: React.FC<
    DashboardVerticalBarchartCardProps
> = ({
    name,
    description,
    data,
    xAxisKey,
    barKey,
    barLabel = "Value",
    barColor = "var(--chart-1)",
    series,
    isStacked = false,
    isResizing = false,
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

    const chartConfig = useMemo(
        () =>
            Object.fromEntries(
                normalizedSeries.map((item) => [
                    item.key,
                    {
                        label: item.label,
                        color: item.color,
                    },
                ]),
            ) satisfies ChartConfig,
        [normalizedSeries],
    );

    const showLegend = isStacked && normalizedSeries.length > 1;

    return (
        <DashboardChartCardBase name={name} description={description}>
            {isResizing ? (
                <div className="flex h-[250px] w-full items-center justify-center px-4">
                    <div className="bg-muted/30 border-border/50 flex w-full items-end gap-2 rounded-xs border p-4">
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
            ) : (
                <ChartContainer
                    config={chartConfig}
                    className="aspect-auto h-[250px] w-full">
                    <BarChart
                        accessibilityLayer
                        data={data}
                        margin={{
                            left: 12,
                            right: 12,
                        }}>
                        <CartesianGrid vertical={false} />
                        <XAxis
                            dataKey={xAxisKey}
                            tickLine={false}
                            axisLine={false}
                            tickMargin={8}
                            minTickGap={24}
                            tickFormatter={(value) =>
                                formatXAxisTick?.(value) ?? String(value)
                            }
                        />
                        <ChartTooltip
                            cursor={false}
                            content={(props) => (
                                <ChartTooltipContent
                                    {...props}
                                    hideLabel
                                    indicator={
                                        normalizedSeries.length > 1 &&
                                        !isStacked
                                            ? "dashed"
                                            : undefined
                                    }
                                    labelFormatter={(value) =>
                                        formatTooltipLabel?.(value) ??
                                        String(value)
                                    }
                                />
                            )}
                        />
                        {showLegend && (
                            <ChartLegend content={<ChartLegendContent />} />
                        )}
                        {normalizedSeries.map((item) => (
                            <Bar
                                key={item.key}
                                dataKey={item.key}
                                fill={`var(--color-${item.key})`}
                                radius={
                                    isStacked && normalizedSeries.length > 1
                                        ? item.key === normalizedSeries[0]?.key
                                            ? [0, 0, 8, 8]
                                            : item.key ===
                                                normalizedSeries[
                                                    normalizedSeries.length - 1
                                                ]?.key
                                              ? [8, 8, 0, 0]
                                              : 0
                                        : 8
                                }
                                stackId={isStacked ? "stack" : undefined}
                            />
                        ))}
                    </BarChart>
                </ChartContainer>
            )}
        </DashboardChartCardBase>
    );
};
