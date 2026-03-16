import { useMemo } from "react";
import {
    ChartConfig,
    ChartContainer,
    ChartLegend,
    ChartLegendContent,
    ChartTooltip,
    ChartTooltipContent,
} from "@serene-ui/shared-frontend";
import { Bar, BarChart, XAxis, YAxis } from "recharts";

import { DashboardChartCardBase } from "./DashboardChartCardBase";

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
                <ChartContainer
                    config={chartConfig}
                    className="aspect-auto flex-1 py-2 min-h-0 w-full flex flex-col items-center justify-center">
                    <BarChart
                        className="flex-1"
                        accessibilityLayer
                        data={data}
                        layout="vertical"
                        margin={{
                            left: 12,
                            right: 12,
                        }}>
                        <XAxis type="number" dataKey={barKey} hide />
                        <YAxis
                            dataKey={yAxisKey}
                            type="category"
                            tickLine={false}
                            axisLine={false}
                            tickMargin={8}
                            width={72}
                            tickFormatter={(value) =>
                                formatYAxisTick?.(value) ?? String(value)
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
                                            ? [6, 0, 0, 6]
                                            : item.key ===
                                                normalizedSeries[
                                                    normalizedSeries.length - 1
                                                ]?.key
                                              ? [0, 6, 6, 0]
                                              : 0
                                        : 6
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
