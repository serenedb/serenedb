import { type ComponentProps, useMemo } from "react";
import {
    ChartConfig,
    ChartContainer,
    ChartTooltip,
    ChartTooltipContent,
} from "@serene-ui/shared-frontend";
import { CartesianGrid, Line, LineChart, XAxis } from "recharts";

import { DashboardChartCardBase } from "./DashboardChartCardBase";

export interface DashboardLinechartCardDatum {
    [key: string]: string | number | null | undefined;
}

export interface DashboardLinechartCardSeries {
    key: string;
    label: string;
    color: string;
}

export type DashboardLinechartType = ComponentProps<typeof Line>["type"];

interface DashboardLinechartCardProps {
    name?: string;
    description?: string;
    onDelete?: () => void | Promise<void>;
    onDuplicate?: () => void | Promise<void>;
    onEdit?: () => void;
    data: DashboardLinechartCardDatum[];
    xAxisKey: string;
    lineKey: string;
    lineLabel?: string;
    lineColor?: string;
    series?: DashboardLinechartCardSeries[];
    lineType?: DashboardLinechartType;
    isMoving?: boolean;
    formatXAxisTick?: (value: string | number) => string;
    formatTooltipLabel?: (value: string | number) => string;
}

export const DashboardLinechartCard: React.FC<DashboardLinechartCardProps> = ({
    name,
    description,
    onDelete,
    onDuplicate,
    onEdit,
    data,
    xAxisKey,
    lineKey,
    lineLabel = "Value",
    lineColor = "var(--chart-1)",
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
                : [{ key: lineKey, label: lineLabel, color: lineColor }],
        [lineColor, lineKey, lineLabel, series],
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
                <ChartContainer
                    config={chartConfig}
                    className="aspect-auto flex-1 pt-4 min-h-0 w-full">
                    <LineChart accessibilityLayer data={data}>
                        <CartesianGrid vertical={false} />
                        <XAxis
                            dataKey={xAxisKey}
                            tickLine={false}
                            axisLine={false}
                            tickMargin={8}
                            padding={{ left: 12, right: 12 }}
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
                                        normalizedSeries.length > 1
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
                        {normalizedSeries.map((item) => (
                            <Line
                                key={item.key}
                                dataKey={item.key}
                                type={lineType}
                                stroke={`var(--color-${item.key})`}
                                strokeWidth={2}
                                dot={false}
                            />
                        ))}
                    </LineChart>
                </ChartContainer>
            )}
        </DashboardChartCardBase>
    );
};
