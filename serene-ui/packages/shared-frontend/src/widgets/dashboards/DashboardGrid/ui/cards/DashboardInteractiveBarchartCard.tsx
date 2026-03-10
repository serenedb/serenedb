import { useEffect, useMemo, useState } from "react";
import {
    ChartConfig,
    ChartContainer,
    ChartTooltip,
    ChartTooltipContent,
    Select,
    SelectContent,
    SelectItem,
    SelectTrigger,
    SelectValue,
} from "@serene-ui/shared-frontend";
import type { LayoutConstraint } from "react-grid-layout/core";
import { Bar, BarChart, CartesianGrid, XAxis } from "recharts";

export interface DashboardInteractiveBarchartSeries {
    key: string;
    label: string;
    color: string;
}

export interface DashboardInteractiveBarchartDatum {
    [key: string]: string | number | null | undefined;
}

interface DashboardInteractiveChartCardProps {
    name?: string;
    description?: string;
    data: DashboardInteractiveBarchartDatum[];
    series: DashboardInteractiveBarchartSeries[];
    xAxisKey: string;
    defaultActiveKey?: string;
    isResizing?: boolean;
    valueLabel?: string;
    formatXAxisTick?: (value: string | number) => string;
    formatTooltipLabel?: (value: string | number) => string;
}

export const DashboardInteractiveChartCard: React.FC<
    DashboardInteractiveChartCardProps
> = ({
    name,
    description,
    data,
    series,
    xAxisKey,
    defaultActiveKey,
    isResizing = false,
    valueLabel = "Value",
    formatXAxisTick,
    formatTooltipLabel,
}) => {
    const chartConfig = useMemo(
        () =>
            Object.fromEntries([
                ["value", { label: valueLabel }],
                ...series.map((item) => [
                    item.key,
                    {
                        label: item.label,
                        color: item.color,
                    },
                ]),
            ]) satisfies ChartConfig,
        [series, valueLabel],
    );

    const [activeChart, setActiveChart] = useState(
        defaultActiveKey ?? series[0]?.key ?? "",
    );

    useEffect(() => {
        if (!series.some((item) => item.key === activeChart)) {
            setActiveChart(defaultActiveKey ?? series[0]?.key ?? "");
        }
    }, [activeChart, defaultActiveKey, series]);

    const totals = useMemo(
        () =>
            Object.fromEntries(
                series.map((item) => [
                    item.key,
                    data.reduce((acc, entry) => {
                        const value = entry[item.key];
                        return acc + (typeof value === "number" ? value : 0);
                    }, 0),
                ]),
            ),
        [data, series],
    );

    const activeSeries = series.find((item) => item.key === activeChart);

    return (
        <div className="bg-background border-1 rounded-xs flex-1 flex flex-col overflow-auto">
            <div className="p-2">
                <Select value={activeChart} onValueChange={setActiveChart}>
                    <SelectTrigger className="w-full h-auto min-h-9">
                        <SelectValue>
                            <div className="flex  items-center gap-1 text-left">
                                <span className="text-xs text-muted-foreground">
                                    {activeSeries?.label ?? valueLabel}
                                </span>
                                <span className="text-xs leading-none">
                                    (
                                    {(
                                        totals[activeChart] ?? 0
                                    ).toLocaleString()}
                                    )
                                </span>
                            </div>
                        </SelectValue>
                    </SelectTrigger>
                    <SelectContent>
                        {series.map((chart) => (
                            <SelectItem key={chart.key} value={chart.key}>
                                <div className="flex w-full items-center justify-between gap-3">
                                    <span className="text-xs text-muted-foreground">
                                        {chart.label}
                                    </span>
                                    <span className="text-muted-foreground text-xs">
                                        (
                                        {(
                                            totals[chart.key] ?? 0
                                        ).toLocaleString()}
                                        )
                                    </span>
                                </div>
                            </SelectItem>
                        ))}
                    </SelectContent>
                </Select>
            </div>
            {isResizing ? (
                <div className="flex h-[250px] w-full items-center justify-center px-4">
                    <div className="bg-muted/30 border-border/50 flex w-full max-w-64 flex-col gap-2 rounded-xs border p-4">
                        <p className="text-xs text-muted-foreground">
                            {activeSeries?.label ?? valueLabel}
                        </p>
                        <p className="text-lg font-semibold">
                            {(totals[activeChart] ?? 0).toLocaleString()}
                        </p>
                        <div className="flex h-24 items-end gap-1">
                            {Array.from({ length: 12 }, (_, index) => (
                                <div
                                    key={index}
                                    className="bg-muted-foreground/20 flex-1 rounded-t-[2px]"
                                    style={{
                                        height: `${40 + ((index % 5) + 1) * 10}%`,
                                    }}
                                />
                            ))}
                        </div>
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
                            minTickGap={32}
                            tickFormatter={(value) =>
                                formatXAxisTick?.(value) ?? String(value)
                            }
                        />
                        <ChartTooltip
                            content={(props) => (
                                <ChartTooltipContent
                                    {...props}
                                    className="w-[150px]"
                                    nameKey="value"
                                    labelFormatter={(value) =>
                                        formatTooltipLabel?.(value) ??
                                        String(value)
                                    }
                                />
                            )}
                        />
                        <Bar
                            dataKey={activeChart}
                            fill={`var(--color-${activeChart})`}
                        />
                    </BarChart>
                </ChartContainer>
            )}
            <div className="flex flex-col border-t-1 p-3 w-full mt-auto gap-0.5">
                <p className="uppercase text-xs font-extrabold text-primary-foreground">
                    {name}
                </p>
                <p className="text-xs text-muted-foreground">{description}</p>
            </div>
        </div>
    );
};
