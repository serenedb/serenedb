import { useMemo } from "react";
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
import { parseDashboardNumericValue } from "../../../model/dashboardChartColumns";
import { useDashboardInteractiveSelection } from "../../model/interactiveSelection";
import { DashboardCardActions } from "./DashboardCardActions";

export interface DashboardInteractiveBarchartSeries {
    key: string;
    label: string;
    color: string;
}

export interface DashboardInteractiveBarchartDatum {
    [key: string]: string | number | null | undefined;
}

interface DashboardInteractiveChartCardProps {
    dashboardId: number;
    blockId: number;
    query: string;
    name?: string;
    description?: string;
    data: DashboardInteractiveBarchartDatum[];
    series: DashboardInteractiveBarchartSeries[];
    xAxisKey: string;
    defaultActiveKey?: string;
    isMoving?: boolean;
    valueLabel?: string;
    formatXAxisTick?: (value: string | number) => string;
    formatTooltipLabel?: (value: string | number) => string;
    onDelete?: () => void | Promise<void>;
    onDuplicate?: () => void | Promise<void>;
    onEdit?: () => void;
}

export const DashboardInteractiveChartCard: React.FC<
    DashboardInteractiveChartCardProps
> = ({
    dashboardId,
    blockId,
    query,
    name,
    description,
    data,
    series,
    xAxisKey,
    defaultActiveKey,
    isMoving = false,
    valueLabel = "Value",
    formatXAxisTick,
    formatTooltipLabel,
    onDelete,
    onDuplicate,
    onEdit,
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
    const availableSeriesKeys = useMemo(
        () => series.map((item) => item.key),
        [series],
    );

    const { selectedValue: activeChart, setSelectedValue: setActiveChart } =
        useDashboardInteractiveSelection({
            dashboardId,
            blockId,
            query,
            availableValues: availableSeriesKeys,
        });

    const totals = useMemo(
        () =>
            Object.fromEntries(
                series.map((item) => [
                    item.key,
                    data.reduce((acc, entry) => {
                        const value = parseDashboardNumericValue(
                            entry[item.key],
                        );
                        return acc + (value ?? 0);
                    }, 0),
                ]),
            ),
        [data, series],
    );

    const activeSeries = series.find((item) => item.key === activeChart);

    return (
        <div className="bg-background border-1 rounded-xs flex min-h-0 flex-1 flex-col overflow-hidden">
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
            {isMoving ? (
                <div className="flex min-h-0 flex-1 items-center justify-center px-2 pb-2">
                    <div className="bg-muted/30 border-border/50 flex h-full w-full max-h-full max-w-full flex-col gap-2 self-center overflow-hidden rounded-xs border p-4">
                        <p className="text-xs text-muted-foreground">
                            {activeSeries?.label ?? valueLabel}
                        </p>
                        <p className="text-lg font-semibold">
                            {(totals[activeChart] ?? 0).toLocaleString()}
                        </p>
                        <div className="flex min-h-0 flex-1 items-end gap-1">
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
                    className="aspect-auto flex-1 min-h-0 w-full">
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
            <div className="mt-auto flex w-full items-center justify-between border-t-1 p-3">
                <div className="flex min-w-0 flex-col gap-0.5">
                    <p className="uppercase text-xs font-extrabold text-primary-foreground">
                        {name}
                    </p>
                    <p className="text-xs text-muted-foreground">
                        {description}
                    </p>
                </div>
                <DashboardCardActions
                    onDelete={onDelete}
                    onDuplicate={onDuplicate}
                    onEdit={onEdit}
                />
            </div>
        </div>
    );
};
