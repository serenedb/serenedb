import { useEffect, useMemo } from "react";
import {
    Select,
    SelectContent,
    SelectItem,
    SelectTrigger,
    SelectValue,
} from "@serene-ui/shared-frontend";
import ReactECharts from "echarts-for-react";
import type { EChartsOption } from "echarts";
import { parseDashboardNumericValue } from "../../../model/dashboardChartColumns";
import { useDashboardInteractiveSelection } from "../../model/useInteractiveSelection";
import { DashboardCardActions } from "./DashboardCardActions";
import {
    baseCartesianGrid,
    buildAxisTooltipFormatter,
    mapLineTypeToECharts,
    resolveEChartColor,
    toNumericOrNull,
} from "./echarts";

export interface DashboardInteractiveAreaChartSeries {
    key: string;
    label: string;
    color: string;
}

export interface DashboardInteractiveAreaChartDatum {
    [key: string]: string | number | null | undefined;
}

export type DashboardInteractiveAreaChartType =
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

interface DashboardInteractiveAreaChartCardProps {
    dashboardId: number;
    blockId: number;
    query: string;
    name?: string;
    description?: string;
    data: DashboardInteractiveAreaChartDatum[];
    series: DashboardInteractiveAreaChartSeries[];
    xAxisKey: string;
    defaultActiveKey?: string;
    isMoving?: boolean;
    valueLabel?: string;
    lineType?: DashboardInteractiveAreaChartType;
    formatXAxisTick?: (value: string | number) => string;
    formatTooltipLabel?: (value: string | number) => string;
    onDelete?: () => void | Promise<void>;
    onDuplicate?: () => void | Promise<void>;
    onEdit?: () => void;
}

export const DashboardInteractiveAreaChartCard: React.FC<
    DashboardInteractiveAreaChartCardProps
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
    lineType = "monotone",
    formatXAxisTick,
    formatTooltipLabel,
    onDelete,
    onDuplicate,
    onEdit,
}) => {
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

    useEffect(() => {
        if (!isMoving) return;

        const preveousUserSelect = document.body.style.userSelect;
        document.body.style.userSelect = "none";

        return () => {
            document.body.style.userSelect = preveousUserSelect;
        };
    }, [isMoving]);

    const totals = useMemo(
        () =>
            Object.fromEntries(
                series.map((item) => [
                    item.key,
                    data.reduce((acc, entry) => {
                        const value = parseDashboardNumericValue(entry[item.key]);
                        return acc + (value ?? 0);
                    }, 0),
                ]),
            ),
        [data, series],
    );

    const activeSeries =
        series.find((item) => item.key === activeChart) ??
        series.find((item) => item.key === defaultActiveKey) ??
        series[0];

    const activeKey = activeSeries?.key ?? "";
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
            series: activeSeries
                ? [
                      (() => {
                          const color = resolveEChartColor(activeSeries.color);

                          return {
                              type: "line",
                              name: activeSeries.label,
                              data: data.map((row) => toNumericOrNull(row[activeKey])),
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
                      })(),
                  ]
                : [],
        };
    }, [
        activeKey,
        activeSeries,
        data,
        formatTooltipLabel,
        formatXAxisTick,
        smooth,
        step,
        xAxisKey,
    ]);

    return (
        <div className="bg-background border-1 rounded-xs flex min-h-0 flex-1 flex-col overflow-hidden">
            <div className="p-2">
                <Select value={activeKey} onValueChange={setActiveChart}>
                    <SelectTrigger className="w-full h-auto min-h-9">
                        <SelectValue>
                            <div className="flex items-center gap-1 text-left">
                                <span className="text-xs text-muted-foreground">
                                    {activeSeries?.label ?? valueLabel}
                                </span>
                                <span className="text-xs leading-none">
                                    ({(totals[activeKey] ?? 0).toLocaleString()})
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
                                        ({(totals[chart.key] ?? 0).toLocaleString()})
                                    </span>
                                </div>
                            </SelectItem>
                        ))}
                    </SelectContent>
                </Select>
            </div>
            {isMoving ? (
                <div className="flex min-h-0 flex-1 items-center justify-center px-4 pb-4">
                    <div className="bg-muted/30 border-border/50 flex aspect-[4/3] w-full max-h-full max-w-full flex-col gap-1 self-center overflow-hidden rounded-xs border px-4 py-3">
                        <p className="text-xs leading-none text-muted-foreground">
                            {activeSeries?.label ?? valueLabel}
                        </p>
                        <p className="text-lg leading-none font-semibold">
                            {(totals[activeKey] ?? 0).toLocaleString()}
                        </p>
                        <div className="flex min-h-0 flex-[2] items-end">
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
                                    strokeWidth="4"
                                    className="text-muted-foreground/30"
                                    points="0,76 22,64 44,68 66,30 88,38 110,28 132,52 154,22 176,26 198,14 220,34 240,18"
                                />
                            </svg>
                        </div>
                    </div>
                </div>
            ) : (
                <div className="aspect-auto h-[250px] min-h-0 w-full">
                    <ReactECharts
                        option={chartOption}
                        notMerge
                        lazyUpdate
                        style={{ height: "100%", width: "100%" }}
                    />
                </div>
            )}
            <div className="mt-auto flex w-full items-center justify-between border-t-1 p-3">
                <div className="flex min-w-0 flex-col gap-0.5">
                    <p className="uppercase text-xs font-extrabold text-primary-foreground">
                        {name}
                    </p>
                    <p className="text-xs text-muted-foreground">{description}</p>
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
