import { useMemo } from "react";
import ReactECharts from "echarts-for-react";
import type { EChartsOption } from "echarts";
import { parseDashboardNumericValue } from "../../../model/dashboardChartColumns";
import { useDashboardInteractiveSelection } from "../../model/useInteractiveSelection";
import { DashboardCardActions } from "./DashboardCardActions";
import { buildItemTooltipFormatter, resolveEChartColor } from "./echarts";

export interface DashboardInteractivePiechartDatum {
    [key: string]: string | number | null | undefined;
}

interface DashboardInteractivePiechartCardProps {
    dashboardId: number;
    blockId: number;
    query: string;
    name?: string;
    description?: string;
    data: DashboardInteractivePiechartDatum[];
    nameKey: string;
    valueKey: string;
    defaultActiveKey?: string;
    isMoving?: boolean;
    valueLabel?: string;
    colorKey?: string;
    formatSliceLabel?: (value: string | number) => string;
    formatTooltipLabel?: (value: string | number) => string;
    onDelete?: () => void | Promise<void>;
    onDuplicate?: () => void | Promise<void>;
    onEdit?: () => void;
}

export const DashboardInteractivePiechartCard: React.FC<
    DashboardInteractivePiechartCardProps
> = ({
    dashboardId,
    blockId,
    query,
    name,
    description,
    data,
    nameKey,
    valueKey,
    defaultActiveKey,
    isMoving = false,
    valueLabel = "Value",
    colorKey = "fill",
    formatSliceLabel,
    formatTooltipLabel,
    onDelete,
    onDuplicate,
    onEdit,
}) => {
    const sliceNames = useMemo(
        () =>
            data
                .map((item) => item[nameKey])
                .filter(
                    (item): item is string | number =>
                        typeof item === "string" || typeof item === "number",
                ),
        [data, nameKey],
    );

    const availableSliceNames = useMemo(
        () => sliceNames.map((item) => String(item)),
        [sliceNames],
    );

    const { selectedValue: activeSlice, setSelectedValue: setActiveSlice } =
        useDashboardInteractiveSelection({
            dashboardId,
            blockId,
            query,
            availableValues: availableSliceNames,
        });

    const fallbackSlice =
        activeSlice ||
        (defaultActiveKey && availableSliceNames.includes(defaultActiveKey)
            ? defaultActiveKey
            : availableSliceNames[0] ?? "");

    const sliceValues = useMemo(
        () =>
            Object.fromEntries(
                data.map((item) => [
                    String(item[nameKey] ?? ""),
                    parseDashboardNumericValue(item[valueKey]) ?? 0,
                ]),
            ),
        [data, nameKey, valueKey],
    );

    const activeValue = sliceValues[fallbackSlice] ?? 0;
    const activeLabel =
        (typeof fallbackSlice === "string"
            ? formatSliceLabel?.(fallbackSlice) ?? fallbackSlice
            : String(fallbackSlice)) || valueLabel;

    const chartOption = useMemo<EChartsOption>(() => {
        const seriesData = data
            .map((item) => {
                const rawName = item[nameKey];
                const value = parseDashboardNumericValue(item[valueKey]);
                const rawColor = item[colorKey];

                if (
                    (typeof rawName !== "string" && typeof rawName !== "number") ||
                    value === null
                ) {
                    return null;
                }

                const name = String(rawName);

                return {
                    name,
                    value,
                    selected: name === fallbackSlice,
                    itemStyle:
                        typeof rawColor === "string"
                            ? { color: resolveEChartColor(rawColor) }
                            : undefined,
                };
            })
            .filter((item): item is NonNullable<typeof item> => item !== null);

        return {
            animation: false,
            tooltip: {
                trigger: "item",
                formatter: buildItemTooltipFormatter({
                    formatTooltipLabel,
                }),
                borderWidth: 1,
                backgroundColor: "var(--background)",
                confine: false,
                appendToBody: true,
            },
            title: {
                text: activeValue.toLocaleString(),
                subtext: valueLabel,
                left: "center",
                top: "center",
                textStyle: {
                    fontSize: 24,
                    fontWeight: 700,
                    color: "var(--foreground)",
                },
                subtextStyle: {
                    fontSize: 12,
                    color: "var(--muted-foreground)",
                },
            },
            series: [
                {
                    type: "pie",
                    radius: ["45%", "75%"],
                    selectedMode: "single",
                    selectedOffset: 8,
                    avoidLabelOverlap: true,
                    label: {
                        show: false,
                    },
                    itemStyle: {
                        borderColor: "var(--background)",
                        borderWidth: 5,
                    },
                    emphasis: {
                        scale: true,
                        scaleSize: 8,
                    },
                    data: seriesData,
                },
            ],
        };
    }, [
        activeValue,
        colorKey,
        data,
        fallbackSlice,
        formatTooltipLabel,
        nameKey,
        valueKey,
        valueLabel,
    ]);

    const chartEvents = useMemo(
        () => ({
            mouseover: (params: { name?: string | number }) => {
                if (params?.name !== undefined) {
                    setActiveSlice(String(params.name));
                }
            },
            click: (params: { name?: string | number }) => {
                if (params?.name !== undefined) {
                    setActiveSlice(String(params.name));
                }
            },
        }),
        [setActiveSlice],
    );

    return (
        <div className="bg-background border-1 rounded-xs flex min-h-0 flex-1 flex-col overflow-hidden">
            {isMoving ? (
                <div className="flex min-h-0 flex-1 items-center justify-center p-2">
                    <div className="bg-muted/30 border-border/50 flex h-full w-full max-h-full max-w-full flex-col justify-center self-center overflow-hidden rounded-xs border p-4">
                        <div className="relative mx-auto aspect-square h-full max-h-full w-auto max-w-full">
                            <div className="border-muted-foreground/20 absolute inset-0 rounded-full border-[18px]" />
                            <div className="bg-muted/30 absolute inset-[28%] rounded-full" />
                            <div className="absolute inset-0 flex flex-col items-center justify-center gap-1 px-2 text-center">
                                <p className="truncate text-xs text-muted-foreground">
                                    {activeLabel}
                                </p>
                                <p className="text-lg font-semibold">
                                    {activeValue.toLocaleString()}
                                </p>
                            </div>
                        </div>
                    </div>
                </div>
            ) : (
                <div className="flex min-h-0 flex-1 items-center justify-center px-4 pb-4">
                    <div className="aspect-square h-full max-h-[300px] w-full max-w-[300px]">
                        <ReactECharts
                            option={chartOption}
                            onEvents={chartEvents}
                            notMerge
                            lazyUpdate
                            style={{ height: "100%", width: "100%" }}
                        />
                    </div>
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
