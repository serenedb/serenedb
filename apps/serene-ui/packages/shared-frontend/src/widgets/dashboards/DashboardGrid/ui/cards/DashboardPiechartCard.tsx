import { useMemo } from "react";
import ReactECharts from "echarts-for-react";
import type { EChartsOption } from "echarts";

import { DashboardChartCardBase } from "./DashboardChartCardBase";
import {
    buildItemTooltipFormatter,
    resolveEChartColor,
    toNumericOrNull,
} from "./echarts";

export interface DashboardPiechartCardDatum {
    [key: string]: string | number | null | undefined;
}

interface DashboardPiechartCardProps {
    name?: string;
    description?: string;
    onDelete?: () => void | Promise<void>;
    onDuplicate?: () => void | Promise<void>;
    onEdit?: () => void;
    data: DashboardPiechartCardDatum[];
    nameKey: string;
    valueKey: string;
    valueLabel?: string;
    colorKey?: string;
    showLabels?: boolean;
    variant?: "pie" | "donut";
    showCenterLabel?: boolean;
    centerValue?: string | number;
    centerLabel?: string;
    isMoving?: boolean;
    formatSliceLabel?: (value: string | number) => string;
}

export const DashboardPiechartCard: React.FC<DashboardPiechartCardProps> = ({
    name,
    description,
    onDelete,
    onDuplicate,
    onEdit,
    data,
    nameKey,
    valueKey,
    valueLabel = "Value",
    colorKey = "fill",
    showLabels = false,
    variant = "pie",
    showCenterLabel = false,
    centerValue,
    centerLabel,
    isMoving = false,
    formatSliceLabel,
}) => {
    const totalValue = useMemo(
        () =>
            data.reduce((acc, item) => {
                const value = toNumericOrNull(item[valueKey]);
                return acc + (value ?? 0);
            }, 0),
        [data, valueKey],
    );

    const resolvedCenterValue = centerValue ?? totalValue;
    const resolvedCenterLabel = centerLabel ?? valueLabel;
    const isDonut = variant === "donut";

    const chartOption = useMemo<EChartsOption>(() => {
        const seriesData = data
            .map((item) => {
                const rawName = item[nameKey];
                const value = toNumericOrNull(item[valueKey]);
                const rawColor = item[colorKey];

                if (
                    (typeof rawName !== "string" &&
                        typeof rawName !== "number") ||
                    value === null
                ) {
                    return null;
                }

                return {
                    name: String(rawName),
                    value,
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
                    formatTooltipLabel: formatSliceLabel,
                }),
                borderWidth: 1,
                backgroundColor: "var(--background)",
                confine: false,
                appendToBody: true,
            },
            title:
                showCenterLabel && !isDonut
                    ? {
                          text:
                              typeof resolvedCenterValue === "number"
                                  ? resolvedCenterValue.toLocaleString()
                                  : String(resolvedCenterValue),
                          subtext: resolvedCenterLabel,
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
                      }
                    : undefined,
            series: [
                {
                    type: "pie",
                    name: valueLabel,
                    radius: isDonut ? ["40%", "70%"] : "75%",
                    avoidLabelOverlap: !isDonut,
                    label: {
                        show: isDonut ? false : showLabels,
                        position: isDonut ? "center" : "outside",
                        formatter: ({ name: labelName }: { name: string }) => {
                            return formatSliceLabel?.(labelName) ?? labelName;
                        },
                    },
                    emphasis: isDonut
                        ? {
                              label: {
                                  show: true,
                                  fontSize: 40,
                                  fontWeight: "bold",
                              },
                          }
                        : undefined,
                    labelLine: { show: isDonut ? false : showLabels },
                    data: seriesData,
                },
            ],
        };
    }, [
        colorKey,
        data,
        formatSliceLabel,
        isDonut,
        nameKey,
        resolvedCenterLabel,
        resolvedCenterValue,
        showCenterLabel,
        showLabels,
        valueKey,
    ]);

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
                        <div className="relative mx-auto aspect-square h-full max-h-full w-auto max-w-full">
                            {isDonut ? (
                                <>
                                    <div className="border-muted-foreground/20 absolute inset-0 rounded-full border-[18px]" />
                                    <div className="bg-muted/30 absolute inset-[28%] rounded-full" />
                                </>
                            ) : (
                                <div className="bg-muted-foreground/20 absolute inset-0 rounded-full" />
                            )}
                        </div>
                    </div>
                </div>
            ) : (
                <div className="flex min-h-0 flex-1 items-center justify-center px-4 pb-4">
                    <div className="aspect-square h-full min-h-0 max-h-[250px] w-full max-w-[250px]">
                        <ReactECharts
                            option={chartOption}
                            notMerge
                            lazyUpdate
                            style={{ height: "100%", width: "100%" }}
                        />
                    </div>
                </div>
            )}
        </DashboardChartCardBase>
    );
};
