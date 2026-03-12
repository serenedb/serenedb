import { useMemo } from "react";
import {
    ChartConfig,
    ChartContainer,
    ChartTooltip,
    ChartTooltipContent,
} from "@serene-ui/shared-frontend";
import { Label, Pie, PieChart } from "recharts";

import { DashboardChartCardBase } from "./DashboardChartCardBase";

export interface DashboardPiechartCardDatum {
    [key: string]: string | number | null | undefined;
}

interface DashboardPiechartCardProps {
    name?: string;
    description?: string;
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
    const chartConfig = useMemo(
        () =>
            Object.fromEntries([
                [valueKey, { label: valueLabel }],
                ...data.flatMap((item) => {
                    const key = item[nameKey];
                    const color = item[colorKey];

                    if (
                        (typeof key !== "string" && typeof key !== "number") ||
                        typeof color !== "string"
                    ) {
                        return [];
                    }

                    return [
                        [
                            String(key),
                            {
                                label:
                                    formatSliceLabel?.(key) ?? String(key),
                                color,
                            },
                        ],
                    ];
                }),
            ]) satisfies ChartConfig,
        [colorKey, data, formatSliceLabel, nameKey, valueKey, valueLabel],
    );

    const totalValue = useMemo(
        () =>
            data.reduce((acc, item) => {
                const value = item[valueKey];
                return acc + (typeof value === "number" ? value : 0);
            }, 0),
        [data, valueKey],
    );

    const resolvedCenterValue = centerValue ?? totalValue;
    const resolvedCenterLabel = centerLabel ?? valueLabel;
    const isDonut = variant === "donut";

    return (
        <DashboardChartCardBase name={name} description={description}>
            {isMoving ? (
                <div className="flex min-h-0 flex-1 items-center justify-center px-4 pb-4">
                    <div
                        className={
                            isDonut
                                ? "bg-muted/30 border-border/50 flex aspect-square h-full max-h-full w-auto max-w-full self-center rounded-full border-[18px]"
                                : "bg-muted/30 border-border/50 flex aspect-square h-full max-h-full w-auto max-w-full self-center rounded-full border"
                        }
                    />
                </div>
            ) : (
                <div className="flex min-h-0 flex-1 items-center justify-center px-4 pb-4">
                    <ChartContainer
                        config={chartConfig}
                        className={
                            showLabels
                                ? "aspect-square h-full min-h-0 max-h-[250px] w-full max-w-[250px] [&_.recharts-pie-label-text]:fill-foreground"
                                : "aspect-square h-full min-h-0 max-h-[250px] w-full max-w-[250px]"
                        }>
                        <PieChart>
                            <ChartTooltip
                                cursor={false}
                                content={(props) => (
                                    <ChartTooltipContent {...props} hideLabel />
                                )}
                            />
                            <Pie
                                data={data}
                                dataKey={valueKey}
                                nameKey={nameKey}
                                label={showLabels}
                                innerRadius={isDonut ? 60 : undefined}
                                strokeWidth={isDonut ? 5 : undefined}>
                                {showCenterLabel && isDonut && (
                                    <Label
                                        content={({ viewBox }) => {
                                            if (
                                                viewBox &&
                                                "cx" in viewBox &&
                                                "cy" in viewBox
                                            ) {
                                                return (
                                                    <text
                                                        x={viewBox.cx}
                                                        y={viewBox.cy}
                                                        textAnchor="middle"
                                                        dominantBaseline="middle">
                                                        <tspan
                                                            x={viewBox.cx}
                                                            y={viewBox.cy}
                                                            className="fill-foreground text-2xl font-bold">
                                                            {typeof resolvedCenterValue ===
                                                            "number"
                                                                ? resolvedCenterValue.toLocaleString()
                                                                : resolvedCenterValue}
                                                        </tspan>
                                                        <tspan
                                                            x={viewBox.cx}
                                                            y={
                                                                (viewBox.cy ||
                                                                    0) + 22
                                                            }
                                                            className="fill-muted-foreground text-xs">
                                                            {resolvedCenterLabel}
                                                        </tspan>
                                                    </text>
                                                );
                                            }
                                        }}
                                    />
                                )}
                            </Pie>
                        </PieChart>
                    </ChartContainer>
                </div>
            )}
        </DashboardChartCardBase>
    );
};
