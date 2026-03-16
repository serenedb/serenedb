import { useMemo } from "react";
import {
    ChartConfig,
    ChartContainer,
    ChartTooltip,
    ChartTooltipContent,
} from "@serene-ui/shared-frontend";
import { Label, Pie, PieChart, Sector } from "recharts";
import type { PieSectorDataItem } from "recharts/types/polar/Pie";
import { parseDashboardNumericValue } from "../../../model/dashboardChartColumns";
import { useDashboardInteractiveSelection } from "../../model/useInteractiveSelection";
import { DashboardCardActions } from "./DashboardCardActions";

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
                                label: formatSliceLabel?.(key) ?? String(key),
                                color,
                            },
                        ],
                    ];
                }),
            ]) satisfies ChartConfig,
        [colorKey, data, formatSliceLabel, nameKey, valueKey, valueLabel],
    );

    const { selectedValue: activeSlice, setSelectedValue: setActiveSlice } =
        useDashboardInteractiveSelection({
            dashboardId,
            blockId,
            query,
            availableValues: availableSliceNames,
        });

    const activeIndex = useMemo(
        () =>
            data.findIndex(
                (item) => String(item[nameKey] ?? "") === activeSlice,
            ),
        [activeSlice, data, nameKey],
    );

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

    const activeItem = activeIndex >= 0 ? data[activeIndex] : undefined;
    const activeValue = parseDashboardNumericValue(activeItem?.[valueKey]) ?? 0;
    const activeLabel = chartConfig[activeSlice]?.label ?? activeSlice;

    return (
        <div className="bg-background border-1 rounded-xs flex min-h-0 flex-1 flex-col overflow-hidden">
            {isMoving ? (
                <div className="flex min-h-0 flex-1 items-center justify-center px-4 pb-4">
                    <div className="bg-muted/30 border-border/50 flex aspect-square h-full max-h-full w-auto max-w-full flex-col items-center justify-center gap-3 self-center overflow-hidden rounded-xs border p-4">
                        <div className="border-muted-foreground/20 aspect-square h-full max-h-32 w-auto max-w-full rounded-full border-[18px]" />
                        <p className="text-xs text-muted-foreground">
                            {activeLabel}
                        </p>
                        <p className="text-lg font-semibold">
                            {activeValue.toLocaleString()}
                        </p>
                    </div>
                </div>
            ) : (
                <div className="flex min-h-0 flex-1 items-center justify-center px-4 pb-4">
                    <ChartContainer
                        config={chartConfig}
                        className="aspect-square h-full max-h-[300px] w-full max-w-[300px]">
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
                                innerRadius={60}
                                strokeWidth={5}
                                onMouseEnter={(_, index) => {
                                    const nextValue = data[index]?.[nameKey];

                                    if (
                                        typeof nextValue === "string" ||
                                        typeof nextValue === "number"
                                    ) {
                                        setActiveSlice(String(nextValue));
                                    }
                                }}
                                shape={(
                                    props: PieSectorDataItem,
                                    index: number,
                                ) => {
                                    const sliceName =
                                        props.payload?.[nameKey] ?? props.name;
                                    const isActive =
                                        String(sliceName ?? index) ===
                                        activeSlice;
                                    const { outerRadius = 0, ...rest } = props;

                                    if (!isActive) {
                                        return <Sector {...props} />;
                                    }

                                    return (
                                        <g>
                                            <Sector
                                                {...rest}
                                                outerRadius={outerRadius + 8}
                                            />
                                            <Sector
                                                {...rest}
                                                outerRadius={outerRadius + 20}
                                                innerRadius={outerRadius + 10}
                                            />
                                        </g>
                                    );
                                }}>
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
                                                        {activeValue.toLocaleString()}
                                                    </tspan>
                                                    <tspan
                                                        x={viewBox.cx}
                                                        y={
                                                            (viewBox.cy || 0) +
                                                            22
                                                        }
                                                        className="fill-muted-foreground text-xs">
                                                        {valueLabel}
                                                    </tspan>
                                                </text>
                                            );
                                        }
                                    }}
                                />
                            </Pie>
                        </PieChart>
                    </ChartContainer>
                </div>
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
