import React from "react";
import ReactGridLayout, { useContainerWidth } from "react-grid-layout";
import {
    gridBounds,
    minMaxSize,
    noCompactor,
    transformStrategy,
} from "react-grid-layout/core";
import "react-grid-layout/css/styles.css";
import "react-resizable/css/styles.css";

import { DashboardScaleButton } from "./DashboardScaleButton";
import { DashboardTextCard } from "./cards/DasboardTextCard";
import {
    DashboardHorizontalBarchartCard,
    type DashboardHorizontalBarchartCardDatum,
    type DashboardHorizontalBarchartCardSeries,
} from "./cards/DashboardHorizontalBarchartCard";
import {
    DashboardInteractiveChartCard,
    type DashboardInteractiveBarchartDatum,
    type DashboardInteractiveBarchartSeries,
} from "./cards/DashboardInteractiveBarchartCard";
import {
    DashboardInteractiveLinechartCard,
    type DashboardInteractiveLinechartDatum,
    type DashboardInteractiveLinechartSeries,
} from "./cards/DashboardInteractiveLinechartCard";
import {
    DashboardInteractivePiechartCard,
    type DashboardInteractivePiechartDatum,
} from "./cards/DashboardInteractivePiechartCard";
import {
    DashboardPiechartCard,
    type DashboardPiechartCardDatum,
} from "./cards/DashboardPiechartCard";
import {
    DashboardLinechartCard,
    type DashboardLinechartCardDatum,
    type DashboardLinechartCardSeries,
} from "./cards/DashboardLinechartCard";
import {
    DashboardVerticalBarchartCard,
    type DashboardVerticalBarchartCardDatum,
    type DashboardVerticalBarchartCardSeries,
} from "./cards/DashboardVerticalBarchartCard";

interface DashboardGridProps {}

const GRID_SCALE_OPTIONS = [0.5, 0.75, 1] as const;
const dashboardBackgroundUrl = new URL(
    "../../../../shared/assets/icons/dashboard-bg.svg",
    import.meta.url,
).href;

const chartData: DashboardInteractiveBarchartDatum[] = [
    { date: "2024-04-01", desktop: 222, mobile: 150 },
    { date: "2024-04-02", desktop: 97, mobile: 180 },
    { date: "2024-04-03", desktop: 167, mobile: 120 },
    { date: "2024-04-04", desktop: 242, mobile: 260 },
    { date: "2024-04-05", desktop: 373, mobile: 290 },
    { date: "2024-04-06", desktop: 301, mobile: 340 },
    { date: "2024-04-07", desktop: 245, mobile: 180 },
    { date: "2024-04-08", desktop: 409, mobile: 320 },
    { date: "2024-04-09", desktop: 59, mobile: 110 },
    { date: "2024-04-10", desktop: 261, mobile: 190 },
    { date: "2024-04-11", desktop: 327, mobile: 350 },
    { date: "2024-04-12", desktop: 292, mobile: 210 },
];

const chartSeries: DashboardInteractiveBarchartSeries[] = [
    {
        key: "desktop",
        label: "Desktop",
        color: "var(--chart-2)",
    },
    {
        key: "mobile",
        label: "Mobile",
        color: "var(--chart-1)",
    },
];

const barChartData: DashboardVerticalBarchartCardDatum[] = [
    { month: "January", desktop: 186, mobile: 80, tablet: 55 },
    { month: "February", desktop: 305, mobile: 200, tablet: 93 },
    { month: "March", desktop: 237, mobile: 120, tablet: 72 },
    { month: "April", desktop: 73, mobile: 190, tablet: 61 },
    { month: "May", desktop: 209, mobile: 130, tablet: 84 },
    { month: "June", desktop: 214, mobile: 140, tablet: 79 },
];

const barChartSeries: DashboardVerticalBarchartCardSeries[] = [
    {
        key: "desktop",
        label: "Desktop",
        color: "var(--chart-1)",
    },
    {
        key: "mobile",
        label: "Mobile",
        color: "var(--chart-2)",
    },
    {
        key: "tablet",
        label: "Tablet",
        color: "var(--chart-3)",
    },
];

const horizontalBarChartData: DashboardHorizontalBarchartCardDatum[] =
    barChartData;
const horizontalBarChartSeries: DashboardHorizontalBarchartCardSeries[] =
    barChartSeries;
const interactiveLineChartData: DashboardInteractiveLinechartDatum[] =
    chartData;
const interactiveLineChartSeries: DashboardInteractiveLinechartSeries[] =
    chartSeries;
const lineChartData: DashboardLinechartCardDatum[] = barChartData;
const lineChartSeries: DashboardLinechartCardSeries[] = barChartSeries;
const pieChartData: DashboardInteractivePiechartDatum[] = [
    { month: "january", visitors: 186, fill: "var(--chart-1)" },
    { month: "february", visitors: 305, fill: "var(--chart-2)" },
    { month: "march", visitors: 237, fill: "var(--chart-3)" },
    { month: "april", visitors: 173, fill: "var(--chart-4)" },
    { month: "may", visitors: 209, fill: "var(--chart-5)" },
];
const simplePieChartData: DashboardPiechartCardDatum[] = [
    { browser: "chrome", visitors: 275, fill: "var(--chart-1)" },
    { browser: "safari", visitors: 200, fill: "var(--chart-2)" },
    { browser: "firefox", visitors: 187, fill: "var(--chart-3)" },
    { browser: "edge", visitors: 173, fill: "var(--chart-4)" },
    { browser: "other", visitors: 90, fill: "var(--chart-5)" },
];

export const DashboardGrid: React.FC<DashboardGridProps> = () => {
    const { width, containerRef, mounted } = useContainerWidth();
    const [scale, setScale] =
        React.useState<(typeof GRID_SCALE_OPTIONS)[number]>(1);
    const [isDragging, setIsDragging] = React.useState(false);
    const [isResizing, setIsResizing] = React.useState(false);
    const isMoving = isResizing || isDragging;
    const freePlacementCompactor = React.useMemo(
        () => ({
            ...noCompactor,
            preventCollision: true,
        }),
        [],
    );
    const positionStrategy = React.useMemo(
        () => ({
            ...transformStrategy,
            scale,
        }),
        [scale],
    );

    const layout = [
        { i: "a", x: 0, y: 0, w: 8, h: 6, minH: 2, minW: 7 },
        {
            i: "b",
            x: 8,
            y: 0,
            w: 10,
            h: 6,
            minW: 10,
            minH: 6,
        },
        {
            i: "c",
            x: 18,
            y: 0,
            w: 9,
            h: 6,
            minW: 8,
            minH: 6,
        },
        {
            i: "d",
            x: 27,
            y: 0,
            w: 9,
            h: 6,
            minW: 8,
            minH: 6,
        },
        {
            i: "e",
            x: 0,
            y: 6,
            w: 12,
            h: 6,
            minW: 10,
            minH: 6,
        },
        {
            i: "f",
            x: 12,
            y: 6,
            w: 12,
            h: 6,
            minW: 10,
            minH: 6,
        },
        {
            i: "g",
            x: 24,
            y: 6,
            w: 12,
            h: 6,
            minW: 10,
            minH: 6,
        },
        {
            i: "h",
            x: 0,
            y: 12,
            w: 12,
            h: 6,
            minW: 10,
            minH: 6,
        },
        {
            i: "i",
            x: 12,
            y: 12,
            w: 12,
            h: 6,
            minW: 10,
            minH: 6,
        },
        {
            i: "j",
            x: 24,
            y: 12,
            w: 12,
            h: 6,
            minW: 10,
            minH: 6,
        },
        {
            i: "k",
            x: 0,
            y: 18,
            w: 12,
            h: 6,
            minW: 10,
            minH: 6,
        },
        {
            i: "l",
            x: 12,
            y: 18,
            w: 12,
            h: 6,
            minW: 10,
            minH: 6,
        },
        {
            i: "m",
            x: 24,
            y: 18,
            w: 12,
            h: 6,
            minW: 10,
            minH: 6,
        },
        {
            i: "n",
            x: 0,
            y: 24,
            w: 12,
            h: 6,
            minW: 10,
            minH: 6,
        },
        {
            i: "o",
            x: 12,
            y: 24,
            w: 12,
            h: 6,
            minW: 10,
            minH: 6,
        },
        {
            i: "p",
            x: 24,
            y: 24,
            w: 12,
            h: 6,
            minW: 10,
            minH: 6,
        },
        {
            i: "q",
            x: 0,
            y: 30,
            w: 12,
            h: 6,
            minW: 10,
            minH: 6,
        },
    ];

    return (
        <div className="relative flex min-h-0 flex-1 overflow-hidden [&_.recharts-sector:focus]:outline-none [&_.recharts-sector:focus-visible]:outline-none [&_.recharts-surface:focus]:outline-none [&_.recharts-surface:focus-visible]:outline-none [&_.recharts-rectangle:focus]:outline-none [&_.recharts-rectangle:focus-visible]:outline-none [&_.recharts-dot:focus]:outline-none [&_.recharts-dot:focus-visible]:outline-none [&_.recharts-symbols:focus]:outline-none [&_.recharts-symbols:focus-visible]:outline-none [&_.recharts-trapezoid:focus]:outline-none [&_.recharts-trapezoid:focus-visible]:outline-none">
            <div className="absolute bottom-4 left-4 z-20">
                <DashboardScaleButton scale={scale} onScaleChange={setScale} />
            </div>

            <div
                className="relative z-10 min-h-0 flex-1 overflow-auto"
                ref={containerRef}
                style={{
                    backgroundImage: `url(${dashboardBackgroundUrl})`,
                    backgroundPosition: "top left",
                    backgroundRepeat: "repeat",
                    backgroundAttachment: "local",
                    backgroundSize: `975px 728px`,
                }}>
                {mounted && (
                    <div
                        className="relative z-10 origin-top-left"
                        style={{
                            transform: `scale(${scale})`,
                            width,
                        }}>
                        <ReactGridLayout
                            layout={layout}
                            width={width}
                            constraints={[gridBounds, minMaxSize]}
                            onDragStart={() => setIsDragging(true)}
                            onDragStop={() => setIsDragging(false)}
                            onResizeStart={() => setIsResizing(true)}
                            onResizeStop={() => setIsResizing(false)}
                            positionStrategy={positionStrategy}
                            gridConfig={{ cols: 36, rowHeight: 36 }}>
                            <div className="flex" key="a">
                                <DashboardTextCard
                                    text={
                                        "SELECT (good, client) FROM a_lot_of_clients WHERE convinience  9 ;"
                                    }
                                />
                            </div>
                            <div className="flex" key="b">
                                <DashboardInteractiveChartCard
                                    data={chartData}
                                    series={chartSeries}
                                    xAxisKey="date"
                                    isMoving={isMoving}
                                    valueLabel="Page Views"
                                    name="My barchart"
                                    description="buth"
                                    formatXAxisTick={(value) =>
                                        new Date(
                                            String(value),
                                        ).toLocaleDateString("en-US", {
                                            month: "short",
                                            day: "numeric",
                                        })
                                    }
                                    formatTooltipLabel={(value) =>
                                        new Date(
                                            String(value),
                                        ).toLocaleDateString("en-US", {
                                            month: "short",
                                            day: "numeric",
                                            year: "numeric",
                                        })
                                    }
                                />
                            </div>
                            <div className="flex" key="c">
                                <DashboardVerticalBarchartCard
                                    data={barChartData}
                                    xAxisKey="month"
                                    barKey="desktop"
                                    barLabel="Desktop"
                                    barColor="var(--chart-1)"
                                    isMoving={isMoving}
                                    name="Vertical single"
                                    description="One series"
                                    formatXAxisTick={(value) =>
                                        String(value).slice(0, 3)
                                    }
                                />
                            </div>
                            <div className="flex" key="d">
                                <DashboardVerticalBarchartCard
                                    data={barChartData}
                                    xAxisKey="month"
                                    barKey="desktop"
                                    series={barChartSeries.slice(0, 2)}
                                    isMoving={isMoving}
                                    name="Vertical multiple"
                                    description="Two grouped series"
                                    formatXAxisTick={(value) =>
                                        String(value).slice(0, 3)
                                    }
                                />
                            </div>
                            <div className="flex" key="e">
                                <DashboardVerticalBarchartCard
                                    data={barChartData}
                                    xAxisKey="month"
                                    barKey="desktop"
                                    series={barChartSeries.slice(0, 2)}
                                    isStacked
                                    isMoving={isMoving}
                                    name="Vertical stacked"
                                    description="Two stacked series"
                                    formatXAxisTick={(value) =>
                                        String(value).slice(0, 3)
                                    }
                                />
                            </div>
                            <div className="flex" key="f">
                                <DashboardHorizontalBarchartCard
                                    data={horizontalBarChartData}
                                    yAxisKey="month"
                                    barKey="desktop"
                                    barLabel="Desktop"
                                    barColor="var(--chart-1)"
                                    isMoving={isMoving}
                                    name="Horizontal single"
                                    description="One series"
                                    formatYAxisTick={(value) =>
                                        String(value).slice(0, 3)
                                    }
                                />
                            </div>
                            <div className="flex" key="g">
                                <DashboardHorizontalBarchartCard
                                    data={horizontalBarChartData}
                                    yAxisKey="month"
                                    barKey="desktop"
                                    series={horizontalBarChartSeries.slice(
                                        0,
                                        2,
                                    )}
                                    isMoving={isMoving}
                                    name="Horizontal multiple"
                                    description="Two grouped series"
                                    formatYAxisTick={(value) =>
                                        String(value).slice(0, 3)
                                    }
                                />
                            </div>
                            <div className="flex" key="h">
                                <DashboardHorizontalBarchartCard
                                    data={horizontalBarChartData}
                                    yAxisKey="month"
                                    barKey="desktop"
                                    series={horizontalBarChartSeries.slice(
                                        0,
                                        2,
                                    )}
                                    isStacked
                                    isMoving={isMoving}
                                    name="Horizontal stacked"
                                    description="Two stacked series"
                                    formatYAxisTick={(value) =>
                                        String(value).slice(0, 3)
                                    }
                                />
                            </div>
                            <div className="flex" key="i">
                                <DashboardInteractiveLinechartCard
                                    data={interactiveLineChartData}
                                    series={interactiveLineChartSeries}
                                    xAxisKey="date"
                                    isMoving={isMoving}
                                    valueLabel="Page Views"
                                    name="Interactive line"
                                    description="Selectable time series"
                                    formatXAxisTick={(value) =>
                                        new Date(
                                            String(value),
                                        ).toLocaleDateString("en-US", {
                                            month: "short",
                                            day: "numeric",
                                        })
                                    }
                                    formatTooltipLabel={(value) =>
                                        new Date(
                                            String(value),
                                        ).toLocaleDateString("en-US", {
                                            month: "short",
                                            day: "numeric",
                                            year: "numeric",
                                        })
                                    }
                                />
                            </div>
                            <div className="flex" key="j">
                                <DashboardLinechartCard
                                    data={lineChartData}
                                    xAxisKey="month"
                                    lineKey="desktop"
                                    series={lineChartSeries.slice(0, 2)}
                                    isMoving={isMoving}
                                    name="Line chart"
                                    description="Single or multiple lines"
                                    formatXAxisTick={(value) =>
                                        String(value).slice(0, 3)
                                    }
                                />
                            </div>
                            <div className="flex" key="k">
                                <DashboardLinechartCard
                                    data={lineChartData}
                                    xAxisKey="month"
                                    lineKey="desktop"
                                    lineLabel="Desktop"
                                    lineColor="var(--chart-1)"
                                    lineType="linear"
                                    isMoving={isMoving}
                                    name="Line linear"
                                    description="Straight segments"
                                    formatXAxisTick={(value) =>
                                        String(value).slice(0, 3)
                                    }
                                />
                            </div>
                            <div className="flex" key="l">
                                <DashboardLinechartCard
                                    data={lineChartData}
                                    xAxisKey="month"
                                    lineKey="desktop"
                                    lineLabel="Desktop"
                                    lineColor="var(--chart-1)"
                                    lineType="step"
                                    isMoving={isMoving}
                                    name="Line step"
                                    description="Stepped segments"
                                    formatXAxisTick={(value) =>
                                        String(value).slice(0, 3)
                                    }
                                />
                            </div>
                            <div className="flex" key="m">
                                <DashboardInteractivePiechartCard
                                    data={pieChartData}
                                    nameKey="month"
                                    valueKey="visitors"
                                    valueLabel="Visitors"
                                    isMoving={isMoving}
                                    name="Interactive pie"
                                    description="Active donut segment"
                                    formatSliceLabel={(value) =>
                                        String(value).replace(/^./, (char) =>
                                            char.toUpperCase(),
                                        )
                                    }
                                />
                            </div>
                            <div className="flex" key="n">
                                <DashboardPiechartCard
                                    data={simplePieChartData}
                                    nameKey="browser"
                                    valueKey="visitors"
                                    valueLabel="Visitors"
                                    isMoving={isMoving}
                                    name="Pie chart"
                                    description="Simple pie distribution"
                                    formatSliceLabel={(value) =>
                                        String(value).replace(/^./, (char) =>
                                            char.toUpperCase(),
                                        )
                                    }
                                />
                            </div>
                            <div className="flex" key="o">
                                <DashboardPiechartCard
                                    data={simplePieChartData}
                                    nameKey="browser"
                                    valueKey="visitors"
                                    valueLabel="Visitors"
                                    showLabels
                                    isMoving={isMoving}
                                    name="Pie chart labels"
                                    description="Simple pie with labels"
                                    formatSliceLabel={(value) =>
                                        String(value).replace(/^./, (char) =>
                                            char.toUpperCase(),
                                        )
                                    }
                                />
                            </div>
                            <div className="flex" key="p">
                                <DashboardPiechartCard
                                    data={simplePieChartData}
                                    nameKey="browser"
                                    valueKey="visitors"
                                    valueLabel="Visitors"
                                    variant="donut"
                                    isMoving={isMoving}
                                    name="Donut chart"
                                    description="Simple donut distribution"
                                    formatSliceLabel={(value) =>
                                        String(value).replace(/^./, (char) =>
                                            char.toUpperCase(),
                                        )
                                    }
                                />
                            </div>
                            <div className="flex" key="q">
                                <DashboardPiechartCard
                                    data={simplePieChartData}
                                    nameKey="browser"
                                    valueKey="visitors"
                                    valueLabel="Visitors"
                                    variant="donut"
                                    showCenterLabel
                                    isMoving={isMoving}
                                    name="Donut chart text"
                                    description="Donut with centered total"
                                    formatSliceLabel={(value) =>
                                        String(value).replace(/^./, (char) =>
                                            char.toUpperCase(),
                                        )
                                    }
                                />
                            </div>
                        </ReactGridLayout>
                    </div>
                )}
            </div>
        </div>
    );
};
