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

export const DashboardGrid: React.FC<DashboardGridProps> = () => {
    const { width, containerRef, mounted } = useContainerWidth();
    const [scale, setScale] =
        React.useState<(typeof GRID_SCALE_OPTIONS)[number]>(1);
    const [isDragging, setIsDragging] = React.useState(false);
    const [isResizing, setIsResizing] = React.useState(false);
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
    ];

    React.useEffect(() => {
        if (!isDragging) return;

        const previousUserSelect = document.body.style.userSelect;

        document.body.style.userSelect = "none";

        return () => {
            document.body.style.userSelect = previousUserSelect;
        };
    }, [isDragging]);

    return (
        <div className="relative flex min-h-0 flex-1 overflow-hidden">
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
                            compactor={freePlacementCompactor}
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
                                    isResizing={isResizing}
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
                                    isResizing={isResizing}
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
                                    isResizing={isResizing}
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
                                    isResizing={isResizing}
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
                                    isResizing={isResizing}
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
                                    isResizing={isResizing}
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
                                    isResizing={isResizing}
                                    name="Horizontal stacked"
                                    description="Two stacked series"
                                    formatYAxisTick={(value) =>
                                        String(value).slice(0, 3)
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
