import React from "react";
import ReactGridLayout, { useContainerWidth } from "react-grid-layout";
import {
    gridBounds,
    minMaxSize,
    noCompactor,
    transformStrategy,
} from "react-grid-layout/core";
import { wrapCompactor } from "react-grid-layout/extras";
import "react-grid-layout/css/styles.css";
import "react-resizable/css/styles.css";

import { DashboardScaleButton } from "./DashboardScaleButton";
import { DashboardTextCard } from "./cards/DasboardTextCard";

interface DashboardGridProps {}

const GRID_SCALE_OPTIONS = [0.5, 0.75, 1] as const;
const dashboardBackgroundUrl = new URL(
    "../../../../shared/assets/icons/dashboard-bg.svg",
    import.meta.url,
).href;

export const DashboardGrid: React.FC<DashboardGridProps> = () => {
    const { width, containerRef, mounted } = useContainerWidth();
    const [scale, setScale] =
        React.useState<(typeof GRID_SCALE_OPTIONS)[number]>(1);
    const [isDragging, setIsDragging] = React.useState(false);
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
        { i: "a", x: 0, y: 0, w: 3, h: 5 },
        { i: "b", x: 3, y: 2, w: 3, h: 2 },
        { i: "c", x: 3, y: 0, w: 1, h: 2 },
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
            <div className="absolute top-4 right-4 z-20">
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
                            positionStrategy={positionStrategy}
                            gridConfig={{ cols: 14, rowHeight: 36 }}>
                            <div className="flex" key="a">
                                <DashboardTextCard
                                    text={
                                        "SELECT (good, client) FROM a_lot_of_clients WHERE convinience  9 ;"
                                    }
                                />
                            </div>
                            <div className="border-1" key="b">
                                b
                            </div>
                            <div className="border-1" key="c">
                                c
                            </div>
                        </ReactGridLayout>
                    </div>
                )}
            </div>
        </div>
    );
};
