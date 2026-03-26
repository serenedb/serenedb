import React from "react";
import type { DashboardSchema } from "@serene-ui/shared-core";
import type { AddDashboardCardInput } from "../../../../entities/dashboard-card";
import {
    toDashboardCardUpdateInput,
    useUpdateDashboardCard,
} from "../../../../entities/dashboard-card";
import type { LayoutItem } from "react-grid-layout";
import { transformStrategy } from "react-grid-layout/core";
import { getDashboardBlockMinSize } from "./dashboardBlockMinSize";

const GRID_SCALE_OPTIONS = [0.5, 0.75, 1] as const;
const RESIZE_SETTLE_DELAY_MS = 220;

interface UseDashboardGridProps {
    currentDashboard?: DashboardSchema | null;
}

export const useDashboardGrid = ({
    currentDashboard,
}: UseDashboardGridProps) => {
    const containerRef = React.useRef<HTMLDivElement | null>(null);
    const { mutate: updateDashboardCard } = useUpdateDashboardCard();
    const [mounted, setMounted] = React.useState(false);
    const [width, setWidth] = React.useState(0);
    const [scale, setScale] =
        React.useState<(typeof GRID_SCALE_OPTIONS)[number]>(1);
    const [isDragging, setIsDragging] = React.useState(false);
    const [isResizing, setIsResizing] = React.useState(false);
    const [isResizeSettling, setIsResizeSettling] = React.useState(false);
    const resizeSettleTimeoutRef = React.useRef<number | null>(null);
    const isMoving = isResizing || isDragging || isResizeSettling;

    const clearResizeSettleTimeout = React.useCallback(() => {
        if (resizeSettleTimeoutRef.current === null) {
            return;
        }

        window.clearTimeout(resizeSettleTimeoutRef.current);
        resizeSettleTimeoutRef.current = null;
    }, []);

    React.useEffect(() => {
        const element = containerRef.current;

        if (!element) {
            return;
        }

        const updateWidth = () => {
            // Keep width in sync with real panel size changes from react-resizable-panels.
            setWidth((prev) => {
                const next = Math.floor(element.clientWidth);
                return prev !== next ? next : prev;
            });
            setMounted(true);
        };

        updateWidth();

        if (typeof ResizeObserver === "undefined") {
            window.addEventListener("resize", updateWidth);

            return () => {
                window.removeEventListener("resize", updateWidth);
            };
        }

        const resizeObserver = new ResizeObserver(() => {
            updateWidth();
        });

        resizeObserver.observe(element);

        return () => {
            resizeObserver.disconnect();
        };
    }, []);

    const positionStrategy = React.useMemo(
        () => ({
            ...transformStrategy,
            scale,
        }),
        [scale],
    );

    const blocks = React.useMemo(
        () =>
            currentDashboard?.blocks
                ?.slice()
                .sort(
                    (left, right) =>
                        left.bounds.y - right.bounds.y ||
                        left.bounds.x - right.bounds.x ||
                        left.id - right.id,
                ) ?? [],
        [currentDashboard],
    );

    const layout = React.useMemo(
        () =>
            blocks.map((block) => ({
                ...getDashboardBlockMinSize(block.type),
                i: String(block.id),
                x: block.bounds.x,
                y: block.bounds.y,
                w: block.bounds.width,
                h: block.bounds.height,
            })),
        [blocks],
    );

    const nextCardBounds = React.useMemo<AddDashboardCardInput["bounds"]>(
        () => ({
            x: 0,
            y: blocks.reduce(
                (maxY, block) =>
                    Math.max(maxY, block.bounds.y + block.bounds.height),
                0,
            ),
            width: 12,
            height: 6,
        }),
        [blocks],
    );

    const handleCardBoundsChange = React.useCallback(
        (layoutItem: LayoutItem | null) => {
            if (!currentDashboard || !layoutItem) {
                return;
            }

            const cardId = Number(layoutItem.i);
            const card = blocks.find((block) => block.id === cardId);

            if (!card || card.id < 0) {
                return;
            }

            const nextBounds = {
                x: layoutItem.x,
                y: layoutItem.y,
                width: layoutItem.w,
                height: layoutItem.h,
            };

            if (
                card.bounds.x === nextBounds.x &&
                card.bounds.y === nextBounds.y &&
                card.bounds.width === nextBounds.width &&
                card.bounds.height === nextBounds.height
            ) {
                return;
            }

            updateDashboardCard({
                dashboardId: currentDashboard.id,
                card: {
                    ...toDashboardCardUpdateInput(card),
                    id: card.id,
                    bounds: nextBounds,
                },
            });
        },
        [blocks, currentDashboard, updateDashboardCard],
    );

    const handleDragStart = React.useCallback(() => {
        setIsDragging(true);
    }, []);

    const handleDragStop = React.useCallback(
        (layoutItem: LayoutItem | null) => {
            setIsDragging(false);
            handleCardBoundsChange(layoutItem);
        },
        [handleCardBoundsChange],
    );

    const handleResizeStart = React.useCallback(() => {
        clearResizeSettleTimeout();
        setIsResizeSettling(false);
        setIsResizing(true);
    }, [clearResizeSettleTimeout]);

    const handleResizeStop = React.useCallback(
        (layoutItem: LayoutItem | null) => {
            setIsResizing(false);
            handleCardBoundsChange(layoutItem);
            clearResizeSettleTimeout();
            setIsResizeSettling(true);
            resizeSettleTimeoutRef.current = window.setTimeout(() => {
                setIsResizeSettling(false);
                resizeSettleTimeoutRef.current = null;
            }, RESIZE_SETTLE_DELAY_MS);
        },
        [clearResizeSettleTimeout, handleCardBoundsChange],
    );

    React.useEffect(
        () => () => {
            clearResizeSettleTimeout();
        },
        [clearResizeSettleTimeout],
    );

    return {
        blocks,
        containerRef,
        handleDragStart,
        handleDragStop,
        handleResizeStart,
        handleResizeStop,
        isMoving,
        layout,
        mounted,
        nextCardBounds,
        positionStrategy,
        scale,
        setScale,
        width,
    };
};
