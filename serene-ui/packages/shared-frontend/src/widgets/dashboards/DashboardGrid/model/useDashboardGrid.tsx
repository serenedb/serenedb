import React from "react";
import type {
    DashboardBlockSchema,
    DashboardSchema,
} from "@serene-ui/shared-core";
import type { AddDashboardCardInput } from "../../../../entities/dashboard-card";
import {
    toDashboardCardUpdateInput,
    useUpdateDashboardCard,
} from "../../../../entities/dashboard-card";
import { type LayoutItem, useContainerWidth } from "react-grid-layout";
import { transformStrategy } from "react-grid-layout/core";

const GRID_SCALE_OPTIONS = [0.5, 0.75, 1] as const;

interface UseDashboardGridProps {
    currentDashboard?: DashboardSchema | null;
}

export const useDashboardGrid = ({
    currentDashboard,
}: UseDashboardGridProps) => {
    const { width, containerRef, mounted } = useContainerWidth();
    const { mutate: updateDashboardCard } = useUpdateDashboardCard();
    const [scale, setScale] =
        React.useState<(typeof GRID_SCALE_OPTIONS)[number]>(1);
    const [isDragging, setIsDragging] = React.useState(false);
    const [isResizing, setIsResizing] = React.useState(false);
    const isMoving = isResizing || isDragging;

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
                i: String(block.id),
                x: block.bounds.x,
                y: block.bounds.y,
                w: block.bounds.width,
                h: block.bounds.height,
                minW: block.bounds.min_width,
                minH: block.bounds.min_height,
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
            min_width: 8,
            min_height: 6,
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
                min_width: layoutItem.minW ?? card.bounds.min_width,
                min_height: layoutItem.minH ?? card.bounds.min_height,
            };

            if (
                card.bounds.x === nextBounds.x &&
                card.bounds.y === nextBounds.y &&
                card.bounds.width === nextBounds.width &&
                card.bounds.height === nextBounds.height &&
                card.bounds.min_width === nextBounds.min_width &&
                card.bounds.min_height === nextBounds.min_height
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
        setIsResizing(true);
    }, []);

    const handleResizeStop = React.useCallback(
        (layoutItem: LayoutItem | null) => {
            setIsResizing(false);
            handleCardBoundsChange(layoutItem);
        },
        [handleCardBoundsChange],
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
