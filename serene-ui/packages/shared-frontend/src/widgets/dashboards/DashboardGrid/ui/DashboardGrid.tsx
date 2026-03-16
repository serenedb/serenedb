import React from "react";
import type {
    DashboardBlockSchema,
    DashboardSchema,
} from "@serene-ui/shared-core";
import {
    toDashboardCardAddInput,
    useAddDashboardCard,
    useDeleteDashboardCard,
} from "../../../../entities/dashboard-card";
import ReactGridLayout from "react-grid-layout";
import { gridBounds, minMaxSize } from "react-grid-layout/core";
import "react-grid-layout/css/styles.css";
import "react-resizable/css/styles.css";

import {
    cleanupDashboardInteractiveSelections,
} from "../model/interactiveSelection";
import { useDashboardGrid } from "../model/useDashboardGrid";
import { DashboardAddCardButton } from "./DashboardAddCardButton";
import { DashboardScaleButton } from "./DashboardScaleButton";
import { DashboardTextCard } from "./cards/DashboardTextCard";
import { DashboardQueryBlockCard } from "./DashboardQueryBlockCard";
import { DashboardSettingsButton } from "./DashboardSettingsButton";

interface DashboardGridProps {
    currentDashboard?: DashboardSchema | null;
    editedBlock?: DashboardBlockSchema | null;
    onCloseEditor?: () => void;
    onEditBlock?: (block: DashboardBlockSchema) => void;
}

const GRID_COLUMNS = 36;

const dashboardBackgroundUrl = new URL(
    "../../../../shared/assets/icons/dashboard-bg.svg",
    import.meta.url,
).href;

const renderDashboardBlock = (
    block: DashboardBlockSchema,
    isMoving: boolean,
    dashboardId: number,
    dashboard?: DashboardSchema | null,
    onDeleteBlock?: (block: DashboardBlockSchema) => void | Promise<void>,
    onDuplicateBlock?: (block: DashboardBlockSchema) => void | Promise<void>,
    onEditBlock?: (block: DashboardBlockSchema) => void,
) => {
    const handleDelete = () => onDeleteBlock?.(block);
    const handleDuplicate = () => onDuplicateBlock?.(block);
    const handleEdit = () => onEditBlock?.(block);

    switch (block.type) {
        case "text":
            return (
                <DashboardTextCard
                    block={block}
                    dashboardId={dashboardId}
                    onDelete={handleDelete}
                    onDuplicate={handleDuplicate}
                />
            );
        case "spacer":
            return <div className="flex-1" />;
        case "table":
        case "single_string":
        case "bar_chart":
        case "line_chart":
        case "pie_chart":
            return (
                <DashboardQueryBlockCard
                    block={block}
                    dashboard={dashboard}
                    isMoving={isMoving}
                    onDelete={handleDelete}
                    onDuplicate={handleDuplicate}
                    onEdit={handleEdit}
                />
            );
    }

    return null;
};

export const DashboardGrid: React.FC<DashboardGridProps> = ({
    currentDashboard,
    editedBlock,
    onCloseEditor,
    onEditBlock,
}) => {
    const { mutateAsync: addDashboardCard } = useAddDashboardCard();
    const { mutateAsync: deleteDashboardCard } = useDeleteDashboardCard();
    const {
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
    } = useDashboardGrid({
        currentDashboard,
    });

    const previewBlocks = React.useMemo(
        () =>
            blocks.map((block) =>
                editedBlock && editedBlock.id === block.id
                    ? editedBlock
                    : block,
            ),
        [blocks, editedBlock],
    );

    React.useEffect(() => {
        if (!currentDashboard) {
            return;
        }

        cleanupDashboardInteractiveSelections({
            dashboardId: currentDashboard.id,
            blocks: previewBlocks,
        });
    }, [currentDashboard, previewBlocks]);

    const handleDuplicateBlock = React.useCallback(
        async (block: DashboardBlockSchema) => {
            if (!currentDashboard || block.id < 0) {
                return;
            }

            const blockIndex = blocks.findIndex(
                (currentBlock) => currentBlock.id === block.id,
            );

            if (blockIndex === -1) {
                return;
            }

            const duplicateCard = toDashboardCardAddInput(block);
            const nextX = Math.min(
                block.bounds.x + 1,
                Math.max(GRID_COLUMNS - block.bounds.width, 0),
            );

            await addDashboardCard({
                dashboardId: currentDashboard.id,
                index: blockIndex + 1,
                card: {
                    ...duplicateCard,
                    bounds: {
                        ...duplicateCard.bounds,
                        x: nextX,
                        y: block.bounds.y + 1,
                    },
                },
            });
        },
        [addDashboardCard, blocks, currentDashboard],
    );

    const handleDeleteBlock = React.useCallback(
        async (block: DashboardBlockSchema) => {
            if (!currentDashboard || block.id < 0) {
                return;
            }

            await deleteDashboardCard({
                dashboardId: currentDashboard.id,
                cardId: block.id,
            });

            if (editedBlock?.id === block.id) {
                onCloseEditor?.();
            }
        },
        [currentDashboard, deleteDashboardCard, editedBlock?.id, onCloseEditor],
    );

    if (!currentDashboard) {
        return (
            <div
                className="flex min-h-0 flex-1 items-center justify-center overflow-hidden"
                data-testid="dashboardGrid-emptyState">
                <div className="rounded-xs border bg-background px-6 py-5 shadow-sm">
                    <p className="text-sm text-primary-foreground/70">
                        select dashboard
                    </p>
                </div>
            </div>
        );
    }

    return (
        <div
            ref={containerRef}
            data-testid="dashboardGrid-root"
            className="relative flex min-h-0 flex-1 overflow-hidden [&_.recharts-sector:focus]:outline-none [&_.recharts-sector:focus-visible]:outline-none [&_.recharts-surface:focus]:outline-none [&_.recharts-surface:focus-visible]:outline-none [&_.recharts-rectangle:focus]:outline-none [&_.recharts-rectangle:focus-visible]:outline-none [&_.recharts-dot:focus]:outline-none [&_.recharts-dot:focus-visible]:outline-none [&_.recharts-symbols:focus]:outline-none [&_.recharts-symbols:focus-visible]:outline-none [&_.recharts-trapezoid:focus]:outline-none [&_.recharts-trapezoid:focus-visible]:outline-none">
            <div className="absolute bottom-4 left-4 z-20">
                <DashboardScaleButton scale={scale} onScaleChange={setScale} />
            </div>
            <div className="absolute bottom-4 right-35 z-20">
                <DashboardSettingsButton currentDashboard={currentDashboard} />
            </div>
            <div className="absolute bottom-4 right-4 z-20">
                <DashboardAddCardButton
                    dashboardId={currentDashboard.id}
                    nextBounds={nextCardBounds}
                />
            </div>
            <div
                data-testid="dashboardGrid-scrollArea"
                className="relative z-10 min-h-0 flex-1 overflow-auto"
                style={{
                    backgroundImage: `url(${dashboardBackgroundUrl})`,
                    backgroundPosition: "top left",
                    backgroundRepeat: "repeat",
                    backgroundAttachment: "local",
                    backgroundSize: "975px 728px",
                }}>
                {mounted && (
                    <div
                        className="relative flex flex-1 z-10 origin-top-left"
                        style={{
                            transform: `scale(${scale})`,
                        }}>
                        <ReactGridLayout
                            layout={layout}
                            width={width}
                            constraints={[gridBounds, minMaxSize]}
                            onDragStart={handleDragStart}
                            onDragStop={(_layout, _oldItem, newItem) => {
                                handleDragStop(newItem);
                            }}
                            onResizeStart={handleResizeStart}
                            onResizeStop={(_layout, _oldItem, newItem) => {
                                handleResizeStop(newItem);
                            }}
                            positionStrategy={positionStrategy}
                            gridConfig={{ cols: 36, rowHeight: 36 }}>
                            {previewBlocks.map((block) => (
                                <div className="flex" key={String(block.id)}>
                                    {renderDashboardBlock(
                                        block,
                                        isMoving,
                                        currentDashboard.id,
                                        currentDashboard,
                                        handleDeleteBlock,
                                        handleDuplicateBlock,
                                        onEditBlock,
                                    )}
                                </div>
                            ))}
                        </ReactGridLayout>
                    </div>
                )}
            </div>
        </div>
    );
};
