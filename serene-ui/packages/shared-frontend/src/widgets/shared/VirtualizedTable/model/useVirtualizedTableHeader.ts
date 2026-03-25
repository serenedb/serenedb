import {
    type MouseEvent as ReactMouseEvent,
    useCallback,
    useEffect,
    useState,
} from "react";

import {
    EMPTY_VISIBLE_REGION,
    SORT_BUTTON_WIDTH,
    SORT_RESIZE_GUARD,
    TABLE_HEADER_HEIGHT,
} from "./consts";
import {
    drawArrowDownIcon,
    drawCellBorders,
    drawChevronsUpDownIcon,
} from "./draw";
import type {
    UseVirtualizedTableHeaderOptions,
    UseVirtualizedTableHeaderResult,
} from "./types";

export const useVirtualizedTableHeader = ({
    columnKeys,
    columnOffsets,
    gridLineColor,
    indexColumnWidth,
    minimumColumnWidth,
    onSortColumnMouseDown,
    resolvedColumnWidths,
    rootRef,
    sortState,
    toggleSort,
}: UseVirtualizedTableHeaderOptions): UseVirtualizedTableHeaderResult => {
    const [visibleRegion, setVisibleRegion] = useState(EMPTY_VISIBLE_REGION);
    const [hoveredSortColumn, setHoveredSortColumn] = useState<number | null>(
        null,
    );

    useEffect(() => {
        setHoveredSortColumn(null);
    }, [visibleRegion.tx, visibleRegion.x]);

    const getSortColumnFromMouseEvent = useCallback(
        (event: ReactMouseEvent<HTMLDivElement>) => {
            if (!rootRef.current) {
                return null;
            }

            const bounds = rootRef.current.getBoundingClientRect();
            const localX = event.clientX - bounds.left;
            const localY = event.clientY - bounds.top;

            if (
                localY < 0 ||
                localY > TABLE_HEADER_HEIGHT ||
                localX < indexColumnWidth ||
                resolvedColumnWidths.length === 0
            ) {
                return null;
            }

            const firstScrollableColumn = Math.max(visibleRegion.x, 1);
            const dataX =
                localX -
                indexColumnWidth -
                visibleRegion.tx +
                (columnOffsets[firstScrollableColumn] ?? indexColumnWidth);

            if (dataX < 0) {
                return null;
            }

            for (
                let columnIndex = 1;
                columnIndex < resolvedColumnWidths.length;
                columnIndex++
            ) {
                const columnStart = columnOffsets[columnIndex] ?? 0;
                const columnEnd = columnOffsets[columnIndex + 1] ?? columnStart;

                if (dataX < columnStart || dataX >= columnEnd) {
                    continue;
                }

                const columnWidth =
                    resolvedColumnWidths[columnIndex] ?? minimumColumnWidth;
                const offsetInsideColumn = dataX - columnStart;
                const sortAreaStart = columnWidth - SORT_BUTTON_WIDTH;
                const resizeAreaStart = columnWidth - SORT_RESIZE_GUARD;

                if (
                    offsetInsideColumn >= sortAreaStart &&
                    offsetInsideColumn < resizeAreaStart
                ) {
                    return columnIndex;
                }

                return null;
            }

            return null;
        },
        [
            columnOffsets,
            indexColumnWidth,
            minimumColumnWidth,
            resolvedColumnWidths,
            rootRef,
            visibleRegion.tx,
            visibleRegion.x,
        ],
    );

    const handleContainerMouseMoveCapture = useCallback(
        (event: ReactMouseEvent<HTMLDivElement>) => {
            setHoveredSortColumn(getSortColumnFromMouseEvent(event));
        },
        [getSortColumnFromMouseEvent],
    );

    const handleContainerMouseDownCapture = useCallback(
        (event: ReactMouseEvent<HTMLDivElement>) => {
            if (event.button !== 0) {
                return;
            }

            const columnIndex = getSortColumnFromMouseEvent(event);

            if (columnIndex === null) {
                return;
            }

            event.preventDefault();
            event.stopPropagation();
            onSortColumnMouseDown();
            toggleSort(columnIndex - 1);
        },
        [getSortColumnFromMouseEvent, onSortColumnMouseDown, toggleSort],
    );

    const handleContainerMouseLeave = useCallback(() => {
        setHoveredSortColumn(null);
    }, []);

    const drawHeader = useCallback<
        UseVirtualizedTableHeaderResult["drawHeader"]
    >(
        (args, drawContent) => {
            args.ctx.save();
            const backgroundColor = args.isSelected
                ? args.theme.accentColor
                : args.hasSelectedCell
                  ? args.theme.bgHeaderHasFocus
                  : args.theme.bgHeader;
            args.ctx.fillStyle = backgroundColor;
            args.ctx.fillRect(
                args.rect.x,
                args.rect.y,
                args.rect.width,
                args.rect.height,
            );

            const isSortHovered = hoveredSortColumn === args.columnIndex;

            if (!args.isSelected && args.hoverAmount > 0 && !isSortHovered) {
                args.ctx.globalAlpha = args.hoverAmount;
                args.ctx.fillStyle = args.theme.bgHeaderHovered;
                args.ctx.fillRect(
                    args.rect.x,
                    args.rect.y,
                    args.rect.width,
                    args.rect.height,
                );
                args.ctx.globalAlpha = 1;
            }

            drawContent();

            const columnKey =
                args.columnIndex > 0
                    ? columnKeys[args.columnIndex - 1]
                    : undefined;

            if (columnKey) {
                const isSorted = sortState?.key === columnKey;
                const iconX = args.rect.x + args.rect.width - SORT_BUTTON_WIDTH;
                const centerX = iconX + SORT_BUTTON_WIDTH / 2;
                const centerY = args.rect.y + args.rect.height / 2;

                args.ctx.fillStyle = isSortHovered
                    ? args.theme.bgHeaderHovered
                    : backgroundColor;
                args.ctx.fillRect(
                    iconX,
                    args.rect.y,
                    SORT_BUTTON_WIDTH,
                    args.rect.height,
                );

                if (!isSorted) {
                    drawChevronsUpDownIcon(
                        args.ctx,
                        centerX,
                        centerY,
                        args.theme.textLight,
                    );
                } else {
                    drawArrowDownIcon(
                        args.ctx,
                        centerX,
                        centerY,
                        sortState.direction,
                        args.theme.accentColor,
                    );
                }
            }

            drawCellBorders(args.ctx, args.rect, gridLineColor);
            args.ctx.restore();
        },
        [columnKeys, gridLineColor, hoveredSortColumn, sortState],
    );

    const handleVisibleRegionChanged = useCallback<
        UseVirtualizedTableHeaderResult["handleVisibleRegionChanged"]
    >((range, tx = 0, ty = 0) => {
        setVisibleRegion((currentRegion) => {
            if (
                currentRegion.x === range.x &&
                currentRegion.y === range.y &&
                currentRegion.width === range.width &&
                currentRegion.height === range.height &&
                currentRegion.tx === tx &&
                currentRegion.ty === ty
            ) {
                return currentRegion;
            }

            return {
                x: range.x,
                y: range.y,
                width: range.width,
                height: range.height,
                tx,
                ty,
            };
        });
    }, []);

    return {
        drawHeader,
        handleContainerMouseDownCapture,
        handleContainerMouseLeave,
        handleContainerMouseMoveCapture,
        handleVisibleRegionChanged,
    };
};
