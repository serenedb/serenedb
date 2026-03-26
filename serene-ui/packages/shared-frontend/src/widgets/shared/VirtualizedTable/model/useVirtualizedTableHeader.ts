import {
    type MouseEvent as ReactMouseEvent,
    useCallback,
    useEffect,
    useMemo,
    useState,
} from "react";

import {
    EMPTY_VISIBLE_REGION,
    SORT_BUTTON_SIZE,
    SORT_BUTTON_WIDTH,
    SORT_RESIZE_GUARD,
    TABLE_HEADER_HEIGHT,
} from "./consts";
import { drawCellBorders } from "./draw";
import type {
    SortButtonDescriptor,
    UseVirtualizedTableHeaderOptions,
    UseVirtualizedTableHeaderResult,
} from "./types";

const getSortButtonOffset = (columnWidth: number) => {
    const buttonTrackWidth = SORT_BUTTON_WIDTH - SORT_RESIZE_GUARD;

    return {
        x:
            columnWidth -
            SORT_BUTTON_WIDTH +
            Math.floor((buttonTrackWidth - SORT_BUTTON_SIZE) / 2),
        y: Math.floor((TABLE_HEADER_HEIGHT - SORT_BUTTON_SIZE) / 2),
    };
};

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

    const viewportWidth = rootRef.current?.clientWidth ?? 0;

    const sortButtons = useMemo<SortButtonDescriptor[]>(() => {
        if (
            viewportWidth <= indexColumnWidth ||
            resolvedColumnWidths.length <= 1 ||
            visibleRegion.width <= 0
        ) {
            return [];
        }

        const firstScrollableColumn = Math.max(visibleRegion.x, 1);
        const lastScrollableColumn = Math.min(
            resolvedColumnWidths.length - 1,
            visibleRegion.x + visibleRegion.width + 1,
        );
        const baseOffset =
            columnOffsets[firstScrollableColumn] ?? indexColumnWidth;

        if (lastScrollableColumn < firstScrollableColumn) {
            return [];
        }

        return Array.from(
            { length: lastScrollableColumn - firstScrollableColumn + 1 },
            (_, index) => firstScrollableColumn + index,
        )
            .map((columnIndex) => {
                const key = columnKeys[columnIndex - 1];

                if (!key) {
                    return null;
                }

                const columnWidth =
                    resolvedColumnWidths[columnIndex] ?? minimumColumnWidth;
                const columnLeft =
                    (columnOffsets[columnIndex] ?? 0) -
                    baseOffset +
                    indexColumnWidth +
                    visibleRegion.tx;
                const columnRight = columnLeft + columnWidth;

                if (
                    columnRight <= indexColumnWidth ||
                    columnLeft >= viewportWidth
                ) {
                    return null;
                }

                const { x, y } = getSortButtonOffset(columnWidth);
                const isSorted = sortState?.key === key;

                return {
                    columnIndex,
                    direction: isSorted ? sortState.direction : null,
                    isSorted,
                    key,
                    left: columnLeft + x,
                    top: y,
                };
            })
            .filter(
                (button): button is SortButtonDescriptor => button !== null,
            );
    }, [
        columnKeys,
        columnOffsets,
        indexColumnWidth,
        minimumColumnWidth,
        resolvedColumnWidths,
        sortState,
        viewportWidth,
        visibleRegion.tx,
        visibleRegion.width,
        visibleRegion.x,
    ]);

    const handleSortButtonMouseEnter = useCallback((columnIndex: number) => {
        setHoveredSortColumn(columnIndex);
    }, []);

    const handleSortButtonMouseLeave = useCallback(() => {
        setHoveredSortColumn(null);
    }, []);

    const blockSortButtonMouseEvent = useCallback(
        (event: ReactMouseEvent<HTMLButtonElement>) => {
            event.preventDefault();
            event.stopPropagation();
        },
        [],
    );

    const handleSortButtonMouseDown = useCallback(
        (columnIndex: number, event: ReactMouseEvent<HTMLButtonElement>) => {
            if (event.button !== 0) {
                return;
            }

            blockSortButtonMouseEvent(event);
            onSortColumnMouseDown();
            toggleSort(columnIndex - 1);
        },
        [blockSortButtonMouseEvent, onSortColumnMouseDown, toggleSort],
    );

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

            const isSortHovered = hoveredSortColumn !== null;

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

            drawCellBorders(args.ctx, args.rect, gridLineColor);
            args.ctx.restore();
        },
        [gridLineColor, hoveredSortColumn],
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
        blockSortButtonMouseEvent,
        drawHeader,
        handleSortButtonMouseDown,
        handleSortButtonMouseEnter,
        handleSortButtonMouseLeave,
        handleVisibleRegionChanged,
        hoveredSortColumn,
        sortButtons,
    };
};
