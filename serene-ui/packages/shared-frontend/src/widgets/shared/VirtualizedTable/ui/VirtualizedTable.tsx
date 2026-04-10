import { DataEditor } from "@glideapps/glide-data-grid";
import "@glideapps/glide-data-grid/dist/index.css";
import { useCallback, useRef } from "react";

import { useChangeTheme } from "@serene-ui/shared-frontend/features";
import { useResizeObserver } from "@serene-ui/shared-frontend/shared";

import {
    SORT_BUTTON_SIZE,
    TABLE_HEADER_HEIGHT,
    TABLE_ROW_HEIGHT,
    type SortDirection,
    useVirtualizedTableCellRenderer,
    useVirtualizedTableData,
    useVirtualizedTableHeader,
    useVirtualizedTableSelection,
    type VirtualizedTableProps,
} from "../model";
import { VirtualizedTableContextMenu } from "./VirtualizedTableContextMenu";

const SortButtonIcon = ({
    color,
    direction,
}: {
    color: string;
    direction: SortDirection | null;
}) =>
    direction === null ? (
        <svg
            aria-hidden
            width="10"
            height="12"
            viewBox="0 0 24 24"
            fill="none">
            <polyline
                points="7 15 12 20 17 15"
                stroke={color}
                strokeWidth="1.35"
                strokeLinecap="round"
                strokeLinejoin="round"
            />
            <polyline
                points="7 9 12 4 17 9"
                stroke={color}
                strokeWidth="1.35"
                strokeLinecap="round"
                strokeLinejoin="round"
            />
        </svg>
    ) : (
        <svg
            aria-hidden
            width="9"
            height="6"
            viewBox="0 0 8 6"
            fill="none"
            style={{
                transform:
                    direction === "asc" ? "rotate(180deg)" : undefined,
            }}>
            <polyline
                points="0.5 1.25 4 4.75 7.5 1.25"
                stroke={color}
                strokeWidth="1.5"
                strokeLinecap="round"
                strokeLinejoin="round"
            />
        </svg>
    );

export const VirtualizedTable = ({
    data,
    colorfulTypes = true,
}: VirtualizedTableProps) => {
    const { theme } = useChangeTheme();
    const { ref: resizeRef, size } = useResizeObserver<HTMLDivElement>();
    const rootRef = useRef<HTMLDivElement | null>(null);

    const setContainerRef = useCallback(
        (node: HTMLDivElement | null) => {
            rootRef.current = node;
            resizeRef(node);
        },
        [resizeRef],
    );

    const tableData = useVirtualizedTableData({
        data,
        colorfulTypes,
        theme,
    });
    const tableSelection = useVirtualizedTableSelection({
        columnCount: tableData.columns.length,
        columnKeys: tableData.columnKeys,
        data,
        rootRef,
        sortedData: tableData.sortedData,
        sortStateKey: tableData.sortStateKey,
    });
    const tableHeader = useVirtualizedTableHeader({
        columnKeys: tableData.columnKeys,
        columnOffsets: tableData.columnOffsets,
        gridLineColor: tableData.gridLineColor,
        indexColumnWidth: tableData.indexColumnWidth,
        minimumColumnWidth: tableData.minimumColumnWidth,
        onSortColumnMouseDown: tableSelection.closeContextMenu,
        resolvedColumnWidths: tableData.resolvedColumnWidths,
        rootRef,
        sortState: tableData.sortState,
        toggleSort: tableData.toggleSort,
    });
    const { drawCell } = useVirtualizedTableCellRenderer({
        columnCount: tableData.columns.length,
        gridLineColor: tableData.gridLineColor,
        gridSelection: tableSelection.gridSelection,
        rowCount: tableData.sortedData.length,
    });

    return (
        <div ref={setContainerRef} className="relative flex-1 min-h-0 overflow-hidden">
            {size.width > 0 && size.height > 0 ? (
                <>
                    <DataEditor
                        width={size.width}
                        height={size.height}
                        theme={tableData.gridTheme}
                        columns={tableData.columns}
                        rows={tableData.sortedData.length}
                        rowHeight={TABLE_ROW_HEIGHT}
                        headerHeight={TABLE_HEADER_HEIGHT}
                        drawHeader={tableHeader.drawHeader}
                        drawCell={drawCell}
                        getCellContent={tableData.getCellContent}
                        getCellsForSelection={true}
                        gridSelection={tableSelection.gridSelection}
                        onGridSelectionChange={tableSelection.setGridSelection}
                        onSelectionCleared={tableSelection.clearSelection}
                        onCellClicked={tableSelection.handleCellClicked}
                        onCellContextMenu={tableSelection.handleCellContextMenu}
                        onVisibleRegionChanged={
                            tableHeader.handleVisibleRegionChanged
                        }
                        onColumnResize={tableData.handleColumnResize}
                        getRowThemeOverride={tableData.getRowThemeOverride}
                        minColumnWidth={tableData.minimumColumnWidth}
                        freezeColumns={1}
                        smoothScrollX
                        smoothScrollY
                        rowSelect="multi"
                        columnSelect="multi"
                        rowSelectionBlending="exclusive"
                        columnSelectionBlending="exclusive"
                        rangeSelect="rect"
                        rangeSelectionBlending="exclusive"
                        rowSelectionMode="multi"
                        fillHandle={false}
                        drawFocusRing
                        onPaste={false}
                    />

                    <div
                        className="pointer-events-none absolute left-0 top-0 z-10 w-full"
                        style={{ height: TABLE_HEADER_HEIGHT }}>
                        {tableHeader.sortButtons.map((button) => {
                            const isHovered =
                                tableHeader.hoveredSortColumn ===
                                button.columnIndex;
                            const backgroundColor = isHovered
                                ? (tableData.gridTheme.bgHeaderHovered ??
                                  tableData.gridTheme.bgIconHeader ??
                                  "transparent")
                                : button.isSorted
                                  ? (tableData.gridTheme.accentLight ??
                                    "transparent")
                                  : "transparent";
                            const iconColor = button.isSorted
                                ? (tableData.gridTheme.accentColor ??
                                  tableData.gridTheme.fgIconHeader ??
                                  tableData.gridTheme.textLight ??
                                  "currentColor")
                                : (tableData.gridTheme.fgIconHeader ??
                                  tableData.gridTheme.textLight ??
                                  "currentColor");

                            return (
                                <button
                                    key={button.columnIndex}
                                    type="button"
                                    aria-label={`Sort by ${button.key}`}
                                    aria-pressed={button.isSorted}
                                    tabIndex={-1}
                                    className="pointer-events-auto absolute flex items-center justify-center rounded"
                                    style={{
                                        backgroundColor,
                                        height: SORT_BUTTON_SIZE,
                                        left: button.left,
                                        top: button.top,
                                        width: SORT_BUTTON_SIZE,
                                    }}
                                    onMouseEnter={() =>
                                        tableHeader.handleSortButtonMouseEnter(
                                            button.columnIndex,
                                        )
                                    }
                                    onMouseLeave={
                                        tableHeader.handleSortButtonMouseLeave
                                    }
                                    onMouseDown={(event) =>
                                        tableHeader.handleSortButtonMouseDown(
                                            button.columnIndex,
                                            event,
                                        )
                                    }
                                    onMouseUp={
                                        tableHeader.blockSortButtonMouseEvent
                                    }
                                    onClick={
                                        tableHeader.blockSortButtonMouseEvent
                                    }
                                    onDoubleClick={
                                        tableHeader.blockSortButtonMouseEvent
                                    }
                                    onContextMenu={
                                        tableHeader.blockSortButtonMouseEvent
                                    }>
                                    <SortButtonIcon
                                        color={iconColor}
                                        direction={button.direction}
                                    />
                                </button>
                            );
                        })}
                    </div>

                    <VirtualizedTableContextMenu
                        contextMenu={tableSelection.contextMenu}
                        onOpenChange={
                            tableSelection.handleContextMenuOpenChange
                        }
                        onCopyCSV={tableSelection.handleCopyCSV}
                        onCopyJSON={tableSelection.handleCopyJSON}
                    />
                </>
            ) : null}
        </div>
    );
};
