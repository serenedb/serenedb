import { DataEditor } from "@glideapps/glide-data-grid";
import "@glideapps/glide-data-grid/dist/index.css";
import { useCallback, useRef } from "react";

import { useChangeTheme } from "@serene-ui/shared-frontend/features";
import { useResizeObserver } from "@serene-ui/shared-frontend/shared";

import {
    TABLE_HEADER_HEIGHT,
    TABLE_ROW_HEIGHT,
    useVirtualizedTableCellRenderer,
    useVirtualizedTableData,
    useVirtualizedTableHeader,
    useVirtualizedTableSelection,
    type VirtualizedTableProps,
} from "../model";
import { VirtualizedTableContextMenu } from "./VirtualizedTableContextMenu";

export const VirtualizedTable = ({ data }: VirtualizedTableProps) => {
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

    const tableData = useVirtualizedTableData({ data, theme });
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
        <div
            ref={setContainerRef}
            className="relative flex-1 min-h-0 overflow-hidden"
            onMouseMoveCapture={tableHeader.handleContainerMouseMoveCapture}
            onMouseDownCapture={tableHeader.handleContainerMouseDownCapture}
            onMouseLeave={tableHeader.handleContainerMouseLeave}>
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
