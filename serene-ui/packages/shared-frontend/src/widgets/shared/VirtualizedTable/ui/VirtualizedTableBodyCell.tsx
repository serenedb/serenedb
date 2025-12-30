import { type Cell, flexRender } from "@tanstack/react-table";
import { useVirtualizedTableContext } from "../model";
import {
    cn,
    ContextMenu,
    ContextMenuContent,
    ContextMenuItem,
    ContextMenuShortcut,
    ContextMenuTrigger,
} from "@serene-ui/shared-frontend/shared";
import { useEffect } from "react";
import { useDownloadResults } from "@serene-ui/shared-frontend/features";

interface VirtualizedTableBodyCellProps {
    cell: Cell<Record<string, any>, unknown>;
}

export const VirtualizedTableBodyCell = ({
    cell,
}: VirtualizedTableBodyCellProps) => {
    const {
        isSelected,
        startAreaSelection,
        updateAreaSelection,
        endAreaSelection,
        startRowSelection,
        updateRowSelection,
        isDragging,
        getBorderClass,
        selection,
    } = useVirtualizedTableContext();

    const { copyJSON, copyCSV } = useDownloadResults();
    const isIndexCol = cell.column.columnDef.header === "index";

    useEffect(() => {
        const handleMouseUp = () => {
            if (isDragging) {
                endAreaSelection();
            }
        };

        document.addEventListener("mouseup", handleMouseUp);
        return () => document.removeEventListener("mouseup", handleMouseUp);
    }, [isDragging, endAreaSelection]);

    const rows = cell.getContext().table.getRowModel().rows;
    const rowIndex = rows.findIndex((r) => r.id === cell.row.id);

    const handleMouseDown = (e: React.MouseEvent) => {
        if (e.button !== 0) return;
        if (isIndexCol) {
            startRowSelection(rowIndex, e.nativeEvent);
        } else {
            startAreaSelection(rowIndex, cell.column.getIndex(), e.nativeEvent);
        }
    };

    const handleMouseEnter = (e: React.MouseEvent) => {
        if (!isDragging) return;

        if (isIndexCol) {
            updateRowSelection(rowIndex, e.nativeEvent);
        } else {
            updateAreaSelection(
                rowIndex,
                cell.column.getIndex(),
                e.nativeEvent,
            );
        }
    };

    const handleContextMenu = () => {
        if (!isSelected(rowIndex, cell.column.getIndex())) {
            if (isIndexCol) {
                startRowSelection(rowIndex);
            } else {
                startAreaSelection(rowIndex, cell.column.getIndex());
            }
        }
    };

    const getSelectedData = (): Record<string, unknown>[] => {
        const table = cell.getContext().table;
        const rows = table.getRowModel().rows;
        const columns = table.getAllColumns();

        if (selection.mode === "none") return [];

        if (selection.mode === "rows") {
            return Array.from(selection.rows).map((rowIdx) => {
                const row = rows[rowIdx];
                return row?.original || {};
            });
        }

        if (selection.mode === "cols") {
            const result: Record<string, unknown>[] = [];
            const colIndices = Array.from(selection.cols);
            rows.forEach((row) => {
                const rowData: Record<string, unknown> = {};
                colIndices.forEach((colIdx) => {
                    const column = columns[colIdx];
                    if (column && column.id !== "index") {
                        rowData[column.id] = (row.original as any)[column.id];
                    }
                });
                result.push(rowData);
            });
            return result;
        }

        if (selection.mode === "area") {
            const { start, end } = selection;
            const rowStart = Math.min(start.row, end.row);
            const rowEnd = Math.max(start.row, end.row);
            const colStart = Math.min(start.col, end.col);
            const colEnd = Math.max(start.col, end.col);

            const result: Record<string, unknown>[] = [];
            for (let rowIdx = rowStart; rowIdx <= rowEnd; rowIdx++) {
                const row = rows[rowIdx];
                if (!row) continue;
                const rowData: Record<string, unknown> = {};
                for (let colIdx = colStart; colIdx <= colEnd; colIdx++) {
                    const column = columns[colIdx];
                    if (column && column.id !== "index") {
                        rowData[column.id] = (row.original as any)[column.id];
                    }
                }
                result.push(rowData);
            }
            return result;
        }

        return [];
    };

    const handleCopyCSV = async () => {
        const data = getSelectedData();
        await copyCSV(data);
    };

    const handleCopyJSON = async () => {
        const data = getSelectedData();
        await copyJSON(data);
    };

    return (
        <ContextMenu>
            <ContextMenuTrigger asChild>
                <td
                    key={cell.id}
                    className={cn(
                        "flex border border-transparent border-r border-r-border",
                        getBorderClass(rowIndex, cell.column.getIndex()),
                    )}
                    style={{
                        width: !isIndexCol ? cell.column.getSize() : "50px",
                    }}>
                    <div
                        onMouseDown={handleMouseDown}
                        onMouseEnter={handleMouseEnter}
                        onContextMenu={handleContextMenu}
                        className="text-xs p-2 overflow-hidden text-ellipsis whitespace-nowrap flex-1 select-none cursor-pointer">
                        {flexRender(
                            cell.column.columnDef.cell,
                            cell.getContext(),
                        )}
                    </div>
                </td>
            </ContextMenuTrigger>
            <ContextMenuContent className="w-52">
                <ContextMenuItem onClick={handleCopyCSV}>
                    Copy as CSV
                    <ContextMenuShortcut>⌘C</ContextMenuShortcut>
                </ContextMenuItem>
                <ContextMenuItem onClick={handleCopyJSON}>
                    Copy as JSON
                    <ContextMenuShortcut>⌘Shift+C</ContextMenuShortcut>
                </ContextMenuItem>
            </ContextMenuContent>
        </ContextMenu>
    );
};
