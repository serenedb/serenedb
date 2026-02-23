import { type Cell, flexRender } from "@tanstack/react-table";
import { useVirtualizedTableContext } from "../model";
import {
    cn,
    ContextMenu,
    ContextMenuContent,
    ContextMenuItem,
    ContextMenuShortcut,
    ContextMenuTrigger,
    Popover,
    PopoverAnchor,
    PopoverContent,
} from "@serene-ui/shared-frontend/shared";
import { useEffect, useState } from "react";
import { useDownloadResults } from "@serene-ui/shared-frontend/features";

interface VirtualizedTableBodyCellProps {
    cell: Cell<Record<string, any>, unknown>;
}

export const VirtualizedTableBodyCell = ({
    cell,
}: VirtualizedTableBodyCellProps) => {
    const [expandedValue, setExpandedValue] = useState<string | null>(null);

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
    const rows = cell.getContext().table.getRowModel().rows;
    const rowIndex = rows.findIndex((r) => r.id === cell.row.id);

    const getCellTextValue = () => {
        const rawValue = isIndexCol
            ? rowIndex + 1
            : (cell.row.original as Record<string, unknown>)[cell.column.id];

        return rawValue === null
            ? "null"
            : rawValue === undefined
              ? "undefined"
              : typeof rawValue === "object"
                ? JSON.stringify(rawValue, null, 2)
                : String(rawValue);
    };

    useEffect(() => {
        const handleMouseUp = () => {
            if (isDragging) {
                endAreaSelection();
            }
        };

        document.addEventListener("mouseup", handleMouseUp);
        return () => document.removeEventListener("mouseup", handleMouseUp);
    }, [isDragging, endAreaSelection]);

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

    const handleDoubleClick = () => {
        setExpandedValue(getCellTextValue());
    };

    return (
        <Popover
            open={expandedValue !== null}
            onOpenChange={(open) => {
                if (!open) setExpandedValue(null);
            }}>
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
                        <PopoverAnchor title="" asChild>
                            <div
                                tabIndex={0}
                                onMouseDown={handleMouseDown}
                                onMouseEnter={handleMouseEnter}
                                onContextMenu={handleContextMenu}
                                onDoubleClick={handleDoubleClick}
                                className="text-xs p-2 overflow-hidden text-ellipsis whitespace-nowrap flex-1 select-none cursor-pointer">
                                {flexRender(
                                    cell.column.columnDef.cell,
                                    cell.getContext(),
                                )}
                            </div>
                        </PopoverAnchor>
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
            <PopoverContent
                side="bottom"
                align="start"
                sideOffset={-36}
                alignOffset={-4}
                className="min-w-80 max-h-[60vh] overflow-auto p-3 z-[60] border-1 border-border rounded-none">
                <pre className=" whitespace-pre-wrap break-words text-xs leading-5">
                    {expandedValue}
                </pre>
            </PopoverContent>
        </Popover>
    );
};
