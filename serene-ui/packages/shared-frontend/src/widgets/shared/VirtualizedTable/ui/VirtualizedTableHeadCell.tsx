import { flexRender, type Header } from "@tanstack/react-table";
import { useVirtualizedTableContext } from "../model";
import { useEffect } from "react";
import { ChevronDown, ChevronUp, ChevronsUpDown } from "lucide-react";
import { cn } from "@serene-ui/shared-frontend/shared";

interface VirtualizedTableHeadCellProps {
    header: Header<Record<string, any>, unknown>;
}

export const VirtualizedTableHeadCell = ({
    header,
}: VirtualizedTableHeadCellProps) => {
    const isIndexCol = header.column.columnDef.header === "index";
    const {
        startColSelection,
        updateColSelection,
        endAreaSelection,
        isDragging,
        clearSelection,
        selection,
        selectAll,
    } = useVirtualizedTableContext();

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
            const table = header.getContext().table;
            const rowCount = table.getRowModel().rows.length;
            const colCount = table.getAllColumns().length;
            selectAll(rowCount, colCount);
        } else {
            startColSelection(header.column.getIndex(), e.nativeEvent);
        }
    };

    const handleMouseEnter = (e: React.MouseEvent) => {
        if (!isDragging || isIndexCol) return;
        updateColSelection(header.column.getIndex(), e.nativeEvent);
    };

    const handleSortClick = (e: React.MouseEvent) => {
        e.stopPropagation();
        e.preventDefault();
        clearSelection();
        header.column.getToggleSortingHandler()?.(e);
    };

    const sortState = header.column.getIsSorted();
    const colIndex = header.column.getIndex();

    const isColSelected =
        !isIndexCol &&
        (() => {
            if (selection.mode === "cols") {
                return selection.cols.has(colIndex);
            }
            if (selection.mode === "area") {
                const { start, end } = selection;
                const rowStart = Math.min(start.row, end.row);
                const rowEnd = Math.max(start.row, end.row);
                const colStart = Math.min(start.col, end.col);
                const colEnd = Math.max(start.col, end.col);

                const table = header.getContext().table;
                const totalRows = table.getRowModel().rows.length;

                const isEntireColumn =
                    rowStart === 0 && rowEnd === totalRows - 1;

                return (
                    isEntireColumn && colIndex >= colStart && colIndex <= colEnd
                );
            }
            return false;
        })();

    return (
        <th
            key={header.id}
            className={cn(
                "flex border-r border-r-border relative border border-t-transparent border-b-transparent border-l-transparent",
                isColSelected && "border-primary border bg-primary/10 border-b",
            )}
            style={{
                width: !isIndexCol ? header.getSize() : "50px",
            }}>
            <div
                onMouseDown={handleMouseDown}
                onMouseEnter={handleMouseEnter}
                className="select-none text-xs p-2 flex-1 text-start overflow-hidden text-ellipsis whitespace-nowrap cursor-pointer flex items-center gap-1">
                {flexRender(
                    !isIndexCol ? header.column.columnDef.header : "",
                    header.getContext(),
                )}
                {!isIndexCol && header.column.getCanSort() && (
                    <button
                        onClick={handleSortClick}
                        onMouseDown={(e) => e.stopPropagation()}
                        className="ml-auto hover:bg-accent rounded p-0.5 transition-colors"
                        title="Sort column">
                        {sortState === "asc" ? (
                            <ChevronUp className="h-3 w-3" />
                        ) : sortState === "desc" ? (
                            <ChevronDown className="h-3 w-3" />
                        ) : (
                            <ChevronsUpDown className="h-3 w-3 opacity-50" />
                        )}
                    </button>
                )}
            </div>
            {!isIndexCol && header.column.getCanResize() && (
                <div
                    className="absolute flex justify-center right-[-4px] top-0 h-full w-2 cursor-col-resize select-none touch-none group"
                    onMouseDown={header.getResizeHandler()}
                    onTouchStart={header.getResizeHandler()}
                    onClick={(e) => e.stopPropagation()}>
                    <div
                        className={`w-px h-full mr-[-1px] group-hover:bg-primary ${
                            header.column.getIsResizing() ? "bg-primary" : ""
                        }`}
                    />
                </div>
            )}
        </th>
    );
};
