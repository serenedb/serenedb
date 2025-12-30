import {
    createColumnHelper,
    getCoreRowModel,
    getSortedRowModel,
    useReactTable,
    type ColumnResizeMode,
} from "@tanstack/react-table";
import { useVirtualizer } from "@tanstack/react-virtual";
import { useMemo, useRef, useEffect } from "react";
import { VirtualizedTableHead } from "./VirtualizedTableHead";
import { VirtualizedTableBody } from "./VirtualizedTableBody";
import { useResizeObserver } from "@serene-ui/shared-frontend/shared";
import { VirtualizedTableProvider, useVirtualizedTableContext } from "../model";

interface VirtualizedTableProps {
    data: Record<string, any>[];
}

interface WrappedVirtualizedTableProps extends VirtualizedTableProps {
    scrollContainerRef: React.RefObject<HTMLDivElement | null>;
}

const WrappedVirtualizedTable: React.FC<WrappedVirtualizedTableProps> = ({
    data,
    scrollContainerRef,
}) => {
    const { ref, size } = useResizeObserver();
    const { clearSelection } = useVirtualizedTableContext();
    const columnHelper = createColumnHelper<Record<string, any>>();

    const columns = useMemo(() => {
        if (data.length <= 0) return [];
        const indexColumn = columnHelper.display({
            id: "index",
            header: "index",
            cell: (info) => {
                const rows = info.table.getRowModel().rows;
                const rowIndex = rows.findIndex((r) => r.id === info.row.id);
                return rowIndex + 1;
            },
        });
        const dataColumns = Object.keys(data[0]).map((key) =>
            columnHelper.accessor(key, {
                header: key,
                cell: (info) => {
                    const value = info.getValue();

                    if (value === null || value === undefined) {
                        return (
                            <span className="text-secondary-foreground italic">
                                null
                            </span>
                        );
                    }
                    if (typeof value === "boolean") {
                        return (
                            <span
                                className={
                                    value ? "text-green-400" : "text-red-400"
                                }>
                                {value.toString()}
                            </span>
                        );
                    }
                    if (typeof value === "number") {
                        return (
                            <span className="text-blue-400">
                                {value.toString()}
                            </span>
                        );
                    }
                    if (typeof value === "object") {
                        return (
                            <span className="text-purple-500">
                                {JSON.stringify(value)}
                            </span>
                        );
                    }
                    return <span className="text-orange-300/60">{value}</span>;
                },
            }),
        );
        return [indexColumn, ...dataColumns];
    }, [data]);

    const table = useReactTable({
        data,
        columns,
        getCoreRowModel: getCoreRowModel(),
        getSortedRowModel: getSortedRowModel(),
        columnResizeMode: "onChange" as ColumnResizeMode,
        enableColumnResizing: true,
        enableRowSelection: true,
    });

    const visibleColumns = table.getVisibleLeafColumns();

    const sortingStateStr = JSON.stringify(table.getState().sorting);
    const prevSortingRef = useRef<string>("");

    useEffect(() => {
        if (
            prevSortingRef.current &&
            sortingStateStr !== prevSortingRef.current
        ) {
            clearSelection();
        }
        prevSortingRef.current = sortingStateStr;
    }, [sortingStateStr, clearSelection]);

    const columnVirtualizer = useVirtualizer<
        HTMLDivElement,
        HTMLTableCellElement
    >({
        count: visibleColumns.length,
        estimateSize: (index) => visibleColumns[index].getSize(),
        getScrollElement: () => scrollContainerRef.current,
        horizontal: true,
        overscan: 3,
    });

    const virtualColumns = columnVirtualizer.getVirtualItems();

    let virtualPaddingLeft: number | undefined;
    let virtualPaddingRight: number | undefined;

    if (columnVirtualizer && virtualColumns?.length) {
        virtualPaddingLeft = virtualColumns[0]?.start ?? 0;
        virtualPaddingRight =
            columnVirtualizer.getTotalSize() -
            (virtualColumns[virtualColumns.length - 1]?.end ?? 0);
    }

    const handleContainerClick = (e: React.MouseEvent) => {
        if (e.target === e.currentTarget) {
            clearSelection();
        }
    };

    return (
        <div ref={ref} className="flex-1">
            <div
                ref={scrollContainerRef as React.RefObject<HTMLDivElement>}
                onClick={handleContainerClick}
                style={{
                    height: size.height,
                    width: size.width,
                }}
                className="overflow-auto relative">
                <table className="grid">
                    <VirtualizedTableHead
                        columnVirtualizer={columnVirtualizer}
                        table={table}
                        virtualPaddingLeft={virtualPaddingLeft}
                        virtualPaddingRight={virtualPaddingRight}
                    />
                    <VirtualizedTableBody
                        columnVirtualizer={columnVirtualizer}
                        table={table}
                        tableContainerRef={scrollContainerRef}
                        virtualPaddingLeft={virtualPaddingLeft}
                        virtualPaddingRight={virtualPaddingRight}
                    />
                </table>
            </div>
        </div>
    );
};

export const VirtualizedTable = ({ data }: VirtualizedTableProps) => {
    const scrollContainerRef = useRef<HTMLDivElement>(null);

    return (
        <VirtualizedTableProvider scrollContainerRef={scrollContainerRef}>
            <WrappedVirtualizedTable
                data={data}
                scrollContainerRef={scrollContainerRef}
            />
        </VirtualizedTableProvider>
    );
};
