import { type Row, type Table } from "@tanstack/react-table";
import { useVirtualizer, Virtualizer } from "@tanstack/react-virtual";
import { VirtualizedTableBodyRow } from "./VirtualizedTableBodyRow";

interface VirtualizedTableBodyProps {
    columnVirtualizer: Virtualizer<HTMLDivElement, HTMLTableCellElement>;
    table: Table<Record<string, any>>;
    tableContainerRef: React.RefObject<HTMLDivElement | null>;
    virtualPaddingLeft: number | undefined;
    virtualPaddingRight: number | undefined;
}

export const VirtualizedTableBody = ({
    columnVirtualizer,
    table,
    tableContainerRef,
    virtualPaddingLeft,
    virtualPaddingRight,
}: VirtualizedTableBodyProps) => {
    const { rows } = table.getRowModel();

    const rowVirtualizer = useVirtualizer<HTMLDivElement, HTMLTableRowElement>({
        count: rows.length,
        estimateSize: () => 32,
        getScrollElement: () => tableContainerRef.current,
        measureElement:
            typeof window !== "undefined" &&
            navigator.userAgent.indexOf("Firefox") === -1
                ? (element) => element?.getBoundingClientRect().height
                : undefined,
        overscan: 5,
    });

    const virtualRows = rowVirtualizer.getVirtualItems();

    return (
        <tbody
            className="grid relative"
            style={{
                display: "grid",
                height: `${rowVirtualizer.getTotalSize()}px`,
            }}>
            {virtualRows.map((virtualRow, rowIndex) => {
                const row = rows[virtualRow.index] as Row<Record<string, any>>;

                return (
                    <VirtualizedTableBodyRow
                        rowIndex={rowIndex}
                        columnVirtualizer={columnVirtualizer}
                        key={row.id}
                        row={row}
                        rowVirtualizer={rowVirtualizer}
                        virtualPaddingLeft={virtualPaddingLeft}
                        virtualPaddingRight={virtualPaddingRight}
                        virtualRow={virtualRow}
                    />
                );
            })}
        </tbody>
    );
};
