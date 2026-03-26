import { type Row, type Table } from "@tanstack/react-table";
import { useVirtualizer, Virtualizer } from "@tanstack/react-virtual";
import { VirtualizedTableBodyRow } from "./VirtualizedTableBodyRow";
import { useHotkeys } from "react-hotkeys-hook";
import { toast } from "sonner";
import { useVirtualizedTableContext } from "../model";

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
    const { selection } = useVirtualizedTableContext();
    const { rows } = table.getRowModel();

    const getSelectedCellValue = (): string | null => {
        if (selection.mode !== "area") return null;

        const { start, end } = selection;
        if (start.row !== end.row || start.col !== end.col) return null;

        const row = rows[start.row];
        if (!row) return null;

        const columns = table.getAllColumns();
        const column = columns[start.col];
        if (!column) return null;

        const rawValue =
            column.id === "index"
                ? start.row + 1
                : (row.original as Record<string, unknown>)[column.id];

        return rawValue === null
            ? "null"
            : rawValue === undefined
              ? "undefined"
              : typeof rawValue === "object"
                ? JSON.stringify(rawValue, null, 2)
                : String(rawValue);
    };

    useHotkeys(
        "ctrl+c, meta+c",
        async () => {
            if (!tableContainerRef.current?.contains(document.activeElement)) {
                return;
            }

            const selectedCellValue = getSelectedCellValue();
            if (!selectedCellValue) return;

            try {
                await navigator.clipboard.writeText(selectedCellValue);
                toast.success("Value copied to clipboard");
            } catch {
                toast.error("Failed to copy value");
            }
        },
        { preventDefault: true, enabled: selection.mode === "area" },
        [selection, rows, table],
    );

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
