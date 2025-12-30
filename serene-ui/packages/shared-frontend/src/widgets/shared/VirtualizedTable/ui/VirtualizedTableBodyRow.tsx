import { type Row } from "@tanstack/react-table";
import { type VirtualItem, Virtualizer } from "@tanstack/react-virtual";
import { VirtualizedTableBodyCell } from "./VirtualizedTableBodyCell";
import { cn } from "@serene-ui/shared-frontend/shared";

interface VirtualizedTableBodyRowProps {
    rowIndex: number;
    columnVirtualizer: Virtualizer<HTMLDivElement, HTMLTableCellElement>;
    row: Row<Record<string, any>>;
    rowVirtualizer: Virtualizer<HTMLDivElement, HTMLTableRowElement>;
    virtualPaddingLeft: number | undefined;
    virtualPaddingRight: number | undefined;
    virtualRow: VirtualItem;
}

export const VirtualizedTableBodyRow = ({
    rowIndex,
    columnVirtualizer,
    row,
    virtualPaddingLeft,
    virtualPaddingRight,
    virtualRow,
}: VirtualizedTableBodyRowProps) => {
    const visibleCells = row.getVisibleCells();
    const virtualColumns = columnVirtualizer.getVirtualItems();

    return (
        <tr
            data-index={virtualRow.index}
            key={row.id}
            className={cn("flex absolute w-full", {
                "bg-secondary/50": rowIndex % 2 === 0,
            })}
            style={{
                transform: `translateY(${virtualRow.start}px)`,
            }}>
            {virtualPaddingLeft ? (
                <td className="flex" style={{ width: virtualPaddingLeft }} />
            ) : null}

            {virtualColumns.map((vc) => {
                const cell = visibleCells[vc.index];
                return <VirtualizedTableBodyCell key={cell.id} cell={cell} />;
            })}
            {virtualPaddingRight ? (
                <td className="flex" style={{ width: virtualPaddingRight }} />
            ) : null}
        </tr>
    );
};
