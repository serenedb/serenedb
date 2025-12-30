import type { HeaderGroup } from "@tanstack/react-table";
import { Virtualizer } from "@tanstack/react-virtual";
import { VirtualizedTableHeadCell } from "./VirtualizedTableHeadCell";

interface VirtualizedTableHeadRowProps {
    columnVirtualizer: Virtualizer<HTMLDivElement, HTMLTableCellElement>;
    headerGroup: HeaderGroup<Record<string, any>>;
    virtualPaddingLeft: number | undefined;
    virtualPaddingRight: number | undefined;
}

export const VirtualizedTableHeadRow = ({
    columnVirtualizer,
    headerGroup,
    virtualPaddingLeft,
    virtualPaddingRight,
}: VirtualizedTableHeadRowProps) => {
    const virtualColumns = columnVirtualizer.getVirtualItems();
    return (
        <tr
            className="flex w-full bg-background border-b border-border"
            key={headerGroup.id}>
            {virtualPaddingLeft ? (
                <th className="flex" style={{ width: virtualPaddingLeft }} />
            ) : null}

            {virtualColumns.map((virtualColumn) => {
                const header = headerGroup.headers[virtualColumn.index];
                return (
                    <VirtualizedTableHeadCell key={header.id} header={header} />
                );
            })}
            {virtualPaddingRight ? (
                <th style={{ display: "flex", width: virtualPaddingRight }} />
            ) : null}
        </tr>
    );
};
