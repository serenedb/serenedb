import { type Table } from "@tanstack/react-table";
import { Virtualizer } from "@tanstack/react-virtual";
import { VirtualizedTableHeadRow } from "./VirtualizedTableHeadRow";

interface TableHeadProps {
    columnVirtualizer: Virtualizer<HTMLDivElement, HTMLTableCellElement>;
    table: Table<Record<string, any>>;
    virtualPaddingLeft: number | undefined;
    virtualPaddingRight: number | undefined;
}

export const VirtualizedTableHead = ({
    columnVirtualizer,
    table,
    virtualPaddingLeft,
    virtualPaddingRight,
}: TableHeadProps) => {
    return (
        <thead className="grid sticky top-0 z-1">
            {table.getHeaderGroups().map((headerGroup) => (
                <VirtualizedTableHeadRow
                    columnVirtualizer={columnVirtualizer}
                    headerGroup={headerGroup}
                    key={headerGroup.id}
                    virtualPaddingLeft={virtualPaddingLeft}
                    virtualPaddingRight={virtualPaddingRight}
                />
            ))}
        </thead>
    );
};
