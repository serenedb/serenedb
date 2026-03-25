import type {
    CellClickedEventArgs,
    DataEditorProps,
    DrawCellCallback,
    DrawHeaderCallback,
    GridColumn,
    GridSelection,
    Item,
    Theme,
} from "@glideapps/glide-data-grid";
import type { MouseEvent as ReactMouseEvent, RefObject } from "react";

export type DataRow = Record<string, unknown>;
export type SortDirection = "asc" | "desc";
export type VirtualizedTableTheme = "dark" | "light" | "system";

export interface VirtualizedTableProps {
    data: DataRow[];
}

export interface SortState {
    key: string;
    direction: SortDirection;
}

export interface ContextMenuState {
    x: number;
    y: number;
}

export interface VisibleRegionState {
    x: number;
    y: number;
    width: number;
    height: number;
    tx: number;
    ty: number;
}

export interface GridRect {
    x: number;
    y: number;
    width: number;
    height: number;
}

export interface SelectionEdges {
    top: boolean;
    right: boolean;
    bottom: boolean;
    left: boolean;
}

export interface ValueColors {
    null: string;
    true: string;
    false: string;
    number: string;
    object: string;
    string: string;
}

export interface UseVirtualizedTableDataOptions {
    data: DataRow[];
    theme: VirtualizedTableTheme;
}

export interface UseVirtualizedTableDataResult {
    columnKeys: string[];
    columns: GridColumn[];
    columnOffsets: number[];
    getCellContent: NonNullable<DataEditorProps["getCellContent"]>;
    getRowThemeOverride: (row: number) => Partial<Theme> | undefined;
    gridLineColor: string;
    gridTheme: Partial<Theme>;
    handleColumnResize: NonNullable<DataEditorProps["onColumnResize"]>;
    indexColumnWidth: number;
    minimumColumnWidth: number;
    resolvedColumnWidths: number[];
    sortedData: DataRow[];
    sortState: SortState | null;
    sortStateKey: string;
    toggleSort: (columnIndex: number) => void;
}

export interface UseVirtualizedTableHeaderOptions {
    columnKeys: string[];
    columnOffsets: number[];
    gridLineColor: string;
    indexColumnWidth: number;
    minimumColumnWidth: number;
    onSortColumnMouseDown: () => void;
    resolvedColumnWidths: number[];
    rootRef: RefObject<HTMLDivElement | null>;
    sortState: SortState | null;
    toggleSort: (columnIndex: number) => void;
}

export interface UseVirtualizedTableHeaderResult {
    drawHeader: DrawHeaderCallback;
    handleContainerMouseDownCapture: (
        event: ReactMouseEvent<HTMLDivElement>,
    ) => void;
    handleContainerMouseLeave: () => void;
    handleContainerMouseMoveCapture: (
        event: ReactMouseEvent<HTMLDivElement>,
    ) => void;
    handleVisibleRegionChanged: NonNullable<
        DataEditorProps["onVisibleRegionChanged"]
    >;
}

export interface UseVirtualizedTableSelectionOptions {
    columnCount: number;
    columnKeys: string[];
    data: DataRow[];
    rootRef: RefObject<HTMLDivElement | null>;
    sortedData: DataRow[];
    sortStateKey: string;
}

export interface UseVirtualizedTableSelectionResult {
    clearSelection: () => void;
    closeContextMenu: () => void;
    contextMenu: ContextMenuState | null;
    gridSelection: GridSelection;
    handleCellClicked: (cell: Item, event: CellClickedEventArgs) => void;
    handleCellContextMenu: (cell: Item, event: CellClickedEventArgs) => void;
    handleContextMenuOpenChange: (open: boolean) => void;
    handleCopyCSV: () => Promise<void>;
    handleCopyJSON: () => Promise<void>;
    setGridSelection: (selection: GridSelection) => void;
}

export interface UseVirtualizedTableCellRendererOptions {
    columnCount: number;
    gridLineColor: string;
    gridSelection: GridSelection;
    rowCount: number;
}

export interface UseVirtualizedTableCellRendererResult {
    drawCell: DrawCellCallback;
}

export interface VirtualizedTableContextMenuProps {
    contextMenu: ContextMenuState | null;
    onCopyCSV: () => Promise<void>;
    onCopyJSON: () => Promise<void>;
    onOpenChange: (open: boolean) => void;
}
