import {
    type CellClickedEventArgs,
    CompactSelection,
    DataEditor,
    type DrawCellCallback,
    GridCellKind,
    type DrawHeaderCallback,
    type GridCell,
    type GridColumn,
    type GridSelection,
    type Item,
    type Theme,
} from "@glideapps/glide-data-grid";
import "@glideapps/glide-data-grid/dist/index.css";
import {
    type MouseEvent as ReactMouseEvent,
    useCallback,
    useEffect,
    useMemo,
    useRef,
    useState,
} from "react";
import { useDownloadResults } from "@serene-ui/shared-frontend/features";
import {
    DropdownMenu,
    DropdownMenuContent,
    DropdownMenuItem,
    DropdownMenuShortcut,
    DropdownMenuTrigger,
    useResizeObserver,
} from "@serene-ui/shared-frontend/shared";
import { useChangeTheme } from "@serene-ui/shared-frontend/features";

interface VirtualizedTableProps {
    data: Record<string, any>[];
}

type DataRow = Record<string, any>;
type SortDirection = "asc" | "desc";

interface SortState {
    key: string;
    direction: SortDirection;
}

interface ContextMenuState {
    x: number;
    y: number;
}

interface VisibleRegionState {
    x: number;
    y: number;
    width: number;
    height: number;
    tx: number;
    ty: number;
}

const EMPTY_SELECTION: GridSelection = {
    columns: CompactSelection.empty(),
    rows: CompactSelection.empty(),
    current: undefined,
};

const TEXT_COMPARATOR = new Intl.Collator(undefined, {
    numeric: true,
    sensitivity: "base",
});

const GRID_LINE_WIDTH = 0.5;
const TABLE_ROW_HEIGHT = 34;
const TABLE_HEADER_HEIGHT = 36;
const SORT_BUTTON_WIDTH = 24;
const SORT_RESIZE_GUARD = 6;
const SORT_ICON_GAP = 20;
const TRANSPARENT_GRID_LINE = "rgba(0, 0, 0, 0)";
const INDEX_COLUMN_ID = "__index__";
const EMPTY_VISIBLE_REGION: VisibleRegionState = {
    x: 0,
    y: 0,
    width: 0,
    height: 0,
    tx: 0,
    ty: 0,
};

const ARROW_DOWN_POINTS = [
    [0.5, 1.25],
    [4, 4.75],
    [7.5, 1.25],
] as const;

const CHEVRONS_UP_DOWN_PATHS = [
    [
        [7, 15],
        [12, 20],
        [17, 15],
    ],
    [
        [7, 9],
        [12, 4],
        [17, 9],
    ],
] as const;

let textMeasureCanvas: HTMLCanvasElement | undefined;

const LIGHT_GRID_THEME: Partial<Theme> = {
    accentColor: "#8555f7",
    accentFg: "#ffffff",
    accentLight: "rgba(133, 85, 247, 0.16)",
    textDark: "#11121d",
    textMedium: "#506182",
    textLight: "#7c8aa5",
    textBubble: "#ffffff",
    bgIconHeader: "#f0f2f5",
    fgIconHeader: "#506182",
    textHeader: "#506182",
    textHeaderSelected: "#11121d",
    bgCell: "#ffffff",
    bgCellMedium: "#f6f7f9",
    bgHeader: "#f0f2f5",
    bgHeaderHasFocus: "#e7eaf1",
    bgHeaderHovered: "#e7eaf1",
    bgBubble: "#506182",
    bgBubbleSelected: "#8555f7",
    bgSearchResult: "rgba(133, 85, 247, 0.12)",
    borderColor: TRANSPARENT_GRID_LINE,
    horizontalBorderColor: TRANSPARENT_GRID_LINE,
    headerBottomBorderColor: TRANSPARENT_GRID_LINE,
    drilldownBorder: "#8555f7",
    linkColor: "#47668b",
    cellHorizontalPadding: 12,
    cellVerticalPadding: 8,
    headerFontStyle: "600 12px",
    headerIconSize: 18,
    baseFontStyle: "12px",
    markerFontStyle: "600 12px",
    fontFamily: "DMSans, sans-serif",
    editorFontSize: "12px",
    lineHeight: 1.4,
    roundingRadius: 8,
};

const DARK_GRID_THEME: Partial<Theme> = {
    accentColor: "#895af8",
    accentFg: "#ffffff",
    accentLight: "rgba(137, 90, 248, 0.18)",
    textDark: "#d5d8df",
    textMedium: "#98a0b3",
    textLight: "#767d8f",
    textBubble: "#ffffff",
    bgIconHeader: "#2a2a2a",
    fgIconHeader: "#bbbbbb",
    textHeader: "#bbbbbb",
    textHeaderSelected: "#ffffff",
    bgCell: "#232323",
    bgCellMedium: "#1c1c1c",
    bgHeader: "#212121",
    bgHeaderHasFocus: "#252525",
    bgHeaderHovered: "#252525",
    bgBubble: "#2d2d2d",
    bgBubbleSelected: "#895af8",
    bgSearchResult: "rgba(137, 90, 248, 0.14)",
    borderColor: TRANSPARENT_GRID_LINE,
    horizontalBorderColor: TRANSPARENT_GRID_LINE,
    headerBottomBorderColor: TRANSPARENT_GRID_LINE,
    drilldownBorder: "#895af8",
    linkColor: "#4a7aad",
    cellHorizontalPadding: 12,
    cellVerticalPadding: 8,
    headerFontStyle: "600 12px",
    headerIconSize: 18,
    baseFontStyle: "12px",
    markerFontStyle: "600 12px",
    fontFamily: "DMSans, sans-serif",
    editorFontSize: "12px",
    lineHeight: 1.4,
    roundingRadius: 8,
};

const LIGHT_VALUE_COLORS = {
    null: "#506182",
    true: "#16a34a",
    false: "#dc2626",
    number: "#2563eb",
    object: "#7c3aed",
    string: "#ea580c",
};

const DARK_VALUE_COLORS = {
    null: "#7f8aa3",
    true: "#4ade80",
    false: "#f87171",
    number: "#60a5fa",
    object: "#a78bfa",
    string: "#fdba74",
};

const LIGHT_GRID_LINE_COLOR = "#e7e7e7";
const DARK_GRID_LINE_COLOR = "#373737";

const safeStringify = (value: unknown) => {
    try {
        return JSON.stringify(value);
    } catch {
        return String(value);
    }
};

const compareValues = (left: unknown, right: unknown) => {
    if (left == null && right == null) return 0;
    if (left == null) return 1;
    if (right == null) return -1;

    if (typeof left === "number" && typeof right === "number") {
        return left - right;
    }

    if (typeof left === "boolean" && typeof right === "boolean") {
        return Number(left) - Number(right);
    }

    return TEXT_COMPARATOR.compare(
        typeof left === "object" ? safeStringify(left) : String(left),
        typeof right === "object" ? safeStringify(right) : String(right),
    );
};

const buildCell = (
    value: unknown,
    valueColors: typeof LIGHT_VALUE_COLORS,
): GridCell => {
    if (value === null || value === undefined) {
        return {
            kind: GridCellKind.Text,
            allowOverlay: false,
            readonly: true,
            style: "faded",
            displayData: "null",
            data: "null",
            copyData: "null",
            themeOverride: {
                textDark: valueColors.null,
            },
        };
    }

    if (typeof value === "boolean") {
        const displayData = value.toString();

        return {
            kind: GridCellKind.Text,
            allowOverlay: false,
            readonly: true,
            displayData,
            data: displayData,
            copyData: displayData,
            themeOverride: {
                textDark: value ? valueColors.true : valueColors.false,
            },
        };
    }

    if (typeof value === "number") {
        const displayData = value.toString();

        return {
            kind: GridCellKind.Text,
            allowOverlay: false,
            readonly: true,
            contentAlign: "right",
            displayData,
            data: displayData,
            copyData: displayData,
            themeOverride: {
                textDark: valueColors.number,
            },
        };
    }

    if (typeof value === "object") {
        const displayData = safeStringify(value);

        return {
            kind: GridCellKind.Text,
            allowOverlay: false,
            readonly: true,
            displayData,
            data: displayData,
            copyData: displayData,
            themeOverride: {
                textDark: valueColors.object,
            },
        };
    }

    const displayData = String(value);

    return {
        kind: GridCellKind.Text,
        allowOverlay: false,
        readonly: true,
        displayData,
        data: displayData,
        copyData: displayData,
        themeOverride: {
            textDark: valueColors.string,
        },
    };
};

const createCellSelection = (col: number, row: number): GridSelection => ({
    columns: CompactSelection.empty(),
    rows: CompactSelection.empty(),
    current: {
        cell: [col, row],
        range: {
            x: col,
            y: row,
            width: 1,
            height: 1,
        },
        rangeStack: [],
    },
});

const createRowSelection = (row: number): GridSelection => ({
    columns: CompactSelection.empty(),
    rows: CompactSelection.fromSingleSelection(row),
    current: undefined,
});

const createRowsSelection = (rows: CompactSelection): GridSelection => ({
    columns: CompactSelection.empty(),
    rows,
    current: undefined,
});

const isCellSelected = (
    cell: Item,
    selection: GridSelection,
    columnCount: number,
) => {
    const [col, row] = cell;

    if (selection.rows.hasIndex(row)) {
        return true;
    }

    if (col >= 0 && selection.columns.hasIndex(col)) {
        return true;
    }

    if (selection.current === undefined) {
        return false;
    }

    const { range } = selection.current;
    const isRowInside =
        row >= range.y && row < range.y + Math.max(range.height, 1);

    if (!isRowInside) {
        return false;
    }

    if (col < 0) {
        return range.x === 0 && range.width >= columnCount;
    }

    return col >= range.x && col < range.x + Math.max(range.width, 1);
};

const getRowStripeColor = (theme: "light" | "dark" | "system") =>
    theme === "dark" ? "#202020" : "#f8f9fb";

const drawPolyline = (
    ctx: CanvasRenderingContext2D,
    points: readonly (readonly [number, number])[],
    viewBoxWidth: number,
    viewBoxHeight: number,
    centerX: number,
    centerY: number,
    width: number,
    height: number,
    color: string,
    rotation = 0,
    lineWidth = 1.25,
) => {
    const scaleX = width / viewBoxWidth;
    const scaleY = height / viewBoxHeight;
    ctx.beginPath();

    points.forEach(([x, y], index) => {
        const translatedX = x - viewBoxWidth / 2;
        const translatedY = y - viewBoxHeight / 2;
        const rotatedX =
            translatedX * Math.cos(rotation) - translatedY * Math.sin(rotation);
        const rotatedY =
            translatedX * Math.sin(rotation) + translatedY * Math.cos(rotation);
        const targetX = centerX + rotatedX * scaleX;
        const targetY = centerY + rotatedY * scaleY;

        if (index === 0) {
            ctx.moveTo(targetX, targetY);
        } else {
            ctx.lineTo(targetX, targetY);
        }
    });

    ctx.strokeStyle = color;
    ctx.lineWidth = lineWidth;
    ctx.lineCap = "round";
    ctx.lineJoin = "round";
    ctx.stroke();
};

const drawArrowDownIcon = (
    ctx: CanvasRenderingContext2D,
    centerX: number,
    centerY: number,
    direction: SortDirection,
    color: string,
) => {
    drawPolyline(
        ctx,
        ARROW_DOWN_POINTS,
        8,
        6,
        centerX,
        centerY,
        9,
        6,
        color,
        direction === "asc" ? Math.PI : 0,
        1.5,
    );
};

const drawChevronsUpDownIcon = (
    ctx: CanvasRenderingContext2D,
    centerX: number,
    centerY: number,
    color: string,
) => {
    CHEVRONS_UP_DOWN_PATHS.forEach((path) => {
        drawPolyline(
            ctx,
            path,
            24,
            24,
            centerX,
            centerY,
            10,
            12,
            color,
            0,
            1.35,
        );
    });
};

const drawCellBorders = (
    ctx: CanvasRenderingContext2D,
    rect: { x: number; y: number; width: number; height: number },
    color: string,
) => {
    ctx.save();
    ctx.fillStyle = color;
    ctx.fillRect(
        rect.x + rect.width - GRID_LINE_WIDTH,
        rect.y,
        GRID_LINE_WIDTH,
        rect.height,
    );
    ctx.fillRect(
        rect.x,
        rect.y + rect.height - GRID_LINE_WIDTH,
        rect.width,
        GRID_LINE_WIDTH,
    );
    ctx.restore();
};

const drawSelectionEdge = (
    ctx: CanvasRenderingContext2D,
    rect: { x: number; y: number; width: number; height: number },
    color: string,
    edges: {
        top: boolean;
        right: boolean;
        bottom: boolean;
        left: boolean;
    },
) => {
    if (!edges.top && !edges.right && !edges.bottom && !edges.left) {
        return;
    }

    ctx.save();
    ctx.fillStyle = color;

    if (edges.top) {
        ctx.fillRect(rect.x, rect.y, rect.width, GRID_LINE_WIDTH);
    }

    if (edges.right) {
        ctx.fillRect(
            rect.x + rect.width - GRID_LINE_WIDTH,
            rect.y,
            GRID_LINE_WIDTH,
            rect.height,
        );
    }

    if (edges.bottom) {
        ctx.fillRect(
            rect.x,
            rect.y + rect.height - GRID_LINE_WIDTH,
            rect.width,
            GRID_LINE_WIDTH,
        );
    }

    if (edges.left) {
        ctx.fillRect(rect.x, rect.y, GRID_LINE_WIDTH, rect.height);
    }

    ctx.restore();
};

const getSelectionEdges = (
    col: number,
    row: number,
    selection: GridSelection,
    columnCount: number,
    rowCount: number,
) => {
    const edges = {
        top: false,
        right: false,
        bottom: false,
        left: false,
    };

    if (selection.rows.hasIndex(row)) {
        edges.top ||= !selection.rows.hasIndex(row - 1);
        edges.right ||= col === columnCount - 1;
        edges.bottom ||= !selection.rows.hasIndex(row + 1);
        edges.left ||= col === 0;
    }

    if (selection.columns.hasIndex(col)) {
        edges.top ||= row === 0;
        edges.right ||= !selection.columns.hasIndex(col + 1);
        edges.bottom ||= row === rowCount - 1;
        edges.left ||= !selection.columns.hasIndex(col - 1);
    }

    return edges;
};

const measureTextWidth = (text: string, font: string) => {
    if (typeof document === "undefined") {
        return text.length * 7;
    }

    textMeasureCanvas ??= document.createElement("canvas");
    const context = textMeasureCanvas.getContext("2d");

    if (!context) {
        return text.length * 7;
    }

    context.font = font;
    return context.measureText(text).width;
};

export const VirtualizedTable = ({ data }: VirtualizedTableProps) => {
    const { theme } = useChangeTheme();
    const { copyCSV, copyJSON } = useDownloadResults();
    const { ref: resizeRef, size } = useResizeObserver<HTMLDivElement>();
    const rootRef = useRef<HTMLDivElement | null>(null);
    const prevSortRef = useRef<string>("");
    const lastSelectedRowRef = useRef<number | null>(null);
    const [gridSelection, setGridSelection] =
        useState<GridSelection>(EMPTY_SELECTION);
    const [columnWidths, setColumnWidths] = useState<Record<string, number>>(
        {},
    );
    const [sortState, setSortState] = useState<SortState | null>(null);
    const [contextMenu, setContextMenu] = useState<ContextMenuState | null>(
        null,
    );
    const [visibleRegion, setVisibleRegion] =
        useState<VisibleRegionState>(EMPTY_VISIBLE_REGION);
    const [hoveredSortColumn, setHoveredSortColumn] = useState<number | null>(
        null,
    );

    const setContainerRef = useCallback(
        (node: HTMLDivElement | null) => {
            rootRef.current = node;
            resizeRef(node);
        },
        [resizeRef],
    );

    const clearSelection = useCallback(() => {
        lastSelectedRowRef.current = null;
        setGridSelection(EMPTY_SELECTION);
    }, []);

    const columnKeys = useMemo(
        () => (data.length > 0 ? Object.keys(data[0] as DataRow) : []),
        [data],
    );

    const sortedData = useMemo(() => {
        if (sortState === null) {
            return data;
        }

        return data
            .map((row, index) => ({ row, index }))
            .sort((left, right) => {
                const direction = sortState.direction === "asc" ? 1 : -1;
                const compared = compareValues(
                    left.row[sortState.key],
                    right.row[sortState.key],
                );

                if (compared !== 0) {
                    return compared * direction;
                }

                return left.index - right.index;
            })
            .map((item) => item.row);
    }, [data, sortState]);

    const gridTheme = useMemo(
        () => (theme === "dark" ? DARK_GRID_THEME : LIGHT_GRID_THEME),
        [theme],
    );

    const valueColors = useMemo(
        () => (theme === "dark" ? DARK_VALUE_COLORS : LIGHT_VALUE_COLORS),
        [theme],
    );

    const gridLineColor = useMemo(
        () => (theme === "dark" ? DARK_GRID_LINE_COLOR : LIGHT_GRID_LINE_COLOR),
        [theme],
    );

    const indexColumnTheme = useMemo<Partial<Theme>>(
        () => ({
            textDark: gridTheme.textMedium ?? LIGHT_GRID_THEME.textMedium,
            textMedium: gridTheme.textLight ?? LIGHT_GRID_THEME.textLight,
            bgCell: gridTheme.bgHeader ?? LIGHT_GRID_THEME.bgHeader,
            bgCellMedium:
                gridTheme.bgHeaderHasFocus ?? LIGHT_GRID_THEME.bgHeaderHasFocus,
        }),
        [gridTheme],
    );

    const indexColumnWidth = useMemo(
        () => (sortedData.length > 9_999 ? 48 : 40),
        [sortedData.length],
    );

    const headerFont = useMemo(
        () =>
            `${gridTheme.headerFontStyle ?? LIGHT_GRID_THEME.headerFontStyle} ${
                gridTheme.fontFamily ?? LIGHT_GRID_THEME.fontFamily
            }`,
        [gridTheme],
    );

    const columnMinWidths = useMemo(
        () =>
            Object.fromEntries(
                columnKeys.map((key) => [
                    key,
                    Math.ceil(
                        measureTextWidth(key, headerFont) +
                            SORT_ICON_GAP +
                            SORT_BUTTON_WIDTH,
                    ),
                ]),
            ) as Record<string, number>,
        [columnKeys, headerFont],
    );

    const minimumColumnWidth = useMemo(() => {
        const widths = Object.values(columnMinWidths);
        return widths.length > 0 ? Math.min(...widths) : 50;
    }, [columnMinWidths]);

    const dataColumns = useMemo<GridColumn[]>(
        () =>
            columnKeys.map((key) => {
                const width = columnWidths[key];
                const isSorted = sortState?.key === key;
                const minWidth = columnMinWidths[key] ?? minimumColumnWidth;

                if (width === undefined) {
                    return {
                        id: key,
                        title: key,
                        width: minWidth,
                        style: isSorted ? "highlight" : "normal",
                    };
                }

                return {
                    id: key,
                    title: key,
                    width: Math.max(width, minWidth),
                    style: isSorted ? "highlight" : "normal",
                };
            }),
        [
            columnKeys,
            columnMinWidths,
            columnWidths,
            minimumColumnWidth,
            sortState,
        ],
    );

    const columns = useMemo<GridColumn[]>(
        () => [
            {
                id: INDEX_COLUMN_ID,
                title: "",
                width: indexColumnWidth,
                themeOverride: indexColumnTheme,
            },
            ...dataColumns,
        ],
        [dataColumns, indexColumnTheme, indexColumnWidth],
    );

    const resolvedColumnWidths = useMemo(
        () =>
            columns.map((column) =>
                "width" in column ? column.width : minimumColumnWidth,
            ),
        [columns, minimumColumnWidth],
    );

    const columnOffsets = useMemo(() => {
        const offsets = [0];

        for (const width of resolvedColumnWidths) {
            offsets.push(offsets[offsets.length - 1] + width);
        }

        return offsets;
    }, [resolvedColumnWidths]);

    useEffect(() => {
        setColumnWidths((currentWidths) => {
            const nextWidths = Object.fromEntries(
                Object.entries(currentWidths).filter(([key]) =>
                    columnKeys.includes(key),
                ),
            );

            return Object.keys(nextWidths).length ===
                Object.keys(currentWidths).length
                ? currentWidths
                : nextWidths;
        });

        setSortState((currentSort) => {
            if (currentSort === null) return currentSort;
            return columnKeys.includes(currentSort.key) ? currentSort : null;
        });
    }, [columnKeys]);

    useEffect(() => {
        clearSelection();
        setContextMenu(null);
    }, [data, clearSelection]);

    const sortStateKey =
        sortState === null ? "" : `${sortState.key}:${sortState.direction}`;

    useEffect(() => {
        if (prevSortRef.current && prevSortRef.current !== sortStateKey) {
            clearSelection();
            setContextMenu(null);
        }

        prevSortRef.current = sortStateKey;
    }, [clearSelection, sortStateKey]);

    useEffect(() => {
        setHoveredSortColumn(null);
    }, [visibleRegion.tx, visibleRegion.x]);

    useEffect(() => {
        const handleClickOutside = (event: MouseEvent) => {
            const target = event.target as HTMLElement | null;

            if (!target) return;

            if (
                target.closest('[role="menu"]') ||
                target.closest("[data-radix-menu-content]")
            ) {
                return;
            }

            if (rootRef.current && !rootRef.current.contains(target)) {
                clearSelection();
                setContextMenu(null);
            }
        };

        document.addEventListener("mousedown", handleClickOutside);

        return () =>
            document.removeEventListener("mousedown", handleClickOutside);
    }, [clearSelection]);

    const getCellContent = useCallback(
        ([col, row]: Item) => {
            if (col === 0) {
                const displayData = String(row + 1);

                return {
                    kind: GridCellKind.Text,
                    allowOverlay: false,
                    readonly: true,
                    contentAlign: "center",
                    displayData,
                    data: displayData,
                    copyData: displayData,
                    themeOverride: indexColumnTheme,
                } satisfies GridCell;
            }

            const rowData = sortedData[row] as DataRow | undefined;
            const columnKey = columnKeys[col - 1];

            return buildCell(rowData?.[columnKey], valueColors);
        },
        [columnKeys, indexColumnTheme, sortedData, valueColors],
    );

    const getSelectedData = useCallback((): Record<string, unknown>[] => {
        if (gridSelection.rows.length > 0) {
            return gridSelection.rows
                .toArray()
                .map((rowIndex) => sortedData[rowIndex])
                .filter((row): row is DataRow => row !== undefined);
        }

        if (gridSelection.columns.length > 0) {
            const selectedKeys = gridSelection.columns
                .toArray()
                .map((columnIndex) =>
                    columnIndex === 0 ? undefined : columnKeys[columnIndex - 1],
                )
                .filter((key): key is string => key !== undefined);

            if (selectedKeys.length === 0) {
                return [];
            }

            return sortedData.map((row) =>
                Object.fromEntries(
                    selectedKeys.map((key) => [key, row[key] as unknown]),
                ),
            );
        }

        if (gridSelection.current !== undefined) {
            const { range } = gridSelection.current;
            const rows: Record<string, unknown>[] = [];

            for (
                let rowIndex = range.y;
                rowIndex < range.y + Math.max(range.height, 1);
                rowIndex++
            ) {
                const row = sortedData[rowIndex];

                if (!row) continue;

                const resultRow: Record<string, unknown> = {};

                for (
                    let columnIndex = Math.max(range.x, 1);
                    columnIndex < range.x + Math.max(range.width, 1);
                    columnIndex++
                ) {
                    const key = columnKeys[columnIndex - 1];

                    if (!key) continue;

                    resultRow[key] = row[key];
                }

                if (Object.keys(resultRow).length > 0) {
                    rows.push(resultRow);
                }
            }

            return rows;
        }

        return [];
    }, [columnKeys, gridSelection, sortedData]);

    const toggleSort = useCallback(
        (columnIndex: number) => {
            const key = columnKeys[columnIndex];

            if (!key) return;

            setSortState((currentSort) => {
                if (currentSort?.key !== key) {
                    return {
                        key,
                        direction: "asc",
                    };
                }

                if (currentSort.direction === "asc") {
                    return {
                        key,
                        direction: "desc",
                    };
                }

                return null;
            });
        },
        [columnKeys],
    );

    const handleColumnResize = useCallback(
        (_column: GridColumn, newSize: number, columnIndex: number) => {
            if (columnIndex === 0) {
                return;
            }

            const columnKey = columnKeys[columnIndex - 1];

            if (!columnKey) return;

            const minWidth = columnMinWidths[columnKey] ?? minimumColumnWidth;
            const nextSize = Math.max(newSize, minWidth);

            setColumnWidths((currentWidths) =>
                currentWidths[columnKey] === nextSize
                    ? currentWidths
                    : {
                          ...currentWidths,
                          [columnKey]: nextSize,
                      },
            );
        },
        [columnKeys, columnMinWidths, minimumColumnWidth],
    );

    const getSortColumnFromMouseEvent = useCallback(
        (event: ReactMouseEvent<HTMLDivElement>) => {
            if (!rootRef.current) {
                return null;
            }

            const bounds = rootRef.current.getBoundingClientRect();
            const localX = event.clientX - bounds.left;
            const localY = event.clientY - bounds.top;

            if (
                localY < 0 ||
                localY > TABLE_HEADER_HEIGHT ||
                localX < indexColumnWidth ||
                resolvedColumnWidths.length === 0
            ) {
                return null;
            }

            const firstScrollableColumn = Math.max(visibleRegion.x, 1);
            const dataX =
                localX -
                indexColumnWidth -
                visibleRegion.tx +
                (columnOffsets[firstScrollableColumn] ?? indexColumnWidth);

            if (dataX < 0) {
                return null;
            }

            for (
                let columnIndex = 1;
                columnIndex < resolvedColumnWidths.length;
                columnIndex++
            ) {
                const columnStart = columnOffsets[columnIndex] ?? 0;
                const columnEnd = columnOffsets[columnIndex + 1] ?? columnStart;

                if (dataX < columnStart || dataX >= columnEnd) {
                    continue;
                }

                const columnWidth =
                    resolvedColumnWidths[columnIndex] ?? minimumColumnWidth;
                const offsetInsideColumn = dataX - columnStart;
                const sortAreaStart = columnWidth - SORT_BUTTON_WIDTH;
                const resizeAreaStart = columnWidth - SORT_RESIZE_GUARD;

                if (
                    offsetInsideColumn >= sortAreaStart &&
                    offsetInsideColumn < resizeAreaStart
                ) {
                    return columnIndex;
                }

                return null;
            }

            return null;
        },
        [
            columnOffsets,
            indexColumnWidth,
            minimumColumnWidth,
            resolvedColumnWidths,
            visibleRegion.tx,
            visibleRegion.x,
        ],
    );

    const handleContainerMouseMoveCapture = useCallback(
        (event: ReactMouseEvent<HTMLDivElement>) => {
            setHoveredSortColumn(getSortColumnFromMouseEvent(event));
        },
        [getSortColumnFromMouseEvent],
    );

    const handleContainerMouseDownCapture = useCallback(
        (event: ReactMouseEvent<HTMLDivElement>) => {
            if (event.button !== 0) {
                return;
            }

            const columnIndex = getSortColumnFromMouseEvent(event);

            if (columnIndex === null) {
                return;
            }

            event.preventDefault();
            event.stopPropagation();
            setContextMenu(null);
            toggleSort(columnIndex - 1);
        },
        [getSortColumnFromMouseEvent, toggleSort],
    );

    const handleContainerMouseLeave = useCallback(() => {
        setHoveredSortColumn(null);
    }, []);

    const drawHeader = useCallback<DrawHeaderCallback>(
        (args, drawContent) => {
            args.ctx.save();
            const backgroundColor = args.isSelected
                ? args.theme.accentColor
                : args.hasSelectedCell
                  ? args.theme.bgHeaderHasFocus
                  : args.theme.bgHeader;
            args.ctx.fillStyle = backgroundColor;
            args.ctx.fillRect(
                args.rect.x,
                args.rect.y,
                args.rect.width,
                args.rect.height,
            );

            const isSortHovered = hoveredSortColumn === args.columnIndex;

            if (!args.isSelected && args.hoverAmount > 0 && !isSortHovered) {
                args.ctx.globalAlpha = args.hoverAmount;
                args.ctx.fillStyle = args.theme.bgHeaderHovered;
                args.ctx.fillRect(
                    args.rect.x,
                    args.rect.y,
                    args.rect.width,
                    args.rect.height,
                );
                args.ctx.globalAlpha = 1;
            }

            drawContent();

            const columnKey =
                args.columnIndex > 0
                    ? columnKeys[args.columnIndex - 1]
                    : undefined;

            if (columnKey) {
                const isSorted = sortState?.key === columnKey;
                const iconX = args.rect.x + args.rect.width - SORT_BUTTON_WIDTH;
                const centerX = iconX + SORT_BUTTON_WIDTH / 2;
                const centerY = args.rect.y + args.rect.height / 2;

                args.ctx.fillStyle = isSortHovered
                    ? args.theme.bgHeaderHovered
                    : backgroundColor;
                args.ctx.fillRect(
                    iconX,
                    args.rect.y,
                    SORT_BUTTON_WIDTH,
                    args.rect.height,
                );

                if (!isSorted) {
                    drawChevronsUpDownIcon(
                        args.ctx,
                        centerX,
                        centerY,
                        args.theme.textLight,
                    );
                } else {
                    drawArrowDownIcon(
                        args.ctx,
                        centerX,
                        centerY,
                        sortState.direction,
                        args.theme.accentColor,
                    );
                }
            }

            drawCellBorders(args.ctx, args.rect, gridLineColor);
            args.ctx.restore();
        },
        [columnKeys, gridLineColor, hoveredSortColumn, sortState],
    );

    const drawCell = useCallback<DrawCellCallback>(
        (args, drawContent) => {
            drawContent();
            drawCellBorders(args.ctx, args.rect, gridLineColor);
            drawSelectionEdge(
                args.ctx,
                args.rect,
                args.theme.accentColor,
                getSelectionEdges(
                    args.col,
                    args.row,
                    gridSelection,
                    columns.length,
                    sortedData.length,
                ),
            );
        },
        [columns.length, gridLineColor, gridSelection, sortedData.length],
    );

    const getContextMenuPosition = useCallback(
        (
            bounds: { x: number; y: number },
            localEventX: number,
            localEventY: number,
        ) => ({
            x: bounds.x + localEventX,
            y: bounds.y + localEventY,
        }),
        [],
    );

    const getNextRowSelection = useCallback(
        (
            row: number,
            event: Pick<
                CellClickedEventArgs,
                "shiftKey" | "ctrlKey" | "metaKey"
            >,
        ) => {
            if (event.shiftKey && lastSelectedRowRef.current !== null) {
                const start = Math.min(lastSelectedRowRef.current, row);
                const end = Math.max(lastSelectedRowRef.current, row) + 1;
                return createRowsSelection(
                    CompactSelection.fromSingleSelection([start, end]),
                );
            }

            if (event.ctrlKey || event.metaKey) {
                const nextRows = gridSelection.rows.hasIndex(row)
                    ? gridSelection.rows.remove(row)
                    : gridSelection.rows.add(row);

                if (nextRows.length === 0) {
                    return EMPTY_SELECTION;
                }

                return createRowsSelection(nextRows);
            }

            return createRowSelection(row);
        },
        [gridSelection.rows],
    );

    const handleCellClicked = useCallback(
        (cell: Item, event: CellClickedEventArgs) => {
            setContextMenu(null);

            if (cell[1] < 0 || cell[0] !== 0) {
                return;
            }

            event.preventDefault();
            lastSelectedRowRef.current = cell[1];
            setGridSelection(getNextRowSelection(cell[1], event));
        },
        [getNextRowSelection],
    );

    const handleCellContextMenu = useCallback(
        (cell: Item, event: CellClickedEventArgs) => {
            event.preventDefault();

            if (cell[1] < 0) {
                return;
            }

            const nextSelection =
                cell[0] === 0
                    ? getNextRowSelection(cell[1], event)
                    : isCellSelected(cell, gridSelection, columns.length)
                      ? gridSelection
                      : createCellSelection(cell[0], cell[1]);

            if (nextSelection !== gridSelection) {
                setGridSelection(nextSelection);
            }

            lastSelectedRowRef.current = cell[1];

            setContextMenu(
                getContextMenuPosition(
                    event.bounds,
                    event.localEventX,
                    event.localEventY,
                ),
            );
        },
        [
            columns.length,
            getContextMenuPosition,
            getNextRowSelection,
            gridSelection,
        ],
    );

    const handleCopyCSV = useCallback(async () => {
        setContextMenu(null);
        await copyCSV(getSelectedData());
    }, [copyCSV, getSelectedData]);

    const handleCopyJSON = useCallback(async () => {
        setContextMenu(null);
        await copyJSON(getSelectedData());
    }, [copyJSON, getSelectedData]);

    const handleVisibleRegionChanged = useCallback(
        (
            range: {
                x: number;
                y: number;
                width: number;
                height: number;
            },
            tx: number,
            ty: number,
        ) => {
            setVisibleRegion((currentRegion) => {
                if (
                    currentRegion.x === range.x &&
                    currentRegion.y === range.y &&
                    currentRegion.width === range.width &&
                    currentRegion.height === range.height &&
                    currentRegion.tx === tx &&
                    currentRegion.ty === ty
                ) {
                    return currentRegion;
                }

                return {
                    x: range.x,
                    y: range.y,
                    width: range.width,
                    height: range.height,
                    tx,
                    ty,
                };
            });
        },
        [],
    );

    const getRowThemeOverride = useCallback(
        (row: number) =>
            row % 2 === 1
                ? {
                      bgCell: getRowStripeColor(theme),
                  }
                : undefined,
        [theme],
    );

    return (
        <div
            ref={setContainerRef}
            className="relative flex-1 min-h-0 overflow-hidden"
            onMouseMoveCapture={handleContainerMouseMoveCapture}
            onMouseDownCapture={handleContainerMouseDownCapture}
            onMouseLeave={handleContainerMouseLeave}>
            {size.width > 0 && size.height > 0 ? (
                <>
                    <DataEditor
                        width={size.width}
                        height={size.height}
                        theme={gridTheme}
                        columns={columns}
                        rows={sortedData.length}
                        rowHeight={TABLE_ROW_HEIGHT}
                        headerHeight={TABLE_HEADER_HEIGHT}
                        drawHeader={drawHeader}
                        drawCell={drawCell}
                        getCellContent={getCellContent}
                        getCellsForSelection={true}
                        gridSelection={gridSelection}
                        onGridSelectionChange={setGridSelection}
                        onSelectionCleared={clearSelection}
                        onCellClicked={handleCellClicked}
                        onCellContextMenu={handleCellContextMenu}
                        onVisibleRegionChanged={handleVisibleRegionChanged}
                        onColumnResize={handleColumnResize}
                        getRowThemeOverride={getRowThemeOverride}
                        minColumnWidth={minimumColumnWidth}
                        freezeColumns={1}
                        smoothScrollX
                        smoothScrollY
                        rowSelect="multi"
                        columnSelect="multi"
                        rowSelectionBlending="exclusive"
                        columnSelectionBlending="exclusive"
                        rangeSelect="rect"
                        rangeSelectionBlending="exclusive"
                        rowSelectionMode="multi"
                        fillHandle={false}
                        drawFocusRing
                        onPaste={false}
                    />

                    {contextMenu !== null ? (
                        <DropdownMenu
                            open={contextMenu !== null}
                            onOpenChange={(open) => {
                                if (!open) {
                                    setContextMenu(null);
                                }
                            }}>
                            <DropdownMenuTrigger asChild>
                                <button
                                    type="button"
                                    aria-hidden="true"
                                    tabIndex={-1}
                                    className="fixed h-px w-px opacity-0 pointer-events-none"
                                    style={{
                                        left: contextMenu.x,
                                        top: contextMenu.y,
                                    }}
                                />
                            </DropdownMenuTrigger>
                            <DropdownMenuContent
                                side="bottom"
                                align="start"
                                sideOffset={0}
                                className="w-52"
                                onCloseAutoFocus={(event) =>
                                    event.preventDefault()
                                }>
                                <DropdownMenuItem
                                    onClick={() => void handleCopyCSV()}>
                                    Copy as CSV
                                    <DropdownMenuShortcut>
                                        CMD+C
                                    </DropdownMenuShortcut>
                                </DropdownMenuItem>
                                <DropdownMenuItem
                                    onClick={() => void handleCopyJSON()}>
                                    Copy as JSON
                                    <DropdownMenuShortcut>
                                        CMD+SHIFT+C
                                    </DropdownMenuShortcut>
                                </DropdownMenuItem>
                            </DropdownMenuContent>
                        </DropdownMenu>
                    ) : null}
                </>
            ) : null}
        </div>
    );
};
