import {
    GridCellKind,
    type GridCell,
    type GridColumn,
    type Theme,
} from "@glideapps/glide-data-grid";
import { useCallback, useEffect, useMemo, useState } from "react";

import {
    DARK_GRID_LINE_COLOR,
    DARK_GRID_THEME,
    DARK_VALUE_COLORS,
    INDEX_COLUMN_ID,
    LIGHT_GRID_LINE_COLOR,
    LIGHT_GRID_THEME,
    LIGHT_VALUE_COLORS,
    SORT_BUTTON_WIDTH,
    SORT_ICON_GAP,
} from "./consts";
import { buildCell, compareValues, measureTextWidth } from "./cells";
import type {
    DataRow,
    SortState,
    UseVirtualizedTableDataOptions,
    UseVirtualizedTableDataResult,
} from "./types";

const getRowStripeColor = (theme: UseVirtualizedTableDataOptions["theme"]) =>
    theme === "dark" ? "#202020" : "#f8f9fb";

export const useVirtualizedTableData = ({
    data,
    colorfulTypes,
    theme,
}: UseVirtualizedTableDataOptions): UseVirtualizedTableDataResult => {
    const [columnWidths, setColumnWidths] = useState<Record<string, number>>(
        {},
    );
    const [sortState, setSortState] = useState<SortState | null>(null);

    const columnKeys = useMemo(
        () => (data.length > 0 ? Object.keys(data[0] ?? {}) : []),
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
        () => {
            if (colorfulTypes) {
                return theme === "dark" ? DARK_VALUE_COLORS : LIGHT_VALUE_COLORS;
            }

            const neutralColor =
                theme === "dark"
                    ? DARK_GRID_THEME.textDark ?? "#d5d8df"
                    : LIGHT_GRID_THEME.textDark ?? "#11121d";
            const mutedNullColor =
                theme === "dark"
                    ? DARK_GRID_THEME.textMedium ?? "#98a0b3"
                    : LIGHT_GRID_THEME.textMedium ?? "#506182";

            return {
                null: mutedNullColor,
                true: neutralColor,
                false: neutralColor,
                number: neutralColor,
                object: neutralColor,
                string: neutralColor,
            };
        },
        [colorfulTypes, theme],
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

                return {
                    id: key,
                    title: key,
                    width:
                        width === undefined
                            ? minWidth
                            : Math.max(width, minWidth),
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
            if (currentSort === null) {
                return currentSort;
            }

            return columnKeys.includes(currentSort.key) ? currentSort : null;
        });
    }, [columnKeys]);

    const getCellContent = useCallback<
        UseVirtualizedTableDataResult["getCellContent"]
    >(
        ([col, row]) => {
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

    const toggleSort = useCallback(
        (columnIndex: number) => {
            const key = columnKeys[columnIndex];

            if (!key) {
                return;
            }

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

    const handleColumnResize = useCallback<
        UseVirtualizedTableDataResult["handleColumnResize"]
    >(
        (_column, newSize, columnIndex) => {
            if (columnIndex === 0) {
                return;
            }

            const columnKey = columnKeys[columnIndex - 1];

            if (!columnKey) {
                return;
            }

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

    const getRowThemeOverride = useCallback<
        UseVirtualizedTableDataResult["getRowThemeOverride"]
    >(
        (row) =>
            row % 2 === 1
                ? {
                      bgCell: getRowStripeColor(theme),
                  }
                : undefined,
        [theme],
    );

    return {
        columnKeys,
        columns,
        columnOffsets,
        getCellContent,
        getRowThemeOverride,
        gridLineColor,
        gridTheme,
        handleColumnResize,
        indexColumnWidth,
        minimumColumnWidth,
        resolvedColumnWidths,
        sortedData,
        sortState,
        sortStateKey:
            sortState === null ? "" : `${sortState.key}:${sortState.direction}`,
        toggleSort,
    };
};
