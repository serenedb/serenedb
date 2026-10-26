import {
    CompactSelection,
    type GridSelection,
} from "@glideapps/glide-data-grid";
import { useCallback, useEffect, useRef, useState } from "react";

import { useDownloadResults } from "@serene-ui/shared-frontend/features";

import { EMPTY_SELECTION } from "./consts";
import {
    createCellSelection,
    createRowSelection,
    createRowsSelection,
    isCellSelected,
} from "./selection";
import type {
    ContextMenuState,
    DataRow,
    UseVirtualizedTableSelectionOptions,
    UseVirtualizedTableSelectionResult,
} from "./types";

const getContextMenuPosition = (
    bounds: { x: number; y: number },
    localEventX: number,
    localEventY: number,
): ContextMenuState => ({
    x: bounds.x + localEventX,
    y: bounds.y + localEventY,
});

export const useVirtualizedTableSelection = ({
    columnCount,
    columnKeys,
    data,
    rootRef,
    sortedData,
    sortStateKey,
}: UseVirtualizedTableSelectionOptions): UseVirtualizedTableSelectionResult => {
    const { copyCSV, copyJSON } = useDownloadResults();
    const prevSortRef = useRef("");
    const lastSelectedRowRef = useRef<number | null>(null);
    const [gridSelection, setGridSelectionState] =
        useState<GridSelection>(EMPTY_SELECTION);
    const [contextMenu, setContextMenu] = useState<ContextMenuState | null>(
        null,
    );

    const setGridSelection = useCallback((selection: GridSelection) => {
        setGridSelectionState(selection);
    }, []);

    const clearSelection = useCallback(() => {
        lastSelectedRowRef.current = null;
        setGridSelectionState(EMPTY_SELECTION);
    }, []);

    const closeContextMenu = useCallback(() => {
        setContextMenu(null);
    }, []);

    const handleContextMenuOpenChange = useCallback((open: boolean) => {
        if (!open) {
            setContextMenu(null);
        }
    }, []);

    useEffect(() => {
        clearSelection();
        setContextMenu(null);
    }, [data, clearSelection]);

    useEffect(() => {
        if (prevSortRef.current && prevSortRef.current !== sortStateKey) {
            clearSelection();
            setContextMenu(null);
        }

        prevSortRef.current = sortStateKey;
    }, [clearSelection, sortStateKey]);

    useEffect(() => {
        const handleClickOutside = (event: MouseEvent) => {
            const target = event.target as HTMLElement | null;

            if (!target) {
                return;
            }

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
    }, [clearSelection, rootRef]);

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

                if (!row) {
                    continue;
                }

                const resultRow: Record<string, unknown> = {};

                for (
                    let columnIndex = Math.max(range.x, 1);
                    columnIndex < range.x + Math.max(range.width, 1);
                    columnIndex++
                ) {
                    const key = columnKeys[columnIndex - 1];

                    if (!key) {
                        continue;
                    }

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

    const getNextRowSelection = useCallback(
        (
            row: number,
            event: {
                ctrlKey: boolean;
                metaKey: boolean;
                shiftKey: boolean;
            },
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

    const handleCellClicked = useCallback<
        UseVirtualizedTableSelectionResult["handleCellClicked"]
    >(
        (cell, event) => {
            setContextMenu(null);

            if (cell[1] < 0 || cell[0] !== 0) {
                return;
            }

            event.preventDefault();
            lastSelectedRowRef.current = cell[1];
            setGridSelectionState(getNextRowSelection(cell[1], event));
        },
        [getNextRowSelection],
    );

    const handleCellContextMenu = useCallback<
        UseVirtualizedTableSelectionResult["handleCellContextMenu"]
    >(
        (cell, event) => {
            event.preventDefault();

            if (cell[1] < 0) {
                return;
            }

            const nextSelection =
                cell[0] === 0
                    ? getNextRowSelection(cell[1], event)
                    : isCellSelected(cell, gridSelection, columnCount)
                      ? gridSelection
                      : createCellSelection(cell[0], cell[1]);

            if (nextSelection !== gridSelection) {
                setGridSelectionState(nextSelection);
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
        [columnCount, getNextRowSelection, gridSelection],
    );

    const handleCopyCSV = useCallback(async () => {
        setContextMenu(null);
        await copyCSV(getSelectedData());
    }, [copyCSV, getSelectedData]);

    const handleCopyJSON = useCallback(async () => {
        setContextMenu(null);
        await copyJSON(getSelectedData());
    }, [copyJSON, getSelectedData]);

    return {
        clearSelection,
        closeContextMenu,
        contextMenu,
        gridSelection,
        handleCellClicked,
        handleCellContextMenu,
        handleContextMenuOpenChange,
        handleCopyCSV,
        handleCopyJSON,
        setGridSelection,
    };
};
