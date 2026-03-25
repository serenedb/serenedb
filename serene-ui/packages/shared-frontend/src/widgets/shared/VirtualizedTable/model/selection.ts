import {
    CompactSelection,
    type GridSelection,
    type Item,
} from "@glideapps/glide-data-grid";

import { EMPTY_SELECTION } from "./consts";
import type { SelectionEdges } from "./types";

export const createCellSelection = (
    col: number,
    row: number,
): GridSelection => ({
    ...EMPTY_SELECTION,
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

export const createRowSelection = (row: number): GridSelection => ({
    ...EMPTY_SELECTION,
    rows: CompactSelection.fromSingleSelection(row),
});

export const createRowsSelection = (rows: CompactSelection): GridSelection => ({
    ...EMPTY_SELECTION,
    rows,
});

export const isCellSelected = (
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

export const getSelectionEdges = (
    col: number,
    row: number,
    selection: GridSelection,
    columnCount: number,
    rowCount: number,
): SelectionEdges => {
    const edges: SelectionEdges = {
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
