import { useCallback } from "react";

import { drawCellBorders, drawSelectionEdge } from "./draw";
import { getSelectionEdges } from "./selection";
import type {
    UseVirtualizedTableCellRendererOptions,
    UseVirtualizedTableCellRendererResult,
} from "./types";

export const useVirtualizedTableCellRenderer = ({
    columnCount,
    gridLineColor,
    gridSelection,
    rowCount,
}: UseVirtualizedTableCellRendererOptions): UseVirtualizedTableCellRendererResult => {
    const drawCell = useCallback<
        UseVirtualizedTableCellRendererResult["drawCell"]
    >(
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
                    columnCount,
                    rowCount,
                ),
            );
        },
        [columnCount, gridLineColor, gridSelection, rowCount],
    );

    return {
        drawCell,
    };
};
