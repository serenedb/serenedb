import {
    ARROW_DOWN_POINTS,
    CHEVRONS_UP_DOWN_PATHS,
    GRID_LINE_WIDTH,
} from "./consts";
import type { GridRect, SelectionEdges, SortDirection } from "./types";

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
            return;
        }

        ctx.lineTo(targetX, targetY);
    });

    ctx.strokeStyle = color;
    ctx.lineWidth = lineWidth;
    ctx.lineCap = "round";
    ctx.lineJoin = "round";
    ctx.stroke();
};

export const drawArrowDownIcon = (
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

export const drawChevronsUpDownIcon = (
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

export const drawCellBorders = (
    ctx: CanvasRenderingContext2D,
    rect: GridRect,
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

export const drawSelectionEdge = (
    ctx: CanvasRenderingContext2D,
    rect: GridRect,
    color: string,
    edges: SelectionEdges,
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
