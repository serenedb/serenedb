import { GridCellKind, type GridCell } from "@glideapps/glide-data-grid";

import { TEXT_COMPARATOR } from "./consts";
import type { ValueColors } from "./types";

let textMeasureCanvas: HTMLCanvasElement | undefined;

const safeStringify = (value: unknown) => {
    try {
        return JSON.stringify(value);
    } catch {
        return String(value);
    }
};

export const compareValues = (left: unknown, right: unknown) => {
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

export const buildCell = (
    value: unknown,
    valueColors: ValueColors,
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

export const measureTextWidth = (text: string, font: string) => {
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
