import {
    CompactSelection,
    type GridSelection,
    type Theme,
} from "@glideapps/glide-data-grid";

import type { ValueColors, VisibleRegionState } from "./types";

export const GRID_LINE_WIDTH = 0.5;
export const TABLE_ROW_HEIGHT = 34;
export const TABLE_HEADER_HEIGHT = 36;
export const TABLE_MAX_COLUMN_WIDTH = 20_000;
export const SORT_BUTTON_SIZE = 16;
export const SORT_BUTTON_WIDTH = 24;
export const SORT_RESIZE_GUARD = 6;
export const SORT_ICON_GAP = 20;
export const INDEX_COLUMN_ID = "__index__";

const TRANSPARENT_GRID_LINE = "rgba(0, 0, 0, 0)";

export const EMPTY_SELECTION: GridSelection = {
    columns: CompactSelection.empty(),
    rows: CompactSelection.empty(),
    current: undefined,
};

export const EMPTY_VISIBLE_REGION: VisibleRegionState = {
    x: 0,
    y: 0,
    width: 0,
    height: 0,
    tx: 0,
    ty: 0,
};

export const TEXT_COMPARATOR = new Intl.Collator(undefined, {
    numeric: true,
    sensitivity: "base",
});

export const ARROW_DOWN_POINTS = [
    [0.5, 1.25],
    [4, 4.75],
    [7.5, 1.25],
] as const;

export const CHEVRONS_UP_DOWN_PATHS = [
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

export const LIGHT_GRID_THEME: Partial<Theme> = {
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

export const DARK_GRID_THEME: Partial<Theme> = {
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

export const LIGHT_VALUE_COLORS: ValueColors = {
    null: "#506182",
    true: "#16a34a",
    false: "#dc2626",
    number: "#2563eb",
    object: "#7c3aed",
    string: "#ea580c",
};

export const DARK_VALUE_COLORS: ValueColors = {
    null: "#7f8aa3",
    true: "#4ade80",
    false: "#f87171",
    number: "#60a5fa",
    object: "#a78bfa",
    string: "#fdba74",
};

export const LIGHT_GRID_LINE_COLOR = "#e7e7e7";
export const DARK_GRID_LINE_COLOR = "#373737";
