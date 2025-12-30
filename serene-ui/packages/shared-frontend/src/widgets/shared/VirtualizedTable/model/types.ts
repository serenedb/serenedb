export type SelectionMode = "none" | "rows" | "cols" | "area";

export type SelectionState =
    | { mode: "none" }
    | { mode: "rows"; rows: Set<number> }
    | { mode: "cols"; cols: Set<number> }
    | {
          mode: "area";
          start: { row: number; col: number };
          end: { row: number; col: number };
      };
