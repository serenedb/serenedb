import {
    createContext,
    useCallback,
    useContext,
    useEffect,
    useRef,
    useState,
} from "react";
import { useHotkeys } from "react-hotkeys-hook";
import { type SelectionState } from "./types";

interface VirtualizedTableProviderType {
    selection: SelectionState;
    toggleColumn: (colId: number) => void;
    toggleRow: (rowId: number) => void;
    setArea: (
        start: { row: number; col: number },
        end: { row: number; col: number },
    ) => void;
    isSelected: (rowId: number, colId: number) => boolean;
    startAreaSelection: (row: number, col: number, event?: MouseEvent) => void;
    updateAreaSelection: (row: number, col: number, event?: MouseEvent) => void;
    endAreaSelection: () => void;
    startRowSelection: (row: number, event?: MouseEvent) => void;
    updateRowSelection: (row: number, event?: MouseEvent) => void;
    startColSelection: (col: number, event?: MouseEvent) => void;
    updateColSelection: (col: number, event?: MouseEvent) => void;
    selectAll: (rowCount: number, colCount: number) => void;
    isDragging: boolean;
    getBorderClass: (rowId: number, colId: number) => string;
    clearSelection: () => void;
}

const VirtualizedTableContext = createContext<
    VirtualizedTableProviderType | undefined
>(undefined);

export const VirtualizedTableProvider = ({
    children,
    scrollContainerRef,
}: {
    children: React.ReactNode;
    scrollContainerRef?: React.RefObject<HTMLDivElement | null>;
}) => {
    const [selection, setSelection] = useState<SelectionState>({
        mode: "none",
    });
    const [isDragging, setIsDragging] = useState(false);
    const dragStartRef = useRef<{ row: number; col: number } | null>(null);
    const autoScrollIntervalRef = useRef<number | null>(null);

    function toggleColumn(colId: number) {
        setSelection({ mode: "cols", cols: new Set([colId]) });
    }

    function toggleRow(rowId: number) {
        setSelection({ mode: "rows", rows: new Set([rowId]) });
    }

    function setArea(
        start: { row: number; col: number },
        end: { row: number; col: number },
    ) {
        setSelection({ mode: "area", start, end });
    }

    const stopAutoScroll = useCallback(() => {
        if (autoScrollIntervalRef.current !== null) {
            window.clearInterval(autoScrollIntervalRef.current);
            autoScrollIntervalRef.current = null;
        }
    }, []);

    const startAutoScroll = useCallback(
        (deltaX: number, deltaY: number) => {
            stopAutoScroll();

            if (!scrollContainerRef?.current) return;

            autoScrollIntervalRef.current = window.setInterval(() => {
                if (scrollContainerRef.current) {
                    scrollContainerRef.current.scrollLeft += deltaX;
                    scrollContainerRef.current.scrollTop += deltaY;
                }
            }, 16);
        },
        [scrollContainerRef, stopAutoScroll],
    );

    const checkAutoScroll = useCallback(
        (event: MouseEvent) => {
            if (!scrollContainerRef?.current) return;

            const container = scrollContainerRef.current;
            const rect = container.getBoundingClientRect();
            const threshold = 50;
            const scrollSpeed = 10;

            const mouseX = event.clientX;
            const mouseY = event.clientY;

            let deltaX = 0;
            let deltaY = 0;

            if (mouseX < rect.left + threshold) {
                deltaX = -scrollSpeed;
            } else if (mouseX > rect.right - threshold) {
                deltaX = scrollSpeed;
            }

            if (mouseY < rect.top + threshold) {
                deltaY = -scrollSpeed;
            } else if (mouseY > rect.bottom - threshold) {
                deltaY = scrollSpeed;
            }

            if (deltaX !== 0 || deltaY !== 0) {
                startAutoScroll(deltaX, deltaY);
            } else {
                stopAutoScroll();
            }
        },
        [scrollContainerRef, startAutoScroll, stopAutoScroll],
    );

    const startAreaSelection = useCallback(
        (row: number, col: number, event?: MouseEvent) => {
            setIsDragging(true);
            dragStartRef.current = { row, col };
            setSelection({
                mode: "area",
                start: { row, col },
                end: { row, col },
            });

            if (event) {
                checkAutoScroll(event);
            }
        },
        [checkAutoScroll],
    );

    const updateAreaSelection = useCallback(
        (row: number, col: number, event?: MouseEvent) => {
            if (!isDragging || !dragStartRef.current) return;
            setSelection({
                mode: "area",
                start: dragStartRef.current,
                end: { row, col },
            });

            if (event) {
                checkAutoScroll(event);
            }
        },
        [isDragging, checkAutoScroll],
    );

    const endAreaSelection = useCallback(() => {
        setIsDragging(false);
        stopAutoScroll();
    }, [stopAutoScroll]);

    const startRowSelection = useCallback(
        (row: number, event?: MouseEvent) => {
            setIsDragging(true);
            dragStartRef.current = { row, col: -1 };
            setSelection({
                mode: "rows",
                rows: new Set([row]),
            });

            if (event) {
                checkAutoScroll(event);
            }
        },
        [checkAutoScroll],
    );

    const updateRowSelection = useCallback(
        (row: number, event?: MouseEvent) => {
            if (!isDragging || !dragStartRef.current) return;

            setSelection({
                mode: "area",
                start: { row: dragStartRef.current.row, col: 0 },
                end: { row, col: Number.MAX_SAFE_INTEGER },
            });

            if (event) {
                checkAutoScroll(event);
            }
        },
        [isDragging, checkAutoScroll],
    );

    const startColSelection = useCallback(
        (col: number, event?: MouseEvent) => {
            setIsDragging(true);
            dragStartRef.current = { row: -1, col };
            setSelection({
                mode: "cols",
                cols: new Set([col]),
            });

            if (event) {
                checkAutoScroll(event);
            }
        },
        [checkAutoScroll],
    );

    const updateColSelection = useCallback(
        (col: number, event?: MouseEvent) => {
            if (!isDragging || !dragStartRef.current) return;

            const startCol = dragStartRef.current.col;
            const minCol = Math.min(startCol, col);
            const maxCol = Math.max(startCol, col);

            const cols = new Set<number>();
            for (let i = minCol; i <= maxCol; i++) {
                cols.add(i);
            }

            setSelection({
                mode: "cols",
                cols,
            });

            if (event) {
                checkAutoScroll(event);
            }
        },
        [isDragging, checkAutoScroll],
    );

    const selectAll = useCallback((rowCount: number, colCount: number) => {
        if (rowCount === 0) return;
        setSelection({
            mode: "area",
            start: { row: 0, col: 0 },
            end: { row: rowCount - 1, col: colCount - 1 },
        });
    }, []);

    const clearSelection = useCallback(() => {
        setSelection({ mode: "none" });
        setIsDragging(false);
        dragStartRef.current = null;
        stopAutoScroll();
    }, [stopAutoScroll]);

    const isSelected = (rowId: number, colId: number) => {
        if (selection.mode === "none") return false;
        if (selection.mode === "rows") return selection.rows.has(rowId);
        if (selection.mode === "cols") return selection.cols.has(colId);
        if (selection.mode === "area") {
            const { start, end } = selection;
            const rowStart = Math.min(start.row, end.row);
            const rowEnd = Math.max(start.row, end.row);
            const colStart = Math.min(start.col, end.col);
            const colEnd = Math.max(start.col, end.col);

            return (
                rowId >= rowStart &&
                rowId <= rowEnd &&
                colId >= colStart &&
                colId <= colEnd
            );
        }
        return false;
    };

    const getBorderClass = (rowId: number, colId: number) => {
        if (!isSelected(rowId, colId)) return "";

        const { start, end } =
            selection.mode === "area"
                ? selection
                : {
                      start: { row: rowId, col: colId },
                      end: { row: rowId, col: colId },
                  };
        const rowStart = Math.min(start.row, end.row);
        const colStart = Math.min(start.col, end.col);

        const isFirstRow = rowId === rowStart;
        const isFirstCol = colId === colStart;

        let classes = "border-primary border bg-primary/10 ";
        if (!isFirstCol) classes += "border-l-transparent ";
        if (!isFirstRow) classes += " border-t-transparent ";
        classes += " border-r border-b";

        return classes;
    };

    useHotkeys("escape", () => {
        clearSelection();
    });

    useEffect(() => {
        const handleClickOutside = (event: MouseEvent) => {
            const target = event.target as HTMLElement;

            if (
                target.closest('[role="menu"]') ||
                target.closest("[data-radix-menu-content]")
            ) {
                return;
            }

            if (
                scrollContainerRef?.current &&
                !scrollContainerRef.current.contains(target)
            ) {
                clearSelection();
            }
        };

        document.addEventListener("mousedown", handleClickOutside);
        return () =>
            document.removeEventListener("mousedown", handleClickOutside);
    }, [scrollContainerRef, clearSelection]);

    return (
        <VirtualizedTableContext.Provider
            value={{
                selection,
                toggleColumn,
                toggleRow,
                setArea,
                isSelected,
                startAreaSelection,
                updateAreaSelection,
                endAreaSelection,
                startRowSelection,
                updateRowSelection,
                startColSelection,
                updateColSelection,
                selectAll,
                isDragging,
                getBorderClass,
                clearSelection,
            }}>
            {children}
        </VirtualizedTableContext.Provider>
    );
};

export const useVirtualizedTableContext = () => {
    const context = useContext(VirtualizedTableContext);
    if (!context) {
        throw new Error(
            "useVirtualizedTableContext must be used within a VirtualizedTableProvider",
        );
    }
    return context;
};
