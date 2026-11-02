import React from "react";
import { useResizeObserver } from "@serene-ui/shared-frontend/shared";
import {
    DASHBOARDS_MENU_SECTION_HANDLE_HEIGHT,
    DASHBOARDS_MENU_SECTION_HEADER_HEIGHT,
    DASHBOARDS_MENU_SECTION_MIN_BODY_HEIGHT,
    type DashboardsMenuSectionId,
    useDashboardsMenu,
} from "./DashboardsMenuProvider";

const SECTION_ORDER: DashboardsMenuSectionId[] = [
    "favorites",
    "dashboards",
    "savedQueries",
];

export const useDashboardsMenuResize = () => {
    const { ref: containerRef, size } = useResizeObserver<HTMLDivElement>();
    const { sections, setSectionSizeUnits } = useDashboardsMenu();
    const dragStateRef = React.useRef<{
        topId: DashboardsMenuSectionId;
        bottomId: DashboardsMenuSectionId;
        startY: number;
        startTopHeight: number;
        startBottomHeight: number;
        availableHeight: number;
        minHeight: number;
    } | null>(null);

    const openSectionIds = React.useMemo(
        () => SECTION_ORDER.filter((sectionId) => sections[sectionId].isOpen),
        [sections],
    );

    const headersHeight =
        SECTION_ORDER.length * DASHBOARDS_MENU_SECTION_HEADER_HEIGHT;
    const handlesHeight =
        Math.max(openSectionIds.length - 1, 0) *
        DASHBOARDS_MENU_SECTION_HANDLE_HEIGHT;
    const availableBodiesHeight = Math.max(
        size.height - headersHeight - handlesHeight,
        0,
    );

    const bodyHeights = React.useMemo(() => {
        const heights: Partial<Record<DashboardsMenuSectionId, number>> = {};

        if (!openSectionIds.length || availableBodiesHeight <= 0) {
            return heights;
        }

        const totalUnits = openSectionIds.reduce(
            (sum, sectionId) => sum + sections[sectionId].sizeUnits,
            0,
        );
        const minHeight =
            openSectionIds.length * DASHBOARDS_MENU_SECTION_MIN_BODY_HEIGHT <=
            availableBodiesHeight
                ? DASHBOARDS_MENU_SECTION_MIN_BODY_HEIGHT
                : 0;
        let remainingHeight = availableBodiesHeight;

        openSectionIds.forEach((sectionId, index) => {
            const isLast = index === openSectionIds.length - 1;

            if (isLast) {
                heights[sectionId] = Math.max(remainingHeight, 0);
                return;
            }

            const nextHeight = Math.max(
                Math.round(
                    (availableBodiesHeight * sections[sectionId].sizeUnits) /
                        totalUnits,
                ),
                minHeight,
            );

            heights[sectionId] = nextHeight;
            remainingHeight -= nextHeight;
        });

        return heights;
    }, [availableBodiesHeight, openSectionIds, sections]);

    React.useEffect(() => {
        const handlePointerMove = (event: PointerEvent) => {
            const dragState = dragStateRef.current;

            if (!dragState || dragState.availableHeight <= 0) {
                return;
            }

            const totalHeight =
                dragState.startTopHeight + dragState.startBottomHeight;
            const nextTopHeight = Math.min(
                Math.max(
                    dragState.startTopHeight + (event.clientY - dragState.startY),
                    dragState.minHeight,
                ),
                totalHeight - dragState.minHeight,
            );
            const nextBottomHeight = totalHeight - nextTopHeight;

            setSectionSizeUnits({
                [dragState.topId]: nextTopHeight / dragState.availableHeight,
                [dragState.bottomId]:
                    nextBottomHeight / dragState.availableHeight,
            });
        };

        const handlePointerUp = () => {
            dragStateRef.current = null;
            document.body.style.removeProperty("cursor");
            document.body.style.removeProperty("user-select");
        };

        window.addEventListener("pointermove", handlePointerMove);
        window.addEventListener("pointerup", handlePointerUp);

        return () => {
            window.removeEventListener("pointermove", handlePointerMove);
            window.removeEventListener("pointerup", handlePointerUp);
        };
    }, [setSectionSizeUnits]);

    const getNextOpenSectionId = (sectionId: DashboardsMenuSectionId) => {
        const index = openSectionIds.indexOf(sectionId);

        if (index === -1) {
            return null;
        }

        return openSectionIds[index + 1] ?? null;
    };

    const startResize = (
        topId: DashboardsMenuSectionId,
        bottomId: DashboardsMenuSectionId,
        event: React.PointerEvent<HTMLDivElement>,
    ) => {
        const startTopHeight = bodyHeights[topId] ?? 0;
        const startBottomHeight = bodyHeights[bottomId] ?? 0;

        if (startTopHeight <= 0 || startBottomHeight <= 0) {
            return;
        }

        const minHeight =
            openSectionIds.length * DASHBOARDS_MENU_SECTION_MIN_BODY_HEIGHT <=
            availableBodiesHeight
                ? DASHBOARDS_MENU_SECTION_MIN_BODY_HEIGHT
                : 0;

        dragStateRef.current = {
            topId,
            bottomId,
            startY: event.clientY,
            startTopHeight,
            startBottomHeight,
            availableHeight: availableBodiesHeight,
            minHeight,
        };

        document.body.style.cursor = "row-resize";
        document.body.style.userSelect = "none";
    };

    const getResizeHandler = (topId: DashboardsMenuSectionId) => {
        return (event: React.PointerEvent<HTMLDivElement>) => {
            const nextSectionId = getNextOpenSectionId(topId);
            if (nextSectionId) {
                startResize(topId, nextSectionId, event);
            }
        };
    };

    return {
        bodyHeights,
        containerRef,
        getNextOpenSectionId,
        getResizeHandler,
    };
};
