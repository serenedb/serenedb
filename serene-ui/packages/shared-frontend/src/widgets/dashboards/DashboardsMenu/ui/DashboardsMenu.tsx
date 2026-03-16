import React, { useEffect, useMemo, useRef } from "react";
import { useResizeObserver } from "@serene-ui/shared-frontend/shared";

import { DashboardsMenuProvider } from "../model";
import {
    DASHBOARDS_MENU_SECTION_HANDLE_HEIGHT,
    DASHBOARDS_MENU_SECTION_HEADER_HEIGHT,
    DASHBOARDS_MENU_SECTION_MIN_BODY_HEIGHT,
    type DashboardsMenuSectionId,
    useDashboardsMenu,
} from "../model";
import { DashboardsList } from "./DashboardsList";
import { FavoritesList } from "./FavoritesList";
import { SavedQueriesList } from "./SavedQueriesList";

interface DashboardsMenuComponentProps {
    onCurrentDashboardChange: (dashboardId: number) => void;
}

const SECTION_ORDER: DashboardsMenuSectionId[] = [
    "favorites",
    "dashboards",
    "savedQueries",
];

const DashboardsMenuContent: React.FC<DashboardsMenuComponentProps> = ({
    onCurrentDashboardChange,
}) => {
    const { ref: containerRef, size } = useResizeObserver<HTMLDivElement>();
    const { sections, setSectionSizeUnits } = useDashboardsMenu();
    const dragStateRef = useRef<{
        topId: DashboardsMenuSectionId;
        bottomId: DashboardsMenuSectionId;
        startY: number;
        startTopHeight: number;
        startBottomHeight: number;
        availableHeight: number;
        minHeight: number;
    } | null>(null);

    const openSectionIds = useMemo(
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

    const bodyHeights = useMemo(() => {
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

    useEffect(() => {
        const handlePointerMove = (event: PointerEvent) => {
            const dragState = dragStateRef.current;

            if (!dragState || dragState.availableHeight <= 0) {
                return;
            }

            const totalHeight =
                dragState.startTopHeight + dragState.startBottomHeight;
            const nextTopHeight = Math.min(
                Math.max(
                    dragState.startTopHeight +
                        (event.clientY - dragState.startY),
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

    const getResizeHandler = (topId: DashboardsMenuSectionId) => {
        return (event: React.PointerEvent<HTMLDivElement>) => {
            const nextSectionId = getNextOpenSectionId(topId);
            if (nextSectionId) {
                startResize(topId, nextSectionId, event);
            }
        };
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

    return (
        <div className="flex flex-1 flex-col">
            <div className="flex h-12 items-center border-b-1 bg-background px-4">
                <p className="text-primary-foreground uppercase text-xs font-extrabold">
                    Dashboards
                </p>
            </div>
            <div ref={containerRef} className="flex min-h-0 flex-1 flex-col">
                <FavoritesList
                    bodyHeight={bodyHeights.favorites ?? 0}
                    showResizeHandle={Boolean(
                        getNextOpenSectionId("favorites"),
                    )}
                    onResizePointerDown={getResizeHandler("favorites")}
                />
                <DashboardsList
                    bodyHeight={bodyHeights.dashboards ?? 0}
                    onCurrentDashboardChange={onCurrentDashboardChange}
                    showResizeHandle={Boolean(
                        getNextOpenSectionId("dashboards"),
                    )}
                    onResizePointerDown={getResizeHandler("dashboards")}
                />
                <SavedQueriesList bodyHeight={bodyHeights.savedQueries ?? 0} />
            </div>
        </div>
    );
};

export const DashboardsMenu: React.FC<DashboardsMenuComponentProps> = ({
    onCurrentDashboardChange,
}) => {
    return (
        <DashboardsMenuProvider>
            <DashboardsMenuContent
                onCurrentDashboardChange={onCurrentDashboardChange}
            />
        </DashboardsMenuProvider>
    );
};
