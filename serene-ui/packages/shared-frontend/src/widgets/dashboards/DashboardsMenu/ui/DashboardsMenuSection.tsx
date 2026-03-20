import React from "react";
import { ArrowDownIcon, cn, DashboardsIcon } from "@serene-ui/shared-frontend";

import {
    DASHBOARDS_MENU_SECTION_HEADER_HEIGHT,
    type DashboardsMenuSectionId,
    useDashboardsMenu,
} from "../model";

interface DashboardsMenuSectionProps {
    sectionId: DashboardsMenuSectionId;
    title: string;
    bodyHeight: number;
    showResizeHandle: boolean;
    onResizePointerDown?: (event: React.PointerEvent<HTMLDivElement>) => void;
    children?: React.ReactNode;
    actions?: React.ReactNode;
}

export const DashboardsMenuSection: React.FC<DashboardsMenuSectionProps> = ({
    sectionId,
    title,
    bodyHeight,
    showResizeHandle,
    onResizePointerDown,
    children,
    actions,
}) => {
    const { sections, toggleSection } = useDashboardsMenu();
    const section = sections[sectionId];

    return (
        <div className="flex min-h-0 flex-col">
            <div
                className={cn(
                    "flex w-full shrink-0 items-center border-b-1 border-transparent",
                    "transition-colors",
                    !section.isOpen && "border-border",
                )}
                style={{ height: DASHBOARDS_MENU_SECTION_HEADER_HEIGHT }}>
                <button
                    type="button"
                    onClick={() => toggleSection(sectionId)}
                    data-testid={`dashboardsMenuSection-toggle-${sectionId}`}
                    className="flex min-w-0 flex-1 items-center gap-2 px-2.5 text-left">
                    <ArrowDownIcon
                        className={cn(
                            "size-4 transition-transform",
                            !section.isOpen && "rotate-[-90deg]",
                        )}
                    />
                    <DashboardsIcon className="size-4" />
                    <p className="text-primary-foreground/70 uppercase text-xs font-extrabold">
                        {title}
                    </p>
                </button>
                {actions ? <div className="pr-1">{actions}</div> : null}
            </div>
            {section.isOpen && (
                <>
                    <div
                        className="min-h-0 overflow-hidden border-b-1"
                        style={{ height: bodyHeight }}>
                        {children}
                    </div>
                    {showResizeHandle && onResizePointerDown && (
                        <div
                            role="separator"
                            aria-orientation="horizontal"
                            data-testid={`dashboardsMenuSection-resize-${sectionId}`}
                            onPointerDown={onResizePointerDown}
                            className="group relative h-1.5 cursor-row-resize touch-none">
                            <div className="absolute inset-x-0 top-1/2 h-px -translate-y-1/2 bg-transparent transition-colors group-hover:bg-border group-active:bg-border" />
                        </div>
                    )}
                </>
            )}
        </div>
    );
};
