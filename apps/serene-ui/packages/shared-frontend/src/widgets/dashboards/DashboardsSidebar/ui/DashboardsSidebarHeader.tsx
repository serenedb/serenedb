import { IPaneviewPanelProps } from "dockview";
import React from "react";
import {
    ArrowDownIcon,
    DashboardsIcon,
    SavedQueriesIcon,
    cn,
    focusSidebarRelativeItem,
    handleSidebarSectionHotkey,
} from "@serene-ui/shared-frontend";
import { CreateDashboardButton } from "../../../../features";
import { ImportSavedQueryButton } from "../../../../features/openSavedQueriesModal/ui/buttons";
import { useDashboardsSidebarContext } from "./DashboardsSidebarContext";

interface DashboardsSidebarHeaderParams {
    title: string;
    kind: "favorites" | "dashboards" | "savedQueries";
}

export const DashboardsSidebarHeader = (
    props: IPaneviewPanelProps<DashboardsSidebarHeaderParams>,
) => {
    const [expanded, setExpanded] = React.useState<boolean>(
        props.api.isExpanded,
    );
    const [shouldHideBorder, setShouldHideBorder] = React.useState(false);
    const { onCurrentDashboardChange } = useDashboardsSidebarContext();

    React.useEffect(() => {
        const disposable = props.api.onDidExpansionChange((event) => {
            setExpanded(event.isExpanded);
        });

        return () => disposable.dispose();
    }, []);

    React.useEffect(() => {
        const updateBorderVisibility = () => {
            setShouldHideBorder(
                props.containerApi.panels.some((item) => item.api.isExpanded),
            );
        };

        updateBorderVisibility();

        const disposable = props.containerApi.onDidLayoutChange(() => {
            updateBorderVisibility();
        });

        return () => disposable.dispose();
    }, []);

    const isFirst = props.api.id === "panel_1";
    const isLast = props.api.id === "panel_3";

    const { kind } = props.params;
    const icon =
        kind === "savedQueries" ? (
            <SavedQueriesIcon className="size-3.5" />
        ) : (
            <DashboardsIcon className="size-3.5" />
        );
    const isSavedQueries = kind === "savedQueries";

    return (
        <div
            className={cn(
                "flex items-center h-full pl-2 pr-1 hover:bg-accent focus-within:bg-accent",
                {
                    "border-t-[0.5px]": !isFirst,
                    "border-b-[0.5px]": isLast && !shouldHideBorder,
                },
            )}>
            <button
                type="button"
                onClick={() => props.api.setExpanded(!expanded)}
                className="flex flex-1 items-center gap-1.5 min-w-0 h-full text-left outline-none focus:outline-none focus-visible:outline-none focus-visible:ring-0"
                data-sidebar-primary-action="true"
                data-sidebar-focus-id={`dashboards-sidebar-section-${props.params.kind}`}
                data-sidebar-section-id={props.params.kind}
                onKeyDown={(event) => {
                    if (handleSidebarSectionHotkey(event)) {
                        return;
                    }

                    if (event.target !== event.currentTarget) {
                        return;
                    }

                    const key = event.key.toLowerCase();

                    if (event.key === "ArrowDown" || key === "j") {
                        event.preventDefault();
                        focusSidebarRelativeItem(
                            event.currentTarget,
                            "next",
                        );
                        return;
                    }

                    if (event.key === "ArrowUp" || key === "k") {
                        event.preventDefault();
                        focusSidebarRelativeItem(
                            event.currentTarget,
                            "previous",
                        );
                        return;
                    }

                    if (event.key === "Enter") {
                        event.preventDefault();
                        props.api.setExpanded(!expanded);
                        return;
                    }

                    if (event.key === "ArrowRight" || key === "l") {
                        event.preventDefault();

                        if (!expanded) {
                            props.api.setExpanded(true);
                            return;
                        }

                        focusSidebarRelativeItem(event.currentTarget, "next");
                        return;
                    }

                    if (
                        (event.key === "ArrowLeft" || key === "h") &&
                        expanded
                    ) {
                        event.preventDefault();
                        props.api.setExpanded(false);
                    }
                }}>
                <ArrowDownIcon
                    className={cn("mr-1", {
                        "rotate-[-90deg]": !expanded,
                    })}
                />
                {icon}
                <p className="uppercase text-foreground text-xs font-black ml-0.5">
                    {props.params.title}
                </p>
            </button>
            {kind === "dashboards" ? (
                <div className="">
                    <CreateDashboardButton
                        onCreateDashboard={onCurrentDashboardChange}
                    />
                </div>
            ) : isSavedQueries ? (
                <div className="flex items-center gap-0.5">
                    <ImportSavedQueryButton size="xsIcon" variant="ghost" />
                </div>
            ) : null}
        </div>
    );
};
