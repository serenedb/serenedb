import { IPaneviewPanelProps } from "dockview";
import React from "react";
import {
    ArrowDownIcon,
    EntitiesIcon,
    SavedQueriesIcon,
    cn,
    focusSidebarRelativeItem,
    handleSidebarSectionHotkey,
} from "@serene-ui/shared-frontend";
import { PinIcon } from "../../../../shared/ui/icons/index";
import { ImportSavedQueryButton } from "../../../../features/openSavedQueriesModal/ui/buttons";

interface ConsoleSidebarHeader {
    title: string;
    icon?: React.ReactNode;
    kind?: "pinned" | "entities" | "savedQueries";
}

export const ConsoleSidebarHeader = (
    props: IPaneviewPanelProps<ConsoleSidebarHeader>,
) => {
    const [expanded, setExpanded] = React.useState<boolean>(
        props.api.isExpanded,
    );

    const [shouldHideBorder, setShouldHideBorder] = React.useState(false);

    React.useEffect(() => {
        const disposable = props.api.onDidExpansionChange((event) => {
            setExpanded(event.isExpanded);
        });

        return () => {
            disposable.dispose();
        };
    }, []);

    React.useEffect(() => {
        const updateBorderVisibility = () => {
            const panels = props.containerApi.panels;

            if (panels.some((item) => item.api.isExpanded)) {
                setShouldHideBorder(true);
                return;
            }

            setShouldHideBorder(false);
        };

        updateBorderVisibility();

        const disposable = props.containerApi.onDidLayoutChange(() => {
            updateBorderVisibility();
        });

        return () => {
            disposable.dispose();
        };
    }, []);

    const onClick = () => {
        props.api.setExpanded(!expanded);
    };

    const isFirst = props.api.id === "panel_1";
    const isLast = props.api.id === "panel_3";
    const fallbackIcon =
        props.params.kind === "pinned" ? (
            <PinIcon className="size-3.5" />
        ) : props.params.kind === "entities" ? (
            <EntitiesIcon className="size-3.5" />
        ) : props.params.kind === "savedQueries" ? (
            <SavedQueriesIcon className="size-3.5" />
        ) : null;
    const icon = React.isValidElement(props.params.icon)
        ? props.params.icon
        : fallbackIcon;
    const isSavedQueries = props.params.kind === "savedQueries";

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
                onClick={onClick}
                className="flex flex-1 items-center min-w-0 h-full text-left outline-none focus:outline-none focus-visible:outline-none focus-visible:ring-0"
                data-sidebar-primary-action="true"
                data-sidebar-focus-id={`console-sidebar-section-${props.params.kind ?? props.api.id}`}
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
                        onClick();
                        return;
                    }

                    if (event.key === "ArrowRight" || key === "l") {
                        event.preventDefault();

                        if (!expanded) {
                            onClick();
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
                        onClick();
                    }
                }}>
                <ArrowDownIcon
                    className={cn("mr-2", {
                        "rotate-[-90deg]": !expanded,
                    })}
                />
                {icon}
                <p className="uppercase text-foreground/80 dark:text-foreground text-xs font-black ml-1.5">
                    {props.params.title}
                </p>
            </button>
            {isSavedQueries ? (
                <div className="flex items-center gap-0.5">
                    <ImportSavedQueryButton size="xsIcon" variant="ghost" />
                </div>
            ) : null}
        </div>
    );
};
