import { IPaneviewPanelProps } from "dockview";
import React from "react";
import {
    ArrowDownIcon,
    EntitiesIcon,
    SavedQueriesIcon,
    cn,
} from "@serene-ui/shared-frontend";
import { PinIcon } from "../../../../shared/ui/icons/index";

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

    return (
        <div
            className={cn("flex items-center h-full px-2 hover:bg-accent", {
                "border-t-[0.5px]": !isFirst,
                "border-b-[0.5px]": isLast && !shouldHideBorder,
            })}
            onClick={onClick}>
            <ArrowDownIcon
                className={cn("mr-2", {
                    "rotate-[-90deg]": !expanded,
                })}
            />
            {icon}
            <p className="uppercase text-foreground text-xs font-black ml-1.5">
                {props.params.title}
            </p>
        </div>
    );
};
