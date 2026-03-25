import { IPaneviewPanelProps } from "dockview";
import React from "react";
import { ArrowDownIcon, cn } from "@serene-ui/shared-frontend";

interface ConsoleSidebarHeader {
    title: string;
    icon: React.ReactNode;
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
        const disposable = props.containerApi.onDidLayoutChange((event) => {
            const panels = props.containerApi.panels;

            if (panels.some((item) => item.api.isExpanded)) {
                setShouldHideBorder(true);
                return;
            }
            setShouldHideBorder(false);
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
            {props.params.icon}
            <p className="uppercase text-foreground text-xs font-black ml-1.5">
                {props.params.title}
            </p>
        </div>
    );
};
