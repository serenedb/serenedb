import React, { useEffect, useState } from "react";
import { useGetConnections } from "@serene-ui/shared-frontend/entities";
import {
    EntitiesIcon,
    QueryHistoryIcon,
    SavedQueriesIcon,
    Tabs,
    TabsContent,
    TabsList,
    TabsTrigger,
    Select,
    SelectContent,
    SelectItem,
    SelectTrigger,
    SelectValue,
} from "@serene-ui/shared-frontend/shared";

import { ConsoleConnectionSelect } from "../../ConsoleEditor/ui/ConsoleConnectionSelect";
import { useResizeObserver } from "@serene-ui/shared-frontend/shared";
import { ConsoleEntitiesTab } from "../../ConsoleEntitiesTab";
import { ConsoleHistoryTab } from "../../ConsoleHistoryTab";
import { ConsoleSavedTab } from "../../ConsoleSavedTab";
import { NoConnectionsBlock } from "../../../shared/NoConnectionsBlock";
import { type ConsoleTab } from "../../ConsoleEditorTabsSelector";

interface ConsoleMenuProps {
    updateTab: (id: number, tabUpdate: Partial<ConsoleTab>) => void;
    selectedTabId: number;
    explorerRef?: React.RefObject<HTMLDivElement | null>;
}

export const ConsoleMenu: React.FC<ConsoleMenuProps> = ({
    updateTab,
    selectedTabId,
    explorerRef,
}) => {
    const [selectedTab, setSelectedTab] = useState<string>("entities");
    const { ref: containerRef, size } = useResizeObserver<HTMLDivElement>();

    const consoleMenuTabs = [
        {
            key: "entities",
            name: "Entities",
            icon: <EntitiesIcon />,
            element: <ConsoleEntitiesTab explorerRef={explorerRef} />,
        },
        {
            key: "saved",
            name: "Saved",
            icon: <SavedQueriesIcon />,
            element: (
                <ConsoleSavedTab
                    updateTab={updateTab}
                    selectedTabId={selectedTabId}
                    explorerRef={explorerRef}
                />
            ),
        },
        {
            key: "history",
            name: "History",
            icon: <QueryHistoryIcon />,
            element: (
                <ConsoleHistoryTab
                    updateTab={updateTab}
                    selectedTabId={selectedTabId}
                    explorerRef={explorerRef}
                />
            ),
        },
    ];

    const { data: connections, isFetched, isLoading } = useGetConnections();

    useEffect(() => {
        if (isFetched && !connections?.length) {
            localStorage.removeItem("system:currentConnection");
        }
    }, [connections]);

    const isSelectMode = size.width > 0 && size.width < 400;
    const currentTab =
        consoleMenuTabs.find((tab) => tab.key === selectedTab) ||
        consoleMenuTabs[0];

    if (!connections?.length && !isLoading && currentTab.key === "entities") {
        return (
            <div className="flex-1 flex items-center justify-center">
                <NoConnectionsBlock />
            </div>
        );
    }
    return (
        <div ref={containerRef} className="flex-1 flex flex-col pt-2 max-h-dvh">
            <ConsoleConnectionSelect />
            {isSelectMode ? (
                <div className="w-full flex-1 flex flex-col max-h-dvh">
                    <Select value={selectedTab} onValueChange={setSelectedTab}>
                        <div className="px-2 mb-px">
                            <SelectTrigger
                                className="mt-[5px] w-full border-transparent"
                                aria-label="Select menu tab">
                                <SelectValue>
                                    <div className="flex items-center gap-2">
                                        {currentTab.icon}
                                        {currentTab.name}
                                    </div>
                                </SelectValue>
                            </SelectTrigger>
                        </div>

                        <SelectContent>
                            {consoleMenuTabs.map((tab) => (
                                <SelectItem value={tab.key} key={tab.key}>
                                    <div className="flex items-center gap-2">
                                        {tab.icon}
                                        {tab.name}
                                    </div>
                                </SelectItem>
                            ))}
                        </SelectContent>
                    </Select>
                    <div className="flex flex-col flex-1">
                        {currentTab.element}
                    </div>
                </div>
            ) : (
                <Tabs
                    value={selectedTab}
                    onValueChange={setSelectedTab}
                    className="w-full flex-1 flex flex-col">
                    <TabsList className="mt-[3px]">
                        {consoleMenuTabs.map((tab) => (
                            <TabsTrigger value={tab.key} key={tab.key}>
                                {tab.icon}
                                {tab.name}
                            </TabsTrigger>
                        ))}
                    </TabsList>
                    {consoleMenuTabs.map((tab) => (
                        <TabsContent
                            className="flex flex-col"
                            value={tab.key}
                            key={tab.key}>
                            {tab.element}
                        </TabsContent>
                    ))}
                </Tabs>
            )}
        </div>
    );
};
